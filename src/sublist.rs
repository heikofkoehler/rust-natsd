//! Subscription registry: stores all subscriptions, resolves the recipient
//! set for a published message (fan-out + queue groups + auto-unsubscribe).
//!
//! Matching is a subject trie, not a linear scan over all subscriptions:
//! trie nodes branch on exact tokens, a `*` child (exactly one token) and
//! a `>` terminator (one or more remaining tokens, stored on the node the
//! prefix ends at). Partial wildcards such as `foo*` or `f>o` are literal
//! tokens, so they live in the exact-token map like any other token.
//! Matching therefore visits only nodes reachable from the published
//! subject's tokens, plus `>` terminators along the path.
//!
//! The public API and semantics are exactly the old linear-scan version's:
//! subscriptions keep insertion order (a per-key sequence number restores
//! it after trie collection), queue-group members are returned in insertion
//! order, and `match_recipients` still picks one member per group in
//! BTreeMap (sorted) group order with a cursor that advances per group.

use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub struct Subscription {
    pub conn_id: u64,
    pub sid: String,
    pub subject: String,
    pub queue: Option<String>,
    /// Account the subscription belongs to; delivery is scoped to it.
    pub account: String,
    /// Auto-unsubscribe after this many delivered messages (total).
    pub max_msgs: Option<u64>,
    pub delivered: u64,
}

impl Subscription {
    pub fn new(
        conn_id: u64,
        sid: String,
        subject: String,
        queue: Option<String>,
        account: String,
    ) -> Self {
        Subscription {
            conn_id,
            sid,
            subject,
            queue,
            account,
            max_msgs: None,
            delivered: 0,
        }
    }
}

/// One chosen recipient of a message.
#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub conn_id: u64,
    pub sid: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct MatchResult {
    pub deliveries: Vec<Delivery>,
    /// Subscriptions that hit their max message count with this delivery and
    /// must be removed.
    pub expired: Vec<(u64, String)>,
}

/// Unselected match: plain deliveries plus queue groups with all members.
#[derive(Debug, Default)]
pub struct GroupMatch {
    pub plain: Vec<Delivery>,
    pub groups: BTreeMap<String, Vec<Delivery>>,
}

/// Key identifying one subscription: (connection, sid).
type SubKey = (u64, String);

/// (account, pattern, queue) triple, as returned by patterns().
type PatternTriple = (String, String, Option<String>);

/// One trie node. `subs` holds subscriptions whose pattern ends exactly
/// at this node; `fwc` holds subscriptions whose pattern ends with `>`
/// at this node (the `>` itself consumes the rest of the subject, so
/// nothing is stored below it — a mid-pattern `>` behaves exactly like
/// a trailing one, matching `subject_matches`, which returns as soon as
/// it sees a whole-token `>`).
#[derive(Debug, Default, PartialEq)]
struct TrieNode {
    exact: HashMap<String, TrieNode>,
    star: Option<Box<TrieNode>>,
    fwc: Vec<SubKey>,
    subs: Vec<SubKey>,
}

impl TrieNode {
    fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.star.is_none() && self.fwc.is_empty() && self.subs.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct Sublist {
    /// Subscription structs by key; the trie and the order map hold keys.
    subs: HashMap<SubKey, Subscription>,
    /// Insertion sequence per key (monotonic; never reused), used to
    /// restore insertion order after trie collection.
    seq: HashMap<SubKey, u64>,
    next_seq: u64,
    trie: TrieNode,
    /// (account, pattern, queue) -> number of subscriptions using it,
    /// so `has_pattern` is O(1) and survives duplicate patterns.
    pattern_counts: HashMap<PatternTriple, usize>,
    /// Rotating cursor for queue-group member selection.
    queue_cursor: u64,
}

impl Sublist {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.subs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.subs.is_empty()
    }

    /// Add a subscription. Go keeps the FIRST subscription with a sid:
    /// a later SUB with the same (conn_id, sid) is ignored entirely, so
    /// the original subject, queue, max, and delivery count all survive.
    pub fn add(&mut self, sub: Subscription) {
        let key: SubKey = (sub.conn_id, sub.sid.clone());
        if self.subs.contains_key(&key) {
            return;
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        *self
            .pattern_counts
            .entry((sub.account.clone(), sub.subject.clone(), sub.queue.clone()))
            .or_insert(0) += 1;
        Self::trie_insert(&mut self.trie, sub.subject.split('.'), &key);
        self.seq.insert(key.clone(), seq);
        self.subs.insert(key, sub);
    }

    fn trie_insert(node: &mut TrieNode, tokens: std::str::Split<'_, char>, key: &SubKey) {
        let mut node = node;
        for tok in tokens {
            match tok {
                ">" => {
                    node.fwc.push(key.clone());
                    return;
                }
                "*" => {
                    node = node
                        .star
                        .get_or_insert_with(|| Box::new(TrieNode::default()));
                }
                _ => {
                    node = node.exact.entry(tok.to_owned()).or_default();
                }
            }
        }
        node.subs.push(key.clone());
    }

    /// Remove one subscription by key. Returns whether it existed.
    fn remove_key(&mut self, key: &SubKey) -> bool {
        let Some(sub) = self.subs.remove(key) else {
            return false;
        };
        self.seq.remove(key);
        let count_key = (sub.account, sub.subject.clone(), sub.queue);
        if let Some(count) = self.pattern_counts.get_mut(&count_key) {
            *count -= 1;
            if *count == 0 {
                self.pattern_counts.remove(&count_key);
            }
        }
        let tokens: Vec<&str> = sub.subject.split('.').collect();
        Self::trie_remove(&mut self.trie, &tokens, key);
        true
    }

    /// Remove `key` from the trie; returns true when the node is now
    /// empty and the caller may prune it.
    fn trie_remove(node: &mut TrieNode, tokens: &[&str], key: &SubKey) -> bool {
        if tokens.is_empty() {
            node.subs.retain(|k| k != key);
            return node.is_empty();
        }
        match tokens[0] {
            ">" => {
                node.fwc.retain(|k| k != key);
                node.is_empty()
            }
            "*" => {
                let prune = match node.star.as_mut() {
                    Some(child) => Self::trie_remove(child, &tokens[1..], key),
                    None => false,
                };
                if prune {
                    node.star = None;
                }
                node.is_empty()
            }
            tok => {
                let prune = match node.exact.get_mut(tok) {
                    Some(child) => Self::trie_remove(child, &tokens[1..], key),
                    None => false,
                };
                if prune {
                    node.exact.remove(tok);
                }
                node.is_empty()
            }
        }
    }

    pub fn remove(&mut self, conn_id: u64, sid: &str) -> bool {
        self.remove_key(&(conn_id, sid.to_owned()))
    }

    pub fn remove_conn(&mut self, conn_id: u64) {
        let keys: Vec<SubKey> = self
            .subs
            .keys()
            .filter(|k| k.0 == conn_id)
            .cloned()
            .collect();
        for key in keys {
            self.remove_key(&key);
        }
    }

    /// Pattern+queue of one subscription, if it exists.
    pub fn pattern_of(&self, conn_id: u64, sid: &str) -> Option<(String, String, Option<String>)> {
        self.subs
            .get(&(conn_id, sid.to_owned()))
            .map(|s| (s.account.clone(), s.subject.clone(), s.queue.clone()))
    }

    /// True when any subscription in `account` uses this exact
    /// pattern+queue.
    pub fn has_pattern(&self, subject: &str, queue: Option<&str>, account: &str) -> bool {
        self.pattern_counts.contains_key(&(
            account.to_owned(),
            subject.to_owned(),
            queue.map(|q| q.to_owned()),
        ))
    }

    /// Distinct (account, pattern, queue) triples for one connection,
    /// in first-subscription order.
    pub fn patterns_for_conn(&self, conn_id: u64) -> Vec<(String, String, Option<String>)> {
        let mut triples: Vec<(u64, PatternTriple)> = self
            .subs
            .values()
            .filter(|s| s.conn_id == conn_id)
            .map(|s| {
                (
                    self.seq
                        .get(&(s.conn_id, s.sid.clone()))
                        .copied()
                        .unwrap_or(u64::MAX),
                    (s.account.clone(), s.subject.clone(), s.queue.clone()),
                )
            })
            .collect();
        triples.sort_by_key(|(seq, _)| *seq);
        dedup_triples(triples.into_iter().map(|(_, t)| t))
    }

    /// Distinct (pattern, queue) pairs currently subscribed, for
    /// RS+/LS+ interest sync to a newly connected route or leaf.
    /// All subscriptions (for /subz).
    pub fn subscriptions(&self) -> Vec<Subscription> {
        let mut subs: Vec<(u64, Subscription)> = self
            .subs
            .values()
            .map(|s| {
                (
                    self.seq
                        .get(&(s.conn_id, s.sid.clone()))
                        .copied()
                        .unwrap_or(u64::MAX),
                    s.clone(),
                )
            })
            .collect();
        subs.sort_by_key(|(seq, _)| *seq);
        subs.into_iter().map(|(_, s)| s).collect()
    }

    pub fn patterns(&self) -> Vec<(String, String, Option<String>)> {
        let mut triples: Vec<(u64, PatternTriple)> = self
            .subs
            .values()
            .map(|s| {
                (
                    self.seq
                        .get(&(s.conn_id, s.sid.clone()))
                        .copied()
                        .unwrap_or(u64::MAX),
                    (s.account.clone(), s.subject.clone(), s.queue.clone()),
                )
            })
            .collect();
        triples.sort_by_key(|(seq, _)| *seq);
        dedup_triples(triples.into_iter().map(|(_, t)| t))
    }

    /// Set the auto-unsubscribe max for a subscription. If the subscription
    /// has already delivered at least `max` messages it is removed
    /// immediately and `true` is returned.
    pub fn set_max(&mut self, conn_id: u64, sid: &str, max: u64) -> bool {
        let key: SubKey = (conn_id, sid.to_owned());
        match self.subs.get(&key).map(|s| s.delivered >= max) {
            Some(true) => {
                self.remove_key(&key);
                true
            }
            Some(false) => {
                self.subs.get_mut(&key).expect("present").max_msgs = Some(max);
                false
            }
            None => false,
        }
    }

    /// Resolve recipients for a message published to `subject` by
    /// `publisher_conn`. When `publisher_echo` is false, the publisher's own
    /// subscriptions are skipped. Updates delivered counters and reports
    /// subscriptions expired by this delivery.
    pub fn match_recipients(
        &mut self,
        subject: &str,
        publisher_conn: u64,
        publisher_echo: bool,
        account: Option<&str>,
    ) -> MatchResult {
        let gm = self.match_groups(subject, publisher_conn, publisher_echo, account);
        let mut deliveries = gm.plain;
        for (_q, members) in gm.groups {
            // Round-robin across the members that matched this message.
            let pick = members[(self.queue_cursor as usize) % members.len()].clone();
            self.queue_cursor = self.queue_cursor.wrapping_add(1);
            deliveries.push(pick);
        }
        let expired = self.note_deliveries(&deliveries);
        MatchResult {
            deliveries,
            expired,
        }
    }

    /// Like `match_recipients`, but queue groups are returned unselected
    /// (group -> all matching members) so the caller can run a global
    /// queue selection across local members, routes, and leafs. Counters
    /// are NOT updated here; the caller records actual deliveries with
    /// `note_deliveries`.
    pub fn match_groups(
        &self,
        subject: &str,
        publisher_conn: u64,
        publisher_echo: bool,
        account: Option<&str>,
    ) -> GroupMatch {
        let tokens: Vec<&str> = subject.split('.').collect();
        let mut keys: Vec<SubKey> = Vec::new();
        Self::trie_collect(&self.trie, &tokens, 0, &mut keys);
        // Trie traversal order is not insertion order, so restore it:
        // plain deliveries and queue members must come out exactly as
        // the old linear scan produced them.
        keys.sort_by_key(|k| self.seq.get(k).copied().unwrap_or(u64::MAX));
        keys.dedup();
        let mut out = GroupMatch::default();
        for key in keys {
            let Some(sub) = self.subs.get(&key) else {
                continue;
            };
            if let Some(account) = account {
                if sub.account != account {
                    continue;
                }
            }
            if !publisher_echo && sub.conn_id == publisher_conn {
                continue;
            }
            let d = Delivery {
                conn_id: sub.conn_id,
                sid: sub.sid.clone(),
            };
            match &sub.queue {
                Some(q) => out.groups.entry(q.clone()).or_default().push(d),
                None => out.plain.push(d),
            }
        }
        out
    }

    /// Collect every subscription key whose pattern matches `tokens`,
    /// following exact and `*` children and picking up `>` terminators
    /// at each node while at least one token remains (a `>` needs at
    /// least one remaining token, exactly as in `subject_matches`).
    fn trie_collect(node: &TrieNode, tokens: &[&str], idx: usize, out: &mut Vec<SubKey>) {
        if idx < tokens.len() {
            out.extend(node.fwc.iter().cloned());
        }
        if idx == tokens.len() {
            out.extend(node.subs.iter().cloned());
            return;
        }
        let tok = tokens[idx];
        if let Some(child) = node.exact.get(tok) {
            Self::trie_collect(child, tokens, idx + 1, out);
        }
        if let Some(star) = &node.star {
            Self::trie_collect(star, tokens, idx + 1, out);
        }
    }

    /// Record actual deliveries: bump counters, return subscriptions that
    /// hit their auto-unsubscribe max with this delivery.
    pub fn note_deliveries(&mut self, deliveries: &[Delivery]) -> Vec<(u64, String)> {
        let mut expired = Vec::new();
        for d in deliveries {
            if let Some(sub) = self.subs.get_mut(&(d.conn_id, d.sid.clone())) {
                sub.delivered += 1;
                if let Some(max) = sub.max_msgs {
                    if sub.delivered >= max {
                        expired.push((sub.conn_id, sub.sid.clone()));
                    }
                }
            }
        }
        expired
    }
}

fn dedup_triples(triples: impl Iterator<Item = PatternTriple>) -> Vec<PatternTriple> {
    let mut seen: HashSet<PatternTriple> = HashSet::new();
    let mut out = Vec::new();
    for t in triples {
        if seen.insert(t.clone()) {
            out.push(t);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(conn: u64, sid: &str, subject: &str, queue: Option<&str>) -> Subscription {
        Subscription::new(
            conn,
            sid.into(),
            subject.into(),
            queue.map(|q| q.into()),
            "$G".into(),
        )
    }

    fn sub_acct(
        conn: u64,
        sid: &str,
        subject: &str,
        queue: Option<&str>,
        account: &str,
    ) -> Subscription {
        Subscription::new(
            conn,
            sid.into(),
            subject.into(),
            queue.map(|q| q.into()),
            account.into(),
        )
    }

    fn conns(sl: &mut Sublist, subject: &str) -> Vec<u64> {
        let mut v: Vec<u64> = sl
            .match_recipients(subject, 99, true, None)
            .deliveries
            .iter()
            .map(|d| d.conn_id)
            .collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn fanout_to_all_plain_subs() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo", None));
        sl.add(sub(2, "1", "foo", None));
        sl.add(sub(3, "1", "bar", None));
        let r = sl.match_recipients("foo", 99, true, None);
        assert_eq!(r.deliveries.len(), 2);
        assert!(r.deliveries.iter().any(|d| d.conn_id == 1));
        assert!(r.deliveries.iter().any(|d| d.conn_id == 2));
    }

    #[test]
    fn wildcard_fanout() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo.*", None));
        sl.add(sub(2, "1", "foo.>", None));
        sl.add(sub(3, "1", ">", None));
        assert_eq!(
            sl.match_recipients("foo.bar", 99, true, None)
                .deliveries
                .len(),
            3
        );
        assert_eq!(
            sl.match_recipients("foo.bar.baz", 99, true, None)
                .deliveries
                .len(),
            2
        );
        assert_eq!(
            sl.match_recipients("other", 99, true, None)
                .deliveries
                .len(),
            1
        );
    }

    #[test]
    fn queue_group_gets_exactly_one_per_message_and_rotates() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "jobs", Some("workers")));
        sl.add(sub(2, "1", "jobs", Some("workers")));
        sl.add(sub(3, "1", "jobs", Some("workers")));
        sl.add(sub(4, "1", "jobs", None)); // plain sub also receives
        let mut counts = [0usize; 5];
        for _ in 0..99 {
            let r = sl.match_recipients("jobs", 99, true, None);
            assert_eq!(r.deliveries.len(), 2, "one queue member + plain sub");
            for d in &r.deliveries {
                counts[d.conn_id as usize] += 1;
            }
        }
        // Plain subscriber got every message; queue members shared evenly.
        assert_eq!(counts[4], 99);
        assert_eq!(counts[1], 33);
        assert_eq!(counts[2], 33);
        assert_eq!(counts[3], 33);
    }

    #[test]
    fn distinct_queue_groups_each_get_one() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "jobs", Some("a")));
        sl.add(sub(2, "1", "jobs", Some("b")));
        let r = sl.match_recipients("jobs", 99, true, None);
        assert_eq!(r.deliveries.len(), 2);
    }

    #[test]
    fn auto_unsubscribe_after_max() {
        let mut sl = Sublist::new();
        let mut s = sub(1, "1", "foo", None);
        s.max_msgs = Some(2);
        sl.add(s);
        let r1 = sl.match_recipients("foo", 99, true, None);
        assert_eq!(r1.deliveries.len(), 1);
        assert!(r1.expired.is_empty());
        let r2 = sl.match_recipients("foo", 99, true, None);
        assert_eq!(r2.deliveries.len(), 1);
        assert_eq!(r2.expired, vec![(1, "1".to_string())]);
        sl.remove(1, "1");
        assert_eq!(
            sl.match_recipients("foo", 99, true, None).deliveries.len(),
            0
        );
    }

    #[test]
    fn set_max_removes_when_already_reached() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo", None));
        let _ = sl.match_recipients("foo", 99, true, None);
        let _ = sl.match_recipients("foo", 99, true, None);
        assert!(
            sl.set_max(1, "1", 2),
            "delivered 2 >= max 2 removes at once"
        );
        assert!(sl.is_empty());
        // set_max on a live sub keeps it until the count is hit.
        sl.add(sub(1, "1", "foo", None));
        assert!(!sl.set_max(1, "1", 5));
        assert_eq!(sl.len(), 1);
    }

    #[test]
    fn replace_same_sid_and_remove_conn() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo", None));
        // Go keeps the first subscription for a sid: the second SUB is
        // ignored, so foo still delivers and bar does not.
        sl.add(sub(1, "1", "bar", None)); // ignored (keep-first)
        assert_eq!(sl.len(), 1);
        assert_eq!(
            sl.match_recipients("foo", 99, true, None).deliveries.len(),
            1
        );
        assert_eq!(
            sl.match_recipients("bar", 99, true, None).deliveries.len(),
            0
        );
        sl.add(sub(2, "1", "bar", None));
        sl.remove_conn(1);
        assert_eq!(sl.len(), 1);
        assert_eq!(
            sl.match_recipients("bar", 99, true, None).deliveries[0].conn_id,
            2
        );
    }

    #[test]
    fn echo_disabled_skips_publisher() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo", None));
        sl.add(sub(2, "1", "foo", None));
        let r = sl.match_recipients("foo", 1, false, None);
        assert_eq!(r.deliveries.len(), 1);
        assert_eq!(r.deliveries[0].conn_id, 2);
        let r = sl.match_recipients("foo", 1, true, None);
        assert_eq!(r.deliveries.len(), 2);
    }

    // --- trie-specific tests ---

    #[test]
    fn trie_deep_and_mid_wildcards() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "a.*.c.*", None));
        sl.add(sub(2, "1", "a.b.c.d", None));
        sl.add(sub(3, "1", "a.b.>", None));
        sl.add(sub(4, "1", "*.b.c.d", None));
        sl.add(sub(5, "1", "a.*.c.e", None));
        // a.b.c.d matches all except the .e pattern.
        assert_eq!(conns(&mut sl, "a.b.c.d"), vec![1, 2, 3, 4]);
        // a.x.c.e matches only the *-mid patterns (1 and 5).
        assert_eq!(conns(&mut sl, "a.x.c.e"), vec![1, 5]);
        // Star matches exactly one token: a.b.c alone matches none of
        // these (a.b.> needs a token after b; a.*.c.* needs four).
        assert_eq!(conns(&mut sl, "a.b.c"), vec![3]);
        assert!(conns(&mut sl, "a.b").is_empty());
    }

    #[test]
    fn trie_fwc_at_root_and_partial_wildcards_are_literal() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", ">", None));
        sl.add(sub(2, "1", "foo*", None));
        sl.add(sub(3, "1", "f>o", None));
        assert_eq!(conns(&mut sl, "anything.at.all"), vec![1]);
        // "foo*" as a subject token matches the literal pattern "foo*"
        // plus ">", but not a bare "foobar" subject.
        assert_eq!(conns(&mut sl, "foo*"), vec![1, 2]);
        assert_eq!(conns(&mut sl, "foobar"), vec![1]);
        assert_eq!(conns(&mut sl, "f>o"), vec![1, 3]);
    }

    #[test]
    fn trie_many_subscriptions_fanout() {
        let mut sl = Sublist::new();
        for i in 0..1000u64 {
            sl.add(sub(i, "1", &format!("svc.{i}.req"), None));
        }
        for i in 0..100u64 {
            sl.add(sub(10_000 + i, "1", "svc.*.req", None));
        }
        // One exact + 100 star matches; the other 999 exacts are skipped.
        assert_eq!(conns(&mut sl, "svc.42.req").len(), 101);
        assert!(conns(&mut sl, "svc.42.other").is_empty());
        assert_eq!(sl.len(), 1100);
    }

    #[test]
    fn trie_queue_plus_wildcard_and_removal_prunes() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "jobs.*", Some("workers")));
        sl.add(sub(2, "1", "jobs.*", Some("workers")));
        sl.add(sub(3, "1", "jobs.*", None));
        // Queue pick + plain, rotating across the two workers.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4 {
            let r = sl.match_recipients("jobs.build", 99, true, None);
            assert_eq!(r.deliveries.len(), 2);
            assert!(r.deliveries.iter().any(|d| d.conn_id == 3));
            seen.insert(
                r.deliveries
                    .iter()
                    .find(|d| d.conn_id != 3)
                    .unwrap()
                    .conn_id,
            );
        }
        assert_eq!(seen.len(), 2, "both workers picked over 4 messages");
        // Removing the wildcard prunes it from the trie entirely.
        sl.remove(1, "1");
        sl.remove(2, "1");
        sl.remove(3, "1");
        assert!(sl.is_empty());
        assert!(conns(&mut sl, "jobs.build").is_empty());
        // And re-adding after prune still matches.
        sl.add(sub(4, "1", "jobs.*", None));
        assert_eq!(conns(&mut sl, "jobs.build"), vec![4]);
    }

    #[test]
    fn trie_account_scoping_and_order_preserved() {
        let mut sl = Sublist::new();
        // Insert wildcard first, exact second: plain deliveries must
        // still come out in insertion order, not trie traversal order.
        sl.add(sub_acct(1, "1", "foo.>", None, "$G"));
        sl.add(sub_acct(2, "1", "foo.bar", None, "$G"));
        sl.add(sub_acct(3, "1", "foo.bar", None, "acct2"));
        let g = sl.match_groups("foo.bar", 99, true, None);
        assert_eq!(
            g.plain.iter().map(|d| d.conn_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let g = sl.match_groups("foo.bar", 99, true, Some("$G"));
        assert_eq!(
            g.plain.iter().map(|d| d.conn_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let g = sl.match_groups("foo.bar", 99, true, Some("acct2"));
        assert_eq!(
            g.plain.iter().map(|d| d.conn_id).collect::<Vec<_>>(),
            vec![3]
        );
        // subscriptions()/patterns() keep insertion order and dedup.
        assert_eq!(
            sl.subscriptions()
                .iter()
                .map(|s| s.conn_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        sl.add(sub_acct(4, "1", "foo.bar", None, "$G"));
        assert_eq!(
            sl.patterns().len(),
            3,
            "foo.>/$G, foo.bar/$G, foo.bar/acct2"
        );
        assert!(sl.has_pattern("foo.bar", None, "$G"));
        sl.remove_conn(2);
        assert!(
            sl.has_pattern("foo.bar", None, "$G"),
            "conn 4 still uses it"
        );
        sl.remove_conn(4);
        assert!(!sl.has_pattern("foo.bar", None, "$G"));
    }

    #[test]
    fn trie_duplicate_sid_ignored_and_auto_unsub_via_wildcard() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo.>", None));
        sl.add(sub(1, "1", "other", None)); // ignored: keep-first
        assert_eq!(conns(&mut sl, "foo.bar"), vec![1]);
        assert!(conns(&mut sl, "other").is_empty());
        let mut s = sub(2, "1", "foo.*", None);
        s.max_msgs = Some(1);
        sl.add(s);
        let r = sl.match_recipients("foo.bar", 99, true, None);
        assert_eq!(r.expired, vec![(2, "1".to_string())]);
        sl.remove(2, "1");
        assert_eq!(conns(&mut sl, "foo.bar"), vec![1]);
    }
}
