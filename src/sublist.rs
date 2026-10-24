//! Subscription registry: stores all subscriptions, resolves the recipient
//! set for a published message (fan-out + queue groups + auto-unsubscribe).

use std::collections::BTreeMap;

use crate::subject::subject_matches;

#[derive(Debug, Clone, PartialEq)]
pub struct Subscription {
    pub conn_id: u64,
    pub sid: String,
    pub subject: String,
    pub queue: Option<String>,
    /// Auto-unsubscribe after this many delivered messages (total).
    pub max_msgs: Option<u64>,
    pub delivered: u64,
}

impl Subscription {
    pub fn new(conn_id: u64, sid: String, subject: String, queue: Option<String>) -> Self {
        Subscription {
            conn_id,
            sid,
            subject,
            queue,
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

#[derive(Debug, Default)]
pub struct Sublist {
    subs: Vec<Subscription>,
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

    /// Add a subscription; an existing subscription with the same
    /// (conn_id, sid) is replaced.
    pub fn add(&mut self, sub: Subscription) {
        self.remove(sub.conn_id, &sub.sid);
        self.subs.push(sub);
    }

    pub fn remove(&mut self, conn_id: u64, sid: &str) -> bool {
        let before = self.subs.len();
        self.subs
            .retain(|s| !(s.conn_id == conn_id && s.sid == sid));
        self.subs.len() != before
    }

    pub fn remove_conn(&mut self, conn_id: u64) {
        self.subs.retain(|s| s.conn_id != conn_id);
    }

    /// Pattern+queue of one subscription, if it exists.
    pub fn pattern_of(&self, conn_id: u64, sid: &str) -> Option<(String, Option<String>)> {
        self.subs
            .iter()
            .find(|s| s.conn_id == conn_id && s.sid == sid)
            .map(|s| (s.subject.clone(), s.queue.clone()))
    }

    /// True when any subscription uses this exact pattern+queue.
    pub fn has_pattern(&self, subject: &str, queue: Option<&str>) -> bool {
        self.subs
            .iter()
            .any(|s| s.subject == subject && s.queue.as_deref() == queue)
    }

    /// Distinct (pattern, queue) pairs for one connection.
    pub fn patterns_for_conn(&self, conn_id: u64) -> Vec<(String, Option<String>)> {
        let mut out: Vec<(String, Option<String>)> = Vec::new();
        for s in &self.subs {
            if s.conn_id == conn_id {
                let key = (s.subject.clone(), s.queue.clone());
                if !out.contains(&key) {
                    out.push(key);
                }
            }
        }
        out
    }

    /// Distinct (pattern, queue) pairs currently subscribed, for
    /// RS+/LS+ interest sync to a newly connected route or leaf.
    pub fn patterns(&self) -> Vec<(String, Option<String>)> {
        let mut out: Vec<(String, Option<String>)> = Vec::new();
        for s in &self.subs {
            let key = (s.subject.clone(), s.queue.clone());
            if !out.contains(&key) {
                out.push(key);
            }
        }
        out
    }

    /// Set the auto-unsubscribe max for a subscription. If the subscription
    /// has already delivered at least `max` messages it is removed
    /// immediately and `true` is returned.
    pub fn set_max(&mut self, conn_id: u64, sid: &str, max: u64) -> bool {
        if let Some(sub) = self
            .subs
            .iter_mut()
            .find(|s| s.conn_id == conn_id && s.sid == sid)
        {
            if sub.delivered >= max {
                self.remove(conn_id, sid);
                return true;
            }
            sub.max_msgs = Some(max);
        }
        false
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
    ) -> MatchResult {
        let gm = self.match_groups(subject, publisher_conn, publisher_echo);
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
    ) -> GroupMatch {
        let mut out = GroupMatch::default();
        for sub in &self.subs {
            if !publisher_echo && sub.conn_id == publisher_conn {
                continue;
            }
            if !subject_matches(&sub.subject, subject) {
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

    /// Record actual deliveries: bump counters, return subscriptions that
    /// hit their auto-unsubscribe max with this delivery.
    pub fn note_deliveries(&mut self, deliveries: &[Delivery]) -> Vec<(u64, String)> {
        let mut expired = Vec::new();
        for d in deliveries {
            if let Some(sub) = self
                .subs
                .iter_mut()
                .find(|s| s.conn_id == d.conn_id && s.sid == d.sid)
            {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(conn: u64, sid: &str, subject: &str, queue: Option<&str>) -> Subscription {
        Subscription::new(conn, sid.into(), subject.into(), queue.map(|q| q.into()))
    }

    #[test]
    fn fanout_to_all_plain_subs() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo", None));
        sl.add(sub(2, "1", "foo", None));
        sl.add(sub(3, "1", "bar", None));
        let r = sl.match_recipients("foo", 99, true);
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
        assert_eq!(sl.match_recipients("foo.bar", 99, true).deliveries.len(), 3);
        assert_eq!(
            sl.match_recipients("foo.bar.baz", 99, true)
                .deliveries
                .len(),
            2
        );
        assert_eq!(sl.match_recipients("other", 99, true).deliveries.len(), 1);
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
            let r = sl.match_recipients("jobs", 99, true);
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
        let r = sl.match_recipients("jobs", 99, true);
        assert_eq!(r.deliveries.len(), 2);
    }

    #[test]
    fn auto_unsubscribe_after_max() {
        let mut sl = Sublist::new();
        let mut s = sub(1, "1", "foo", None);
        s.max_msgs = Some(2);
        sl.add(s);
        let r1 = sl.match_recipients("foo", 99, true);
        assert_eq!(r1.deliveries.len(), 1);
        assert!(r1.expired.is_empty());
        let r2 = sl.match_recipients("foo", 99, true);
        assert_eq!(r2.deliveries.len(), 1);
        assert_eq!(r2.expired, vec![(1, "1".to_string())]);
        sl.remove(1, "1");
        assert_eq!(sl.match_recipients("foo", 99, true).deliveries.len(), 0);
    }

    #[test]
    fn set_max_removes_when_already_reached() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo", None));
        let _ = sl.match_recipients("foo", 99, true);
        let _ = sl.match_recipients("foo", 99, true);
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
        sl.add(sub(1, "1", "bar", None)); // replaces
        assert_eq!(sl.len(), 1);
        assert_eq!(sl.match_recipients("foo", 99, true).deliveries.len(), 0);
        assert_eq!(sl.match_recipients("bar", 99, true).deliveries.len(), 1);
        sl.add(sub(2, "1", "bar", None));
        sl.remove_conn(1);
        assert_eq!(sl.len(), 1);
        assert_eq!(
            sl.match_recipients("bar", 99, true).deliveries[0].conn_id,
            2
        );
    }

    #[test]
    fn echo_disabled_skips_publisher() {
        let mut sl = Sublist::new();
        sl.add(sub(1, "1", "foo", None));
        sl.add(sub(2, "1", "foo", None));
        let r = sl.match_recipients("foo", 1, false);
        assert_eq!(r.deliveries.len(), 1);
        assert_eq!(r.deliveries[0].conn_id, 2);
        let r = sl.match_recipients("foo", 1, true);
        assert_eq!(r.deliveries.len(), 2);
    }
}
