//! Subject validation and wildcard matching.
//!
//! Subjects are dot-separated tokens. In subscription subjects, `*` matches
//! exactly one token and `>` (only valid as the final token) matches one or
//! more tokens. Publish subjects may not contain wildcards.

/// Go's publish-subject rule (nats-server v2.15.1, verified by raw probe):
/// a publish subject is *pedantically* invalid when it has an empty token
/// or a whole-token wildcard (`*` or `>`). Partial wildcards (`foo*`,
/// `f>o`) are literal tokens and are NOT invalid, even in pedantic mode.
/// In non-pedantic mode an invalid publish subject is still accepted and
/// delivered literally (except empty-token subjects, which route nowhere);
/// in pedantic mode Go additionally sends `-ERR 'Invalid Publish Subject'`
/// but still delivers.
pub fn is_valid_publish_subject(subject: &str) -> bool {
    !subject.is_empty()
        && subject.split('.').all(|tok| {
            !tok.is_empty()
                && tok != "*"
                && tok != ">"
                && tok.chars().all(|c| !c.is_whitespace() && !c.is_control())
        })
}

/// True when the subject has an empty token (e.g. `foo..bar`, `foo.`,
/// `.foo`, `.`). Go accepts such publish subjects without error in
/// non-pedantic mode but delivers them to no subscription at all, not even
/// `>`, so the publisher must drop them instead of delivering.
pub fn has_empty_token(s: &str) -> bool {
    s.is_empty() || s.split('.').any(|t| t.is_empty())
}

/// Go's subscription-subject rule (nats-server v2.15.1, verified by raw
/// probe): a SUB subject is rejected when it has an empty token or when a
/// whole-token `>` appears anywhere except as the final token. Partial
/// wildcards (`foo*`, `f>o`) are accepted as literal tokens. A rejected
/// SUB gets `-ERR 'Invalid Subject'` but the connection stays alive and
/// no subscription is created.
pub fn is_valid_subscribe_subject(subject: &str) -> bool {
    if subject.is_empty() {
        return false;
    }
    let tokens: Vec<&str> = subject.split('.').collect();
    for (i, tok) in tokens.iter().enumerate() {
        if tok.is_empty() || tok.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return false;
        }
        if *tok == ">" && i != tokens.len() - 1 {
            return false;
        }
    }
    true
}

/// Translate a concrete `subject` that matched `from_pattern` into the
/// corresponding subject under `to_pattern`, by carrying wildcard
/// captures across: each `*` in `to_pattern` takes the next captured
/// single token, each `>` takes the remaining captured tokens. Returns
/// None when `subject` does not match `from_pattern`, or when
/// `to_pattern` needs a capture `from_pattern` did not produce (e.g.
/// `from` is literal but `to` has a `*`). Used for account
/// import/export subject mapping (service and stream imports).
pub fn translate_subject(from_pattern: &str, to_pattern: &str, subject: &str) -> Option<String> {
    if !subject_matches(from_pattern, subject) {
        return None;
    }
    let from: Vec<&str> = from_pattern.split('.').collect();
    let subj: Vec<&str> = subject.split('.').collect();
    // Captures: single tokens (from `*`) in order, then the `>` tail
    // (if any) as a token list.
    let mut singles: Vec<&str> = Vec::new();
    let mut tail: Vec<&str> = Vec::new();
    let mut i = 0;
    for tok in &from {
        match *tok {
            ">" => {
                tail = subj[i..].to_vec();
                i = subj.len();
            }
            "*" => {
                singles.push(subj[i]);
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    let to: Vec<&str> = to_pattern.split('.').collect();
    let mut single_idx = 0;
    let mut out: Vec<String> = Vec::new();
    for tok in &to {
        match *tok {
            "*" => {
                let captured = singles.get(single_idx)?;
                single_idx += 1;
                out.push((*captured).to_string());
            }
            ">" => {
                if tail.is_empty() {
                    return None;
                }
                out.extend(tail.iter().map(|t| (*t).to_string()));
            }
            literal => out.push(literal.to_string()),
        }
    }
    Some(out.join("."))
}

/// True when every subject matched by `import_pattern` is also matched
/// by `export_pattern` (pattern-coverage, not subject matching): an
/// export of `svc.>` covers an import of `svc.echo` and of `svc.>`;
/// an export of `svc.echo` does not cover an import of `svc.>`.
/// Used to decide whether an account import is authorized by an export.
pub fn pattern_covers(export_pattern: &str, import_pattern: &str) -> bool {
    let exp: Vec<&str> = export_pattern.split('.').collect();
    let imp: Vec<&str> = import_pattern.split('.').collect();
    let mut i = 0;
    while i < exp.len() {
        match exp[i] {
            ">" => return true,
            "*" => {
                if i >= imp.len() {
                    return false;
                }
                // `*` covers a literal token or a `*`; it does not
                // cover a terminal `>` (different arity).
                if imp[i] == ">" {
                    return false;
                }
            }
            literal => {
                if i >= imp.len() || imp[i] != literal {
                    return false;
                }
            }
        }
        i += 1;
    }
    i == imp.len()
}

/// Returns true if a published `subject` matches subscription pattern
/// `pattern` (which may contain `*` / `>` wildcards).
pub fn subject_matches(pattern: &str, subject: &str) -> bool {
    let mut pat = pattern.split('.').peekable();
    let mut subj = subject.split('.');
    loop {
        match pat.next() {
            None => return subj.next().is_none(),
            Some(">") => {
                // `>` requires at least one remaining subject token.
                return subj.next().is_some();
            }
            Some("*") => {
                if subj.next().is_none() {
                    return false;
                }
            }
            Some(tok) => match subj.next() {
                Some(s) if s == tok => continue,
                _ => return false,
            },
        }
        if pat.peek().is_none() {
            return subj.next().is_none();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_subject_validation() {
        // Go behaviour (verified against nats-server v2.15.1): partial
        // wildcards are literal tokens, not invalid, even in pedantic mode.
        assert!(is_valid_publish_subject("foo"));
        assert!(is_valid_publish_subject("foo.bar.baz"));
        assert!(is_valid_publish_subject("FOO_123.-x-"));
        assert!(is_valid_publish_subject("foo*"));
        assert!(is_valid_publish_subject("f>o"));
        assert!(!is_valid_publish_subject(""));
        assert!(!is_valid_publish_subject("foo..bar"));
        assert!(!is_valid_publish_subject(".foo"));
        assert!(!is_valid_publish_subject("foo."));
        assert!(!is_valid_publish_subject("foo.*"));
        assert!(!is_valid_publish_subject("foo.>"));
        assert!(!is_valid_publish_subject("foo bar"));
        assert!(!is_valid_publish_subject("foo\tbar"));
        // Empty-token subjects route nowhere on Go, even to `>`.
        assert!(has_empty_token("foo..bar"));
        assert!(has_empty_token("foo."));
        assert!(has_empty_token(".foo"));
        assert!(has_empty_token("."));
        assert!(!has_empty_token("foo.*"));
        assert!(!has_empty_token("foo.bar"));
    }

    #[test]
    fn subscribe_subject_validation() {
        // Go behaviour (verified against nats-server v2.15.1): partial
        // wildcards are accepted as literal tokens.
        assert!(is_valid_subscribe_subject("foo.*"));
        assert!(is_valid_subscribe_subject("*"));
        assert!(is_valid_subscribe_subject(">"));
        assert!(is_valid_subscribe_subject("foo.>"));
        assert!(is_valid_subscribe_subject("foo.*.baz"));
        assert!(is_valid_subscribe_subject("foo*"));
        assert!(is_valid_subscribe_subject("f>o"));
        assert!(!is_valid_subscribe_subject("foo.>.bar"));
        assert!(!is_valid_subscribe_subject("foo..*"));
        assert!(!is_valid_subscribe_subject(""));
    }

    #[test]
    fn translate_subject_mapping() {
        assert_eq!(
            translate_subject("local.>", "svc.>", "local.echo"),
            Some("svc.echo".to_string())
        );
        assert_eq!(
            translate_subject("local.>", "svc.>", "local.a.b"),
            Some("svc.a.b".to_string())
        );
        assert_eq!(
            translate_subject("local.*.x", "svc.*.y", "local.echo.x"),
            Some("svc.echo.y".to_string())
        );
        assert_eq!(translate_subject("local.>", "svc.>", "other.echo"), None);
        assert_eq!(translate_subject("local", "svc.*", "local"), None);
        assert_eq!(
            translate_subject("events.>", "feed.>", "events.news"),
            Some("feed.news".to_string())
        );
    }

    #[test]
    fn pattern_covers_cases() {
        assert!(pattern_covers("svc.>", "svc.echo"));
        assert!(pattern_covers("svc.>", "svc.>"));
        assert!(pattern_covers("svc.*", "svc.echo"));
        assert!(pattern_covers("svc.*", "svc.*"));
        assert!(!pattern_covers("svc.echo", "svc.>"));
        assert!(!pattern_covers("svc.*", "svc.>"));
        assert!(!pattern_covers("svc.echo", "svc.other"));
        assert!(!pattern_covers("svc.>", "other.>"));
    }

    #[test]
    fn matching_truth_table() {
        let cases = [
            ("foo", "foo", true),
            ("foo", "bar", false),
            ("foo", "foo.bar", false),
            ("foo.bar", "foo", false),
            ("foo.*", "foo.bar", true),
            ("foo.*", "foo.bar.baz", false),
            ("foo.*", "foo", false),
            ("*", "foo", true),
            ("*", "foo.bar", false),
            ("foo.>", "foo.bar", true),
            ("foo.>", "foo.bar.baz", true),
            ("foo.>", "foo", false),
            (">", "foo", true),
            (">", "foo.bar.baz", true),
            ("foo.*.baz", "foo.bar.baz", true),
            ("foo.*.baz", "foo.bar.qux", false),
            ("*.bar", "foo.bar", true),
            ("*.bar", "foo.baz.bar", false),
        ];
        for (pat, subj, want) in cases {
            assert_eq!(subject_matches(pat, subj), want, "{pat} vs {subj}");
        }
    }
}
