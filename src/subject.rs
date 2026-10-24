//! Subject validation and wildcard matching.
//!
//! Subjects are dot-separated tokens. In subscription subjects, `*` matches
//! exactly one token and `>` (only valid as the final token) matches one or
//! more tokens. Publish subjects may not contain wildcards.

/// Returns true if `subject` is a valid subject to publish to:
/// non-empty tokens, no wildcards, no whitespace/control characters.
pub fn is_valid_publish_subject(subject: &str) -> bool {
    !subject.is_empty()
        && subject.split('.').all(|tok| {
            !tok.is_empty()
                && !tok.contains(['*', '>'])
                && tok.chars().all(|c| !c.is_whitespace() && !c.is_control())
        })
}

/// Returns true if `subject` is a valid subscription subject: publish rules,
/// except whole-token `*` anywhere and whole-token `>` as the last token.
pub fn is_valid_subscribe_subject(subject: &str) -> bool {
    if subject.is_empty() {
        return false;
    }
    let tokens: Vec<&str> = subject.split('.').collect();
    for (i, tok) in tokens.iter().enumerate() {
        if tok.is_empty() || tok.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return false;
        }
        if tok.contains('*') && *tok != "*" {
            return false;
        }
        if tok.contains('>') && (*tok != ">" || i != tokens.len() - 1) {
            return false;
        }
    }
    true
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
        assert!(is_valid_publish_subject("foo"));
        assert!(is_valid_publish_subject("foo.bar.baz"));
        assert!(is_valid_publish_subject("FOO_123.-x-"));
        assert!(!is_valid_publish_subject(""));
        assert!(!is_valid_publish_subject("foo..bar"));
        assert!(!is_valid_publish_subject(".foo"));
        assert!(!is_valid_publish_subject("foo."));
        assert!(!is_valid_publish_subject("foo.*"));
        assert!(!is_valid_publish_subject("foo.>"));
        assert!(!is_valid_publish_subject("foo bar"));
        assert!(!is_valid_publish_subject("foo\tbar"));
    }

    #[test]
    fn subscribe_subject_validation() {
        assert!(is_valid_subscribe_subject("foo.*"));
        assert!(is_valid_subscribe_subject("*"));
        assert!(is_valid_subscribe_subject(">"));
        assert!(is_valid_subscribe_subject("foo.>"));
        assert!(is_valid_subscribe_subject("foo.*.baz"));
        assert!(!is_valid_subscribe_subject("foo.>.bar"));
        assert!(!is_valid_subscribe_subject("foo*"));
        assert!(!is_valid_subscribe_subject("f>o"));
        assert!(!is_valid_subscribe_subject("foo..*"));
        assert!(!is_valid_subscribe_subject(""));
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
