//! Redis-style glob matching for KEYS / PSUBSCRIBE / SCAN MATCH: `*`, `?`, `[abc]`, `[^a-z]`, `\x`.

pub fn glob_match(p: &[u8], s: &[u8]) -> bool {
    let (mut pi, mut si) = (0usize, 0usize);
    let mut back: Option<(usize, usize)> = None;
    while si < s.len() {
        if pi < p.len() {
            match p[pi] {
                b'*' => {
                    back = Some((pi, si));
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    si += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, len)) = class(&p[pi..], s[si]) {
                        if matched {
                            pi += len;
                            si += 1;
                            continue;
                        }
                    }
                }
                b'\\' if pi + 1 < p.len() => {
                    if p[pi + 1] == s[si] {
                        pi += 2;
                        si += 1;
                        continue;
                    }
                }
                c if c == s[si] => {
                    pi += 1;
                    si += 1;
                    continue;
                }
                _ => {}
            }
        }
        match back {
            Some((bp, bs)) => {
                pi = bp + 1;
                si = bs + 1;
                back = Some((bp, bs + 1));
            }
            None => return false,
        }
    }
    p[pi..].iter().all(|&c| c == b'*')
}

/// Matches a `[...]` class at the start of `p`; returns (matched, pattern length).
fn class(p: &[u8], c: u8) -> Option<(bool, usize)> {
    let mut i = 1;
    let negate = p.get(1) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut hit = false;
    while i < p.len() && p[i] != b']' {
        if p[i] == b'\\' && i + 1 < p.len() {
            hit |= p[i + 1] == c;
            i += 2;
        } else if i + 2 < p.len() && p[i + 1] == b'-' && p[i + 2] != b']' {
            let (lo, hi) = if p[i] <= p[i + 2] { (p[i], p[i + 2]) } else { (p[i + 2], p[i]) };
            hit |= (lo..=hi).contains(&c);
            i += 3;
        } else {
            hit |= p[i] == c;
            i += 1;
        }
    }
    if i >= p.len() {
        return None; // unterminated class: treat '[' literally
    }
    Some((hit != negate, i + 1))
}

#[cfg(test)]
mod tests {
    use super::glob_match as g;

    #[test]
    fn patterns() {
        assert!(g(b"*", b"anything"));
        assert!(g(b"user:*", b"user:42"));
        assert!(!g(b"user:*", b"session:1"));
        assert!(g(b"h?llo", b"hello") && g(b"h?llo", b"hallo"));
        assert!(g(b"h[ae]llo", b"hello") && !g(b"h[ae]llo", b"hillo"));
        assert!(g(b"h[^e]llo", b"hallo") && !g(b"h[^e]llo", b"hello"));
        assert!(g(b"key[0-9]", b"key7") && !g(b"key[0-9]", b"keyx"));
        assert!(g(b"a\\*b", b"a*b") && !g(b"a\\*b", b"axb"));
        assert!(g(b"*a*b*c*", b"xxaxxbxxcxx"));
    }
}
