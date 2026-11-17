//! RESP2 encoding/decoding plus redis-cli style pretty printing.

#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    NilArray,
    Array(Vec<Reply>),
}

impl Reply {
    pub fn ok() -> Reply {
        Reply::Simple("OK".into())
    }
    pub fn err(msg: impl Into<String>) -> Reply {
        Reply::Error(msg.into())
    }
    pub fn bulk(b: impl Into<Vec<u8>>) -> Reply {
        Reply::Bulk(b.into())
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Reply::Simple(s) => out.extend_from_slice(format!("+{s}\r\n").as_bytes()),
            Reply::Error(s) => out.extend_from_slice(format!("-{s}\r\n").as_bytes()),
            Reply::Int(i) => out.extend_from_slice(format!(":{i}\r\n").as_bytes()),
            Reply::Bulk(b) => {
                out.extend_from_slice(format!("${}\r\n", b.len()).as_bytes());
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Reply::Nil => out.extend_from_slice(b"$-1\r\n"),
            Reply::NilArray => out.extend_from_slice(b"*-1\r\n"),
            Reply::Array(items) => {
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                for i in items {
                    i.encode(out);
                }
            }
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v);
        v
    }

    /// Formats like redis-cli does.
    pub fn pretty(&self) -> String {
        fn go(r: &Reply, indent: usize, out: &mut String) {
            match r {
                Reply::Simple(s) => out.push_str(s),
                Reply::Error(e) => out.push_str(&format!("(error) {e}")),
                Reply::Int(i) => out.push_str(&format!("(integer) {i}")),
                Reply::Bulk(b) => out.push_str(&format!("{:?}", String::from_utf8_lossy(b))),
                Reply::Nil | Reply::NilArray => out.push_str("(nil)"),
                Reply::Array(items) if items.is_empty() => out.push_str("(empty array)"),
                Reply::Array(items) => {
                    let w = items.len().to_string().len();
                    for (i, item) in items.iter().enumerate() {
                        if i > 0 {
                            out.push('\n');
                            out.push_str(&" ".repeat(indent));
                        }
                        let prefix = format!("{:>w$}) ", i + 1);
                        out.push_str(&prefix);
                        go(item, indent + prefix.len(), out);
                    }
                }
            }
        }
        let mut s = String::new();
        go(self, 0, &mut s);
        s
    }
}

#[derive(Debug, PartialEq)]
pub enum Parse {
    /// A full command and how many bytes it used.
    Command(Vec<Vec<u8>>, usize),
    Incomplete,
    Invalid(String),
}

fn line(buf: &[u8], from: usize) -> Option<(&[u8], usize)> {
    let rel = buf[from..].windows(2).position(|w| w == b"\r\n")?;
    Some((&buf[from..from + rel], from + rel + 2))
}

/// Parses one command: a RESP array of bulk strings, or an inline command (`SET a 1\r\n`).
pub fn parse_command(buf: &[u8]) -> Parse {
    if buf.is_empty() {
        return Parse::Incomplete;
    }
    if buf[0] != b'*' {
        let Some(end) = buf.iter().position(|&b| b == b'\n') else {
            return if buf.len() > 64 * 1024 { Parse::Invalid("inline command too long".into()) } else { Parse::Incomplete };
        };
        let text = String::from_utf8_lossy(&buf[..end]);
        return match split_args(text.trim_end_matches('\r')) {
            Ok(args) => Parse::Command(args, end + 1),
            Err(e) => Parse::Invalid(e),
        };
    }
    let Some((head, mut pos)) = line(buf, 1) else { return Parse::Incomplete };
    let Ok(n) = std::str::from_utf8(head).unwrap_or("x").parse::<i64>() else { return Parse::Invalid("invalid multibulk length".into()) };
    if !(0..=1024 * 1024).contains(&n) {
        return Parse::Invalid("invalid multibulk length".into());
    }
    let mut args = Vec::with_capacity(n as usize);
    for _ in 0..n {
        if pos >= buf.len() {
            return Parse::Incomplete;
        }
        if buf[pos] != b'$' {
            return Parse::Invalid(format!("expected '$', got '{}'", buf[pos] as char));
        }
        let Some((len, next)) = line(buf, pos + 1) else { return Parse::Incomplete };
        let Ok(len) = std::str::from_utf8(len).unwrap_or("x").parse::<usize>() else { return Parse::Invalid("invalid bulk length".into()) };
        if len > 512 * 1024 * 1024 {
            return Parse::Invalid("invalid bulk length".into());
        }
        if buf.len() < next + len + 2 {
            return Parse::Incomplete;
        }
        args.push(buf[next..next + len].to_vec());
        pos = next + len + 2;
    }
    Parse::Command(args, pos)
}

/// Splits a command line the way redis-cli does: whitespace separated, "double" or 'single' quotes.
pub fn split_args(line: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let Some(&c) = chars.peek() else { break };
        let mut cur = String::new();
        if c == '"' || c == '\'' {
            chars.next();
            let mut closed = false;
            while let Some(ch) = chars.next() {
                if ch == c {
                    closed = true;
                    break;
                }
                if ch == '\\' && c == '"' {
                    match chars.next() {
                        Some('n') => cur.push('\n'),
                        Some('t') => cur.push('\t'),
                        Some('r') => cur.push('\r'),
                        Some(o) => cur.push(o),
                        None => break,
                    }
                } else {
                    cur.push(ch);
                }
            }
            if !closed {
                return Err("unbalanced quotes".into());
            }
        } else {
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() {
                    break;
                }
                cur.push(ch);
                chars.next();
            }
        }
        out.push(cur.into_bytes());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resp_arrays_incrementally() {
        let full = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n";
        for cut in 0..full.len() {
            assert_eq!(parse_command(&full[..cut]), Parse::Incomplete, "cut at {cut}");
        }
        assert_eq!(parse_command(full), Parse::Command(vec![b"SET".to_vec(), b"key".to_vec(), b"hello".to_vec()], full.len()));
    }

    #[test]
    fn parses_inline_commands() {
        let Parse::Command(args, used) = parse_command(b"set greeting \"hello world\"\r\nPING\r\n") else { panic!() };
        assert_eq!(args, vec![b"set".to_vec(), b"greeting".to_vec(), b"hello world".to_vec()]);
        assert_eq!(used, 28);
    }

    #[test]
    fn encodes_and_pretty_prints() {
        let r = Reply::Array(vec![Reply::bulk("a"), Reply::Int(2), Reply::Nil]);
        assert_eq!(r.to_bytes(), b"*3\r\n$1\r\na\r\n:2\r\n$-1\r\n");
        assert_eq!(r.pretty(), "1) \"a\"\n2) (integer) 2\n3) (nil)");
    }
}
