//! The keyspace and every data-type command. Pure and synchronous: time is passed in,
//! so the same code runs in the server, the tests and the WebAssembly demo.

use crate::glob::glob_match;
use crate::resp::Reply;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Score(pub f64);
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Score {
    fn cmp(&self, o: &Self) -> Ordering {
        self.0.total_cmp(&o.0)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ZSet {
    scores: HashMap<Vec<u8>, f64>,
    order: BTreeSet<(Score, Vec<u8>)>,
}

impl ZSet {
    /// Inserts or updates; returns true if the member is new.
    fn insert(&mut self, member: Vec<u8>, score: f64) -> bool {
        let new = match self.scores.insert(member.clone(), score) {
            Some(old) => {
                self.order.remove(&(Score(old), member.clone()));
                false
            }
            None => true,
        };
        self.order.insert((Score(score), member));
        new
    }
    fn remove(&mut self, member: &[u8]) -> bool {
        match self.scores.remove(member) {
            Some(s) => {
                self.order.remove(&(Score(s), member.to_vec()));
                true
            }
            None => false,
        }
    }
    fn rank(&self, member: &[u8]) -> Option<usize> {
        let s = *self.scores.get(member)?;
        Some(self.order.range(..(Score(s), member.to_vec())).count())
    }
    pub fn len(&self) -> usize {
        self.scores.len()
    }
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Str(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    ZSet(ZSet),
}

impl Value {
    fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }
    fn is_empty_container(&self) -> bool {
        match self {
            Value::Str(_) => false,
            Value::List(l) => l.is_empty(),
            Value::Hash(h) => h.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::ZSet(z) => z.is_empty(),
        }
    }
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const NOT_FLOAT: &str = "ERR value is not a valid float";
const SYNTAX: &str = "ERR syntax error";

#[derive(Default)]
pub struct Db {
    map: HashMap<Vec<u8>, Value>,
    expires: HashMap<Vec<u8>, u64>,
    pub dirty: u64,
    pub expired_keys: u64,
}

fn int(b: &[u8]) -> Result<i64, Reply> {
    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).ok_or_else(|| Reply::err(NOT_INT))
}
fn float(b: &[u8]) -> Result<f64, Reply> {
    let s = std::str::from_utf8(b).map_err(|_| Reply::err(NOT_FLOAT))?.to_ascii_lowercase();
    let v = match s.as_str() {
        "+inf" | "inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        _ => s.parse().map_err(|_| Reply::err(NOT_FLOAT))?,
    };
    if v.is_nan() {
        return Err(Reply::err(NOT_FLOAT));
    }
    Ok(v)
}
pub fn fmt_float(f: f64) -> String {
    if f.is_infinite() {
        return if f > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if f.fract() == 0.0 && f.abs() < 1e17 {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}
/// Converts Redis-style inclusive (possibly negative) indexes into a Rust range.
fn index_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let s = if start < 0 { (len + start).max(0) } else { start };
    let e = if stop < 0 { len + stop } else { stop.min(len - 1) };
    if s > e || s >= len {
        None
    } else {
        Some((s as usize, e as usize))
    }
}
fn bulks<I: IntoIterator<Item = Vec<u8>>>(it: I) -> Reply {
    Reply::Array(it.into_iter().map(Reply::Bulk).collect())
}

/// Commands that change data (used for AOF logging and the dirty counter).
pub fn is_write(cmd: &str) -> bool {
    matches!(
        cmd,
        "set" | "setnx" | "setex" | "psetex" | "getset" | "getdel" | "mset" | "msetnx" | "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" | "append"
            | "setrange" | "del" | "unlink" | "expire" | "pexpire" | "expireat" | "pexpireat" | "persist" | "rename" | "flushdb" | "flushall"
            | "lpush" | "rpush" | "lpushx" | "rpushx" | "lpop" | "rpop" | "lset" | "lrem" | "ltrim" | "hset" | "hmset" | "hsetnx" | "hdel"
            | "hincrby" | "hincrbyfloat" | "sadd" | "srem" | "zadd" | "zincrby" | "zrem" | "getex"
    )
}

impl Db {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn expires_count(&self) -> usize {
        self.expires.len()
    }
    pub fn expire_at(&self, key: &[u8]) -> Option<u64> {
        self.expires.get(key).copied()
    }

    /// Lazily expires `key`; returns true if it still exists.
    fn alive(&mut self, key: &[u8], now: u64) -> bool {
        if let Some(&at) = self.expires.get(key) {
            if at <= now {
                self.expires.remove(key);
                self.map.remove(key);
                self.expired_keys += 1;
                return false;
            }
        }
        self.map.contains_key(key)
    }

    fn get(&mut self, key: &[u8], now: u64) -> Option<&Value> {
        if self.alive(key, now) {
            self.map.get(key)
        } else {
            None
        }
    }

    fn get_mut(&mut self, key: &[u8], now: u64) -> Option<&mut Value> {
        if self.alive(key, now) {
            self.map.get_mut(key)
        } else {
            None
        }
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        self.expires.remove(key);
        self.map.remove(key).is_some()
    }

    fn set_str(&mut self, key: &[u8], v: Vec<u8>) {
        self.expires.remove(key);
        self.map.insert(key.to_vec(), Value::Str(v));
    }

    /// Drops containers that became empty, like Redis does.
    fn cleanup(&mut self, key: &[u8]) {
        if self.map.get(key).is_some_and(Value::is_empty_container) {
            self.remove(key);
        }
    }

    /// Active expiry: samples up to 20 keys with TTLs and evicts expired ones.
    pub fn active_expire(&mut self, now: u64) -> usize {
        let victims: Vec<Vec<u8>> = self.expires.iter().take(20).filter(|(_, &at)| at <= now).map(|(k, _)| k.clone()).collect();
        for k in &victims {
            self.remove(k);
            self.expired_keys += 1;
        }
        victims.len()
    }

    /// Commands that rebuild the current dataset (used to compact the AOF).
    pub fn dump(&self, now: u64) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let b = |s: &str| s.as_bytes().to_vec();
        for (k, v) in &self.map {
            if self.expires.get(k).is_some_and(|&at| at <= now) {
                continue;
            }
            match v {
                Value::Str(s) => out.push(vec![b("SET"), k.clone(), s.clone()]),
                Value::List(l) => out.push([vec![b("RPUSH"), k.clone()], l.iter().cloned().collect()].concat()),
                Value::Hash(h) => out.push([vec![b("HSET"), k.clone()], h.iter().flat_map(|(f, v)| [f.clone(), v.clone()]).collect()].concat()),
                Value::Set(s) => out.push([vec![b("SADD"), k.clone()], s.iter().cloned().collect()].concat()),
                Value::ZSet(z) => out.push([vec![b("ZADD"), k.clone()], z.order.iter().flat_map(|(s, m)| [fmt_float(s.0).into_bytes(), m.clone()]).collect()].concat()),
            }
            if let Some(at) = self.expires.get(k) {
                out.push(vec![b("PEXPIREAT"), k.clone(), at.to_string().into_bytes()]);
            }
        }
        out
    }

    pub fn keys_matching(&mut self, pattern: &[u8], now: u64) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = self.map.keys().filter(|k| glob_match(pattern, k)).cloned().collect();
        keys.retain(|k| self.alive(k, now));
        keys.sort();
        keys
    }

    /// Executes one command. `args[0]` is the command name.
    pub fn execute(&mut self, args: &[Vec<u8>], now: u64) -> Reply {
        let Some(first) = args.first() else { return Reply::err("ERR empty command") };
        let cmd = String::from_utf8_lossy(first).to_ascii_lowercase();
        match self.dispatch(&cmd, &args[1..], now) {
            Ok(r) => {
                if is_write(&cmd) {
                    self.dirty += 1;
                }
                r
            }
            Err(e) => e,
        }
    }

    fn dispatch(&mut self, cmd: &str, a: &[Vec<u8>], now: u64) -> Result<Reply, Reply> {
        let arity = |min: usize, max: Option<usize>| {
            if a.len() < min || max.is_some_and(|m| a.len() > m) {
                Err(Reply::err(format!("ERR wrong number of arguments for '{cmd}' command")))
            } else {
                Ok(())
            }
        };
        macro_rules! typed {
            ($key:expr, $pat:ident) => {
                match self.get($key, now) {
                    None => None,
                    Some(Value::$pat(v)) => Some(v),
                    Some(_) => return Err(Reply::err(WRONGTYPE)),
                }
            };
        }
        macro_rules! typed_mut {
            ($key:expr, $pat:ident, $default:expr) => {{
                if !self.alive($key, now) {
                    self.map.insert($key.to_vec(), Value::$pat($default));
                }
                match self.map.get_mut($key) {
                    Some(Value::$pat(v)) => v,
                    _ => return Err(Reply::err(WRONGTYPE)),
                }
            }};
        }

        Ok(match cmd {
            // ------------------------------------------------------------ generic
            "ping" => {
                arity(0, Some(1))?;
                a.first().map_or(Reply::Simple("PONG".into()), |m| Reply::Bulk(m.clone()))
            }
            "echo" => {
                arity(1, Some(1))?;
                Reply::Bulk(a[0].clone())
            }
            "del" | "unlink" => {
                arity(1, None)?;
                Reply::Int(a.iter().filter(|k| self.alive(k, now) && self.remove(k)).count() as i64)
            }
            "exists" => {
                arity(1, None)?;
                Reply::Int(a.iter().filter(|k| self.alive(k, now)).count() as i64)
            }
            "type" => {
                arity(1, Some(1))?;
                Reply::Simple(self.get(&a[0], now).map_or("none", Value::type_name).into())
            }
            "keys" => {
                arity(1, Some(1))?;
                bulks(self.keys_matching(&a[0], now))
            }
            "scan" => {
                arity(1, None)?;
                let cursor = int(&a[0])?.max(0) as usize;
                let (mut pattern, mut count) = (b"*".to_vec(), 10usize);
                let mut i = 1;
                while i < a.len() {
                    match (String::from_utf8_lossy(&a[i]).to_ascii_lowercase().as_str(), a.get(i + 1)) {
                        ("match", Some(p)) => pattern = p.clone(),
                        ("count", Some(c)) => count = int(c)?.max(1) as usize,
                        _ => return Err(Reply::err(SYNTAX)),
                    }
                    i += 2;
                }
                let mut all: Vec<Vec<u8>> = self.map.keys().cloned().collect();
                all.sort();
                let end = (cursor + count).min(all.len());
                let page: Vec<Vec<u8>> = all.get(cursor..end).unwrap_or(&[]).iter().filter(|k| glob_match(&pattern, k)).cloned().collect();
                let page: Vec<Vec<u8>> = page.into_iter().filter(|k| self.alive(k, now)).collect();
                let next = if end >= all.len() { 0 } else { end };
                Reply::Array(vec![Reply::bulk(next.to_string()), bulks(page)])
            }
            "dbsize" => Reply::Int(self.map.len() as i64),
            "flushdb" | "flushall" => {
                self.map.clear();
                self.expires.clear();
                Reply::ok()
            }
            "expire" | "pexpire" | "expireat" | "pexpireat" => {
                arity(2, Some(3))?;
                let n = int(&a[1])?;
                let at = match cmd {
                    "expire" => now as i64 + n.saturating_mul(1000),
                    "pexpire" => now as i64 + n,
                    "expireat" => n.saturating_mul(1000),
                    _ => n,
                };
                if !self.alive(&a[0], now) {
                    return Ok(Reply::Int(0));
                }
                let cur = self.expires.get(&a[0]).copied();
                let ok = match a.get(2).map(|f| String::from_utf8_lossy(f).to_ascii_lowercase()) {
                    None => true,
                    Some(f) if f == "nx" => cur.is_none(),
                    Some(f) if f == "xx" => cur.is_some(),
                    Some(f) if f == "gt" => cur.is_some_and(|c| at as u64 > c),
                    Some(f) if f == "lt" => cur.map_or(true, |c| (at as u64) < c),
                    _ => return Err(Reply::err(SYNTAX)),
                };
                if !ok {
                    return Ok(Reply::Int(0));
                }
                if at <= now as i64 {
                    self.remove(&a[0]);
                } else {
                    self.expires.insert(a[0].clone(), at as u64);
                }
                Reply::Int(1)
            }
            "ttl" | "pttl" => {
                arity(1, Some(1))?;
                if !self.alive(&a[0], now) {
                    Reply::Int(-2)
                } else {
                    match self.expires.get(&a[0]) {
                        None => Reply::Int(-1),
                        Some(&at) if cmd == "ttl" => Reply::Int(((at - now) as f64 / 1000.0).round() as i64),
                        Some(&at) => Reply::Int((at - now) as i64),
                    }
                }
            }
            "persist" => {
                arity(1, Some(1))?;
                Reply::Int((self.alive(&a[0], now) && self.expires.remove(&a[0]).is_some()) as i64)
            }
            "rename" => {
                arity(2, Some(2))?;
                if !self.alive(&a[0], now) {
                    return Err(Reply::err("ERR no such key"));
                }
                let v = self.map.remove(&a[0]).unwrap();
                let ttl = self.expires.remove(&a[0]);
                self.remove(&a[1]);
                self.map.insert(a[1].clone(), v);
                if let Some(t) = ttl {
                    self.expires.insert(a[1].clone(), t);
                }
                Reply::ok()
            }

            // ------------------------------------------------------------ strings
            "get" => {
                arity(1, Some(1))?;
                typed!(&a[0], Str).map_or(Reply::Nil, |v| Reply::Bulk(v.clone()))
            }
            "set" => {
                arity(2, None)?;
                let (mut nx, mut xx, mut get, mut keepttl, mut px) = (false, false, false, false, None);
                let mut i = 2;
                while i < a.len() {
                    match String::from_utf8_lossy(&a[i]).to_ascii_lowercase().as_str() {
                        "nx" => nx = true,
                        "xx" => xx = true,
                        "get" => get = true,
                        "keepttl" => keepttl = true,
                        o @ ("ex" | "px" | "exat" | "pxat") => {
                            let n = int(a.get(i + 1).ok_or_else(|| Reply::err(SYNTAX))?)?;
                            if n <= 0 {
                                return Err(Reply::err(format!("ERR invalid expire time in '{cmd}' command")));
                            }
                            px = Some(match o {
                                "ex" => now + n as u64 * 1000,
                                "px" => now + n as u64,
                                "exat" => n as u64 * 1000,
                                _ => n as u64,
                            });
                            i += 1;
                        }
                        _ => return Err(Reply::err(SYNTAX)),
                    }
                    i += 1;
                }
                if nx && xx {
                    return Err(Reply::err(SYNTAX));
                }
                let old = if get { typed!(&a[0], Str).cloned() } else { None };
                let exists = self.alive(&a[0], now);
                if (nx && exists) || (xx && !exists) {
                    return Ok(if get { old.map_or(Reply::Nil, Reply::Bulk) } else { Reply::Nil });
                }
                let ttl = if keepttl { self.expires.get(&a[0]).copied() } else { None };
                self.set_str(&a[0], a[1].clone());
                if let Some(t) = px.or(ttl) {
                    self.expires.insert(a[0].clone(), t);
                }
                if get {
                    old.map_or(Reply::Nil, Reply::Bulk)
                } else {
                    Reply::ok()
                }
            }
            "setnx" => {
                arity(2, Some(2))?;
                if self.alive(&a[0], now) {
                    Reply::Int(0)
                } else {
                    self.set_str(&a[0], a[1].clone());
                    Reply::Int(1)
                }
            }
            "setex" | "psetex" => {
                arity(3, Some(3))?;
                let n = int(&a[1])?;
                if n <= 0 {
                    return Err(Reply::err(format!("ERR invalid expire time in '{cmd}' command")));
                }
                self.set_str(&a[0], a[2].clone());
                self.expires.insert(a[0].clone(), now + if cmd == "setex" { n as u64 * 1000 } else { n as u64 });
                Reply::ok()
            }
            "getset" => {
                arity(2, Some(2))?;
                let old = typed!(&a[0], Str).cloned();
                self.set_str(&a[0], a[1].clone());
                old.map_or(Reply::Nil, Reply::Bulk)
            }
            "getdel" => {
                arity(1, Some(1))?;
                let old = typed!(&a[0], Str).cloned();
                self.remove(&a[0]);
                old.map_or(Reply::Nil, Reply::Bulk)
            }
            "getex" => {
                arity(1, Some(3))?;
                let v = typed!(&a[0], Str).cloned();
                if v.is_some() {
                    match a.get(1).map(|f| String::from_utf8_lossy(f).to_ascii_lowercase()).as_deref() {
                        None => {}
                        Some("persist") => {
                            self.expires.remove(&a[0]);
                        }
                        Some(f @ ("ex" | "px")) => {
                            let n = int(a.get(2).ok_or_else(|| Reply::err(SYNTAX))?)?.max(1) as u64;
                            self.expires.insert(a[0].clone(), now + if f == "ex" { n * 1000 } else { n });
                        }
                        _ => return Err(Reply::err(SYNTAX)),
                    }
                }
                v.map_or(Reply::Nil, Reply::Bulk)
            }
            "mget" => {
                arity(1, None)?;
                Reply::Array(
                    a.iter()
                        .map(|k| match self.get(k, now) {
                            Some(Value::Str(v)) => Reply::Bulk(v.clone()),
                            _ => Reply::Nil,
                        })
                        .collect(),
                )
            }
            "mset" | "msetnx" => {
                if a.is_empty() || a.len() % 2 != 0 {
                    return Err(Reply::err(format!("ERR wrong number of arguments for '{cmd}' command")));
                }
                if cmd == "msetnx" && a.chunks(2).any(|kv| self.alive(&kv[0], now)) {
                    return Ok(Reply::Int(0));
                }
                for kv in a.chunks(2) {
                    self.set_str(&kv[0], kv[1].clone());
                }
                if cmd == "msetnx" {
                    Reply::Int(1)
                } else {
                    Reply::ok()
                }
            }
            "incr" | "decr" | "incrby" | "decrby" => {
                let by = match cmd {
                    "incr" => {
                        arity(1, Some(1))?;
                        1
                    }
                    "decr" => {
                        arity(1, Some(1))?;
                        -1
                    }
                    "incrby" => {
                        arity(2, Some(2))?;
                        int(&a[1])?
                    }
                    _ => {
                        arity(2, Some(2))?;
                        int(&a[1])?.checked_neg().ok_or_else(|| Reply::err(NOT_INT))?
                    }
                };
                let cur = match typed!(&a[0], Str) {
                    Some(v) => int(v)?,
                    None => 0,
                };
                let n = cur.checked_add(by).ok_or_else(|| Reply::err("ERR increment or decrement would overflow"))?;
                let ttl = self.expires.get(&a[0]).copied();
                self.set_str(&a[0], n.to_string().into_bytes());
                if let Some(t) = ttl {
                    self.expires.insert(a[0].clone(), t);
                }
                Reply::Int(n)
            }
            "incrbyfloat" => {
                arity(2, Some(2))?;
                let cur = match typed!(&a[0], Str) {
                    Some(v) => float(v)?,
                    None => 0.0,
                };
                let n = cur + float(&a[1])?;
                if !n.is_finite() {
                    return Err(Reply::err("ERR increment would produce NaN or Infinity"));
                }
                let s = fmt_float(n);
                let ttl = self.expires.get(&a[0]).copied();
                self.set_str(&a[0], s.clone().into_bytes());
                if let Some(t) = ttl {
                    self.expires.insert(a[0].clone(), t);
                }
                Reply::bulk(s)
            }
            "append" => {
                arity(2, Some(2))?;
                let v = typed_mut!(&a[0], Str, Vec::new());
                v.extend_from_slice(&a[1]);
                Reply::Int(v.len() as i64)
            }
            "strlen" => {
                arity(1, Some(1))?;
                Reply::Int(typed!(&a[0], Str).map_or(0, |v| v.len()) as i64)
            }
            "getrange" => {
                arity(3, Some(3))?;
                let (s, e) = (int(&a[1])?, int(&a[2])?);
                let v = typed!(&a[0], Str).cloned().unwrap_or_default();
                Reply::Bulk(index_range(s, e, v.len()).map(|(s, e)| v[s..=e].to_vec()).unwrap_or_default())
            }
            "setrange" => {
                arity(3, Some(3))?;
                let off = int(&a[1])?;
                if !(0..512 * 1024 * 1024).contains(&off) {
                    return Err(Reply::err("ERR offset is out of range"));
                }
                let v = typed_mut!(&a[0], Str, Vec::new());
                let end = off as usize + a[2].len();
                if v.len() < end {
                    v.resize(end, 0);
                }
                v[off as usize..end].copy_from_slice(&a[2]);
                Reply::Int(v.len() as i64)
            }

            // ------------------------------------------------------------ lists
            "lpush" | "rpush" | "lpushx" | "rpushx" => {
                arity(2, None)?;
                if cmd.ends_with('x') && typed!(&a[0], List).is_none() {
                    return Ok(Reply::Int(0));
                }
                let l = typed_mut!(&a[0], List, VecDeque::new());
                for v in &a[1..] {
                    if cmd.starts_with('l') {
                        l.push_front(v.clone());
                    } else {
                        l.push_back(v.clone());
                    }
                }
                Reply::Int(l.len() as i64)
            }
            "lpop" | "rpop" => {
                arity(1, Some(2))?;
                let count = a.get(1).map(|c| int(c)).transpose()?;
                if count.is_some_and(|c| c < 0) {
                    return Err(Reply::err("ERR value is out of range, must be positive"));
                }
                let Some(Value::List(l)) = self.get_mut(&a[0], now) else {
                    return match self.get(&a[0], now) {
                        Some(_) => Err(Reply::err(WRONGTYPE)),
                        None => Ok(if count.is_some() { Reply::NilArray } else { Reply::Nil }),
                    };
                };
                let n = count.unwrap_or(1) as usize;
                let mut out = Vec::new();
                for _ in 0..n {
                    match if cmd == "lpop" { l.pop_front() } else { l.pop_back() } {
                        Some(v) => out.push(v),
                        None => break,
                    }
                }
                self.cleanup(&a[0]);
                match count {
                    None => out.pop().map_or(Reply::Nil, Reply::Bulk),
                    Some(_) => bulks(out),
                }
            }
            "llen" => {
                arity(1, Some(1))?;
                Reply::Int(typed!(&a[0], List).map_or(0, |l| l.len()) as i64)
            }
            "lrange" => {
                arity(3, Some(3))?;
                let (s, e) = (int(&a[1])?, int(&a[2])?);
                match typed!(&a[0], List) {
                    None => Reply::Array(vec![]),
                    Some(l) => bulks(index_range(s, e, l.len()).map(|(s, e)| l.range(s..=e).cloned().collect::<Vec<_>>()).unwrap_or_default()),
                }
            }
            "lindex" => {
                arity(2, Some(2))?;
                let i = int(&a[1])?;
                match typed!(&a[0], List) {
                    None => Reply::Nil,
                    Some(l) => {
                        let idx = if i < 0 { l.len() as i64 + i } else { i };
                        l.get(idx.max(0) as usize).filter(|_| idx >= 0).map_or(Reply::Nil, |v| Reply::Bulk(v.clone()))
                    }
                }
            }
            "lset" => {
                arity(3, Some(3))?;
                let i = int(&a[1])?;
                let Some(Value::List(l)) = self.get_mut(&a[0], now) else { return Err(Reply::err("ERR no such key")) };
                let idx = if i < 0 { l.len() as i64 + i } else { i };
                match l.get_mut(idx.max(0) as usize).filter(|_| idx >= 0) {
                    Some(slot) => {
                        *slot = a[2].clone();
                        Reply::ok()
                    }
                    None => return Err(Reply::err("ERR index out of range")),
                }
            }
            "lrem" => {
                arity(3, Some(3))?;
                let count = int(&a[1])?;
                let Some(Value::List(l)) = self.get_mut(&a[0], now) else { return Ok(Reply::Int(0)) };
                let target = &a[2];
                let limit = if count == 0 { usize::MAX } else { count.unsigned_abs() as usize };
                let mut removed = 0;
                let items: Vec<Vec<u8>> = l.drain(..).collect();
                let mut kept: Vec<Vec<u8>> = Vec::with_capacity(items.len());
                if count >= 0 {
                    for v in items {
                        if removed < limit && &v == target {
                            removed += 1;
                        } else {
                            kept.push(v);
                        }
                    }
                } else {
                    for v in items.into_iter().rev() {
                        if removed < limit && &v == target {
                            removed += 1;
                        } else {
                            kept.push(v);
                        }
                    }
                    kept.reverse();
                }
                l.extend(kept);
                self.cleanup(&a[0]);
                Reply::Int(removed as i64)
            }
            "ltrim" => {
                arity(3, Some(3))?;
                let (s, e) = (int(&a[1])?, int(&a[2])?);
                if let Some(Value::List(l)) = self.get_mut(&a[0], now) {
                    match index_range(s, e, l.len()) {
                        Some((s, e)) => {
                            l.truncate(e + 1);
                            l.drain(..s);
                        }
                        None => l.clear(),
                    }
                }
                self.cleanup(&a[0]);
                Reply::ok()
            }

            // ------------------------------------------------------------ hashes
            "hset" | "hmset" => {
                if a.len() < 3 || a.len() % 2 == 0 {
                    return Err(Reply::err(format!("ERR wrong number of arguments for '{cmd}' command")));
                }
                let h = typed_mut!(&a[0], Hash, HashMap::new());
                let added = a[1..].chunks(2).filter(|kv| h.insert(kv[0].clone(), kv[1].clone()).is_none()).count();
                if cmd == "hmset" {
                    Reply::ok()
                } else {
                    Reply::Int(added as i64)
                }
            }
            "hsetnx" => {
                arity(3, Some(3))?;
                let h = typed_mut!(&a[0], Hash, HashMap::new());
                if h.contains_key(&a[1]) {
                    Reply::Int(0)
                } else {
                    h.insert(a[1].clone(), a[2].clone());
                    Reply::Int(1)
                }
            }
            "hget" => {
                arity(2, Some(2))?;
                typed!(&a[0], Hash).and_then(|h| h.get(&a[1])).map_or(Reply::Nil, |v| Reply::Bulk(v.clone()))
            }
            "hmget" => {
                arity(2, None)?;
                let h = typed!(&a[0], Hash);
                Reply::Array(a[1..].iter().map(|f| h.and_then(|h| h.get(f)).map_or(Reply::Nil, |v| Reply::Bulk(v.clone()))).collect())
            }
            "hdel" => {
                arity(2, None)?;
                let n = match self.get_mut(&a[0], now) {
                    Some(Value::Hash(h)) => a[1..].iter().filter(|f| h.remove(*f).is_some()).count(),
                    Some(_) => return Err(Reply::err(WRONGTYPE)),
                    None => 0,
                };
                self.cleanup(&a[0]);
                Reply::Int(n as i64)
            }
            "hgetall" | "hkeys" | "hvals" => {
                arity(1, Some(1))?;
                let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = typed!(&a[0], Hash).map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default();
                pairs.sort();
                bulks(match cmd {
                    "hkeys" => pairs.into_iter().map(|p| p.0).collect::<Vec<_>>(),
                    "hvals" => pairs.into_iter().map(|p| p.1).collect(),
                    _ => pairs.into_iter().flat_map(|(k, v)| [k, v]).collect(),
                })
            }
            "hlen" => {
                arity(1, Some(1))?;
                Reply::Int(typed!(&a[0], Hash).map_or(0, |h| h.len()) as i64)
            }
            "hexists" => {
                arity(2, Some(2))?;
                Reply::Int(typed!(&a[0], Hash).is_some_and(|h| h.contains_key(&a[1])) as i64)
            }
            "hincrby" => {
                arity(3, Some(3))?;
                let by = int(&a[2])?;
                let h = typed_mut!(&a[0], Hash, HashMap::new());
                let cur = match h.get(&a[1]) {
                    Some(v) => int(v).map_err(|_| Reply::err("ERR hash value is not an integer"))?,
                    None => 0,
                };
                let n = cur.checked_add(by).ok_or_else(|| Reply::err("ERR increment or decrement would overflow"))?;
                h.insert(a[1].clone(), n.to_string().into_bytes());
                Reply::Int(n)
            }
            "hincrbyfloat" => {
                arity(3, Some(3))?;
                let by = float(&a[2])?;
                let h = typed_mut!(&a[0], Hash, HashMap::new());
                let cur = match h.get(&a[1]) {
                    Some(v) => float(v)?,
                    None => 0.0,
                };
                let s = fmt_float(cur + by);
                h.insert(a[1].clone(), s.clone().into_bytes());
                Reply::bulk(s)
            }

            // ------------------------------------------------------------ sets
            "sadd" => {
                arity(2, None)?;
                let s = typed_mut!(&a[0], Set, HashSet::new());
                Reply::Int(a[1..].iter().filter(|m| s.insert((*m).clone())).count() as i64)
            }
            "srem" => {
                arity(2, None)?;
                let n = match self.get_mut(&a[0], now) {
                    Some(Value::Set(s)) => a[1..].iter().filter(|m| s.remove(*m)).count(),
                    Some(_) => return Err(Reply::err(WRONGTYPE)),
                    None => 0,
                };
                self.cleanup(&a[0]);
                Reply::Int(n as i64)
            }
            "smembers" => {
                arity(1, Some(1))?;
                let mut m: Vec<Vec<u8>> = typed!(&a[0], Set).map(|s| s.iter().cloned().collect()).unwrap_or_default();
                m.sort();
                bulks(m)
            }
            "sismember" => {
                arity(2, Some(2))?;
                Reply::Int(typed!(&a[0], Set).is_some_and(|s| s.contains(&a[1])) as i64)
            }
            "scard" => {
                arity(1, Some(1))?;
                Reply::Int(typed!(&a[0], Set).map_or(0, |s| s.len()) as i64)
            }
            "sinter" | "sunion" | "sdiff" => {
                arity(1, None)?;
                let mut sets = Vec::new();
                for k in a {
                    sets.push(typed!(k, Set).cloned().unwrap_or_default());
                }
                let mut acc = sets[0].clone();
                for s in &sets[1..] {
                    acc = match cmd {
                        "sinter" => acc.intersection(s).cloned().collect(),
                        "sunion" => acc.union(s).cloned().collect(),
                        _ => acc.difference(s).cloned().collect(),
                    };
                }
                let mut v: Vec<Vec<u8>> = acc.into_iter().collect();
                v.sort();
                bulks(v)
            }

            // ------------------------------------------------------------ sorted sets
            "zadd" => {
                arity(3, None)?;
                let (mut nx, mut xx, mut ch, mut incr) = (false, false, false, false);
                let mut i = 1;
                while i < a.len() {
                    match String::from_utf8_lossy(&a[i]).to_ascii_lowercase().as_str() {
                        "nx" => nx = true,
                        "xx" => xx = true,
                        "ch" => ch = true,
                        "incr" => incr = true,
                        _ => break,
                    }
                    i += 1;
                }
                let rest = &a[i..];
                if rest.is_empty() || rest.len() % 2 != 0 || (nx && xx) || (incr && rest.len() != 2) {
                    return Err(Reply::err(SYNTAX));
                }
                let pairs: Vec<(f64, Vec<u8>)> = rest.chunks(2).map(|p| Ok((float(&p[0])?, p[1].clone()))).collect::<Result<_, Reply>>()?;
                let z = typed_mut!(&a[0], ZSet, ZSet::default());
                let (mut added, mut changed) = (0, 0);
                let mut last = None;
                for (score, m) in pairs {
                    let cur = z.scores.get(&m).copied();
                    if (nx && cur.is_some()) || (xx && cur.is_none()) {
                        continue;
                    }
                    let new = if incr { cur.unwrap_or(0.0) + score } else { score };
                    if cur != Some(new) {
                        changed += 1;
                    }
                    if z.insert(m, new) {
                        added += 1;
                    }
                    last = Some(new);
                }
                self.cleanup(&a[0]);
                if incr {
                    last.map_or(Reply::Nil, |s| Reply::bulk(fmt_float(s)))
                } else {
                    Reply::Int(if ch { changed } else { added })
                }
            }
            "zincrby" => {
                arity(3, Some(3))?;
                let by = float(&a[1])?;
                let z = typed_mut!(&a[0], ZSet, ZSet::default());
                let n = z.scores.get(&a[2]).copied().unwrap_or(0.0) + by;
                z.insert(a[2].clone(), n);
                Reply::bulk(fmt_float(n))
            }
            "zscore" => {
                arity(2, Some(2))?;
                typed!(&a[0], ZSet).and_then(|z| z.scores.get(&a[1])).map_or(Reply::Nil, |s| Reply::bulk(fmt_float(*s)))
            }
            "zrem" => {
                arity(2, None)?;
                let n = match self.get_mut(&a[0], now) {
                    Some(Value::ZSet(z)) => a[1..].iter().filter(|m| z.remove(m)).count(),
                    Some(_) => return Err(Reply::err(WRONGTYPE)),
                    None => 0,
                };
                self.cleanup(&a[0]);
                Reply::Int(n as i64)
            }
            "zcard" => {
                arity(1, Some(1))?;
                Reply::Int(typed!(&a[0], ZSet).map_or(0, |z| z.len()) as i64)
            }
            "zrank" | "zrevrank" => {
                arity(2, Some(2))?;
                match typed!(&a[0], ZSet).and_then(|z| z.rank(&a[1]).map(|r| (r, z.len()))) {
                    Some((r, n)) => Reply::Int(if cmd == "zrank" { r } else { n - 1 - r } as i64),
                    None => Reply::Nil,
                }
            }
            "zrange" | "zrevrange" => {
                arity(3, Some(5))?;
                let (s, e) = (int(&a[1])?, int(&a[2])?);
                let flags: Vec<String> = a[3..].iter().map(|f| String::from_utf8_lossy(f).to_ascii_lowercase()).collect();
                if flags.iter().any(|f| f != "withscores" && f != "rev") {
                    return Err(Reply::err(SYNTAX));
                }
                let with = flags.iter().any(|f| f == "withscores");
                let rev = cmd == "zrevrange" || flags.iter().any(|f| f == "rev");
                let Some(z) = typed!(&a[0], ZSet) else { return Ok(Reply::Array(vec![])) };
                let mut items: Vec<&(Score, Vec<u8>)> = z.order.iter().collect();
                if rev {
                    items.reverse();
                }
                let picked = index_range(s, e, items.len()).map(|(s, e)| &items[s..=e]).unwrap_or(&[]);
                let mut out = Vec::new();
                for (sc, m) in picked {
                    out.push(Reply::Bulk(m.clone()));
                    if with {
                        out.push(Reply::bulk(fmt_float(sc.0)));
                    }
                }
                Reply::Array(out)
            }
            "zrangebyscore" | "zcount" => {
                arity(3, None)?;
                let bound = |b: &[u8]| -> Result<(f64, bool), Reply> {
                    match b.strip_prefix(b"(") {
                        Some(rest) => Ok((float(rest)?, true)),
                        None => Ok((float(b)?, false)),
                    }
                };
                let ((min, min_ex), (max, max_ex)) = (bound(&a[1])?, bound(&a[2])?);
                let (mut with, mut offset, mut limit) = (false, 0usize, usize::MAX);
                let mut i = 3;
                while i < a.len() {
                    match String::from_utf8_lossy(&a[i]).to_ascii_lowercase().as_str() {
                        "withscores" => with = true,
                        "limit" if i + 2 < a.len() => {
                            offset = int(&a[i + 1])?.max(0) as usize;
                            let l = int(&a[i + 2])?;
                            limit = if l < 0 { usize::MAX } else { l as usize };
                            i += 2;
                        }
                        _ => return Err(Reply::err(SYNTAX)),
                    }
                    i += 1;
                }
                let Some(z) = typed!(&a[0], ZSet) else { return Ok(if cmd == "zcount" { Reply::Int(0) } else { Reply::Array(vec![]) }) };
                let inside = |s: f64| (if min_ex { s > min } else { s >= min }) && (if max_ex { s < max } else { s <= max });
                let hits = z.order.iter().filter(|(s, _)| inside(s.0));
                if cmd == "zcount" {
                    return Ok(Reply::Int(hits.count() as i64));
                }
                let mut out = Vec::new();
                for (s, m) in hits.skip(offset).take(limit) {
                    out.push(Reply::Bulk(m.clone()));
                    if with {
                        out.push(Reply::bulk(fmt_float(s.0)));
                    }
                }
                Reply::Array(out)
            }
            _ => {
                let shown: Vec<String> = a.iter().take(3).map(|x| format!("'{}'", String::from_utf8_lossy(x))).collect();
                return Err(Reply::err(format!("ERR unknown command '{cmd}', with args beginning with: {}", shown.join(" "))));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut Db, line: &str, now: u64) -> Reply {
        db.execute(&crate::resp::split_args(line).unwrap(), now)
    }

    #[test]
    fn strings_and_counters() {
        let mut db = Db::new();
        assert_eq!(run(&mut db, "SET k hello", 0), Reply::ok());
        assert_eq!(run(&mut db, "APPEND k _world", 0), Reply::Int(11));
        assert_eq!(run(&mut db, "GETRANGE k 0 4", 0), Reply::bulk("hello"));
        assert_eq!(run(&mut db, "GETRANGE k -5 -1", 0), Reply::bulk("world"));
        assert_eq!(run(&mut db, "INCR n", 0), Reply::Int(1));
        assert_eq!(run(&mut db, "INCRBY n 41", 0), Reply::Int(42));
        assert_eq!(run(&mut db, "INCRBYFLOAT n 0.5", 0), Reply::bulk("42.5"));
        assert_eq!(run(&mut db, "INCR k", 0), Reply::err(NOT_INT));
        assert_eq!(run(&mut db, "SET k v NX", 0), Reply::Nil);
        assert_eq!(run(&mut db, "SET k v XX GET", 0), Reply::bulk("hello_world"));
        assert_eq!(run(&mut db, "MGET k nope", 0), Reply::Array(vec![Reply::bulk("v"), Reply::Nil]));
    }

    #[test]
    fn expiry_is_lazy_and_exact() {
        let mut db = Db::new();
        run(&mut db, "SET s v EX 10", 1_000);
        assert_eq!(run(&mut db, "TTL s", 1_000), Reply::Int(10));
        assert_eq!(run(&mut db, "PTTL s", 6_000), Reply::Int(5_000));
        assert_eq!(run(&mut db, "GET s", 10_999), Reply::bulk("v"));
        assert_eq!(run(&mut db, "GET s", 11_000), Reply::Nil);
        assert_eq!(run(&mut db, "TTL s", 11_000), Reply::Int(-2));
        run(&mut db, "SET p v", 0);
        assert_eq!(run(&mut db, "EXPIRE p 5 XX", 0), Reply::Int(0));
        assert_eq!(run(&mut db, "EXPIRE p 5", 0), Reply::Int(1));
        assert_eq!(run(&mut db, "PERSIST p", 0), Reply::Int(1));
        assert_eq!(run(&mut db, "TTL p", 0), Reply::Int(-1));
        run(&mut db, "SET q v PX 1", 0);
        assert_eq!(db.active_expire(5), 1);
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn lists() {
        let mut db = Db::new();
        assert_eq!(run(&mut db, "RPUSH l a b c d e", 0), Reply::Int(5));
        assert_eq!(run(&mut db, "LPUSH l z", 0), Reply::Int(6));
        assert_eq!(run(&mut db, "LRANGE l 0 -1", 0).pretty(), "1) \"z\"\n2) \"a\"\n3) \"b\"\n4) \"c\"\n5) \"d\"\n6) \"e\"");
        assert_eq!(run(&mut db, "LPOP l 2", 0), bulks(vec![b"z".to_vec(), b"a".to_vec()]));
        assert_eq!(run(&mut db, "LINDEX l -1", 0), Reply::bulk("e"));
        run(&mut db, "RPUSH l b b", 0);
        assert_eq!(run(&mut db, "LREM l -2 b", 0), Reply::Int(2));
        assert_eq!(run(&mut db, "LRANGE l 0 -1", 0), bulks(["b", "c", "d", "e"].map(|s| s.as_bytes().to_vec())));
        run(&mut db, "LTRIM l 1 2", 0);
        assert_eq!(run(&mut db, "LRANGE l 0 -1", 0), bulks(["c", "d"].map(|s| s.as_bytes().to_vec())));
        run(&mut db, "RPOP l 5", 0);
        assert_eq!(run(&mut db, "EXISTS l", 0), Reply::Int(0), "empty lists are deleted");
    }

    #[test]
    fn hashes_sets_and_types() {
        let mut db = Db::new();
        assert_eq!(run(&mut db, "HSET u name ada lang rust", 0), Reply::Int(2));
        assert_eq!(run(&mut db, "HINCRBY u visits 3", 0), Reply::Int(3));
        assert_eq!(run(&mut db, "HGET u name", 0), Reply::bulk("ada"));
        assert_eq!(run(&mut db, "HLEN u", 0), Reply::Int(3));
        assert_eq!(run(&mut db, "SADD a 1 2 3", 0), Reply::Int(3));
        run(&mut db, "SADD b 2 3 4", 0);
        assert_eq!(run(&mut db, "SINTER a b", 0), bulks(["2", "3"].map(|s| s.as_bytes().to_vec())));
        assert_eq!(run(&mut db, "SDIFF a b", 0), bulks(vec![b"1".to_vec()]));
        assert_eq!(run(&mut db, "LPUSH u x", 0), Reply::err(WRONGTYPE));
        assert_eq!(run(&mut db, "TYPE u", 0), Reply::Simple("hash".into()));
        assert_eq!(run(&mut db, "KEYS ?", 0), bulks(["a", "b", "u"].map(|s| s.as_bytes().to_vec())));
    }

    #[test]
    fn sorted_sets() {
        let mut db = Db::new();
        assert_eq!(run(&mut db, "ZADD board 100 alice 250 bob 175 carol", 0), Reply::Int(3));
        assert_eq!(run(&mut db, "ZINCRBY board 100 alice", 0), Reply::bulk("200"));
        assert_eq!(run(&mut db, "ZREVRANGE board 0 1 WITHSCORES", 0).pretty(), "1) \"bob\"\n2) \"250\"\n3) \"alice\"\n4) \"200\"");
        assert_eq!(run(&mut db, "ZRANK board carol", 0), Reply::Int(0));
        assert_eq!(run(&mut db, "ZRANGEBYSCORE board (175 +inf", 0), bulks(["alice", "bob"].map(|s| s.as_bytes().to_vec())));
        assert_eq!(run(&mut db, "ZCOUNT board -inf 200", 0), Reply::Int(2));
        assert_eq!(run(&mut db, "ZADD board XX CH 1 bob 5 dan", 0), Reply::Int(1));
        assert_eq!(run(&mut db, "ZSCORE board dan", 0), Reply::Nil);
    }

    #[test]
    fn errors() {
        let mut db = Db::new();
        assert!(matches!(run(&mut db, "GET", 0), Reply::Error(e) if e.contains("wrong number")));
        assert!(matches!(run(&mut db, "FOO bar", 0), Reply::Error(e) if e.starts_with("ERR unknown command 'foo'")));
        assert_eq!(run(&mut db, "SET k v EX 0", 0), Reply::err("ERR invalid expire time in 'set' command"));
    }
}
