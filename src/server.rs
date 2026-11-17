//! TCP server: connections, transactions, blocking pops, pub/sub and AOF persistence.

use crate::db::{is_write, Db};
use crate::glob::glob_match;
use crate::resp::{parse_command, Parse, Reply};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Notify};

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[derive(Default)]
struct Subs {
    channels: HashSet<Vec<u8>>,
    patterns: HashSet<Vec<u8>>,
}

pub struct Shared {
    pub db: Mutex<Db>,
    changed: Notify,
    bus: broadcast::Sender<(Vec<u8>, Vec<u8>)>,
    subs: Mutex<HashMap<u64, Subs>>,
    aof: Option<Mutex<std::io::BufWriter<std::fs::File>>>,
    aof_path: Option<PathBuf>,
    started: Instant,
    next_id: AtomicU64,
    pub clients: AtomicUsize,
    pub commands: AtomicU64,
}

fn encode_cmd(args: &[Vec<u8>]) -> Vec<u8> {
    Reply::Array(args.iter().cloned().map(Reply::Bulk).collect()).to_bytes()
}

impl Shared {
    /// Opens the server state, replaying `aof` if it exists.
    pub fn open(aof: Option<PathBuf>) -> std::io::Result<Arc<Self>> {
        let mut db = Db::new();
        if let Some(p) = &aof {
            if let Ok(data) = std::fs::read(p) {
                let mut pos = 0;
                let now = now_ms();
                while pos < data.len() {
                    match parse_command(&data[pos..]) {
                        Parse::Command(args, used) => {
                            db.execute(&args, now);
                            pos += used;
                        }
                        _ => break, // torn tail from a crash: ignore the partial command
                    }
                }
                db.dirty = 0;
            }
        }
        let file = match &aof {
            Some(p) => Some(Mutex::new(std::io::BufWriter::new(std::fs::OpenOptions::new().create(true).append(true).open(p)?))),
            None => None,
        };
        let (bus, _) = broadcast::channel(4096);
        Ok(Arc::new(Shared {
            db: Mutex::new(db),
            changed: Notify::new(),
            bus,
            subs: Mutex::new(HashMap::new()),
            aof: file,
            aof_path: aof,
            started: Instant::now(),
            next_id: AtomicU64::new(1),
            clients: AtomicUsize::new(0),
            commands: AtomicU64::new(0),
        }))
    }

    fn log(&self, args: &[Vec<u8>], db: &Db) {
        let Some(aof) = &self.aof else { return };
        let mut w = aof.lock().unwrap();
        let _ = w.write_all(&encode_cmd(args));
        // relative TTLs are stored as absolute deadlines so a replay does not extend them
        if let Some(key) = args.get(1) {
            if let Some(at) = db.expire_at(key) {
                let _ = w.write_all(&encode_cmd(&[b"PEXPIREAT".to_vec(), key.clone(), at.to_string().into_bytes()]));
            }
        }
    }

    pub fn flush_aof(&self) {
        if let Some(aof) = &self.aof {
            let mut w = aof.lock().unwrap();
            let _ = w.flush();
            let _ = w.get_ref().sync_data();
        }
    }

    /// Rewrites the AOF from the live dataset (BGREWRITEAOF).
    fn rewrite_aof(&self) -> std::io::Result<()> {
        let (Some(aof), Some(path)) = (&self.aof, &self.aof_path) else { return Ok(()) };
        let db = self.db.lock().unwrap();
        let mut w = aof.lock().unwrap();
        w.flush()?;
        let tmp = path.with_extension("rewrite");
        let mut f = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
        for cmd in db.dump(now_ms()) {
            f.write_all(&encode_cmd(&cmd))?;
        }
        f.flush()?;
        f.get_ref().sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)?;
        *w = std::io::BufWriter::new(std::fs::OpenOptions::new().append(true).open(path)?);
        Ok(())
    }

    /// Runs a data command against the keyspace, logging writes.
    pub fn exec(&self, args: &[Vec<u8>]) -> Reply {
        self.commands.fetch_add(1, Ordering::Relaxed);
        let cmd = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
        let mut db = self.db.lock().unwrap();
        let before = db.dirty;
        let r = db.execute(args, now_ms());
        if is_write(&cmd) && db.dirty != before && !matches!(r, Reply::Error(_)) {
            self.log(args, &db);
            drop(db);
            self.changed.notify_waiters();
        }
        r
    }

    fn publish(&self, channel: &[u8], msg: &[u8]) -> i64 {
        let subs = self.subs.lock().unwrap();
        let n: usize = subs.values().map(|s| s.channels.contains(channel) as usize + s.patterns.iter().filter(|p| glob_match(p, channel)).count()).sum();
        let _ = self.bus.send((channel.to_vec(), msg.to_vec()));
        n as i64
    }

    fn info(&self) -> String {
        let db = self.db.lock().unwrap();
        format!(
            "# Server\r\nredis_version:7.0.0-rs\r\nredis_mode:standalone\r\nuptime_in_seconds:{}\r\n\r\n# Clients\r\nconnected_clients:{}\r\n\r\n# Persistence\r\naof_enabled:{}\r\nchanges_since_start:{}\r\n\r\n# Stats\r\ntotal_commands_processed:{}\r\nexpired_keys:{}\r\n\r\n# Keyspace\r\ndb0:keys={},expires={}\r\n",
            self.started.elapsed().as_secs(),
            self.clients.load(Ordering::Relaxed),
            self.aof.is_some() as u8,
            db.dirty,
            self.commands.load(Ordering::Relaxed),
            db.expired_keys,
            db.len(),
            db.expires_count()
        )
    }
}

struct Conn {
    id: u64,
    queued: Option<Vec<Vec<Vec<u8>>>>,
    subscribed: bool,
}

pub async fn serve(shared: Arc<Shared>, listener: TcpListener) -> std::io::Result<()> {
    // background housekeeping: active expiry every 100 ms, AOF fsync every second
    let s = shared.clone();
    tokio::spawn(async move {
        let mut tick = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            while s.db.lock().unwrap().active_expire(now_ms()) >= 5 {}
            tick += 1;
            if tick % 10 == 0 {
                s.flush_aof();
            }
        }
    });
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true).ok();
        let s = shared.clone();
        tokio::spawn(async move {
            s.clients.fetch_add(1, Ordering::Relaxed);
            let _ = connection(s.clone(), stream).await;
            s.clients.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

async fn connection(s: Arc<Shared>, mut stream: TcpStream) -> std::io::Result<()> {
    let mut conn = Conn { id: s.next_id.fetch_add(1, Ordering::Relaxed), queued: None, subscribed: false };
    let mut rx = s.bus.subscribe();
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = vec![0u8; 16 * 1024];
    let result = loop {
        // process every complete command already buffered
        let mut out = Vec::new();
        let mut close = false;
        loop {
            match parse_command(&buf) {
                Parse::Command(args, used) => {
                    buf.drain(..used);
                    if args.is_empty() {
                        continue;
                    }
                    let (reply, quit) = command(&s, &mut conn, args).await;
                    for r in reply {
                        r.encode(&mut out);
                    }
                    if quit {
                        close = true;
                        break;
                    }
                }
                Parse::Incomplete => break,
                Parse::Invalid(e) => {
                    Reply::err(format!("ERR Protocol error: {e}")).encode(&mut out);
                    close = true;
                    break;
                }
            }
        }
        if !out.is_empty() {
            stream.write_all(&out).await?;
        }
        if close {
            break Ok(());
        }
        tokio::select! {
            n = stream.read(&mut chunk) => {
                let n = n?;
                if n == 0 { break Ok(()); }
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() > 512 * 1024 * 1024 { break Ok(()); }
            }
            msg = rx.recv(), if conn.subscribed => {
                let Ok((channel, payload)) = msg else { continue };
                let mut out = Vec::new();
                let subs = s.subs.lock().unwrap().get(&conn.id).map(|x| (x.channels.contains(&channel), x.patterns.iter().filter(|p| glob_match(p, &channel)).cloned().collect::<Vec<_>>()));
                if let Some((direct, patterns)) = subs {
                    if direct {
                        Reply::Array(vec![Reply::bulk("message"), Reply::Bulk(channel.clone()), Reply::Bulk(payload.clone())]).encode(&mut out);
                    }
                    for p in patterns {
                        Reply::Array(vec![Reply::bulk("pmessage"), Reply::Bulk(p), Reply::Bulk(channel.clone()), Reply::Bulk(payload.clone())]).encode(&mut out);
                    }
                }
                if !out.is_empty() { stream.write_all(&out).await?; }
            }
        }
    };
    s.subs.lock().unwrap().remove(&conn.id);
    result
}

/// Handles connection-level commands; everything else goes to the keyspace.
async fn command(s: &Arc<Shared>, conn: &mut Conn, args: Vec<Vec<u8>>) -> (Vec<Reply>, bool) {
    let cmd = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let one = |r: Reply| (vec![r], false);

    if let Some(q) = &mut conn.queued {
        match cmd.as_str() {
            "exec" | "discard" | "multi" => {}
            _ => {
                q.push(args);
                return one(Reply::Simple("QUEUED".into()));
            }
        }
    }
    if conn.subscribed && !matches!(cmd.as_str(), "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" | "ping" | "quit") {
        return one(Reply::err(format!("ERR Can't execute '{cmd}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context")));
    }

    match cmd.as_str() {
        "quit" => (vec![Reply::ok()], true),
        "multi" => {
            if conn.queued.is_some() {
                return one(Reply::err("ERR MULTI calls can not be nested"));
            }
            conn.queued = Some(Vec::new());
            one(Reply::ok())
        }
        "discard" => match conn.queued.take() {
            Some(_) => one(Reply::ok()),
            None => one(Reply::err("ERR DISCARD without MULTI")),
        },
        "exec" => match conn.queued.take() {
            None => one(Reply::err("ERR EXEC without MULTI")),
            Some(cmds) => {
                // hold the lock for the whole batch so it is atomic
                let mut replies = Vec::new();
                let mut changed = false;
                {
                    let mut db = s.db.lock().unwrap();
                    for c in &cmds {
                        s.commands.fetch_add(1, Ordering::Relaxed);
                        let name = String::from_utf8_lossy(&c[0]).to_ascii_lowercase();
                        let before = db.dirty;
                        let r = db.execute(c, now_ms());
                        if is_write(&name) && db.dirty != before && !matches!(r, Reply::Error(_)) {
                            s.log(c, &db);
                            changed = true;
                        }
                        replies.push(r);
                    }
                }
                if changed {
                    s.changed.notify_waiters();
                }
                one(Reply::Array(replies))
            }
        },
        "publish" if args.len() == 3 => one(Reply::Int(s.publish(&args[1], &args[2]))),
        "subscribe" | "psubscribe" | "unsubscribe" | "punsubscribe" => {
            let pattern = cmd.starts_with('p');
            let mut subs = s.subs.lock().unwrap();
            let entry = subs.entry(conn.id).or_default();
            let mut replies = Vec::new();
            let targets: Vec<Vec<u8>> = if args.len() > 1 {
                args[1..].to_vec()
            } else if pattern {
                entry.patterns.iter().cloned().collect()
            } else {
                entry.channels.iter().cloned().collect()
            };
            for t in targets {
                let set = if pattern { &mut entry.patterns } else { &mut entry.channels };
                if cmd.contains("unsub") {
                    set.remove(&t);
                } else {
                    set.insert(t.clone());
                }
                let total = entry.channels.len() + entry.patterns.len();
                replies.push(Reply::Array(vec![Reply::bulk(cmd.clone()), Reply::Bulk(t), Reply::Int(total as i64)]));
            }
            conn.subscribed = entry.channels.len() + entry.patterns.len() > 0;
            if replies.is_empty() {
                replies.push(Reply::Array(vec![Reply::bulk(cmd.clone()), Reply::Nil, Reply::Int(0)]));
            }
            (replies, false)
        }
        "ping" if conn.subscribed => one(Reply::Array(vec![Reply::bulk("pong"), Reply::Bulk(args.get(1).cloned().unwrap_or_default())])),
        "blpop" | "brpop" if args.len() >= 3 => {
            let timeout: f64 = String::from_utf8_lossy(args.last().unwrap()).parse().unwrap_or(-1.0);
            if timeout < 0.0 {
                return one(Reply::err("ERR timeout is not a float or out of range"));
            }
            let keys = &args[1..args.len() - 1];
            let deadline = (timeout > 0.0).then(|| tokio::time::Instant::now() + Duration::from_secs_f64(timeout));
            let pop = if cmd == "blpop" { b"LPOP".to_vec() } else { b"RPOP".to_vec() };
            loop {
                let notified = s.changed.notified();
                for k in keys {
                    // pop without notify_waiters: a removal must not wake ourselves (busy loop)
                    let args = [pop.clone(), k.clone()];
                    let mut db = s.db.lock().unwrap();
                    if let Reply::Bulk(v) = db.execute(&args, now_ms()) {
                        s.commands.fetch_add(1, Ordering::Relaxed);
                        s.log(&args, &db);
                        return one(Reply::Array(vec![Reply::Bulk(k.clone()), Reply::Bulk(v)]));
                    }
                }
                match deadline {
                    Some(d) => {
                        if tokio::time::timeout_at(d, notified).await.is_err() {
                            return one(Reply::NilArray);
                        }
                    }
                    None => notified.await,
                }
            }
        }
        "info" => one(Reply::bulk(s.info())),
        "bgrewriteaof" => match s.rewrite_aof() {
            Ok(()) => one(Reply::Simple("Background append only file rewriting started".into())),
            Err(e) => one(Reply::err(format!("ERR {e}"))),
        },
        "save" | "bgsave" => {
            s.flush_aof();
            one(Reply::ok())
        }
        "select" => match args.get(1).map(|a| a.as_slice()) {
            Some(b"0") => one(Reply::ok()),
            _ => one(Reply::err("ERR DB index is out of range")),
        },
        "command" | "config" => one(Reply::Array(vec![])), // enough for redis-cli's startup probes
        "client" | "hello" => one(Reply::ok()),
        _ => one(s.exec(&args)),
    }
}

/// Reads an AOF and returns how many commands it contains (for diagnostics).
pub fn aof_command_count(path: &std::path::Path) -> usize {
    let Ok(f) = std::fs::File::open(path) else { return 0 };
    std::io::BufReader::new(f).lines().map_while(Result::ok).filter(|l| l.starts_with('*')).count()
}
