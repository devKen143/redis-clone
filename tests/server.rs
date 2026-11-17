use redis_clone::resp::{parse_command, Reply};
use redis_clone::server::Shared;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Minimal RESP client: sends inline commands, reads raw reply text.
struct Cli(TcpStream);

impl Cli {
    async fn connect(addr: &str) -> Self {
        Cli(TcpStream::connect(addr).await.unwrap())
    }
    async fn send(&mut self, args: &[&str]) {
        let r = Reply::Array(args.iter().map(|a| Reply::bulk(a.as_bytes().to_vec())).collect());
        self.0.write_all(&r.to_bytes()).await.unwrap();
    }
    async fn read(&mut self) -> String {
        let mut buf = vec![0u8; 65536];
        let n = tokio::time::timeout(Duration::from_secs(3), self.0.read(&mut buf)).await.expect("reply timeout").unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }
    async fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args).await;
        self.read().await
    }
}

async fn start(aof: Option<std::path::PathBuf>) -> String {
    let shared = Shared::open(aof).unwrap();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(redis_clone::server::serve(shared, l));
    addr
}

#[tokio::test]
async fn basic_commands_and_pipelining() {
    let addr = start(None).await;
    let mut c = Cli::connect(&addr).await;
    assert_eq!(c.cmd(&["PING"]).await, "+PONG\r\n");
    assert_eq!(c.cmd(&["SET", "a", "1"]).await, "+OK\r\n");
    // pipelined: three commands in one write
    c.0.write_all(b"INCR a\r\nINCR a\r\nGET a\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(c.read().await, ":2\r\n:3\r\n$1\r\n3\r\n");
}

#[tokio::test]
async fn transactions_are_atomic_batches() {
    let addr = start(None).await;
    let mut c = Cli::connect(&addr).await;
    assert_eq!(c.cmd(&["MULTI"]).await, "+OK\r\n");
    assert_eq!(c.cmd(&["SET", "x", "10"]).await, "+QUEUED\r\n");
    assert_eq!(c.cmd(&["INCRBY", "x", "5"]).await, "+QUEUED\r\n");
    assert_eq!(c.cmd(&["EXEC"]).await, "*2\r\n+OK\r\n:15\r\n");
    assert_eq!(c.cmd(&["MULTI"]).await, "+OK\r\n");
    c.cmd(&["SET", "x", "99"]).await;
    assert_eq!(c.cmd(&["DISCARD"]).await, "+OK\r\n");
    assert_eq!(c.cmd(&["GET", "x"]).await, "$2\r\n15\r\n");
}

#[tokio::test]
async fn pubsub_with_patterns() {
    let addr = start(None).await;
    let mut sub = Cli::connect(&addr).await;
    assert!(sub.cmd(&["SUBSCRIBE", "news"]).await.contains("subscribe"));
    assert!(sub.cmd(&["PSUBSCRIBE", "news.*"]).await.contains("psubscribe"));
    let mut publisher = Cli::connect(&addr).await;
    assert_eq!(publisher.cmd(&["PUBLISH", "news", "hello"]).await, ":1\r\n");
    assert_eq!(sub.read().await, "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n");
    assert_eq!(publisher.cmd(&["PUBLISH", "news.tech", "rust"]).await, ":1\r\n");
    assert!(sub.read().await.contains("pmessage"));
    assert!(sub.cmd(&["GET", "x"]).await.starts_with("-ERR Can't execute"));
}

#[tokio::test]
async fn blpop_waits_for_push() {
    let addr = start(None).await;
    let mut waiter = Cli::connect(&addr).await;
    waiter.send(&["BLPOP", "jobs", "5"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    Cli::connect(&addr).await.cmd(&["RPUSH", "jobs", "job-1"]).await;
    assert_eq!(waiter.read().await, "*2\r\n$4\r\njobs\r\n$5\r\njob-1\r\n");
    assert_eq!(waiter.cmd(&["BLPOP", "jobs", "0.1"]).await, "*-1\r\n");
}

#[tokio::test]
async fn aof_survives_restart_and_rewrite() {
    let dir = std::env::temp_dir().join(format!("redis-clone-aof-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let aof = dir.join("appendonly.aof");
    let _ = std::fs::remove_file(&aof);
    {
        let shared = Shared::open(Some(aof.clone())).unwrap();
        for (i, cmd) in [["SET", "k", "v"], ["RPUSH", "l", "a"], ["RPUSH", "l", "b"], ["SET", "tmp", "x"], ["DEL", "tmp", "_"]].iter().enumerate() {
            let args: Vec<Vec<u8>> = cmd.iter().map(|s| s.as_bytes().to_vec()).collect();
            let _ = (i, shared.exec(&args));
        }
        shared.exec(&[b"SETEX".to_vec(), b"session".to_vec(), b"100".to_vec(), b"abc".to_vec()]);
        shared.flush_aof();
    }
    let shared = Shared::open(Some(aof.clone())).unwrap();
    let mut db = shared.db.lock().unwrap();
    assert_eq!(db.execute(&[b"LRANGE".to_vec(), b"l".to_vec(), b"0".to_vec(), b"-1".to_vec()], 0).pretty(), "1) \"a\"\n2) \"b\"");
    assert_eq!(db.execute(&[b"EXISTS".to_vec(), b"tmp".to_vec()], 0), Reply::Int(0));
    assert!(db.expire_at(b"session").is_some(), "TTL must survive replay");
    drop(db);
    // torn tail: a half-written command is ignored on replay
    std::fs::OpenOptions::new().append(true).open(&aof).unwrap().write_all(b"*3\r\n$3\r\nSET\r\n$1").unwrap();
    assert!(matches!(parse_command(b"*3\r\n$3\r\nSET\r\n$1"), redis_clone::resp::Parse::Incomplete));
    let shared = Shared::open(Some(aof.clone())).unwrap();
    assert_eq!(shared.db.lock().unwrap().len(), 3);
    std::fs::remove_dir_all(dir).ok();
}

use std::io::Write as _;
