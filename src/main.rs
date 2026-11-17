use redis_clone::server::{aof_command_count, Shared};
use std::path::PathBuf;

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        println!("redis-clone [--port 6379] [--bind 127.0.0.1] [--appendonly file.aof]");
        return;
    }
    let port = flag(&args, "--port").unwrap_or_else(|| "6379".into());
    let bind = flag(&args, "--bind").unwrap_or_else(|| "127.0.0.1".into());
    let aof = flag(&args, "--appendonly").map(PathBuf::from);
    if let Some(p) = &aof {
        println!("replaying {} command(s) from {}", aof_command_count(p), p.display());
    }
    let shared = match Shared::open(aof.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };
    let listener = match tokio::net::TcpListener::bind(format!("{bind}:{port}")).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: cannot bind {bind}:{port}: {e}");
            std::process::exit(1);
        }
    };
    println!("redis-clone ready on {bind}:{port} ({} keys loaded, AOF {})", shared.db.lock().unwrap().len(), if aof.is_some() { "on" } else { "off" });
    let s = shared.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        s.flush_aof();
        std::process::exit(0);
    });
    if let Err(e) = redis_clone::server::serve(shared, listener).await {
        eprintln!("error: {e}");
    }
}
