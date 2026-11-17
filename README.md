# redis-clone

A Redis-compatible in-memory data store in Rust. It speaks RESP2, so `redis-cli` and ordinary client libraries work unchanged. The command engine also compiles to WebAssembly for an in-browser terminal (see the repo homepage).

## Features
- **Strings**: GET SET(EX/PX/EXAT/PXAT/NX/XX/GET/KEEPTTL) SETNX SETEX PSETEX GETSET GETDEL GETEX MGET MSET MSETNX INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND STRLEN GETRANGE SETRANGE
- **Lists**: LPUSH RPUSH LPUSHX RPUSHX LPOP RPOP (with count) LLEN LRANGE LINDEX LSET LREM LTRIM BLPOP BRPOP
- **Hashes**: HSET HMSET HSETNX HGET HMGET HDEL HGETALL HKEYS HVALS HLEN HEXISTS HINCRBY HINCRBYFLOAT
- **Sets**: SADD SREM SMEMBERS SISMEMBER SCARD SINTER SUNION SDIFF
- **Sorted sets**: ZADD(NX/XX/CH/INCR) ZINCRBY ZSCORE ZREM ZCARD ZRANK ZREVRANK ZRANGE(WITHSCORES/REV) ZREVRANGE ZRANGEBYSCORE(LIMIT) ZCOUNT
- **Keys**: DEL UNLINK EXISTS TYPE KEYS SCAN EXPIRE(NX/XX/GT/LT) PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST RENAME DBSIZE FLUSHDB FLUSHALL
- **Server**: PING ECHO INFO MULTI EXEC DISCARD SUBSCRIBE PSUBSCRIBE UNSUBSCRIBE PUNSUBSCRIBE PUBLISH BGREWRITEAOF SAVE SELECT QUIT
- Lazy + active expiry, pipelining, inline commands
- AOF persistence with absolute TTLs, 1 s fsync, torn-tail tolerant replay, rewrite/compaction

## Usage
```bash
cargo run --release -- --port 6379 --appendonly appendonly.aof
redis-cli -p 6379
```

## Tests
`cargo test` — data-type semantics, expiry, RESP parsing, transactions, pub/sub, BLPOP and AOF recovery over real TCP connections.

## WebAssembly
`./build-wasm.sh` compiles `db.rs` + `resp.rs` (no networking) into `web/public/redis.wasm`.

## License
MIT
