export default {
  name: 'redis-clone',
  stack: 'Rust · tokio · RESP · WebAssembly',
  tagline: 'A Redis-compatible in-memory data store. Point redis-cli at it — or use the terminal below, which runs the real Rust engine compiled to WebAssembly.',
  repo: 'https://github.com/devKen143/redis-clone',
  description:
    'Speaks RESP2 over TCP, so redis-cli and standard client libraries work unchanged. Strings, lists, hashes, sets and sorted sets with Redis-accurate semantics and error messages, key expiry (lazy plus active sampling), MULTI/EXEC transactions, BLPOP, pub/sub with patterns and an append-only file that survives crashes.',
  features: [
    '70+ commands across strings, lists, hashes, sets and sorted sets',
    'SET with EX/PX/EXAT/NX/XX/GET/KEEPTTL; EXPIRE NX/XX/GT/LT; TTL/PTTL/PERSIST',
    'Sorted sets on a BTreeSet + HashMap: ZRANGE … WITHSCORES REV, ZRANGEBYSCORE with (exclusive bounds and LIMIT',
    'Pipelining, inline commands, MULTI/EXEC/DISCARD, BLPOP/BRPOP with timeouts',
    'SUBSCRIBE / PSUBSCRIBE / PUBLISH with glob patterns',
    'AOF persistence: TTLs stored as absolute deadlines, fsync every second, torn-tail tolerant replay, BGREWRITEAOF compaction',
    'KEYS / SCAN with Redis glob syntax (*, ?, [a-z], [^x], \\x)',
  ],
  demoNote: 'This keyspace lives in your browser tab, inside the same db.rs used by the server. Try the commands on the right, or type your own.',
  architecture: [
    { title: 'resp.rs', text: 'Incremental RESP parser (returns Incomplete until a full command is buffered) plus inline commands with quoting, and a redis-cli style pretty printer.' },
    { title: 'db.rs', text: 'One synchronous execute(args, now) for every data command. Time is a parameter, which makes expiry deterministic in tests and lets it run in WebAssembly.' },
    { title: 'server.rs', text: 'tokio connection loop with select! between the socket and the pub/sub bus. Transactions run the whole queue under one lock.' },
    { title: 'Persistence', text: 'Each successful write is appended in RESP. Commands that set TTLs are followed by PEXPIREAT so a replay never extends a deadline.' },
  ],
  usage: `git clone https://github.com/devKen143/redis-clone && cd redis-clone
cargo run --release -- --port 6379 --appendonly appendonly.aof

redis-cli -p 6379
127.0.0.1:6379> SET user:1 ada EX 60
127.0.0.1:6379> ZADD leaderboard 120 ada 98 grace
127.0.0.1:6379> ZREVRANGE leaderboard 0 -1 WITHSCORES`,
}
