import { useEffect, useRef, useState } from 'react'

const SUGGESTIONS = [
  'SET greeting "hello world"',
  'GET greeting',
  'SET session abc EX 30',
  'TTL session',
  'INCR visits',
  'RPUSH queue job1 job2 job3',
  'LPOP queue',
  'LRANGE queue 0 -1',
  'HSET user:1 name Ada lang Rust',
  'HGETALL user:1',
  'SADD tags rust redis wasm',
  'SMEMBERS tags',
  'ZADD scores 120 ada 98 grace 143 linus',
  'ZREVRANGE scores 0 -1 WITHSCORES',
  'KEYS *',
  'TYPE scores',
]

export default function Demo() {
  const [lines, setLines] = useState([{ kind: 'info', text: 'Loading redis.wasm…' }])
  const [input, setInput] = useState('')
  const [history, setHistory] = useState([])
  const [hpos, setHpos] = useState(-1)
  const [keys, setKeys] = useState(0)
  const wasm = useRef(null)
  const end = useRef(null)

  useEffect(() => {
    WebAssembly.instantiateStreaming(fetch('/redis.wasm'))
      .then(({ instance }) => { wasm.current = instance.exports; setLines([{ kind: 'info', text: 'Connected to an in-browser redis-clone. Type HELP-style commands, e.g. SET k v' }]) })
      .catch((e) => setLines([{ kind: 'err', text: `failed to load wasm: ${e}` }]))
  }, [])
  useEffect(() => { end.current?.scrollIntoView({ block: 'nearest' }) }, [lines])

  function run(cmd) {
    const e = wasm.current
    if (!e || !cmd.trim()) return
    const bytes = new TextEncoder().encode(cmd)
    const p = e.alloc(bytes.length)
    new Uint8Array(e.memory.buffer, p, bytes.length).set(bytes)
    const code = e.exec(p, bytes.length, Date.now())
    e.dealloc(p, bytes.length)
    const out = new TextDecoder().decode(new Uint8Array(e.memory.buffer, e.output_ptr(), e.output_len()))
    setLines((l) => [...l, { kind: 'cmd', text: cmd }, { kind: code ? 'err' : 'out', text: out }].slice(-200))
    setHistory((h) => [cmd, ...h].slice(0, 50))
    setHpos(-1)
    setKeys(e.dbsize())
  }

  function onKey(ev) {
    if (ev.key === 'Enter') { run(input); setInput('') }
    else if (ev.key === 'ArrowUp' && history.length) { const n = Math.min(hpos + 1, history.length - 1); setHpos(n); setInput(history[n]); ev.preventDefault() }
    else if (ev.key === 'ArrowDown') { const n = hpos - 1; setHpos(n); setInput(n >= 0 ? history[n] : '') }
  }

  return (
    <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 2fr) minmax(200px, 1fr)' }}>
      <div>
        <div className="log" style={{ minHeight: 300, maxHeight: 380 }} onClick={() => document.getElementById('redis-in')?.focus()}>
          {lines.map((l, i) => (
            <div key={i} style={{ color: l.kind === 'err' ? 'var(--err)' : l.kind === 'info' ? 'var(--muted)' : 'var(--text)' }}>
              {l.kind === 'cmd' ? <><span style={{ color: 'var(--accent)' }}>127.0.0.1:6379&gt; </span>{l.text}</> : l.text}
            </div>
          ))}
          <div ref={end} />
        </div>
        <div className="row">
          <input id="redis-in" value={input} onChange={(e) => setInput(e.target.value)} onKeyDown={onKey} placeholder="type a command and press Enter" style={{ fontFamily: 'var(--mono)' }} aria-label="redis command" />
          <button className="primary" onClick={() => { run(input); setInput('') }}>Send</button>
          <span className="pill">{keys} keys</span>
        </div>
      </div>
      <div>
        <h3>Try these</h3>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {SUGGESTIONS.map((s) => (
            <button key={s} onClick={() => run(s)} style={{ textAlign: 'left', fontFamily: 'var(--mono)', fontSize: 12 }}>{s}</button>
          ))}
        </div>
      </div>
    </div>
  )
}
