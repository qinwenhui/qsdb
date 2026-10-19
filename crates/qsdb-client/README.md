# qsdb-client

Thin Rust client for [QSDB](https://github.com/qinwenhui/qsdb) — a
high-performance columnar analytics database. Arrow-native, Parquet-backed,
one static binary, zero external services.

Hand-rolled HTTP/1.1 on top of `serde`: **zero network dependencies** unless
you opt into Arrow IPC (`features = ["ipc"]`).

## Install

```bash
cargo add qsdb-client
```

## Quick start

```rust
use qsdb_client::{Auth, Client};

let c = Client::connect("http://127.0.0.1:9430", Auth::Basic {
    user: "admin".into(),
    password: "…".into(),
})?;

// Reads: columnar batches by default, zero extra dependencies
let rs = c.query("SELECT cat, count(*) FROM t GROUP BY cat")?;
let t = rs.into_bulk()?;
for row in t.rows() {
    let (cat, n) = (row.get(0)?, row.get(1)?);
    println!("{cat} {n}");
}

// Bulk loading: never row-by-row INSERT (measured 37x slower)
c.append("t", &[
    ("id", vec![Some(1i64), Some(2)].into()),
    ("cat", vec![Some("x".to_string()), None].into()),
])?;

// Large results: streaming with constant memory
let mut s = c.query_stream("SELECT * FROM big", &Default::default())?;
while let Some(chunk) = s.next_chunk()? { /* … */ }
```

## What you get

- **Connection reuse** — keep-alive with transparent reconnect (only when a
  request was provably not sent; `append` is never transparently retried)
- **Parameterized queries** — `$1` / `?` placeholders; values never enter the
  SQL text (bound server-side)
- **Sessions** — login, per-database grants awareness, automatic re-login on
  `SESSION_REVOKED`
- **Cancellation** — cooperative server-side cancel plus client-side abort
- **Streaming reads** — constant memory over arbitrarily large results
- **A runtime error-code catalog** — don't hardcode error handling; fetch
  `/api/v1/error-codes`

## Starting a server

```bash
cargo install --git https://github.com/qinwenhui/qsdb qsdb-cli
qsdb server --path /data/qsdb --addr 0.0.0.0:9430
```

## Documentation

- [Repository & docs](https://github.com/qinwenhui/qsdb)
- [Client guide](https://github.com/qinwenhui/qsdb/blob/main/docs/客户端与嵌入方指南.md)
- [HTTP API contract](https://github.com/qinwenhui/qsdb/blob/main/docs/对接契约.md)
- [Error codes](https://github.com/qinwenhui/qsdb/blob/main/docs/error-codes.md)

## License

MIT
