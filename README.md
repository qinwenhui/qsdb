# QSDB

**A standalone, single-node, embeddable columnar analytical database.
Arrow-native. Parquet-backed. Zero external services.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![CI](https://github.com/qinwenhui/qsdb/actions/workflows/ci.yml/badge.svg)](https://github.com/qinwenhui/qsdb/actions/workflows/ci.yml)

QSDB does three things — **read/write speed, resource footprint, and concurrency** —
and deliberately nothing else. No cluster, no external metastore, no JVM:
one static binary, usable as an embeddable Rust library or deployed as an HTTP server.

---

## Highlights

- **Columnar, immutable files** — append + partition-level atomic overwrite; no row-level UPDATE/DELETE (analytics don't need it, and immutability is what makes the rest cheap).
- **Arrow end-to-end** — `RecordBatch` with zero-copy handoff in embedded mode; Arrow IPC on the wire when you want it.
- **Parquet storage** — column statistics, partition- and row-group-level pruning baked into the scan path.
- **Vectorized execution engine, written from scratch** — hash join (zero-copy string keys), window functions over DECIMAL, sort/aggregate spill, and a dense direct-index path for high-cardinality `count(DISTINCT)`.
- **A wide SQL surface** — Spark-dialect friendly: `stack` / `PIVOT`, CTEs (including dead-CTE elimination), `least`/`greatest`, non-ANSI double-quoted strings, multi-value `IN` pushdown, and parameterized queries (`$1` / `?` — values never enter the SQL text).
- **A real server, not a toy** — streaming results with constant memory, cooperative cancellation (sub-100ms end-to-end), sessions, multi-database mounts, per-database grants, hot-reconfigurable guards (result rows/bytes, timeouts, memory), and a machine-readable error-code catalog your clients can fetch at runtime.
- **Metadata with zero moving parts** — local files + in-memory cache + partition-level file locks. No HMS, no MySQL, no ZooKeeper.
- **Minimal dependencies by design** — arrow-rs, parquet-rs, sqlparser-rs, tokio/axum. That's most of the list.

## Quick start

```bash
# build
cargo build --release -p qsdb-cli

# run a server (HTTP API on :9430)
./target/release/qsdb server --path /data/qsdb --addr 0.0.0.0:9430

# or talk to a database directly
./target/release/qsdb shell  --path /data/qsdb
./target/release/qsdb query  --path /data/qsdb "SELECT count(*) FROM t"
```

```sql
CREATE TABLE events (uid VARCHAR, amount DECIMAL(38,8), ts TIMESTAMP)
  PARTITIONED BY (dt DATE);

INSERT INTO events VALUES (...);

SELECT uid, sum(amount) AS total
FROM events
WHERE dt = '2026-10-01'
GROUP BY uid ORDER BY total DESC LIMIT 100;
```

## Rust client: `qsdb-client`

```bash
cargo add qsdb-client
```

```rust
use qsdb_client::{Auth, Client};

let c = Client::connect("http://127.0.0.1:9430", Auth::Basic {
    user: "admin".into(),
    password: "…".into(),
})?;

// reads: columnar batches by default, zero extra dependencies
let rs = c.query("SELECT cat, count(*) FROM t GROUP BY cat")?;
let t = rs.into_bulk()?;
for row in t.rows() {
    let (cat, n) = (row.get(0)?, row.get(1)?);
    println!("{cat} {n}");
}

// bulk loading: never row-by-row INSERT (measured 37x slower)
c.append("t", &[
    ("id", vec![Some(1i64), Some(2)].into()),
    ("cat", vec![Some("x".to_string()), None].into()),
])?;

// large results: streaming with constant memory
let mut s = c.query_stream("SELECT * FROM big", &Default::default())?;
while let Some(chunk) = s.next_chunk()? { /* … */ }
```

The client is hand-rolled HTTP/1.1 on top of `serde` — **zero network
dependencies** unless you opt into Arrow IPC (`features = ["ipc"]`).
See [`docs/客户端与嵌入方指南.md`](docs/客户端与嵌入方指南.md) for the
embedded-vs-server decision table.

## Embedded API

```rust
use qsdb::{Qsdb, QsdbOptions};

let db = Qsdb::open("/data/qsdb", QsdbOptions::default())?;
db.append("events", &[batch])?;                 // partition routing + batching
db.overwrite_partition("events", &[(...)], &batches)?; // atomic swap
for batch in db.scan(Default::default())? { /* RecordBatch stream */ }
```

Embedding is the right call when the database lives and dies with your process.
Multi-process sharing, fault isolation, or remote access → use the server.

## Performance

The benchmark suite lives in `crates/qsdb-bench` — 19 cases (load, scan,
aggregate, join, distinct, export) each with value assertions, run against
DuckDB as the reference engine on a dedicated machine:

```bash
cargo run -p qsdb-bench --bin bench_suite -- --dir <data-dir> --rows 5000000 --all
```

Methodology, per-case numbers, and the reasoning behind every remaining gap
are tracked in [`docs/真机基准对账.md`](docs/真机基准对账.md). We hold
ourselves to a simple rule there: **every number has exactly one source of
truth, and every regression gets a root cause, not a shrug.**

## Documentation

| Doc | What it covers |
|---|---|
| [`docs/客户端与嵌入方指南.md`](docs/客户端与嵌入方指南.md) | Client vs embedded, transport, performance levers |
| [`docs/对接契约.md`](docs/对接契约.md) | Full HTTP API contract (endpoints, headers, error bodies) |
| [`docs/error-codes.md`](docs/error-codes.md) | Every error code: HTTP status, category, SQLSTATE, gRPC mapping |
| [`docs/configuration.md`](docs/configuration.md) | All configuration knobs, by effect level |
| [`docs/format-spec.md`](docs/format-spec.md) | On-disk layout & commit protocol |
| [`docs/bulk-format.md`](docs/bulk-format.md) / [`docs/rows-format.md`](docs/rows-format.md) | Binary batch formats (write path, streaming reads) |
| [`docs/多库访问控制与会话设计.md`](docs/多库访问控制与会话设计.md) | Sessions, mounts, per-database grants |
| [`docs/文件管理面设计.md`](docs/文件管理面设计.md) | Browse/inspect/trash storage files safely |

More design notes live in `docs/` — each document is the design of record for
the batch that shipped it.

## Repository layout

```
crates/
  qsdb         # the kernel: storage, execution, SQL → plan → vectorized exec
  qsdb-cli     # the binary: server (HTTP), shell, query, admin commands
  qsdb-client  # thin Rust client (published to crates.io)
  qsdb-bench   # benchmark suite + probe binaries
```

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI runs fmt + clippy + tests on Linux and Windows, plus a release-binary size
guard. Behavior changes come with regression tests; error-code changes must
update `docs/error-codes.md` (it is enforced against the runtime catalog).

## Roadmap

| Area | Status |
|---|---|
| Type system, storage, commit protocol, pruning scans | ✅ |
| Vectorized execution: join, window, spill, distinct | ✅ |
| HTTP server, sessions, multi-db, per-db grants, guards | ✅ |
| COPY-out (partitioned parquet), external tables, S3/HDFS backends | ✅ |
| Parameterized queries, cancellation, streaming, error catalog | ✅ |
| Flight/gRPC surface | ✅ (enhancing) |
| Quotas, materialized views | planned |

## Ecosystem

- [`qsdb-studio`](https://github.com/qinwenhui/qsdb-studio) — official desktop workbench (Tauri + Vue 3, built on `qsdb-client`).

## License

[MIT](LICENSE)
