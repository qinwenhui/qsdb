# Changelog

All notable changes to QSDB are documented here.

## [0.1.0] - 2026-10-18

First public release.

- Columnar storage engine: immutable parquet segments, partition-level atomic
  overwrite, manifest-based commit protocol, partition/row-group pruning.
- Vectorized execution: hash join (with zero-copy string keys), window
  functions (including DECIMAL accumulation), sort/aggregate spill, dense
  direct-index `count(DISTINCT)`.
- SQL surface: Spark-dialect friendly parsing (`stack`, `PIVOT`, non-ANSI
  double quotes), CTEs with dead-CTE elimination, parameterized queries
  (`$1` / `?`), `COPY … TO` partitioned export, `read_parquet` external
  tables (local / S3 / HDFS).
- Server: HTTP API with streaming results, cooperative cancellation,
  sessions, multi-database mounts, per-database grants, hot-reconfigurable
  guards (result rows/bytes, timeout, memory), watch endpoint for commit
  events, runtime error-code catalog, storage-file browse/inspect/trash API.
- `qsdb-client` Rust crate: hand-rolled HTTP/1.1, zero network dependencies,
  columnar bulk channel, streaming reads, automatic session recovery.
- Benchmark suite: 19 cases with value assertions, DuckDB as the reference
  engine.
