# Third-party notices

QSDB depends on the following third-party libraries. The list below covers
the **direct** workspace dependencies; for the full transitive graph (with
exact versions and licenses) see [`Cargo.lock`](Cargo.lock), or generate a
complete report with a tool such as `cargo-about` / `cargo-den
y`.

| Crate | License | Used for |
|---|---|---|
| `arrow` (arrow-rs) | Apache-2.0 | Columnar memory model |
| `parquet` (arrow-rs) | Apache-2.0 | Storage format |
| `sqlparser` | Apache-2.0 | SQL parsing |
| `tokio`, `tokio-util` | MIT | Async runtime |
| `axum` | MIT | HTTP server |
| `serde`, `serde_json` | MIT OR Apache-2.0 | Metadata serialization |
| `postcard` | MIT OR Apache-2.0 | Manifest encoding |
| `thiserror` | MIT OR Apache-2.0 | Error types |
| `tracing` | MIT | Structured logging |
| `rayon` | MIT OR Apache-2.0 | Data parallelism |
| `hashbrown` | MIT OR Apache-2.0 | Hash tables |
| `fs2` | MIT OR Apache-2.0 | File locks |
| `uuid` | MIT OR Apache-2.0 | Query/segment ids |
| `clap` | MIT OR Apache-2.0 | CLI parsing |
| `rustyline` | MIT | Interactive shell |
| `windows-sys` | MIT OR Apache-2.0 | Windows file-system APIs |
| `object_store`, `hdfs-native` | Apache-2.0 / MIT | Optional S3/HDFS backends (feature-gated, off by default) |

Where a crate is dual-licensed, the license actually applying is whichever
option of the pair the upstream project offers — see each crate's own
`LICENSE` file for the authoritative text.
