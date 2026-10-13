#!/bin/sh
# 本地 feature 矩阵检查（一键）。
#
# **为什么需要它**：F23 一次报了两条"编不过"——`feature hdfs`（`tracing` 未声明依赖）
# 与 `feature s3`（`FileLoc` 的 owned/借用混用）。两条同源：**改代码时没跑对应的
# feature 组合**（本地只跑过默认与 `flight`）。这类事故不会在默认构建里露头，
# 却会直接挡住用 s3/hdfs 后端的部署与生态工具（qsdb-sync 的 HDFS e2e 就被挡过）。
#
# 用法：`sh tools/check.sh`（会跑几分钟；`-D warnings` 让告警也算失败）
set -e
cd "$(dirname "$0")/.."

echo "== fmt =="
cargo fmt --all -- --check

echo "== clippy: 默认（qsdb + qsdb-cli，含 --all-targets）=="
cargo clippy -p qsdb -p qsdb-cli --all-targets -- -D warnings

echo "== clippy: flight =="
cargo clippy -p qsdb-cli --all-targets --features flight -- -D warnings

echo "== clippy: s3 =="
cargo clippy -p qsdb --all-targets --features s3 -- -D warnings

echo "== clippy: hdfs =="
cargo clippy -p qsdb --all-targets --features hdfs -- -D warnings

echo "== clippy: s3+hdfs =="
cargo clippy -p qsdb --all-targets --features s3,hdfs -- -D warnings

echo "== 测试（默认 feature）=="
cargo test -p qsdb -p qsdb-cli

echo "ALL GREEN ✓"
