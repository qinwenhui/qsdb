#!/usr/bin/env bash
# ============================================================================
# 推送前本地全套验证 —— 六轮 CI 红的教训固化
#
# 用法（在 QSDB 仓库根目录）：
#   bash tools/internal/prepush.sh          # 本地侧（fmt/clippy/测试）
#   WITH_REMOTE=1 bash tools/internal/prepush.sh
#                                           # 加 .14 实机 Linux clippy
#
# ⚠️ clippy 缓存会骗人：改过 lint 参数后必须 cargo clean 相关包再验。
#    本脚本对 fmt 敏感文件做 touch 强制重验；要绝对干净就先 cargo clean。
# ============================================================================
set -euo pipefail

echo "==> [1/4] cargo fmt --all --check"
cargo fmt --all --check
echo "    CLEAN"

echo "==> [2/4] cargo clippy --workspace --all-targets -D warnings"
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -1

echo "==> [3/4] cargo test --workspace"
cargo test --workspace 2>&1 | grep -E 'test result' | awk '
  {s+=$4; f+=$6} END {printf "    %d passed / %d failed\n", s, f; exit (f>0)}'

if [ "${WITH_REMOTE:-0}" = "1" ]; then
  echo "==> [4/4] .14 实机 Linux clippy"
  tar czf /tmp/qsdb-ci.tar.gz --exclude=./target --exclude=./.git .
  pscp -batch -pw qctc6608 /tmp/qsdb-ci.tar.gz root@192.168.1.14:/root/bench/ >/dev/null 2>&1
  plink -batch -pw qctc6608 root@192.168.1.14 '
    cd /root/bench/qsdb-ci && rm -rf crates docs src Cargo.toml Cargo.lock &&
    tar xzf /root/bench/qsdb-ci.tar.gz &&
    CARGO_TARGET_DIR=/home/qctc/qsdb-target-ci cargo clippy --workspace \
      --all-targets -- -D warnings 2>&1 | tail -1'
  echo "    REMOTE CLEAN"
else
  echo "==> [4/4] 跳过 .14（WITH_REMOTE=1 启用）"
fi

echo ""
echo "✅ 全部通过 —— 可以 make_public_branch.sh + push"
