#!/usr/bin/env bash
# ============================================================================
# 生成公开发布分支 —— 孤立单提交，不含开发历史
#
# 用法（在 QSDB 仓库根目录）：
#   bash tools/internal/make_public_branch.sh              # 重置本仓 public 分支
#   bash tools/internal/make_public_branch.sh --out <dir>  # 另出一份干净仓库目录
#
# 做法：
#   1. 按**白名单**把文件复制进临时目录（业务/内部文件根本不进）；
#   2. 对临时目录跑**敏感串扫描**，任何命中即失败退出（宁可发不出去）；
#   3. 在临时目录 git init + 单提交（author/committer = qinwh，无任何尾注）；
#   4. 默认 fetch 成本仓的 public 分支；--out 则直接交付目录。
#
# ⚠️ 开发分支的历史里有内部信息（密码/内网 IP 都进过 git 历史）——
#    所以公开面**永远**走本脚本生成，**永远不要** push 开发分支本身。
# ============================================================================
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
MODE=branch
OUT=""
if [ "${1:-}" = "--out" ]; then
  MODE=out
  OUT="${2:?--out 需要一个目录参数}"
fi

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

# ---------------------------------------------------------------------------
# 一、白名单复制
# ---------------------------------------------------------------------------
echo "==> 组装文件到 $STAGE"

mkdir -p "$STAGE/crates" "$STAGE/docs" "$STAGE/.github/workflows"

# 内核四件套（全部公开）
cp -r crates/qsdb crates/qsdb-cli crates/qsdb-bench crates/qsdb-client "$STAGE/crates/"
# 顶层文件
cp README.md README.zh-CN.md LICENSE Cargo.toml Cargo.lock .gitignore .gitattributes \
   SECURITY.md CONTRIBUTING.md CHANGELOG.md THIRD_PARTY_NOTICES.md "$STAGE/"
cp .github/workflows/*.yml "$STAGE/.github/workflows/"

# docs 白名单（与 tools/internal/RELEASING.md 的清单保持一致）
# 定案（2026-10-18）：公开面只保留规范/契约 5 份，其余设计稿留在开发分支
DOCS=(
  configuration.md error-codes.md format-spec.md bulk-format.md rows-format.md
)
for d in "${DOCS[@]}"; do
  if [ ! -f "docs/$d" ]; then
    echo "✗ 白名单文档缺失: docs/$d" >&2
    exit 1
  fi
  cp "docs/$d" "$STAGE/docs/"
done

# 剔除可能的构建产物（crates 下不该有，防手滑）
find "$STAGE" -type d -name target -prune -exec rm -rf {} + 2>/dev/null || true

echo "    文件数: $(find "$STAGE" -type f | wc -l)"

# ---------------------------------------------------------------------------
# 二、敏感串扫描（硬闸：任何命中即失败，不做自动替换）
# ---------------------------------------------------------------------------
echo "==> 敏感串扫描"

check() {
  local name="$1" pattern="$2" exclude="${3:-}"
  local hits
  hits=$(grep -rInE "$pattern" "$STAGE" 2>/dev/null | grep -vF "$exclude" || true)
  if [ -n "$hits" ]; then
    echo "✗ 敏感串命中 [$name]：" >&2
    echo "$hits" | head -20 >&2
    exit 1
  fi
}

check '密码'        'qctc6608|plink |pscp '
check '内网地址'    'qctc/|192\.168\.|hadoop104'
check '内网简称'    '(^|[^0-9])\.14([^0-9.]|$)'
check '业务词-日清' '日清'
check '业务词-结算' '结算|SETTLE' 'not settled'
check '业务词-电力' '电力|电厂|机组|电网|售电|跨区跨省|电价|电量|电费'
check '业务字段'    '变动成本|dgyg|t_cal_|t_dnl|PHID|JS_OBJECT|power_company|trade_types|assess_type'
check '内部协作文档' '进度与待办|算子优化对账|参考项目设计|参考架构层|超越DuckDB|生态工具升级通知|常用分析sql|大sql2|mysql迁移|sql算子解析'
check 'AI 尾注'     'Co-Authored-By|[Cc]laude'

echo "    扫描通过（零命中）"

# ---------------------------------------------------------------------------
# 三、孤立单提交
# ---------------------------------------------------------------------------
echo "==> 生成单提交"

cd "$STAGE"
git init -q -b main
git add -A
git -c user.name="qinwh" -c user.email="1510365643@qq.com" \
    commit -q -m "Initial public release v0.1.0"

# 自检：身份与尾注
AUTHOR=$(git log -1 --format='%an <%ae> | %cn <%ce>')
BODY=$(git log -1 --format=%B)
echo "    提交身份: $AUTHOR"
if [ "$AUTHOR" != "qinwh <1510365643@qq.com> | qinwh <1510365643@qq.com>" ]; then
  echo "✗ 提交身份不是 qinwh，中止（公开历史不能带别的身份）" >&2
  exit 1
fi
case "$BODY" in
  *Co-Authored*|*[Cc]laude*)
    echo "✗ 提交信息含尾注，中止" >&2
    exit 1
    ;;
esac
echo "    提交数: $(git rev-list --count HEAD)（必须为 1）"

# ---------------------------------------------------------------------------
# 四、交付
# ---------------------------------------------------------------------------
cd "$ROOT"
if [ "$MODE" = "out" ]; then
  rm -rf "$OUT"
  mkdir -p "$(dirname "$OUT")"
  cp -r "$STAGE" "$OUT"
  rm -rf "$OUT/.git"
  echo "==> 干净仓库目录已交付: $OUT（无 .git，自行 git init 或直接使用）"
else
  git branch -D public >/dev/null 2>&1 || true
  git fetch -q "$STAGE" main:public
  echo "==> 本仓分支 public 已更新: $(git rev-parse --short public)"
  echo "    后续步骤见 tools/internal/RELEASING.md"
fi
