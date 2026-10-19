# 公开发布步骤（内部文档，不入公开仓）

> 本文件与 `make_public_branch.sh` 都**不进**公开分支（脚本按白名单复制，`tools/` 整个排除）。

## 一次性前置

1. `cargo test --workspace` 全绿、`cargo clippy --workspace --all-targets -- -D warnings` 干净。
2. 确认 `LICENSE` / `Cargo.toml`（workspace `license = "MIT"`）/ README 的许可证声明一致。
3. `crates/qsdb-client` 版本号就位（当前 `0.1.0`，随 workspace）。**改过就要重发 crates.io**（见下）。

## 每次发布

### 0. 推送前本地全套验证（**必跑**）

```bash
bash tools/internal/prepush.sh              # fmt / clippy / 全量测试
WITH_REMOTE=1 bash tools/internal/prepush.sh  # 加 .14 实机 Linux clippy
```

> 六轮 CI 红的教训：三次挂在"改完没跑全套验证"（两次 fmt、一次缓存假绿）。
> **这个脚本不过，不生成、不推送。** clippy 缓存会骗人——改过代码后必须
> 验证；要绝对干净先 `cargo clean`。

### 1. 生成公开分支（推送前现场生成，**平时不保留**）

```bash
bash tools/internal/make_public_branch.sh
```

> **分支生命周期 = 生成 → 立即推送 → 删除**。平时仓库里只有开发分支；
> 每次 public 重新生成都替换旧分支，推不出去的旧生成物一律 `git branch -D public`
> + `git gc --prune=now` 清掉——不给本地 .git 留任何公开面历史残留。

脚本自动做：白名单复制 → 敏感串扫描（密码/内网 IP/业务词/AI 尾注，**命中即失败**）→
孤立单提交（author/committer = qinwh，无尾注）→ 更新本仓 `public` 分支。

抽查（第一次发布务必人肉过一遍）：

```bash
git log public --format='%h %an <%ae> | %cn <%ce>'   # 1 个提交，身份 qinwh
git log public -1 --format=%B                        # 无 Co-Authored-By
git ls-tree -r public --name-only | wc -l            # 文件数与预期一致
git ls-tree -r public --name-only | grep docs/       # 只有白名单文档
```

### 2. 发 `qsdb-client` 到 crates.io（首次必须；client 变更后重发）

crates.io 发布**不可撤销**（版本号作废后不能复用），执行前确认：

```bash
cargo publish -p qsdb-client --dry-run   # 打包 + 编译验证，不上传
cargo login <TOKEN>                      # 一次即可（token 存本地）
cargo publish -p qsdb-client
```

- 只有 `qsdb-client` 是对外 crate；内核 `qsdb` 不发（嵌入式用户从源码构建）。
- Studio 等外部仓库的 `Cargo.toml` 写 `qsdb-client = "0.1.0"`，依赖 crates.io 版本。
- **发布后**重新生成 public 分支（带新 lock 与版本号）再 push。

### 3. 建 GitHub 仓库并推送（首次）

```bash
# GitHub 上建空仓 qinwenhui/qsdb：Public，且**不要**用 README/LICENSE 初始化
git remote add origin https://github.com/qinwenhui/qsdb.git
git push -u origin public:main
git tag -a v0.1.0 public -m "QSDB v0.1.0"
git push origin v0.1.0
```

⚠️ **推的是 `public` 分支，永远不是当前开发分支**（开发历史里有内部信息）。

### 4. 首次 CI 观察

`.github/workflows/ci.yml` 从未在真实 Actions 上跑过（只有本地/语法校验）。
首推后盯一次：Linux/Windows 矩阵 + 二进制体积闸。红了按报错修——
大概率是平台差异（比如 Windows 大小写、bash-only 测试），属于正常磨合。

### 5. 后续版本的例行更新

```bash
# 开发分支上改完、提交后：
bash tools/internal/make_public_branch.sh
git push --force origin public:main   # 每次重新生成都替换远程为全新单提交
git branch -D public                  # 推完即删，本地不保留
```

> 用 `--force` 是**设计内**的：public 永远只有一个"Initial public release"式
> 快照提交，历史不积累。要对外可见的演进记录，靠 tag（v0.1.0 / v0.2.0 …）。
> 每次正式发布前打新 tag 指向当时的 public。

## 文档白名单变更时

`make_public_branch.sh` 里的 `DOCS` 数组是**唯一清单**。新写文档要公开 → 加进去；
含业务/内部信息 → 不加（留在开发分支）。敏感扫描是兜底闸，不是替代品。

## 红线回顾（为什么有这套流程）

- 开发分支 git 历史里**确实有**：内部密码明文、内网 IP、业务表名、内部协作文档。
- 公开分支 = 白名单快照 + 扫描闸 + 孤立提交，三道防线任何一道都独立兜底。
- 宁可发布失败（扫描闸拦停），不可发布出去再删——互联网没有删除键。
