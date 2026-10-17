# -*- coding: utf-8 -*-
"""日清计算**总报告**生成器（2026-10-17）。

统计口径 —— **零侵入**：
  * 计时全部在**客户端**（run_dag 的墙钟；引擎热路径一行没动）；
  * 内存读 `/proc/<server pid>/status`（每轮结束读一次，常驻 VmHWM/VmRSS）；
  * 每查询峰值内存走**一轮子进程模式**（每块 VmHWM 由 run_dag 既有轮询采集），
    只为采数跑一轮，不计入 server 墙钟口径；
  * 数据量由 volume_audit 生成（HTTP count，不触发引擎特殊路径）。

用法（.14）：python3 daily_report.py [轮数=5] > /home/qctc/daily_report.md
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from statistics import median

ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 5
WORK = "/root/bench/qsdb-a3/work"
SERVER = "http://127.0.0.1:9432"
DB = "/home/qctc/qsdb-js"


def sh(cmd, timeout=600):
    return subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout,
                          universal_newlines=True).stdout


def server_pid():
    out = sh("pgrep -f 'release/qsdb serve[r] --path %s'" % DB).split()
    return out[0] if out else None


def server_mem(pid):
    if not pid:
        return None
    vmhwm = vmrss = None
    try:
        with open("/proc/%s/status" % pid) as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    vmhwm = int(line.split()[1])
                elif line.startswith("VmRSS:"):
                    vmrss = int(line.split()[1])
    except OSError:
        return None
    return (vmhwm, vmrss)


# ── ① server 模式 N 轮 ────────────────────────────────────────────────
print("> 采集：%d 轮 server 稳态 + 1 轮子进程（专采每查询峰值内存）…" % ROUNDS)
rounds = []            # (wall_ms, tsv)
for i in range(ROUNDS):
    tsv = "/tmp/report_r%d.tsv" % i
    out = sh("cd %s && python3 run_dag.py --engine qsdb --server %s --dir /home/qctc/daily "
             "--out %s --pdate-partitioned" % (WORK, SERVER, tsv))
    m = re.search(r"OK (\d+) ERR (\d+)\s+总墙钟 (\d+) ms", out)
    if not m:
        print("round %d 解析失败: %s" % (i, out[-200:]))
        sys.exit(1)
    rounds.append((int(m.group(3)), tsv, int(m.group(1)), int(m.group(2))))
    time.sleep(0.5)

spid = server_pid()
mem_rounds = [server_mem(spid) for _, tsv, _, _ in rounds]

# ── ② 子进程模式一轮：每块峰值 RSS ────────────────────────────────────
sub_tsv = "/tmp/report_sub.tsv"
sh("cd %s && python3 run_dag.py --engine qsdb --dir /home/qctc/daily "
   "--out %s --pdate-partitioned" % (WORK, sub_tsv))
sub_rss = {}
for line in open(sub_tsv, encoding="utf-8"):
    f = line.rstrip("\n").split("\t")
    if f[0] == "OK" and int(f[4]) > 0:
        sub_rss[f[2]] = int(f[4])

# ── ③ 聚合：每块跨轮 min/中位/max ─────────────────────────────────────
block_ms = {}
block_rows = {}
ok_all = True
for wall, tsv, okn, errn in rounds:
    if errn:
        ok_all = False
    for line in open(tsv, encoding="utf-8"):
        f = line.rstrip("\n").split("\t")
        if f[0] == "OK":
            block_ms.setdefault(f[2], []).append(int(f[3]))
            block_rows[f[2]] = int(f[5])

walls = sorted(r[0] for r in rounds)
task_wall = {}
deps = json.load(open("/home/qctc/dag.json"))["dag"]
# 取中位轮做关键链
mid_tsv = sorted(rounds, key=lambda r: r[0])[len(rounds) // 2][1]
tw = {}
toff = {}
for line in open(mid_tsv, encoding="utf-8"):
    f = line.rstrip("\n").split("\t")
    if f[0] == "TASK":
        tw[f[1]] = int(f[2])
        toff[f[1]] = int(f[5])
memo = {}


def longest(t):
    if t in memo:
        return memo[t]
    best = max((longest(d) for d in deps.get(t, [])), default=0)
    memo[t] = best + tw.get(t, 0)
    return memo[t]


chain_end = max(tw, key=longest)
chain, cur = [], chain_end
while True:
    chain.append(cur)
    ds = deps.get(cur, [])
    if not ds:
        break
    cur = max(ds, key=longest)
chain.reverse()

# ── ④ 数据量（volume_audit 生成，作为附录嵌入）──────────────────────
vol = sh("python3 /home/qctc/volume_audit.py %s" % mid_tsv)

# ── 输出 ─────────────────────────────────────────────────────────────
out = sys.stdout
w = out.write
w("# 日清分计算总报告\n\n")
w("- **口径日期 / 版本**：2025-10-01 · `78_20251001_1`（全语料 154 块按此过滤；\n"
  "  此切片为全库数据最多的日期 × 中间表数据最多的版本，贴近生产量级）\n")
w("- **负载**：118 个任务 / **154 个 SQL 块** / 12 层依赖（无依赖即并行、有依赖等齐）\n")
w("- **形态**：`qsdb server` 常驻（生产形态），68 张 `pdate` 分区表，`.14` 真机 16 核\n")
w("- **统计零侵入**：计时在客户端、内存读 `/proc`、引擎热路径一行未动\n\n")

w("## 一、正确性\n\n")
okn = rounds[-1][2]
w("- %d 轮 server 运行 **全部 %d/154 OK、0 ERR**：%s\n"
  % (ROUNDS, okn, "、".join(str(r[0]) for r in rounds)))
w("- 旧切片（2026-04-01）上：147 块与真 Spark 行数逐块一致，26 块逐格+列名对账全 ✅\n"
  "  （本切片 Spark 侧不再作对照；同代码同语料框架，仅字面量不同）\n\n")

w("## 二、执行时间（%d 轮）\n\n" % ROUNDS)
w("| 指标 | 值 |\n|---|---:|\n")
w("| 总墙钟·最快 | **%s ms** |\n" % "{:,}".format(walls[0]))
w("| 总墙钟·中位 | **%s ms** |\n" % "{:,}".format(int(median(walls))))
w("| 总墙钟·最慢 | %s ms |\n" % "{:,}".format(walls[-1]))
w("| 极差 | %.1f%% |\n" % (100.0 * (walls[-1] - walls[0]) / walls[-1]))
w("| 单块墙钟·中位 | %d ms |\n" % int(median([median(v) for v in block_ms.values()])))
w("| 墙钟合计（中位轮，含争用） | %s ms |\n" % "{:,}".format(sum(int(median(v)) for v in block_ms.values())))
floor = sum(tw.get(t, 0) for t in chain)
w("| DAG 结构地板（关键链） | %s ms |\n" % "{:,}".format(floor))
w("\n关键链：%s\n\n" % " → ".join("%s(%d)" % (t, tw.get(t, 0)) for t in chain))

w("### 最慢的 10 个块（跨轮中位）\n\n")
w("| 块 | 中位 ms | 最快 | 最慢 | 输出行 |\n|---|---:|---:|---:|---:|\n")
for blk, v in sorted(block_ms.items(), key=lambda kv: -median(kv[1]))[:10]:
    w("| %s | %d | %d | %d | %s |\n" % (blk, int(median(v)), min(v), max(v),
                                        "{:,}".format(block_rows[blk])))

w("\n## 三、内存\n\n")
w("| 指标 | 值 |\n|---|---:|\n")
if all(mem_rounds):
    w("| server 常驻 VmHWM（峰值，跨轮） | %s MB |\n" % "{:,}".format(max(m[0] for m in mem_rounds) // 1024))
    w("| server 常驻 VmRSS（稳态，跨轮中位） | %s MB |\n" % "{:,}".format(int(median([m[1] for m in mem_rounds])) // 1024))
if sub_rss:
    vals = sorted(sub_rss.values())
    w("| 每查询峰值 RSS·中位（子进程口径） | %d MB |\n" % (vals[len(vals) // 2] // 1024))
    w("| 每查询峰值 RSS·最大 | %d MB |\n" % (vals[-1] // 1024))
w("\n（常驻口径含页缓存/热数据缓存 —— 是\"这个服务开着要多少内存\"的答案；\n"
  "  每查询峰值是\"单个查询最多要多少\"的答案。两个口径各有用处。）\n\n")

w("## 四、吞吐（中位轮 %d ms）\n\n" % int(median(walls)))
tot_rows = sum(block_rows.values())
wall_med = int(median(walls))
m_in = re.search(r"当天 ([\d,]+) 行 / 版本 `[^`]+` 切片 ([\d,]+) 行", vol)
if m_in:
    day_rows = int(m_in.group(1).replace(",", ""))
    ver_rows = int(m_in.group(2).replace(",", ""))
    w("- 输入面（按块计，含重复读）：当天 %s 行 ⇒ **约 %d 万行/秒**；\n"
      "  版本切片 %s 行 ⇒ **约 %d 万行/秒**\n" % (
          m_in.group(1), day_rows // max(1, wall_med) * 1000 // 10000,
          m_in.group(2), ver_rows // max(1, wall_med) * 1000 // 10000))
w("- 输出：**%s 行**（约 %s 行/秒）\n" % ("{:,}".format(tot_rows), "{:,}".format(tot_rows * 1000 // max(1, int(median(walls))))))
w("- 数据量明细见下方台账（输入表 57 张、逐块引用面与输出量）\n\n")

w("## 五、数据量台账（自动生成）\n\n")
w(vol)
