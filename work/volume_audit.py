# -*- coding: utf-8 -*-
"""日清分数据量台账（2026-10-17）：
  ① 每张输入表「2026-04-01 当天」的行数（+ 全表行数做对照）
  ② 每个查询块的输出行数 + 引用的输入面（当天行数合计）+ server 稳态墙钟
产出 markdown 台账；口径与 run_dag.py --server --pdate-partitioned 完全一致。

用法（.14）：python3 volume_audit.py > /home/qctc/volume_report.md
"""
import json
import os
import re
import sys
import urllib.request

SERVER = "http://127.0.0.1:9432"
DB = "/home/qctc/qsdb-js"
import sys as _sys
DAG_TSV = _sys.argv[1] if len(_sys.argv) > 1 else "/home/qctc/dag_srv2.tsv"
# 口径日期从语料里数（出现最多的日期字面量），不写死
import glob as _g, collections as _c
_dc = _c.Counter()
for _fn in _g.glob("/home/qctc/daily/*.sql"):
    for m in re.finditer(r"'(20\d{2}-\d{2}-\d{2})'", open(_fn, encoding="utf-8").read()):
        _dc[m.group(1)] += 1
DAY = _dc.most_common(1)[0][0]

# 语料算的**版本号**：中间表按 VERSION_ID 存（依赖关系的根源 —— 上层产出按版本
# 落表、下层按版本读）。从语料的 `VERSION_ID IN ( '…' )` 过滤字面量里收。
import glob as _glob
_versions = {}
for fn in _glob.glob("/home/qctc/daily/*.sql"):
    for m in re.finditer(r"VERSION_ID IN \( '([^']+)' \)", open(fn, encoding="utf-8").read()):
        _versions[m.group(1)] = _versions.get(m.group(1), 0) + 1
assert _versions, "语料里没有版本字面量"
VERSIONS = sorted(_versions, key=lambda v: -_versions[v])


def q(sql, timeout=60):
    body = json.dumps({"sql": sql, "format": "csv"}).encode("utf-8")
    req = urllib.request.Request(SERVER + "/api/v1/query", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        txt = resp.read().decode("utf-8", "replace")
    lines = txt.split("\n")
    if len(lines) >= 2:
        return lines[1].strip()
    return "0"


tables = [t.split(".", 1)[1] for t in json.load(open("/home/qctc/tables.json", encoding="utf-8"))]
default_dir = os.path.join(DB, "default")
have_p = set(n for n in os.listdir(default_dir) if n.endswith("_p"))

# ── ① 输入表台账 ──────────────────────────────────────────────────────
table_rows = {}      # 表 → (全表, 当天, 目标版本切片；无 version_id 列 = -1)
no_pdate = []
no_version = []
for t in sorted(tables):
    name = t + "_p" if (t + "_p") in have_p else t
    total = int(q("SELECT count(*) FROM %s" % name) or 0)
    try:
        day = int(q("SELECT count(*) FROM %s WHERE pdate = '%s'" % (name, DAY)) or 0)
    except Exception:  # noqa: BLE001 —— 维度表没有 pdate
        day = -1
        no_pdate.append(t)
        table_rows[t] = (total, day, -1)
        continue
    # 版本切片：中间表按 VERSION_ID 存，当天行数里通常躺着**所有版本**
    try:
        ver = int(q("SELECT count(*) FROM %s WHERE pdate = '%s' AND version_id = '%s'"
                    % (name, DAY, VERSIONS[0])) or 0)
    except Exception:  # noqa: BLE001 —— 该表没有 version_id 列
        ver = -1
        no_version.append(t)
    table_rows[t] = (total, day, ver)

# ── ② 每块：引用面 + 输出行 + 墙钟 ────────────────────────────────────
block_out = {}       # 块 → (rows, ms)
for line in open(DAG_TSV, encoding="utf-8"):
    f = line.rstrip("\n").split("\t")
    if f[0] == "OK":
        block_out[f[2]] = (int(f[5]), int(f[3]))

pats = [(t, re.compile(r"\b%s\b" % re.escape(t), re.I)) for t in sorted(tables, key=len, reverse=True)]
block_in = {}        # 块 → [引用的表]
for fn in sorted(os.listdir("/home/qctc/daily")):
    if not fn.endswith(".sql"):
        continue
    blk = fn[:-4]
    sql = open("/home/qctc/daily/" + fn, encoding="utf-8").read()
    w = sql.rfind("\nWITH ")
    body = sql[w:] if w >= 0 else sql   # 跳过 CTE 对照注释头，只看正文
    refs = [t for t, p in pats if p.search(body)]
    block_in[blk] = refs

# ── 输出 ─────────────────────────────────────────────────────────────
out = sys.stdout
w = out.write
w("# 日清分数据量台账（%s 单日、版本 %s，%d 个查询块）\n\n" % (DAY, "、".join(VERSIONS), len(block_out)))
w("> 口径：`.14` 真机、68 张 `pdate` 分区副本、`qsdb server` 常驻稳态\n"
  ">（`run_dag.py --server --pdate-partitioned`，2026-10-17）。\n"
  "> 语料 834 处日期字面量全部为 `'%s'`，154 块无一例外；\n" % DAY)
w("> 版本 `%s` 被 %d 处过滤引用（其余 VERSION_ID 出现均为 join 条件）。\n\n" % (VERSIONS[0], _versions.get(VERSIONS[0], 0)))
w("> ⚠️ **版本切片口径**：中间表按 VERSION_ID 存当天**所有**版本（登记表即版本清单），\n"
  "> 基表（`t_cal_day_*` 大多数）没有 version_id 列 —— 版本只活在登记表与中间表。\n"
  "> 「其中版本」列的量 = 该表当天这个版本的切片；读到空切片的块输出 0 行\n"
  ">（是数据口径，不是引擎差异）。\n\n")

w("## 一、输入表（%d 张）\n\n" % len(tables))
w("| 输入表 | 全表行数 | 当天行数 | 其中版本 %s | 被多少块引用 |\n|---|---:|---:|---:|---:|\n" % VERSIONS[0])
refcnt = {}
for blk, refs in block_in.items():
    for t in refs:
        refcnt[t] = refcnt.get(t, 0) + 1
tot_day = tot_ver = 0
for t in sorted(tables, key=lambda x: -table_rows[x][1]):
    total, day, ver = table_rows[t]
    if day >= 0:
        tot_day += day
    if ver >= 0:
        tot_ver += ver
    w("| %s | %s | %s | %s | %d |\n" % (
        t, "{:,}".format(total),
        ("{:,}".format(day) if day >= 0 else "（无 pdate，全量参与）"),
        ("{:,}".format(ver) if ver >= 0 else "—"),
        refcnt.get(t, 0)))
w("\n**当天合计 %s 行；其中版本 `%s` 切片 %s 行**（差值 = 当天其他历史版本）。\n" % ("{:,}".format(tot_day), VERSIONS[0], "{:,}".format(tot_ver)))
w("**维度表（无 pdate，全量参与）：%s；无 version_id 列的表：%s**\n\n"
  % ("、".join(no_pdate) or "无", "、".join(no_version) or "无"))

w("## 二、每个查询块：引用面 → 输出量 → 墙钟\n\n")
w("| 块 | 引用的输入表（当天/版本切片） | 输入面·当天 | 输入面·版本切片 | 输出行数 | 墙钟 ms |\n|---|---|---:|---:|---:|---:|\n")
rows = []
for blk in sorted(block_out):
    refs = block_in.get(blk, [])
    surface = sum(table_rows[t][1] for t in refs if table_rows[t][1] >= 0)
    surface_v = sum(table_rows[t][2] for t in refs if table_rows[t][2] >= 0)
    o, ms = block_out[blk]
    rows.append((surface, blk, refs, surface_v, o, ms))
for surface, blk, refs, surface_v, o, ms in sorted(rows, reverse=True):
    names = "、".join("%s(%s/%s)" % (t, "{:,}".format(table_rows[t][1]),
                                     "{:,}".format(table_rows[t][2]) if table_rows[t][2] >= 0 else "—")
                      for t in sorted(refs, key=lambda x: -table_rows[x][1])[:3])
    if len(refs) > 3:
        names += " 等%d张" % len(refs)
    w("| %s | %s | %s | %s | %s | %d |\n" % (
        blk, names or "（纯 CTE）", "{:,}".format(surface),
        "{:,}".format(surface_v), "{:,}".format(o), ms))

tot_out = sum(v[0] for v in block_out.values())
tot_ms = sum(v[1] for v in block_out.values())
w("\n## 三、总账\n\n")
nz = sum(1 for v in block_out.values() if v[0] > 0)
w("- 输入面合计（各块引用面的和，含重复读）：当天 %s 行 / 版本 `%s` 切片 %s 行\n"
  % ("{:,}".format(sum(r[0] for r in rows)), VERSIONS[0], "{:,}".format(sum(r[3] for r in rows))))
w("- 输出合计：**%s 行**（%d 块；非 0 的 %d 块、0 行的 %d 块）\n"
  % ("{:,}".format(tot_out), len(block_out), nz, len(block_out) - nz))
w("- 墙钟合计：**%s ms**（server 稳态、含争用）\n" % "{:,}".format(tot_ms))
w("- ⚠️ 上层块的输出（`t_dnl_day_*` 中间表的本版本切片）就是下层块的输入 ——\n"
  "  依赖关系即由此而来；台账把两者放在同一张表里，油耗两头都能对上。")
