#!/usr/bin/env python3
"""给所有带 `pdate` 的表建一份 `pdate` 分区副本（`<t>_p`）。

**不动原表** —— 这样 A/B 干净：改 SQL 里的表名指向 `_p` 就能比，
不满意随时删掉副本回到原样。

为什么做：语料**每条查询都带单值 pdate 过滤**（`PDATE IN ('2026-04-01')`，
单值 ⇒ 退化成 `Eq` ⇒ 能下推到分区裁剪），而热表有 15~119 个日期
⇒ 扫描量理论上降到 1/15~1/119。
"""
import os
import subprocess
import sys

Q = "/home/qctc/qsdb-target-suite/release/qsdb"
DB = "/home/qctc/qsdb-js"


def run(sql):
    # ⚠️ `.14` 是 **Python 3.6**：没有 `capture_output=` / `text=`（3.7+ 才有）——
    # 用它会在第一行就 TypeError 崩掉（实测）。
    p = subprocess.run([Q, "query", "--path", DB, "--", sql],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       universal_newlines=True)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def tables():
    """表名从**库目录**取，**不用 `SHOW TABLES`**。

    ⚠️ CLI 的 `SHOW TABLES` 显示**会截断长表名**（`t_cal_day_ahead_unit_…`）——
    照着截断后的名字去 `DESCRIBE` 必然失败，于是表被**静默跳过**
    （2026-10-16 实测：39 张被这么漏掉，还以为是"没有 pdate 列"）。
    文件系统才是真值。
    """
    d = os.path.join(DB, "default")
    ts = []
    for name in sorted(os.listdir(d)):
        if name.startswith("t_") and not name.endswith("_p"):
            if os.path.isdir(os.path.join(d, name)):
                ts.append(name)
    return ts


def describe(t):
    _, out = run("DESCRIBE %s" % t)
    cols = []
    for line in out.splitlines():
        parts = [x.strip() for x in line.split("|")]
        if len(parts) >= 4 and parts[3] in ("true", "false"):
            cols.append((parts[0], parts[1], parts[2], parts[3] == "true"))
    return cols


def main():
    n_ok = n_skip = n_err = 0
    for t in tables():
        cols = describe(t)
        if not cols:
            print("SKIP-desc\t%s" % t, flush=True); n_skip += 1; continue
        if any(c[3] for c in cols):
            print("ALREADY-PART\t%s" % t, flush=True); n_skip += 1; continue
        pdate = next((c[0] for c in cols if c[0].lower() == "pdate"), None)
        if pdate is None:
            print("NO-PDATE\t%s" % t, flush=True); n_skip += 1; continue
        names = [c[0] for c in cols]
        # ⚠️ **分区列必须可空** —— 本仓口径："值活在目录名里"，NOT NULL 会被拒
        # （实测：`partition column 'pdate' must be nullable (values live in directory names)`）。
        defs = ", ".join(
            "%s %s%s" % (c[0], c[1],
                         " NOT NULL" if (c[2] == "NO" and c[0] != pdate) else "")
            for c in cols)
        nt = t + "_p"
        rc, out = run("CREATE TABLE %s (%s) PARTITIONED BY (%s)" % (nt, defs, pdate))
        if rc:
            print("ERR-CREATE\t%s\t%s" % (t, " ".join(out.split())[:150]), flush=True)
            n_err += 1; continue
        rc, out = run("INSERT INTO %s SELECT %s FROM %s" % (nt, ", ".join(names), t))
        if rc:
            print("ERR-INSERT\t%s\t%s" % (t, " ".join(out.split())[:150]), flush=True)
            n_err += 1; continue
        print("OK\t%s" % t, flush=True); n_ok += 1
    print("=== 完成：OK %d / SKIP %d / ERR %d ===" % (n_ok, n_skip, n_err), flush=True)


main()
