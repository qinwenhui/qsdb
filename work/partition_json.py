#!/usr/bin/env python3
"""补建缺的分区副本 —— **从 `table.json` 直接读 schema**，不经过 CLI 显示层。

为什么要绕：CLI 的输出列会**截断长名字**（加 `…`），`DESCRIBE` 也一样
—— 长列名被截成 `deviation_income_shar…`，拼进 DDL 就是语法错
（2026-10-16 实测：`Expected: a data type name, found: …`）。
`table.json` 是磁盘真值，不截断。
"""
import json
import os
import subprocess

Q = "/home/qctc/qsdb-target-suite/release/qsdb"
DB = "/home/qctc/qsdb-js"
TDIR = os.path.join(DB, "default")

SIMPLE = {"Date": "DATE", "Time": "TIME", "Boolean": "BOOLEAN", "Int8": "TINYINT",
          "Int16": "SMALLINT", "Int32": "INT", "Int64": "BIGINT",
          "Float32": "FLOAT", "Float64": "DOUBLE"}


def run(sql):
    p = subprocess.run([Q, "query", "--path", DB, "--", sql],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       universal_newlines=True)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def ddl_type(lt):
    if isinstance(lt, str):
        return SIMPLE.get(lt)
    if "Varchar" in lt:
        return "VARCHAR"
    if "Decimal" in lt:
        d = lt["Decimal"]
        return "DECIMAL(%d,%d)" % (d["precision"], d["scale"])
    if "Timestamp" in lt:
        return "TIMESTAMP"
    return None


def main():
    ok = err = 0
    for name in sorted(os.listdir(TDIR)):
        if not name.startswith("t_") or name.endswith("_p"):
            continue
        if os.path.isdir(os.path.join(TDIR, name + "_p")):
            continue
        tj = os.path.join(TDIR, name, "table.json")
        if not os.path.isfile(tj):
            print("NO-JSON\t%s" % name, flush=True); continue
        d = json.load(open(tj, encoding="utf-8"))
        cols = d.get("columns", [])
        if not any(c["name"].lower() == "pdate" for c in cols):
            print("NO-PDATE\t%s" % name, flush=True); continue
        defs = []
        bad = False
        for c in cols:
            t = ddl_type(c["logical_type"])
            if t is None:
                print("UNKNOWN-TYPE\t%s.%s\t%s" % (name, c["name"], c["logical_type"]), flush=True)
                bad = True; break
            nn = "" if (c.get("nullable", True) or c["name"].lower() == "pdate") else " NOT NULL"
            defs.append("%s %s%s" % (c["name"], t, nn))
        if bad:
            err += 1; continue
        nt = name + "_p"
        rc, out = run("CREATE TABLE %s (%s) PARTITIONED BY (pdate)" % (nt, ", ".join(defs)))
        if rc:
            print("ERR-CREATE\t%s\t%s" % (name, " ".join(out.split())[:160]), flush=True)
            err += 1; continue
        cols_sql = ", ".join(c["name"] for c in cols)
        rc, out = run("INSERT INTO %s SELECT %s FROM %s" % (nt, cols_sql, name))
        if rc:
            print("ERR-INSERT\t%s\t%s" % (name, " ".join(out.split())[:160]), flush=True)
            err += 1; continue
        print("OK\t%s" % name, flush=True); ok += 1
    print("=== 补齐：OK %d ERR %d ===" % (ok, err), flush=True)


main()
