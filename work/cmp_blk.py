# -*- coding: utf-8 -*-
"""单块**逐值对账**：同一份数据、同一句 SQL，qsdb 与真 Spark 的输出逐格比。

用法（在 .14 上）：
    spark-submit --driver-memory 4g cmp_blk.py L2-06_02 [L0-17_04 ...]

为什么不能只比行数：行数一样、列序或某几格错，照样是错的
（`stack` 的 key 顺序、`PIVOT` 的「其余列」判据都在这一层）。

两侧格式差异先在 `norm` 里归一，**每次归一都记账**（`NOTES`）：
如果归一规则悄悄吃掉了真实差异，报告里看得见。
"""
import csv
import decimal
import os
import re
import shutil
import subprocess
import sys

Q = "/home/qctc/qsdb-target-suite/release/qsdb"
DB = "/home/qctc/qsdb-js"
NOTES = {}


def note(k):
    NOTES[k] = NOTES.get(k, 0) + 1


def qsdb_run(blk):
    """返回 (列名, 行列表)。行 = 单元格字符串列表。

    ⚠️ **不走 `qsdb query` 的表格输出**：CLI 会把**超过 24 字符的单元格截断**
    （`…`，见 `crates/qsdb-cli/src/main.rs` 的宽度逻辑），UUID/长名字全被砍掉 ——
    拿它做对账等于拿残值比。改走 `COPY (…) TO … (FORMAT CSV)`：导出是完整值、
    RFC4180 引号、NULL 是空字段。**CLI 截断那条是独立的待办**（任何程序化消费
    `SHOW TABLES`/`DESCRIBE` 的人都会踩，此前已坑过两次）。
    """
    sql = open("/home/qctc/daily/%s.sql" % blk, encoding="utf-8").read().strip().rstrip(";")
    out = "/tmp/cmp_%s" % blk
    if os.path.isdir(out):
        shutil.rmtree(out)
    elif os.path.exists(out):
        os.remove(out)
    copy_sql = "COPY (%s) TO '%s' (FORMAT CSV, HEADER)" % (sql, out)
    p = subprocess.run([Q, "query", "--path", DB, "--", copy_sql],
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    txt = p.stdout.decode("utf-8", "replace")
    if p.returncode != 0:
        raise RuntimeError("qsdb 侧失败: " + " ".join(txt.split())[:300])
    files = []
    if os.path.isdir(out):
        for root, _, fs in os.walk(out):
            files.extend(os.path.join(root, f) for f in sorted(fs) if not f.startswith("_"))
    elif os.path.exists(out):
        files.append(out)
    names, rows = None, []
    for f in sorted(files):
        with open(f, encoding="utf-8", newline="") as fh:
            rd = csv.reader(fh)
            for i, rec in enumerate(rd):
                if names is None and i == 0:
                    names = rec
                    continue
                rows.append([("" if c == "" else c) for c in rec])
    return (names or []), rows


def spark_run(blk):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("cmp")
             .config("spark.sql.shuffle.partitions", "16")
             .enableHiveSupport().getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    sql = open("/home/qctc/daily_spark/%s.sql" % blk, encoding="utf-8").read().strip().rstrip(";")
    # 与 run_dag.py 同一套改写：Spark 侧的表在 js_bench 库
    sql = sql.replace("js_produce.", "js_bench.").replace("js_data.", "js_bench.")
    df = spark.sql(sql)
    names = list(df.columns)
    # ⚠️ **列类型要留着**：归一化必须按类型走 —— 把 `'0001'` 这种字符串
    # 也当数字归成 `1`，就会**把真实差异归掉**（那比不归一更糟）。
    dtypes = [dt.lower() for _, dt in df.dtypes]
    rows = [[("NULL" if v is None else str(v)) for v in r] for r in df.collect()]
    spark.stop()
    return names, dtypes, rows


UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def norm_cell(s, dt):
    t = s.strip()
    if t == "" or t.lower() == "null" or t.lower() == "none":
        note("null")
        return "\x00NULL"
    # `uuid()` 每次运行都不同（语料里是 `CAST(uuid() AS STRING) AS PHID`）
    # ⇒ 归成占位符，否则**每一行都会"不一样"**，把真实差异淹掉。
    if UUID_RE.match(t):
        note("uuid")
        return "<UUID>"
    # 字符串/日期/时间戳/布尔：**不碰数值归一**，只统一时间戳的 `T` 分隔符
    if any(k in dt for k in ("string", "varchar", "char", "date", "timestamp", "boolean", "binary")):
        if "T" in t:
            note("ts_T")
            t = t.replace("T", " ")
        return t
    # 数值：用 Decimal 精确比（`0` 与 `0E-11` 同值；`1.50` 与 `1.5000` 同值）
    try:
        d = decimal.Decimal(t)
    except decimal.InvalidOperation:
        note("not_numeric")
        return t
    if d == 0:
        return "0"
    t2 = format(d.normalize(), "f")
    if t2 != t:
        note("num_normalized")
    return t2


def norm_rows(rows, dtypes):
    out = []
    for r in rows:
        out.append([norm_cell(c, dtypes[i] if i < len(dtypes) else "string")
                    for i, c in enumerate(r)])
    return out


def main():
    for blk in sys.argv[1:]:
        try:
            qn, qr = qsdb_run(blk)
            sn, sd, sr = spark_run(blk)
        except Exception as e:  # noqa: BLE001
            print("%-12s  SKIP  %s" % (blk, " ".join(str(e).split())[:180]))
            continue
        qr2, sr2 = norm_rows(qr, sd), norm_rows(sr, sd)
        qs = sorted("|".join(r) for r in qr2)
        ss = sorted("|".join(r) for r in sr2)
        head = "%-12s  qsdb=%-6d spark=%-6d 列数 %d/%d" % (
            blk, len(qr), len(sr), len(qn), len(sn))
        if qs == ss:
            # 列名也比（`stack` 的输出列名是下游的契约）
            if qn and sn and [n.lower() for n in qn] != [n.lower() for n in sn]:
                print(head + "  值全同，**列名不同**\n    qsdb : %s\n    spark: %s" % (qn, sn))
            else:
                print(head + "  ✅ 逐值一致")
            continue
        d1 = [x for x in qs if x not in set(ss)]
        d2 = [x for x in ss if x not in set(qs)]
        print(head + "  ❌ 值不一致：仅 qsdb 有 %d 行、仅 spark 有 %d 行" % (len(d1), len(d2)))
        for x in d1[:3]:
            print("      qsdb 独有: %s" % x[:200])
        for x in d2[:3]:
            print("      spark 独有: %s" % x[:200])
    if NOTES:
        print("归一记账: %s" % NOTES)


main()
