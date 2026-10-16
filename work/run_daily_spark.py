#!/usr/bin/env python3
"""按层级跑日清分语料（**Spark 侧**）：同层并发、层间串行 —— 与 qsdb 侧同一口径。

用法：spark-submit run_daily_spark.py [sql目录] [输出tsv]

设计要点（为了能和 qsdb 侧比）：
  * **一个 SparkSession** 跑完全部 154 条 —— 不是每条起一个 `spark-sql` 进程，
    否则 83 个块的 L0 会同时起 83 个 JVM，量到的全是 JVM 启动时间。
    这与 qsdb 侧"一个引擎实例处理并发查询"是对齐的。
  * 同层用线程池**全并发**提交 `spark.sql()`；层间 `wait`。
  * 计时**含 `collect()`** —— qsdb 侧那条 `query` 也是把结果materialize出来打印，
    两边都算"把结果拿到手"。⚠️ qsdb 还多一层文本格式化，这个差异记在报告里。
  * `spark.sql.shuffle.partitions=16`（= nproc）：数据是百万级以下，
    默认 200 分区光调度就压过计算。两边都按"调过的"比。

输出 TSV（与 qsdb 侧同格式）：
  OK\t<block>\t<ms>\t<rows>\t<layer>\t
  ERR\t<block>\t<ms>\t<rows>\t<layer>\t<msg>
  LAYER\t<L>\t<wall_ms>\t<n>
"""
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession

DIR = sys.argv[1] if len(sys.argv) > 1 else "/home/qctc/daily_spark"
OUT = sys.argv[2] if len(sys.argv) > 2 else "/home/qctc/daily_run_spark.tsv"


def main():
    spark = (
        SparkSession.builder.appName("daily-suite")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    blocks = sorted(f[:-4] for f in os.listdir(DIR) if f.endswith(".sql"))
    layers = {}
    for b in blocks:
        m = re.match(r"L(\d+)-", b)
        if m:
            layers.setdefault(int(m.group(1)), []).append(b)

    def run_block(b):
        sql = open(os.path.join(DIR, b + ".sql"), encoding="utf-8").read().strip()
        sql = sql.rstrip().rstrip(";")
        t0 = time.perf_counter()
        try:
            rows = spark.sql(sql).collect()
            ms = int((time.perf_counter() - t0) * 1000)
            return ("OK", b, ms, len(rows), "")
        except Exception as e:  # noqa: BLE001 — 一条挂了不该带走整层
            ms = int((time.perf_counter() - t0) * 1000)
            msg = " ".join(str(e).split())[:300]
            return ("ERR", b, ms, 0, msg)

    out = []
    for L in sorted(layers):
        bks = layers[L]
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=len(bks)) as ex:
            res = list(ex.map(run_block, bks))
        wall = int((time.perf_counter() - t0) * 1000)
        for st, b, ms, n, msg in res:
            out.append("%s\t%s\t%d\t%d\tL%d\t%s" % (st, b, ms, n, L, msg))
        out.append("LAYER\tL%d\t%d\t%d" % (L, wall, len(bks)))
        print("layer L%d done: %d blocks, %d ms" % (L, len(bks), wall), flush=True)

    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out) + "\n")
    print("OK %d ERR %d" % (
        sum(1 for r in out if r.startswith("OK")),
        sum(1 for r in out if r.startswith("ERR")),
    ))

    spark.stop()


main()
