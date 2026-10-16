#!/usr/bin/env python3
"""把测试平台要用的表从 MySQL 灌进 Spark（Hive）—— **绕开 parquet 互操作**：

Spark 读不了我们导出的 parquet（`TIMESTAMP(NANOS)` 是 Spark 拒收的类型，见 P0），
所以**不碰我们的导出**：直接从 MySQL 源抽，写成 **Spark 自己的表**。
之后所有语料查询读的都是 Spark 本地的表 ⇒ 对 Spark 公平（不走 JDBC 往返）。

用法：spark-submit --jars <mysql-connector>.jar load_tables.py <tables.json>
"""
import json
import sys

from pyspark.sql import SparkSession

TABLES = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "/home/qctc/tables.json",
                        encoding="utf-8"))
HOST = "jdbc:mysql://192.168.1.10:3306"
USER = "root"
PW = "123456@"


def main():
    # ⚠️ **必须 `enableHiveSupport()`**：不写它，`saveAsTable` 会落到**会话内存目录**里
    # —— 建表报成功、HDFS 上数据目录也在，但会话一结束**表名就没了**
    # （实测：57 张表全灌完，`SHOW TABLES` 只剩 3 张，其余全成孤儿目录）。
    spark = (SparkSession.builder.appName("load-tables")
             .config("spark.sql.shuffle.partitions", "8")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # ⚠️ 刻意灌进**全新的库** `js_bench`，而不是原库名：
    # 原库 `js_produce` 在 HDFS 上**已经有一堆同名目录**（Hive 表被删过、数据目录留下了），
    # 直接建托管表会撞 `LOCATION_ALREADY_EXISTS`。而删别人的 HDFS 目录有风险 ——
    # 所以另起一个干净的库，查询时把 `js_produce.` / `js_data.` 前缀改写过来。
    BENCH = "js_bench"
    spark.sql("CREATE DATABASE IF NOT EXISTS %s" % BENCH)
    print("database: %s" % BENCH, flush=True)

    ok = err = 0
    for full in sorted(TABLES):
        db, t = full.split(".")
        try:
            df = (spark.read.format("jdbc")
                  .option("url", "%s/%s?useSSL=false&allowPublicKeyRetrieval=true" % (HOST, db))
                  .option("dbtable", t)
                  .option("user", USER).option("password", PW)
                  .option("driver", "com.mysql.cj.jdbc.Driver")
                  # 并行拉 + 分批取：单连接一把拉 33 万行会把驱动堆吃爆
                  # （2026-10-16 实测 `GC overhead limit exceeded`）。
                  .option("numPartitions", "8")
                  .option("fetchsize", "5000")
                  .load())
            target = "%s.%s" % (BENCH, t)
            spark.sql("DROP TABLE IF EXISTS %s PURGE" % target)
            # ⚠️ **先写、再从表里 count** —— 不要 `df.count()` 再写：
            # 那等于让 JDBC **读两遍**，就是它把堆吃爆的（实测）。
            df.write.mode("overwrite").saveAsTable(target)
            n = spark.sql("SELECT count(*) c FROM %s" % target).collect()[0]["c"]
            print("OK\t%s\t%d" % (target, n), flush=True)
            ok += 1
        except Exception as e:  # noqa: BLE001 — 一张失败不该带走整批
            print("ERR\t%s\t%s" % (full, " ".join(str(e).split())[:200]), flush=True)
            err += 1
    print("=== 灌完：OK %d ERR %d ===" % (ok, err), flush=True)
    spark.stop()


main()
