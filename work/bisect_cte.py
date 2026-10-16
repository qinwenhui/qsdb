# -*- coding: utf-8 -*-
"""逐 CTE 二分：对块里每个 `tN`，以「WITH 前缀 + SELECT count(*) FROM tN」计数。

用途：找出整条链从哪一层开始把行数算错（比如 5832 行在中途变 0）。
用法：python3 bisect_cte.py <engine> <block>   # engine = qsdb | spark
"""
import subprocess
import sys

BLK = sys.argv[2]
ENGINE = sys.argv[1]


def split_top_level(s, sep=","):
    """按括号深度切逗号。"""
    out, depth, cur = [], 0, []
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == sep and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return out


DIR = "/home/qctc/daily_spark" if ENGINE == "spark" else "/home/qctc/daily"
sql = open("%s/%s.sql" % (DIR, BLK), encoding="utf-8").read().strip().rstrip(";")
if ENGINE == "spark":
    sql = sql.replace("js_produce.", "js_bench.").replace("js_data.", "js_bench.")
else:
    sql = sql.replace("js_produce.", "").replace("js_data.", "")
# 块文件带 `--` 注释头，WITH 在正文里 —— 截到最后一个顶层 WITH
w = sql.rfind("\nWITH ")
assert w >= 0, "找不到 WITH"
body = sql[w + 1:]
if body.upper().startswith("WITH"):
    body = body[4:].lstrip()
# 最后一个顶层逗号后面是主查询 —— 用括号深度找
depth, main_at = 0, -1
for i, ch in enumerate(body):
    if ch == "(":
        depth += 1
    elif ch == ")":
        depth -= 1
    elif ch == "," and depth == 0:
        main_at = i
ctes_txt, main_txt = body[:main_at], body[main_at + 1:]
ctes = split_top_level(ctes_txt)
names = []
for c in ctes:
    c = c.strip()
    assert c.upper().startswith("T") or c[0].isalpha()
    nm = c.split("AS", 1)[0].strip().strip("`")
    names.append(nm)

if ENGINE == "qsdb":
    Q = "/home/qctc/qsdb-target-suite/release/qsdb"

    def run(q):
        # ⚠️ 不用 `SELECT count(*)` + 表格输出：count 永远返回 **1 行**（值是 0 也
        # 占一行），解析页脚 `(1 rows, …)` 会把**每个 CTE 都数成 1** —— 本批头一版
        # 就这么把 0 行的 CTE 全看成了 1，还差点冤枉 count 快捷路径（查过，守卫
        # 完好：要求 Scan 输入才短路）。改 COPY 成 CSV 数行数 —— 那是真实的行。
        import shutil, os
        out = "/tmp/bs_out"
        if os.path.isdir(out):
            shutil.rmtree(out)
        elif os.path.exists(out):
            os.remove(out)
        # ⚠️ timeout 必须给：CSV 导出偶发"文件已写完但进程不退出"
        #（疑关停竞态，2026-10-17 对账时实测多次；没 timeout 会把整条对账挂死）
        p = subprocess.run([Q, "query", "--path", "/home/qctc/qsdb-js", "--",
                            "COPY (%s) TO '%s' (FORMAT CSV)" % (q, out)],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
        t = p.stdout.decode("utf-8", "replace")
        if p.returncode != 0:
            return "ERR " + " ".join(t.split())[:150]
        n = 0
        if os.path.isdir(out):
            for root, _, fs in os.walk(out):
                for f in fs:
                    with open(os.path.join(root, f), "rb") as fh:
                        n += sum(1 for _ in fh)
        elif os.path.exists(out):
            with open(out, "rb") as fh:
                n = sum(1 for _ in fh)
        return n

    for i, nm in enumerate(names):
        prefix = "WITH " + ", ".join(c.strip() for c in ctes[: i + 1])
        q = "%s SELECT * FROM %s" % (prefix, nm)
        print("%-6s %s" % (nm, run(q)), flush=True)
else:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("bisect")
             .config("spark.sql.shuffle.partitions", "16")
             .enableHiveSupport().getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    for i, nm in enumerate(names):
        prefix = "WITH " + ", ".join(c.strip() for c in ctes[: i + 1])
        q = "%s SELECT count(*) FROM %s" % (prefix, nm)
        try:
            n = spark.sql(q).collect()[0][0]
            print("%-6s %d" % (nm, n), flush=True)
        except Exception as e:  # noqa: BLE001
            print("%-6s ERR %s" % (nm, " ".join(str(e).split())[:150]), flush=True)
