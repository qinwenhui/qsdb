#!/usr/bin/env python3
"""日清分方案测试平台：**按真实任务依赖（DAG）驱动**，两个引擎共用一套调度。

规则（用户 2026-10-17 定）：
  * 任务之间按 `日清分方案-任务依赖.json` 的依赖关系走 ——
    **没有依赖的任务立刻并行跑**，有依赖的**等依赖全部跑完**再跑；
  * **同一个任务内的多个 SQL 块并行**。

用量：
  qsdb  : python3 run_dag.py --engine qsdb  --dir /home/qctc/daily_plain --out /home/qctc/dag_qsdb.tsv
  spark : spark-submit --jars <mysql.jar> run_dag.py --engine spark --dir /home/qctc/daily --out /home/qctc/dag_spark.tsv

内存：**只对 qsdb 采**（用户：Spark 那边难记，可以不用）。
用 `/proc/<pid>/status` 的 `VmHWM`（峰值 RSS 高水位）**边跑边采**取最大 ——
`.14` 上没有 GNU `/usr/bin/time`，而 VmHWM 是单调的，进程结束前最后一次采到就是真峰值。

输出 TSV：
  OK\t<task>\t<block>\t<ms>\t<rss_kb>\t<rows>\t
  ERR\t<task>\t<block>\t<ms>\t<rss_kb>\t<rows>\t<msg>
  TASK\t<task>\t<wall_ms>\t<blocks>\t<peak_rss_kb>\t<start_offset_ms>
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

Q = "/home/qctc/qsdb-target-suite/release/qsdb"
DB = "/home/qctc/qsdb-js"
DAGFILE = "/home/qctc/dag.json"
LOCK = threading.Lock()


def read_rss_kb(pid):
    """读 `/proc/<pid>/status` 的 VmHWM（峰值 RSS，KB）。进程没了返回 None。"""
    try:
        with open("/proc/%d/status" % pid) as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


def run_qsdb(sql, tmp):
    """跑一条，返回 (ok, ms, rss_peak_kb, out_text)。

    ⚠️ **输出重定向到文件，不用 `subprocess.PIPE`**：
    用 PIPE 而等进程退出后才 `read()` 会**死锁** —— 管道缓冲区只有 64KB，
    输出超过就子进程阻塞在写、永远不退出（2026-10-17 实测踩到，第一批跑批卡死）。
    """
    t0 = time.perf_counter()
    with open(tmp, "wb") as fo:
        p = subprocess.Popen([Q, "query", "--path", DB, "--", sql],
                             stdout=fo, stderr=subprocess.STDOUT)
        peak = 0
        while p.poll() is None:
            v = read_rss_kb(p.pid)
            if v and v > peak:
                peak = v
            time.sleep(0.01)
    try:
        with open(tmp, "rb") as fi:
            out = fi.read().decode("utf-8", "replace")
    except OSError:
        out = ""
    ms = int((time.perf_counter() - t0) * 1000)
    return (p.returncode == 0, ms, peak, out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=["qsdb", "spark"], required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--dag", default=DAGFILE)
    ap.add_argument("--max-tasks", type=int, default=0, help="并发任务数上限（0=不限，走真实场景）")
    # **把结果真的落表**（生产里每条查询都是要写回表的）——
    # 但**写进 `bench_<块名>` 这样的新表**，绝不碰现有的 `t_*` 表（那些已经有数据，
    # 回写会污染）。表名带 `-` 不合法，换成 `_`。
    ap.add_argument("--write", action="store_true",
                    help="把 SELECT 包成 CREATE TABLE bench_<块> AS …（计上写入时间）")
    # **分区 A/B**：把表名指向 `pdate` 分区副本 `<t>_p`（由 partition_all.py 建的）。
    # 原表不动 ⇒ 想回退就把这个开关去掉。
    ap.add_argument("--pdate-partitioned", action="store_true",
                    help="表名改指向 <t>_p 分区副本（A/B 用）")
    # **server 模式**（生产形态）：`qsdb server` 常驻，全部查询走
    # `POST /api/v1/query` —— 热数据缓存跨查询共享（CLI 子进程模式每块冷起）。
    # 内存口径随之改变：不再有"每块 VmHWM"，采的是 **server 进程的 VmHWM**
    #（结束后读一次，就是常驻形态的真实足迹）。
    ap.add_argument("--server", default=None, metavar="URL",
                    help="如 http://127.0.0.1:9432 —— 走常驻 server 而不是每块起子进程")
    args = ap.parse_args()

    dag = json.load(open(args.dag, encoding="utf-8"))
    blocks_of = dag["blocks_by_task"]
    deps_of = dag["dag"]
    tasks = sorted(blocks_of)

    spark = None
    rewrite = None
    if args.engine == "spark":
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.appName("daily-dag")
                 .config("spark.sql.shuffle.partitions", "16")
                 .enableHiveSupport().getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        # 表在 `js_bench` 库（刻意灌进新库，见 load_tables.py 的说明）
        rewrite = lambda s: s.replace("js_produce.", "js_bench.").replace("js_data.", "js_bench.")
    else:
        # ⚠️ qsdb 是**单库模型**，`库.表` 限定名不支持 —— 语料里全是
        # `js_produce.` / `js_data.` 前缀（266 处），全部剥掉（L0-06_01 当初
        # 就是漏网的 js_data. 前缀）。口径与交接文档一致：**数据侧改写，
        # 引擎不收限定名**（收了就得定义"前缀不匹配算什么错"，没有需求不加）。
        rewrite = lambda s: s.replace("js_produce.", "").replace("js_data.", "")

    # 分区副本表名（`_p` 结尾）—— 只在开了开关时用
    ptabs = []
    if args.pdate_partitioned:
        # ⚠️ **从库目录取表名，不用 `SHOW TABLES`**：CLI 的输出列**会截断长名字**
        # （加 `…`），照着数只能数到短名的那些 —— 实测 68 张只数出 23 张，
        # 于是 A/B 只改写了三分之一，数字看着"没什么变化"纯属假象。
        _d = os.path.join(DB, "default")
        ptabs = [n for n in sorted(os.listdir(_d))
                 if n.startswith("t_") and n.endswith("_p")
                 and os.path.isdir(os.path.join(_d, n))]
        # 长的先替，避免前缀重叠（`\b` 已经挡住 `unit` vs `unit_report`，双保险）
        ptabs.sort(key=len, reverse=True)
        print("分区副本 %d 张" % len(ptabs), flush=True)

    def to_partitioned(sql):
        import re as _re
        for b in ptabs:
            sql = _re.sub(r"\b%s\b" % _re.escape(b[:-2]), b, sql)
        return sql

    out_rows = []
    t_start = time.perf_counter()

    # server 模式的 HTTP 客户端（stdlib，Python 3.6 兼容；csv 格式零解析歧义）
    server_url = args.server
    if server_url:
        import urllib.request

        def server_query(sql, timeout=120):
            body = json.dumps({"sql": sql, "format": "csv"}).encode("utf-8")
            req = urllib.request.Request(
                server_url.rstrip("/") + "/api/v1/query", data=body,
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read().decode("utf-8", "replace")

    def run_block(task, blk):
        path = os.path.join(args.dir, blk + ".sql")
        sql = open(path, encoding="utf-8").read().strip().rstrip().rstrip(";")
        if rewrite:
            sql = rewrite(sql)
        if args.pdate_partitioned:
            sql = to_partitioned(sql)
        if args.write and args.engine == "qsdb":
            tname = "bench_" + blk.replace("-", "_")
            sql = "CREATE TABLE %s AS %s" % (tname, sql)
        if server_url:
            t0 = time.perf_counter()
            try:
                status, txt = server_query(sql)
                ok = status == 200
                # csv 首行是表头；行数 = 数据行（空结果只有表头，甚至没有）
                rows = max(0, txt.count("\n") - 1) if txt else 0
                msg = "" if ok else " ".join(txt.split())[:300]
            except Exception as e:  # noqa: BLE001
                rows, ok, msg = 0, False, " ".join(str(e).split())[:300]
            ms = int((time.perf_counter() - t0) * 1000)
            return ("OK" if ok else "ERR", task, blk, ms, 0, rows, msg)
        if args.engine == "qsdb":
            tmp = "/tmp/qsdbrun_%s_%s.txt" % (task, blk)
            ok, ms, rss, txt = run_qsdb(sql, tmp)
            rows = txt.count("\n") if ok else 0
            msg = "" if ok else " ".join(txt.split())[:300]
        else:
            # Spark 侧**不采内存**（用户：难记，可以不用）
            t0 = time.perf_counter()
            try:
                rows = len(spark.sql(sql).collect())
                ok, rss, msg = True, 0, ""
            except Exception as e:  # noqa: BLE001
                rows, ok, rss = 0, False, 0
                msg = " ".join(str(e).split())[:300]
            ms = int((time.perf_counter() - t0) * 1000)
        return ("OK" if ok else "ERR", task, blk, ms, rss, rows, msg)

    def run_task(task):
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=max(1, len(blocks_of[task]))) as ex:
            res = list(ex.map(lambda b: run_block(task, b), blocks_of[task]))
        wall = int((time.perf_counter() - t0) * 1000)
        peak = max(r or 0 for _, _, _, _, r, _, _ in res)
        return res, wall, peak, int((t0 - t_start) * 1000)

    # ---- DAG 调度：没有依赖的任务立刻并行；有依赖的等依赖全完 ----
    started, finished = set(), set()
    futures = {}
    cap = args.max_tasks or len(tasks)
    with ThreadPoolExecutor(max_workers=cap) as pool:
        while len(finished) < len(tasks):
            ready = [t for t in tasks
                     if t not in started and all(d in finished for d in deps_of.get(t, []))]
            for t in ready:
                started.add(t)
                futures[t] = pool.submit(run_task, t)
            if not futures:
                sys.stderr.write("!! 依赖图有环或悬空，剩下 %d 个任务跑不动\n" % (len(tasks) - len(finished)))
                break
            done, _ = wait(list(futures.values()), return_when=FIRST_COMPLETED)
            for f in done:
                t = next(k for k, v in futures.items() if v is f)
                res, wall, peak, off = f.result()
                del futures[t]
                finished.add(t)
                with LOCK:
                    for st, tk, blk, ms, rss, rows, msg in res:
                        out_rows.append("%s\t%s\t%s\t%d\t%d\t%d\t%s" % (st, tk, blk, ms, rss, rows, msg))
                    out_rows.append("TASK\t%s\t%d\t%d\t%d\t%d" % (t, wall, len(blocks_of[t]), peak, off))

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out_rows) + "\n")
    ok = sum(1 for r in out_rows if r.startswith("OK\t"))
    err = sum(1 for r in out_rows if r.startswith("ERR\t"))
    total = int((time.perf_counter() - t_start) * 1000)
    print("=== %s：OK %d ERR %d  总墙钟 %d ms ===" % (args.engine, ok, err, total), flush=True)
    if server_url:
        # 常驻形态的内存口径：采 server 进程的 VmHWM（峰值 RSS 高水位）
        import re as _re2
        pid = subprocess.run(["pgrep", "-f", "qsdb server --path %s" % DB],
                             stdout=subprocess.PIPE).stdout.decode().split()
        if pid:
            try:
                with open("/proc/%s/status" % pid[0]) as f:
                    for line in f:
                        if line.startswith("VmHWM:"):
                            print("server VmHWM: %s kB (pid %s)" % (line.split()[1], pid[0]),
                                  flush=True)
                            break
            except OSError:
                pass


main()
