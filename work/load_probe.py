#!/usr/bin/env python3
"""并发探针：N 线程打 qsdb server 的 /api/v1/query，报吞吐/分位/错误。

用法：python3 load_probe.py <url> <并发数> <每线程查询数> <light|med|point>
输出单行 JSON：{"C":..,"kind":..,"ok":..,"err":..,"err_codes":{..},"wall_s":..,
                "qps":..,"p50_ms":..,"p95_ms":..,"max_ms":..}
口径：纯 stdlib（urllib + threading，与 run_dag.py 同款）；token 未启用时免认证。
"""

import json
import sys
import threading
import time
import urllib.request
from collections import Counter

QUERIES = {
    "light": "SELECT count(*) FROM t",
    "med": "SELECT g, count(*) c, sum(v) s FROM t GROUP BY g",
    "point": "SELECT count(*) FROM t WHERE id = 1234567",
}


def main():
    url, conc, per, kind = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
    sql = QUERIES[kind]
    lat: list = []
    lock = threading.Lock()
    codes: Counter = Counter()
    stop = time.time() + 600

    def worker():
        local = []
        body = json.dumps({"sql": sql}).encode()
        for _ in range(per):
            if time.time() > stop:
                break
            t0 = time.time()
            code = "ok"
            try:
                req = urllib.request.Request(
                    url.rstrip("/") + "/api/v1/query", data=body,
                    headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=120) as r:
                    r.read()
            except urllib.error.HTTPError as e:
                code = str(e.code)
                e.read()
            except Exception:
                code = "net"
            local.append((time.time() - t0) * 1000.0)
            with lock:
                lat.append(local[-1])
                codes[code] += 1

    threads = [threading.Thread(target=worker) for _ in range(conc)]
    t_start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.time() - t_start
    lat.sort()
    n = len(lat)

    def pct(p):
        return lat[min(n - 1, int(n * p))] if n else 0.0

    ok = codes.get("ok", 0)
    print(json.dumps({
        "C": conc, "kind": kind, "queries": n, "ok": ok,
        "err": n - ok, "err_codes": dict(codes),
        "wall_s": round(wall, 2),
        "qps": round(ok / wall, 1) if wall > 0 else 0.0,
        "p50_ms": round(pct(0.50), 1),
        "p95_ms": round(pct(0.95), 1),
        "max_ms": round(lat[-1], 1) if n else 0.0,
    }))


if __name__ == "__main__":
    main()
