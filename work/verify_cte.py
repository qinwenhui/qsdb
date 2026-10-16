# -*- coding: utf-8 -*-
"""Dump the CTE graph of one extracted block, so liveness can be eyeballed."""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extract_daily import parse, refs_in, strip_comments

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

path = sys.argv[1]
sql = open(path, encoding='utf-8').read()
defs, mainq, _ = parse(sql)
names = [d[0] for d in defs]
refs = {n: refs_in(b, names) for n, b in defs}
main_refs = refs_in(mainq, names)

live = set()
stack = list(main_refs)
while stack:
    c = stack.pop()
    if c in live:
        continue
    live.add(c)
    stack.extend(refs.get(c, ()))

out = []
out.append('block: %s' % os.path.basename(path))
out.append('main query references: %s' % (sorted(main_refs, key=lambda x: int(x[1:])) or '-'))
out.append('')
for n, body in defs:
    first = re.sub(r'\s+', ' ', strip_comments(body).strip())[:90]
    out.append('%-4s %-5s refs=%-22s %s' % (
        n, 'LIVE' if n in live else 'DEAD',
        ','.join(sorted(refs[n], key=lambda x: int(x[1:]))) or '-', first))
out.append('')
out.append('main query head: %s' % re.sub(r'\s+', ' ', strip_comments(mainq).strip())[:200])

open(os.path.join(ROOT, 'work', 'cte_graph.txt'), 'w', encoding='utf-8', newline='\n').write('\n'.join(out) + '\n')
print('wrote work/cte_graph.txt')
