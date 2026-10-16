# -*- coding: utf-8 -*-
"""Extract per-task SQL from the daily-settlement corpus, and build the CTE
dependency graph so we can tell **which CTEs are dead** (defined but never
reached from the main query -- transitively).

ASCII-only stdout; the full report goes to a UTF-8 file.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, 'docs', '日清分方案-分层SQL清单.md')
OUTDIR = os.path.join(ROOT, 'work', 'daily')
REPORT = os.path.join(ROOT, 'work', 'report.txt')


def strip_comments(s):
    return '\n'.join(re.sub(r'--.*$', '', l) for l in s.split('\n'))


def skip_quoted(s, j):
    """If s[j] starts a string literal, return index just past it, else j."""
    c = s[j]
    if c not in ("'", '"'):
        return j
    j += 1
    while j < len(s):
        if s[j] == c:
            if j + 1 < len(s) and s[j + 1] == c:      # doubled quote escape
                j += 2
                continue
            return j + 1
        if s[j] == '\\' and c == "'":
            j += 2
            continue
        j += 1
    return j


def match_paren(s, start):
    """start points at '('. Return index of the matching ')'."""
    depth = 0
    j = start
    while j < len(s):
        c = s[j]
        if c in ("'", '"'):
            j = skip_quoted(s, j)
            continue
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
            if depth == 0:
                return j
        j += 1
    raise ValueError('unbalanced parens')


CTE_HEAD = re.compile(r'\s*([A-Za-z_]\w*)\s+AS\s*\(', re.I)
TBL = re.compile(r'\b(?:FROM|JOIN)\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)', re.I)


def parse(sql):
    """-> (defs[(name, body)], main_query, found_with)"""
    s = strip_comments(sql)
    m = re.search(r'\bWITH\b', s, re.I)
    if not m:
        return [], s, False
    i = m.end()
    # tolerate "RECURSIVE"
    mr = re.match(r'\s*RECURSIVE\b', s[i:], re.I)
    if mr:
        i += mr.end()
    defs = []
    while True:
        m2 = CTE_HEAD.match(s, i)
        if not m2:
            break
        open_paren = m2.end() - 1
        close = match_paren(s, open_paren)
        defs.append((m2.group(1), s[open_paren + 1:close]))
        i = close + 1
        m3 = re.match(r'\s*,\s*', s[i:])
        if m3 and CTE_HEAD.match(s, i + m3.end()):
            i += m3.end()
            continue
        break
    return defs, s[i:], True


def refs_in(text, names):
    out = set()
    for n in names:
        if re.search(r'(?<![A-Za-z0-9_])' + re.escape(n) + r'(?![A-Za-z0-9_])', text):
            out.add(n)
    return out


def base_tables(text, cte_names):
    out = set()
    for m in TBL.finditer(text):
        raw = m.group(1)
        head = raw.split('.')[0]
        if head in cte_names:
            continue
        out.add(raw[len('js_produce.'):] if raw.lower().startswith('js_produce.') else raw)
    return out


def main():
    text = open(SRC, encoding='utf-8').read()
    lines = text.split('\n')

    task_re = re.compile(r'^### \[(L\d+)-(\d+)\]\s*(.*)$')
    tasks = []
    for i, ln in enumerate(lines):
        m = task_re.match(ln)
        if m:
            tasks.append({'id': '%s-%s' % (m.group(1), m.group(2)),
                          'name': m.group(3).strip(), 'line': i})
    for k, t in enumerate(tasks):
        t['end'] = tasks[k + 1]['line'] if k + 1 < len(tasks) else len(lines)

    os.makedirs(OUTDIR, exist_ok=True)
    rep = []
    blocks = []

    for t in tasks:
        seg = lines[t['line']:t['end']]
        bi = 0
        j = 0
        while j < len(seg):
            if seg[j].strip().startswith('```'):
                k = j + 1
                body = []
                while k < len(seg) and not seg[k].strip().startswith('```'):
                    body.append(seg[k])
                    k += 1
                # nearest preceding "**输出 i/n → table**"
                target = ''
                for b in range(j - 1, max(-1, j - 6), -1):
                    mm = re.match(r'\*\*输出\s+\d+/\d+\s*→\s*(.+?)\*\*', seg[b].strip())
                    if mm:
                        target = mm.group(1).strip()
                        break
                bi += 1
                blocks.append({'task': t['id'], 'layer': t['id'].split('-')[0],
                               'name': t['name'], 'idx': bi, 'target': target,
                               'sql': '\n'.join(body)})
                j = k + 1
            else:
                j += 1

    n_defs_tot = n_dead_tot = 0
    dead_with_pivot = []
    live_with_pivot = []
    all_tables = {}
    rows = []

    for b in blocks:
        sql = b['sql']
        defs, mainq, has_with = parse(sql)
        names = [d[0] for d in defs]
        refs = {}
        for nm, body in defs:
            refs[nm] = refs_in(body, names)
        main_refs = refs_in(mainq, names)

        live = set()
        stack = list(main_refs)
        while stack:
            c = stack.pop()
            if c in live:
                continue
            live.add(c)
            stack.extend(refs.get(c, ()))
        dead = [n for n in names if n not in live]

        b.update(dict(n_defs=len(names), live=live, dead=dead, has_with=has_with,
                      defs=defs, main=mainq, refs=refs))

        n_defs_tot += len(names)
        n_dead_tot += len(dead)

        def has_piv(s):
            s2 = strip_comments(s)
            return bool(re.search(r'\bPIVOT\b', s2, re.I)), bool(re.search(r'\bstack\s*\(', s2, re.I))

        for n in dead:
            body = dict(defs)[n]
            p, st = has_piv(body)
            if p or st:
                dead_with_pivot.append((b['task'], b['idx'], n, p, st, len(body.split('\n'))))
        live_sql = mainq + '\n' + '\n'.join(dict(defs)[n] for n in live)
        p, st = has_piv(live_sql)
        if p or st:
            live_with_pivot.append((b['task'], b['idx'], b['name'], p, st))

        for tb in base_tables(sql, set(names)):
            all_tables[tb] = all_tables.get(tb, 0) + 1

        fn = '%s_%02d.sql' % (b['task'], b['idx'])
        body = sql.rstrip()
        if not body.endswith(';'):
            body += '\n;'
        with open(os.path.join(OUTDIR, fn), 'w', encoding='utf-8', newline='\n') as f:
            f.write('-- %s %s (block %d) -> %s\n' % (b['task'], b['name'], b['idx'], b['target']))
            f.write(body + '\n')
        b['file'] = fn

    rep.append('== 提取 ==')
    rep.append('任务 %d / SQL 块 %d' % (len(tasks), len(blocks)))
    rep.append('CTE 定义总数 %d，其中**死的** %d' % (n_defs_tot, n_dead_tot))
    nblocks_dead = sum(1 for b in blocks if b['dead'])
    rep.append('**含死 CTE 的块** %d / %d' % (nblocks_dead, len(blocks)))
    rep.append('')
    rep.append('== 死 CTE 里含 pivot/stack 的（本来会拦路，现在不必）==')
    for t, i, n, p, st, nl in dead_with_pivot:
        rep.append('  %s blk%d  %s  pivot=%s stack=%s  %d 行' % (t, i, n, p, st, nl))
    if not dead_with_pivot:
        rep.append('  （无）')
    rep.append('')
    rep.append('== 活 CTE + 主查询里含 pivot/stack 的（真拦路）==')
    for t, i, nm, p, st in live_with_pivot:
        rep.append('  %s blk%d  %s  pivot=%s stack=%s' % (t, i, nm, p, st))
    if not live_with_pivot:
        rep.append('  （无）')
    rep.append('')
    rep.append('== 死 CTE 分布（按块）==')
    for b in blocks:
        if b['dead']:
            rep.append('  %s blk%d  defs=%d dead=%s' % (b['task'], b['idx'], b['n_defs'], ','.join(b['dead'])))
    rep.append('')
    rep.append('== 引用到的基表 ==')
    for tb, c in sorted(all_tables.items(), key=lambda x: -x[1]):
        rep.append('  %-45s %d' % (tb, c))
    rep.append('')
    rep.append('== 无 WITH 的块 ==')
    for b in blocks:
        if not b['has_with']:
            rep.append('  %s blk%d %s' % (b['task'], b['idx'], b['name']))
    rep.append('')
    rep.append('== 每块一览 ==')
    for b in blocks:
        rep.append('  %s blk%d defs=%d live=%d dead=%d lines=%d -> %s' % (
            b['task'], b['idx'], b['n_defs'], len(b['live']), len(b['dead']),
            len(b['sql'].split('\n')), b['target']))

    with open(REPORT, 'w', encoding='utf-8', newline='\n') as f:
        f.write('\n'.join(rep) + '\n')

    # ASCII console summary only
    print('tasks=%d blocks=%d ctes=%d dead_ctes=%d blocks_with_dead=%d' % (
        len(tasks), len(blocks), n_defs_tot, n_dead_tot, nblocks_dead))
    print('dead_cte_with_pivot_or_stack=%d' % len(dead_with_pivot))
    print('live_pivot_or_stack_blocks=%d' % len(live_with_pivot))
    print('distinct_base_tables=%d' % len(all_tables))
    print('blocks_without_with=%d' % sum(1 for b in blocks if not b['has_with']))


if __name__ == '__main__':
    main()
