# -*- coding: utf-8 -*-
"""把 docs/日清分-总报告.md 渲染成**单文件 HTML 汇报**（无外部依赖，可离线/直接发）。

用法：python3 work/build_html_report.py   # 读 md，写 docs/日清分-总报告.html
数据口径与 md 同源；图表为内联 SVG + 原生 JS（柱状/条形 + 悬浮提示 + 明暗主题）。
调色板为 dataviz 参考实例（已跑 validate_palette：明/暗两模式 2 系列 ALL PASS）。
"""
import json
import re
import sys

MD = "docs/日清分-总报告.md"
OUT = "docs/日清分-总报告.html"

md = open(MD, encoding="utf-8").read()


def grab_table(header_re):
    """抓 md 里紧跟 header_re 的第一张管道表 → [[c1..cn], ...]"""
    m = re.search(header_re + r"\s*\n\|[^\n]+\|\n\|[-| :]+\|\n((?:\|[^\n]+\n)+)", md)
    if not m:
        return []
    rows = []
    for line in m.group(1).strip().split("\n"):
        rows.append([c.strip() for c in line.strip("|").split("|")])
    return rows


def grab_line(pat):
    m = re.search(pat, md)
    return m.group(1) if m else ""


# ── 从 md 抽数据 ──────────────────────────────────────────────────────
slowest = grab_table(r"### 最慢的 10 个块（跨轮中位）")
input_tbl = grab_table(r"## 一、输入表（57 张）")
block_tbl = grab_table(r"## 二、每个查询块：引用面 → 输出量 → 墙钟")
rounds_line = grab_line(r"- 5 轮 server 运行 \*\*全部 154/154 OK、0 ERR\*\*：([\d、]+)")
rounds = [int(x) for x in rounds_line.split("、")]
chain_line = grab_line(r"关键链：([\s\S]+?)\n")
chain = [(a, int(b)) for a, b in re.findall(r"([L][\d-]+)\((\d+)\)", chain_line)]
floor = int(grab_line(r"\| DAG 结构地板（关键链） \| ([\d,]+) ms \|").replace(",", ""))

SLOW = [[r[0], int(r[1].replace(",", "")), int(r[2].replace(",", "")),
         int(r[3].replace(",", "")), r[4].replace(",", "")] for r in slowest]
INPUT = [[r[0], r[1].replace(",", ""), r[2].replace(",", ""), r[3].replace(",", ""), r[4]] for r in input_tbl]
BLOCKS = [[r[0], r[1], r[2], r[3], r[4], r[5]] for r in block_tbl]

HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>日清分计算总报告 · QSDB</title>
<style>
  .report {
    color-scheme: light;
    --surface-0: #f6f5f2;  --surface-1: #fcfcfb;  --border: #e5e4e0;
    --text-1: #0b0b0b;     --text-2: #52514e;     --text-3: #8a8984;
    --grid: #f0efec;       --track: #cde2fb;
    --s1: #2a78d6;  --s2: #eb6834;
    --good: #0ca30c; --good-ink: #006300;
    --tip-bg: #ffffff; --tip-bd: #d8d7d2;
  }
  .report.dark {
    color-scheme: dark;
    --surface-0: #141413;  --surface-1: #1a1a19;  --border: #2e2e2c;
    --text-1: #ffffff;     --text-2: #c3c2b7;     --text-3: #8f8e86;
    --grid: #262624;       --track: #1c3a63;
    --s1: #3987e5;  --s2: #d95926;
    --good: #0ca30c; --good-ink: #4fc24f;
    --tip-bg: #232322; --tip-bd: #3a3a37;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--surface-0); }
  .report {
    font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", system-ui, sans-serif;
    color: var(--text-1); background: var(--surface-0);
    max-width: 1100px; margin: 0 auto; padding: 28px 20px 60px;
    font-size: 14px; line-height: 1.55;
  }
  .rep-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
  h1 { font-size: 24px; margin: 0 0 6px; font-weight: 650; }
  .meta { color: var(--text-2); font-size: 13px; }
  .meta b { color: var(--text-1); font-weight: 600; }
  .chip { display: inline-block; border: 1px solid var(--border); border-radius: 999px;
          padding: 1px 10px; margin: 2px 4px 2px 0; font-size: 12px; color: var(--text-2); background: var(--surface-1); }
  .toggle { border: 1px solid var(--border); background: var(--surface-1); color: var(--text-2);
            border-radius: 8px; padding: 5px 12px; font-size: 13px; cursor: pointer; }
  h2 { font-size: 17px; margin: 30px 0 10px; font-weight: 650; }
  h2 .sub { font-weight: 400; color: var(--text-3); font-size: 13px; margin-left: 8px; }
  .kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(165px, 1fr)); gap: 10px; margin-top: 14px; }
  .tile { background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px; padding: 14px 16px; }
  .tile .lb { font-size: 12.5px; color: var(--text-2); }
  .tile .v { font-size: 27px; font-weight: 600; margin-top: 3px; letter-spacing: -0.01em; }
  .tile .v small { font-size: 14px; font-weight: 500; color: var(--text-2); margin-left: 2px; }
  .tile .sub { font-size: 12px; color: var(--text-3); margin-top: 3px; }
  .hero .v { font-size: 40px; }
  .status { display: flex; align-items: center; gap: 8px; background: var(--surface-1);
            border: 1px solid var(--border); border-radius: 12px; padding: 12px 16px; margin-top: 10px;
            font-size: 13.5px; color: var(--text-2); flex-wrap: wrap; }
  .status .ok { color: var(--good-ink); font-weight: 600; }
  .status .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--good);
                 box-shadow: 0 0 0 3px color-mix(in srgb, var(--good) 18%, transparent); }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  @media (max-width: 860px) { .grid2 { grid-template-columns: 1fr; } }
  .card { background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px; padding: 16px 18px 10px; }
  .card h3 { margin: 0 0 2px; font-size: 14.5px; font-weight: 600; }
  .card .cap { color: var(--text-3); font-size: 12.5px; margin-bottom: 10px; }
  svg { display: block; width: 100%; height: auto; }
  svg text { font-family: inherit; }
  .tip { position: fixed; z-index: 9; pointer-events: none; background: var(--tip-bg);
         border: 1px solid var(--tip-bd); border-radius: 8px; padding: 7px 10px;
         font-size: 12.5px; color: var(--text-1); box-shadow: 0 4px 14px rgba(0,0,0,.14);
         opacity: 0; transition: opacity .08s; max-width: 340px; }
  .tip b { font-weight: 650; }
  .tip .k { color: var(--text-2); }
  .note { color: var(--text-3); font-size: 12.5px; margin-top: 8px; }
  details { background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px;
            padding: 12px 18px; margin-top: 14px; }
  summary { cursor: pointer; font-weight: 600; font-size: 14.5px; }
  .tblwrap { overflow-x: auto; margin-top: 10px; }
  table { border-collapse: collapse; width: 100%; font-size: 12.5px;
          font-variant-numeric: tabular-nums; }
  th { text-align: left; color: var(--text-2); font-weight: 600; padding: 6px 10px;
       border-bottom: 1px solid var(--border); white-space: nowrap; }
  td { padding: 5px 10px; border-bottom: 1px solid var(--grid); white-space: nowrap; }
  td.num, th.num { text-align: right; }
  .foot { margin-top: 26px; color: var(--text-3); font-size: 12.5px; }
</style>
</head>
<body>
<div class="report" id="report">
  <div class="rep-head">
    <div>
      <h1>日清分计算总报告</h1>
      <div class="meta">
        口径 <b>2025-10-01</b> · 版本 <b>78_20251001_1</b>（全库数据最多的日期 × 中间表数据最多的版本）<br>
        <span class="chip">118 任务 / 154 SQL 块</span><span class="chip">12 层依赖</span>
        <span class="chip">68 张 pdate 分区表</span><span class="chip">qsdb server 常驻</span>
        <span class="chip">.14 真机 16 核</span>
      </div>
    </div>
    <button class="toggle" onclick="toggleTheme()">明暗切换</button>
  </div>

  <div class="kpis">
    <div class="tile hero"><div class="lb">总墙钟 · 中位（5 轮）</div>
      <div class="v">2,098<small>ms</small></div>
      <div class="sub">最快 2,003 · 最慢 2,180 · 极差 8.1%</div></div>
    <div class="tile"><div class="lb">正确性</div><div class="v">154<span style="font-size:16px;color:var(--text-2)">/154</span></div>
      <div class="sub">5 轮全过 · 0 错误</div></div>
    <div class="tile"><div class="lb">输入面 / 轮</div><div class="v">3,091<small>万行</small></div>
      <div class="sub">按块计含重复读；版本切片 269 万</div></div>
    <div class="tile"><div class="lb">输出</div><div class="v">78,376<small>行</small></div>
      <div class="sub">154 块合计</div></div>
    <div class="tile"><div class="lb">吞吐（输入面）</div><div class="v">1,473<small>万行/秒</small></div>
      <div class="sub">版本切片口径 128 万行/秒</div></div>
    <div class="tile"><div class="lb">常驻内存 · 峰值</div><div class="v">993<small>MB</small></div>
      <div class="sub">每查询峰值中位 23 MB</div></div>
  </div>

  <div class="status"><span class="dot"></span>
    <span class="ok">全部通过</span>
    <span>5 轮运行 154/154 块 OK、0 错误（轮次墙钟：<b id="roundStr"></b> ms）；
    旧切片上 147 块与真 Spark 行数逐块一致、26 块逐格+列名对账一致。</span>
  </div>

  <h2>执行时间 <span class="sub">统计零侵入：计时在客户端、内存读 /proc、引擎热路径未动</span></h2>
  <div class="grid2">
    <div class="card">
      <h3>5 轮总墙钟</h3>
      <div class="cap">ms · 单位越低越好；轮次间极差 8.1%（无长尾漂移）</div>
      <div id="c_rounds"></div>
    </div>
    <div class="card">
      <h3>关键链（DAG 串行地板）</h3>
      <div class="cap">八跳合计 <b id="floorStr"></b> ms —— 墙钟的结构性下限</div>
      <div id="c_chain"></div>
    </div>
  </div>

  <div class="card" style="margin-top:14px">
    <h3>最慢的 10 个块（跨轮中位）</h3>
    <div class="cap">条 = 中位；浅色轨道 = 最快～最慢区间；悬浮看明细</div>
    <div id="c_slow"></div>
  </div>

  <h2>内存</h2>
  <div class="kpis">
    <div class="tile"><div class="lb">常驻 · 峰值 VmHWM</div><div class="v">993<small>MB</small></div>
      <div class="sub">服务开着要多少（含热缓存）</div></div>
    <div class="tile"><div class="lb">常驻 · 稳态 VmRSS</div><div class="v">900<small>MB</small></div>
      <div class="sub">5 轮中位</div></div>
    <div class="tile"><div class="lb">每查询峰值 · 中位</div><div class="v">23<small>MB</small></div>
      <div class="sub">子进程口径</div></div>
    <div class="tile"><div class="lb">每查询峰值 · 最大</div><div class="v">366<small>MB</small></div>
      <div class="sub">单查询上界样本</div></div>
  </div>

  <h2>数据量与吞吐 <span class="sub">中位轮 2,098 ms</span></h2>
  <div class="grid2">
    <div class="card">
      <h3>输入面 Top 10 表（2025-10-01）</h3>
      <div class="cap">蓝 = 当天行数；橙 = 其中版本 78_20251001_1 切片</div>
      <div id="c_input"></div>
      <div class="note">基表（t_cal_*）多数无 version_id 列 —— 版本只活在登记表与中间表。</div>
    </div>
    <div class="card">
      <h3>吞吐与总账</h3>
      <div class="cap">中位轮口径</div>
      <table>
        <tr><th>指标</th><th class="num">值</th></tr>
        <tr><td>输入面（按块计，含重复读）· 当天</td><td class="num">30,907,852 行</td></tr>
        <tr><td>输入面 · 版本切片</td><td class="num">2,688,429 行</td></tr>
        <tr><td>输入吞吐 · 当天口径</td><td class="num"><b>≈1,473 万行/秒</b></td></tr>
        <tr><td>输入吞吐 · 版本切片口径</td><td class="num">≈128 万行/秒</td></tr>
        <tr><td>输出合计</td><td class="num">78,376 行（≈3.7 万行/秒）</td></tr>
        <tr><td>输出非 0 的块</td><td class="num">104 / 154</td></tr>
        <tr><td>块墙钟合计（含争用）</td><td class="num">11,929 ms</td></tr>
      </table>
      <div class="note">上层块的输出（t_dnl_* 中间表的本版本切片）即下层块的输入 —— 依赖关系由此而来。</div>
    </div>
  </div>

  <details>
    <summary>附录 A · 输入表台账（57 张全量）</summary>
    <div class="tblwrap"><table id="t_input"></table></div>
  </details>
  <details>
    <summary>附录 B · 逐块台账（154 块全量：引用面 → 输出量 → 墙钟）</summary>
    <div class="tblwrap"><table id="t_blocks"></table></div>
  </details>

  <div class="foot">
    统计零侵入：计时在客户端完成、内存读 /proc、引擎热路径一行未动；
    本报告由 <code>work/daily_report.py</code>（数据）与 <code>work/build_html_report.py</code>（本页）自动生成。
  </div>
</div>
<div class="tip" id="tip"></div>

<script>
window.onerror = function (msg, src, line, col) {
  var d = document.createElement('div');
  d.style.cssText = 'position:fixed;top:0;left:0;right:0;background:#d03b3b;color:#fff;padding:8px;font:12px monospace;z-index:99';
  d.textContent = 'JS ERROR: ' + msg + ' @' + line + ':' + col;
  document.body.appendChild(d);
};
const DATA = __DATA__;

/* ── 主题 ── */
const rep = document.getElementById('report');
function setTheme(t) {
  rep.classList.toggle('dark', t === 'dark');
  try { localStorage.setItem('rep-theme', t); } catch (e) {}
  if (typeof fmt === 'function') renderAll();
}
function toggleTheme() {
  setTheme(rep.classList.contains('dark') ? 'light' : 'dark');
}
function initTheme() {
  let t = null;
  try { t = localStorage.getItem('rep-theme'); } catch (e) {}
  if (!t) t = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  rep.classList.toggle('dark', t === 'dark');
}
function C() {
  const d = rep.classList.contains('dark');
  return d
    ? { s1: '#3987e5', s2: '#d95926', track: '#1c3a63', grid: '#262624',
        t1: '#ffffff', t2: '#c3c2b7', t3: '#8f8e86', bd: '#2e2e2c' }
    : { s1: '#2a78d6', s2: '#eb6834', track: '#cde2fb', grid: '#f0efec',
        t1: '#0b0b0b', t2: '#52514e', t3: '#8a8984', bd: '#e5e4e0' };
}
const fmt = n => Number(n).toLocaleString('en-US');

/* ── 提示层 ── */
const tip = document.getElementById('tip');
function tipShow(html, ev) {
  tip.innerHTML = html; tip.style.opacity = 1;
  const x = Math.min(ev.clientX + 14, window.innerWidth - 360);
  tip.style.left = x + 'px';
  tip.style.top = (ev.clientY + 14) + 'px';
}
function tipHide() { tip.style.opacity = 0; }

/* ── SVG 帮手：只在数据端做 4px 圆角（贴基线端直角）── */
function colTop(x, y, w, h, r) {
  r = Math.min(r, w / 2, h);
  return `M${x},${y + h} L${x},${y + r} Q${x},${y} ${x + r},${y} L${x + w - r},${y} Q${x + w},${y} ${x + w},${y + r} L${x + w},${y + h} Z`;
}
function barRight(x, y, w, h, r) {
  r = Math.min(r, w, h / 2);
  return `M${x},${y} L${x + w - r},${y} Q${x + w},${y} ${x + w},${y + r} L${x + w},${y + h - r} Q${x + w},${y + h} ${x + w - r},${y + h} L${x},${y + h} Z`;
}

/* ── ① 5 轮墙钟：柱状，单色（量级比较）── */
function drawRounds() {
  const el = document.getElementById('c_rounds');
  const W = el.clientWidth || 480, H = 210, padL = 46, padB = 26, padT = 18;
  const data = DATA.rounds, max = 2400;
  const iw = W - padL - 12, ih = H - padT - padB;
  const med = DATA.roundMedian;
  let s = `<svg viewBox="0 0 ${W} ${H}">`;
  for (const g of [1200, 1800, 2400]) {
    const y = padT + ih - ih * g / max;
    s += `<line x1="${padL}" y1="${y}" x2="${W - 8}" y2="${y}" stroke="${C().grid}" stroke-width="1"/>` +
         `<text x="${padL - 6}" y="${y + 4}" text-anchor="end" font-size="10.5" fill="${C().t3}">${g / 1000}s</text>`;
  }
  const bw = 40, gap = (iw - bw * data.length) / (data.length + 1);
  data.forEach((v, i) => {
    const x = padL + gap + i * (bw + gap);
    const h = ih * v / max, y = padT + ih - h;
    const isMed = v === med;
    s += `<path d="${colTop(x, y, bw, h, 4)}" fill="${C().s1}" opacity="${isMed ? 1 : 0.82}"/>` +
         `<text x="${x + bw / 2}" y="${y - 5}" text-anchor="middle" font-size="11" fill="${C().t2}">${fmt(v)}</text>` +
         `<rect x="${x - gap / 2}" y="${padT}" width="${bw + gap}" height="${ih}" fill="transparent"/>` +
         `<text x="${x + bw / 2}" y="${padT + ih + 16}" text-anchor="middle" font-size="11" fill="${C().t3}">第 ${i + 1} 轮${isMed ? '（中位）' : ''}</text>`;
  });
  s += `<line x1="${padL}" y1="${padT + ih}" x2="${W - 8}" y2="${padT + ih}" stroke="${C().bd}" stroke-width="1"/></svg>`;
  el.innerHTML = s;
  el.querySelectorAll('rect[fill="transparent"]').forEach((r, i) => {
    r.addEventListener('pointermove', ev => tipShow(
      `<b>第 ${i + 1} 轮</b>　<b>${fmt(data[i])} ms</b><br><span class="k">154 块 · 0 错误</span>`, ev));
    r.addEventListener('pointerleave', tipHide);
  });
}

/* ── 通用水平条 ── */
function hbars(elId, rows, opts) {
  const el = document.getElementById(elId);
  const rowH = opts.rowH || 30, gap = opts.gap != null ? opts.gap : 8;
  const W = el.clientWidth || 480, padL = opts.padL || 118, padR = 64;
  const H = rows.length * (rowH + gap) + 8;
  const iw = W - padL - padR;
  let s = `<svg viewBox="0 0 ${W} ${H}">`;
  rows.forEach((r, i) => {
    const y = 4 + i * (rowH + gap);
    s += `<text x="${padL - 8}" y="${y + rowH / 2 + 4}" text-anchor="end" font-size="11.5" fill="${C().t2}">${r.label}</text>`;
    if (opts.track) {
      const tx = padL + iw * r.trackMin / opts.max, tw = iw * (r.trackMax - r.trackMin) / opts.max;
      s += `<rect x="${tx}" y="${y + rowH / 2 - 2}" width="${Math.max(tw, 2)}" height="4" rx="2" fill="${C().track}"/>`;
    }
    const w = Math.max(iw * r.value / opts.max, 2);
    s += `<path d="${barRight(padL, y + 2, w, rowH - 4, 4)}" fill="${r.color || C().s1}"/>` +
         `<text x="${padL + w + 7}" y="${y + rowH / 2 + 4}" font-size="11" fill="${C().t2}">${r.dlabel}</text>` +
         `<rect x="0" y="${y - gap / 2}" width="${W}" height="${rowH + gap}" fill="transparent"/>`;
  });
  s += '</svg>';
  el.innerHTML = s;
  el.querySelectorAll('rect[fill="transparent"]').forEach((r, i) => {
    r.addEventListener('pointermove', ev => tipShow(opts.tip(rows[i]), ev));
    r.addEventListener('pointerleave', tipHide);
  });
}

/* ── ② 关键链 ── */
function drawChain() {
  hbars('c_chain', DATA.chain.map(c => ({
    label: c[0], value: c[1], dlabel: fmt(c[1]) + ' ms',
    tipHtml: `<b>${c[0]}</b>　<b>${fmt(c[1])} ms</b>` })), {
    max: 480, rowH: 22, gap: 7, padL: 76,
    tip: r => r.tipHtml });
}

/* ── ③ 最慢 10 块：中位条 + 最快～最慢轨道 ── */
function drawSlow() {
  hbars('c_slow', DATA.slowest.map(r => ({
    label: r[0], value: r[1], min: r[2], max: r[3], rows: r[4],
    trackMin: r[2], trackMax: r[3], dlabel: fmt(r[1]) })), {
    max: 480, rowH: 24, gap: 9, padL: 92, track: true,
    tip: r => `<b>${r.label}</b>　中位 <b>${fmt(r.value)} ms</b><br>` +
              `<span class="k">最快 ${fmt(r.min)} · 最慢 ${fmt(r.max)} · 输出 ${fmt(r.rows)} 行</span>` });
}

/* ── ④ 输入面 Top10：双系列（当天 / 版本切片）── */
function drawInput() {
  const el = document.getElementById('c_input');
  const rows = DATA.input.slice(0, 10);   // 卡片只画 Top 10；全量在附录 A
  const rowH = 15, gap = 13, W = el.clientWidth || 480, padL = 168, padR = 70;
  const H = rows.length * (rowH * 2 + gap) + 10;
  const iw = W - padL - padR;
  const max = 560000;
  let s = `<svg viewBox="0 0 ${W} ${H}">
    <rect x="${padL}" y="0" width="10" height="10" rx="2" fill="${C().s1}"/>
    <text x="${padL + 15}" y="9" font-size="11" fill="${C().t2}">当天行数</text>
    <rect x="${padL + 78}" y="0" width="10" height="10" rx="2" fill="${C().s2}"/>
    <text x="${padL + 93}" y="9" font-size="11" fill="${C().t2}">版本切片</text>`;
  rows.forEach((r, i) => {
    // INPUT 行 = [表名, 全表, 当天, 版本切片, 被引用数] —— 图里画 **当天/版本**，
    // 全表只在悬浮里出现（此前错拿全表画"当天"，蓝条全部顶格，量级全错）。
    const y = 20 + i * (rowH * 2 + gap);
    s += `<text x="${padL - 8}" y="${y + rowH + 3}" text-anchor="end" font-size="10.5" fill="${C().t2}">${r[0]}</text>`;
    const day = Number(r[2]);
    const w1 = Math.max(iw * day / max, 2);
    s += `<path d="${barRight(padL, y, w1, rowH - 2, 3)}" fill="${C().s1}"/>` +
         `<text x="${padL + w1 + 5}" y="${y + rowH - 4}" font-size="9.5" fill="${C().t2}">${day >= 10000 ? (day / 10000).toFixed(1) + '万' : fmt(day)}</text>`;
    if (r[3] !== '—') {
      const v = Number(r[3]);
      const w2 = Math.max(iw * v / max, 2);
      s += `<path d="${barRight(padL, y + rowH, w2, rowH - 2, 3)}" fill="${C().s2}"/>` +
           `<text x="${padL + w2 + 5}" y="${y + rowH * 2 - 4}" font-size="9.5" fill="${C().t2}">${v >= 10000 ? (v / 10000).toFixed(1) + '万' : fmt(v)}</text>`;
    } else {
      s += `<text x="${padL + 4}" y="${y + rowH * 2 - 4}" font-size="9.5" fill="${C().t3}">无 version_id 列</text>`;
    }
    s += `<rect x="0" y="${y - 4}" width="${W}" height="${rowH * 2 + gap}" fill="transparent"/>`;
  });
  s += '</svg>';
  el.innerHTML = s;
  el.querySelectorAll('rect[fill="transparent"]').forEach((r, i) => {
    const d = DATA.input[i];
    r.addEventListener('pointermove', ev => tipShow(
      `<b>${d[0]}</b><br><span class="k">全表</span> <b>${d[1]}</b>　` +
      `<span class="k">当天</span> <b>${d[2]}</b><br><span class="k">版本切片</span> <b>${d[3]}</b>　` +
      `<span class="k">被 ${d[4]} 块引用</span>`, ev));
    r.addEventListener('pointerleave', tipHide);
  });
}

/* ── 附录表 ── */
function fillTable(id, head, rows, numCols) {
  const t = document.getElementById(id);
  let s = '<thead><tr>' + head.map((h, i) =>
    `<th class="${numCols.includes(i) ? 'num' : ''}">${h}</th>`).join('') + '</tr></thead><tbody>';
  for (const r of rows) {
    s += '<tr>' + r.map((c, i) => `<td class="${numCols.includes(i) ? 'num' : ''}">${c}</td>`).join('') + '</tr>';
  }
  t.innerHTML = s + '</tbody>';
}

function renderAll() {
  document.getElementById('roundStr').textContent = DATA.rounds.join('、');
  document.getElementById('floorStr').textContent = fmt(DATA.floor);
  drawRounds(); drawChain(); drawSlow(); drawInput();
  fillTable('t_input',
    ['输入表', '全表行数', '当天行数', '版本切片', '被引用块数'],
    DATA.input, [1, 2, 3, 4]);
  fillTable('t_blocks',
    ['块', '引用的输入表（当天/版本切片）', '输入面·当天', '输入面·切片', '输出行', '墙钟 ms'],
    DATA.blocks, [2, 3, 4, 5]);
}
window.addEventListener('resize', () => { clearTimeout(window.__rt); window.__rt = setTimeout(renderAll, 150); });
initTheme();
renderAll();
</script>
</body>
</html>
"""

data = {
    "rounds": rounds,
    "roundMedian": sorted(rounds)[len(rounds) // 2],
    "floor": floor,
    "chain": [[a, b] for a, b in chain],
    "slowest": SLOW,
    "input": INPUT,
    "blocks": BLOCKS,
}

html = HTML.replace("__DATA__", json.dumps(data, ensure_ascii=False))
open(OUT, "w", encoding="utf-8", newline="\n").write(html)
print("written %s (%d KB); rounds=%s chain=%d floor=%d slow=%d input=%d blocks=%d"
      % (OUT, len(html) // 1024, rounds, len(chain), floor, len(SLOW), len(INPUT), len(BLOCKS)))
