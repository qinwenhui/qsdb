//! 分组内核原型（方案 A 验证）：按 key hash 预分区 vs 单表哈希探测。
//!
//! 脱离扫描/并行/聚合状态，只测**分组 id 分配**这一段的访存行为——
//! S0 已测出它是高基数 group by 的 95% 成本（85~104ns/行）。
//!
//! 假设：单表 2M 槽 × 16B = 32MB 超出 L3 → 每次探测一次内存访问；
//! 按 hash 高比特分到 P 桶后，每桶表 ≈ 32MB/P → 进 L3。
//!
//! 运行：cargo run --release -p qsdb-bench --bin grp_proto -- [rows] [card]

use std::time::Instant;

const EMPTY: u32 = u32::MAX;

#[inline]
fn fib_hash(k: i64) -> u64 {
    (k as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// 基线：单表开放寻址（复刻 GroupTable 的 fast_slots_inline 路径）。
fn baseline(keys: &[i64], card: usize) -> f64 {
    let mut cap = 16usize;
    while cap < card * 2 {
        cap <<= 1;
    }
    let mut slots: Vec<(i64, u32)> = vec![(0, EMPTY); cap];
    let mask = cap - 1;
    let mut n_groups = 0u32;
    let mut acc = 0u64;
    let t = Instant::now();
    for &k in keys {
        let h = fib_hash(k);
        let mut idx = (h as usize) & mask;
        loop {
            let (sk, sg) = slots[idx];
            if sg == EMPTY {
                slots[idx] = (k, n_groups);
                acc = acc.wrapping_add(u64::from(n_groups));
                n_groups += 1;
                break;
            }
            if sk == k {
                acc = acc.wrapping_add(u64::from(sg));
                break;
            }
            idx = (idx + 1) & mask;
        }
    }
    let el = t.elapsed().as_secs_f64();
    std::hint::black_box(acc);
    debug_assert_eq!(n_groups as usize, card.min(keys.len()));
    el
}

/// 方案 A：按 hash 高比特分桶（计数 → 前缀和 → 散列 → 每桶独立探测）。
fn partitioned(keys: &[i64], _card: usize, p_bits: u32) -> (f64, f64) {
    let p = 1usize << p_bits;
    let t0 = Instant::now();
    // ① 计数
    let mut cnt = vec![0u32; p + 1];
    for &k in keys {
        cnt[((fib_hash(k) >> (64 - p_bits)) as usize) + 1] += 1;
    }
    // ② 前缀和
    for i in 0..p {
        cnt[i + 1] += cnt[i];
    }
    // ③ 散列（顺序写，cache 友好）
    let mut pos = cnt.clone();
    let mut buf: Vec<i64> = vec![0; keys.len()];
    for &k in keys {
        let b = (fib_hash(k) >> (64 - p_bits)) as usize;
        buf[pos[b] as usize] = k;
        pos[b] += 1;
    }
    let t_scatter = t0.elapsed().as_secs_f64();

    // ④ 每桶独立探测（桶内表 ≈ 总表 / P）
    let t1 = Instant::now();
    let mut acc = 0u64;
    let mut n_groups = 0u32;
    let per_cap = {
        let mut c = 16usize;
        while c < (cnt[p] as usize / p + 1) * 4 {
            c <<= 1;
        }
        c
    };
    let mut slots: Vec<(i64, u32)> = vec![(0, EMPTY); per_cap];
    for b in 0..p {
        let (lo, hi) = (cnt[b] as usize, cnt[b + 1] as usize);
        if hi == lo {
            continue;
        }
        let mask = per_cap - 1;
        for s in slots.iter_mut() {
            s.1 = EMPTY;
        }
        for &k in &buf[lo..hi] {
            let h = fib_hash(k);
            let mut idx = (h as usize) & mask;
            loop {
                let (sk, sg) = slots[idx];
                if sg == EMPTY {
                    slots[idx] = (k, n_groups);
                    acc = acc.wrapping_add(u64::from(n_groups));
                    n_groups += 1;
                    break;
                }
                if sk == k {
                    acc = acc.wrapping_add(u64::from(sg));
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }
    }
    let t_agg = t1.elapsed().as_secs_f64();
    std::hint::black_box(acc);
    (t_scatter, t_agg)
}

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000_000);
    let card: usize = std::env::args()
        .nth(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let keys: Vec<i64> = (0..rows)
        .map(|i| ((i as i64).wrapping_mul(7919)) % card as i64)
        .collect();
    let per_row = |s: f64| s * 1e9 / rows as f64;

    println!("=== 分组内核原型：{rows} 行 / {card} 组 ===\n");
    let mut best = f64::INFINITY;
    let mut ref_acc = 0u64;
    for _ in 0..3 {
        let t = baseline(&keys, card);
        ref_acc = std::hint::black_box(ref_acc.wrapping_add(t as u64));
        best = best.min(t);
    }
    println!(
        "基线（单表开放寻址）      {best:.3}s  ({:.1} ns/行)",
        per_row(best)
    );
    println!(
        "  表大小 ≈ {} MB（2M 槽 × 16B）\n",
        card * 2 * 16 / 1_048_576
    );

    for p_bits in [1u32, 2, 3, 4, 5] {
        let mut best_s = f64::INFINITY;
        let mut best_a = f64::INFINITY;
        for _ in 0..3 {
            let (s, a) = partitioned(&keys, card, p_bits);
            if s + a < best_s + best_a {
                best_s = s;
                best_a = a;
            }
        }
        let tot = best_s + best_a;
        println!(
            "分区 P={:<3} 分发 {:.1} + 聚合 {:.1} = {:.1} ns/行   {:.2}x   每桶表 ≈ {} MB",
            (1 << p_bits),
            per_row(best_s),
            per_row(best_a),
            per_row(tot),
            best / tot,
            card * 2 * 16 / (1 << p_bits) / 1_048_576
        );
    }
    // ★ 取值方式对照：原型用连续 Vec，真实路径用 arrow 的 col.value(row)
    let arr: arrow::array::Int64Array = keys.iter().copied().collect();
    let mut b_arrow = f64::INFINITY;
    let mut b_slice = f64::INFINITY;
    for _ in 0..3 {
        b_arrow = b_arrow.min(baseline_arrow(&arr, card));
        b_slice = b_slice.min(baseline_slice(&arr, card));
    }
    println!("--- 取值方式对照（同一算法，只换取数方式）---");
    println!("连续 Vec 迭代（原型）     {:.1} ns/行", per_row(best));
    println!(
        "arrow col.value(row)     {:.1} ns/行   ← 真实路径用法",
        per_row(b_arrow)
    );
    println!("arrow values() 切片       {:.1} ns/行", per_row(b_slice));
    println!();

    println!("\n--- 模拟 push_group 登记开销（真实 GroupTable 每新建组要做的事）---");
    let base_ns = per_row(best);
    for n_agg in [1usize, 2, 3] {
        let mut b = f64::INFINITY;
        for _ in 0..3 {
            b = b.min(with_group_bookkeeping(&keys, card, n_agg));
        }
        println!(
            "基线+登记({n_agg} 聚合)  {b:.3}s  ({:.1} ns/行)   较纯探测 +{:.1} ns/行",
            per_row(b),
            per_row(b) - base_ns
        );
    }
    println!("\n判读：若「登记开销」解释了 10ns→85ns 的大部分差距，则真凶是 push_group 而非探测");
}

// ── 追加实验：模拟 push_group 的登记开销（真实 GroupTable 每新建组要做的事）──
#[allow(dead_code)]
fn with_group_bookkeeping(keys: &[i64], card: usize, n_agg: usize) -> f64 {
    let mut cap = 16usize;
    while cap < card * 2 {
        cap <<= 1;
    }
    let mut slots: Vec<(i64, u32)> = vec![(0, EMPTY); cap];
    let mask = cap - 1;
    // 复刻 GroupTable 的 SoA 登记数组
    let mut key_arena: Vec<u8> = Vec::new();
    let mut key_off: Vec<u32> = Vec::with_capacity(card);
    let mut group_lits: Vec<Vec<i64>> = vec![Vec::with_capacity(card)];
    let mut counts: Vec<Vec<u64>> = (0..n_agg).map(|_| Vec::with_capacity(card)).collect();
    let mut sums: Vec<Vec<i128>> = (0..n_agg).map(|_| Vec::with_capacity(card)).collect();
    let mut minmax: Vec<Vec<Option<i64>>> = (0..n_agg).map(|_| Vec::with_capacity(card)).collect();
    let mut n_groups = 0u32;
    let mut acc = 0u64;
    let t = Instant::now();
    for &k in keys {
        let h = fib_hash(k);
        let mut idx = (h as usize) & mask;
        loop {
            let (sk, sg) = slots[idx];
            if sg == EMPTY {
                // ★ 真实 push_group 的工作：arena 追加 + 各登记数组 push
                key_off.push(key_arena.len() as u32);
                key_arena.extend_from_slice(&k.to_le_bytes());
                group_lits[0].push(k);
                for a in 0..n_agg {
                    counts[a].push(0);
                    sums[a].push(0);
                    minmax[a].push(None);
                }
                slots[idx] = (k, n_groups);
                acc = acc.wrapping_add(u64::from(n_groups));
                n_groups += 1;
                break;
            }
            if sk == k {
                acc = acc.wrapping_add(u64::from(sg));
                break;
            }
            idx = (idx + 1) & mask;
        }
    }
    let el = t.elapsed().as_secs_f64();
    std::hint::black_box((acc, key_arena.len(), key_off.len()));
    el
}

// ── 追加：用 arrow Int64Array + value() 复刻真实路径的取值方式 ──
#[allow(dead_code)]
fn baseline_arrow(col: &arrow::array::Int64Array, card: usize) -> f64 {
    let mut cap = 16usize;
    while cap < card * 2 {
        cap <<= 1;
    }
    let mut slots: Vec<(i64, u32)> = vec![(0, EMPTY); cap];
    let mask = cap - 1;
    let mut n_groups = 0u32;
    let mut acc = 0u64;
    let n = col.len();
    let t = Instant::now();
    for row in 0..n {
        let k = col.value(row); // ← 真实路径的取值方式
        let h = fib_hash(k);
        let mut idx = (h as usize) & mask;
        loop {
            let (sk, sg) = slots[idx];
            if sg == EMPTY {
                slots[idx] = (k, n_groups);
                acc = acc.wrapping_add(u64::from(n_groups));
                n_groups += 1;
                break;
            }
            if sk == k {
                acc = acc.wrapping_add(u64::from(sg));
                break;
            }
            idx = (idx + 1) & mask;
        }
    }
    let el = t.elapsed().as_secs_f64();
    std::hint::black_box(acc);
    el
}

/// 对照：先取 values() 切片再索引（一次性拿 slice）
#[allow(dead_code)]
fn baseline_slice(col: &arrow::array::Int64Array, card: usize) -> f64 {
    let vals = col.values(); // ← 一次取 slice
    let mut cap = 16usize;
    while cap < card * 2 {
        cap <<= 1;
    }
    let mut slots: Vec<(i64, u32)> = vec![(0, EMPTY); cap];
    let mask = cap - 1;
    let mut n_groups = 0u32;
    let mut acc = 0u64;
    let n = vals.len();
    let t = Instant::now();
    for &k in vals {
        let h = fib_hash(k);
        let mut idx = (h as usize) & mask;
        loop {
            let (sk, sg) = slots[idx];
            if sg == EMPTY {
                slots[idx] = (k, n_groups);
                acc = acc.wrapping_add(u64::from(n_groups));
                n_groups += 1;
                break;
            }
            if sk == k {
                acc = acc.wrapping_add(u64::from(sg));
                break;
            }
            idx = (idx + 1) & mask;
        }
    }
    let el = t.elapsed().as_secs_f64();
    std::hint::black_box((acc, n));
    el
}
