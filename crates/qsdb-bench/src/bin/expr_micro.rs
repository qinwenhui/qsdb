//! M7-B 表达式微基准：CASE / coalesce / || / round(Decimal) 直接对内存批求值
//! （隔离求值内核——无扫描/存储开销）。整型算术列作对照。
//! 运行：cargo run --release -p qsdb-bench --bin expr_micro -- [rows]

use arrow::array::{Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::expr::eval::eval;
use qsdb::expr::{ArithOp, CmpOp, PhysExpr};
use qsdb::types::{DecimalMeta, Literal, LogicalType};
use std::sync::Arc;
use std::time::Instant;

fn col(idx: usize, name: &str, ty: LogicalType) -> PhysExpr {
    PhysExpr::Column {
        idx,
        name: name.into(),
        ty,
    }
}

fn int_arith(a: PhysExpr, b: PhysExpr) -> PhysExpr {
    PhysExpr::Arith {
        op: ArithOp::Add,
        l: Box::new(a),
        r: Box::new(b),
        ty: LogicalType::Int64,
    }
}

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(4_000_000);
    let bs = 8192usize;
    let nb = rows / bs;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, false),
        Field::new("s1", DataType::Utf8, false),
        Field::new("s2", DataType::Utf8, false),
        Field::new("d", DataType::Decimal128(18, 4), true),
    ]));

    let mut batches = Vec::with_capacity(nb);
    for bi in 0..nb {
        let base = (bi * bs) as i64;
        let a: Int64Array = (0..bs)
            .map(|i| {
                if i % 8 == 0 {
                    None
                } else {
                    Some(base + i as i64)
                }
            })
            .collect();
        let b: Int64Array = (0..bs).map(|i| base * 3 + i as i64 * 7).collect();
        let s1: Vec<Option<String>> = (0..bs)
            .map(|i| Some(format!("tenant-{}/{:05}", i % 97, i)))
            .collect();
        let s2: Vec<Option<String>> = (0..bs).map(|i| Some(format!("-tag-{}", i % 13))).collect();
        let d: Decimal128Array = (0..bs)
            .map(|i| {
                if i % 16 == 0 {
                    None
                } else {
                    Some(i128::from(base + i as i64) * 10_000 + 125)
                }
            })
            .collect::<Decimal128Array>()
            .with_data_type(DataType::Decimal128(18, 4));
        batches.push(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(a),
                    Arc::new(b),
                    Arc::new(StringArray::from(s1)),
                    Arc::new(StringArray::from(s2)),
                    Arc::new(d),
                ],
            )
            .unwrap(),
        );
    }
    let total = (nb * bs) as f64 / 1e6;

    let a64 = col(0, "a", LogicalType::Int64);
    let b64 = col(1, "b", LogicalType::Int64);

    // 对照：整型 a + b（直接 typed array kernel）
    let exprs: Vec<(&str, PhysExpr)> = vec![
        ("int_add(对照)", int_arith(a64.clone(), b64.clone())),
        (
            "case_2分支",
            PhysExpr::Case {
                conds: vec![(
                    PhysExpr::Cmp {
                        op: CmpOp::Lt,
                        l: Box::new(a64.clone()),
                        r: Box::new(PhysExpr::Lit(Literal::Int64(1 << 40))),
                    },
                    a64.clone(),
                )],
                else_expr: Box::new(b64.clone()),
                ty: LogicalType::Int64,
            },
        ),
        (
            "coalesce(a,b)",
            PhysExpr::Coalesce(vec![a64.clone(), b64.clone()], LogicalType::Int64),
        ),
        (
            "s1||s2",
            PhysExpr::Concat(
                Box::new(col(2, "s1", LogicalType::Varchar { max_chars: None })),
                Box::new(col(3, "s2", LogicalType::Varchar { max_chars: None })),
            ),
        ),
        (
            "round(d,2)",
            PhysExpr::Fn {
                name: "round".into(),
                args: vec![col(4, "d", LogicalType::Decimal(DecimalMeta::new(18, 4)))],
                ty: LogicalType::Decimal(DecimalMeta::new(18, 4)),
            },
        ),
        (
            "dec_mul(对照)",
            PhysExpr::Arith {
                op: ArithOp::Mul,
                l: Box::new(col(4, "d", LogicalType::Decimal(DecimalMeta::new(18, 4)))),
                r: Box::new(col(4, "d", LogicalType::Decimal(DecimalMeta::new(18, 4)))),
                ty: LogicalType::Decimal(DecimalMeta::new(36, 8)),
            },
        ),
    ];

    println!("rows = {}（批 {}×{}）\n", nb * bs, nb, bs);
    for (name, e) in &exprs {
        // 预热 + 最优轮（微基准惯例：取 best 排除调度噪声）
        let mut best = f64::INFINITY;
        for _ in 0..5 {
            let t = Instant::now();
            let mut sink = 0usize;
            for b in &batches {
                let out = eval(e, b).unwrap();
                sink += out.len();
            }
            let el = t.elapsed().as_secs_f64();
            assert_eq!(sink, nb * bs);
            if el < best {
                best = el;
            }
        }
        println!(
            "{:<16} {:>8.0} ms   {:>8.1} Mrows/s",
            name,
            best * 1e3,
            total / best
        );
    }
}
