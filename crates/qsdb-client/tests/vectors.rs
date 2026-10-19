//! 格式一致性测试：**逐字节**对死契约文档。
//!
//! 为什么值得这么写：`bulk`/`rows` 各有**两份独立实现**（内核一份、本客户端一份），
//! 两份互不引用。只做"自己编、自己解"的往返测试是**循环论证**——两边一起错
//! （比如把 NULL 位图的字节数理解成行数）照样全绿。所以这里钉的是**字节**：
//!
//! - `bulk`：手工推出的期望字节 ↔ 编码器输出（编不出来就说明两侧对格式的理解分叉）；
//! - `rows`：`docs/rows-format.md` §三.3 的示例向量（内核侧也用同一条
//!   `spec_example_from_docs` 锁着它）。
//!
//! 另外覆盖**坏字节必须显式报错**（截断、错 magic、未知标签、越界偏移）——
//! 静默"能读多少算多少"在协议面就是把半份数据当全份。

use qsdb_client::{Column, Error, Tag, Value, decode_bulk, decode_rows, encode_bulk};

// ---------------------------------------------------------------------------
// bulk
// ---------------------------------------------------------------------------

/// 手工按 `docs/bulk-format.md` 推出的期望字节（2 列 2 行，各含一个 NULL）。
#[test]
fn bulk_编码器输出与手推字节逐字节一致() {
    let cols = [
        ("id", vec![Some(1i64), None].into()),
        ("name", vec![Some("a".to_string()), None].into()),
    ];
    let got = encode_bulk(&cols).expect("编码");
    let mut want: Vec<u8> = Vec::new();
    want.extend_from_slice(b"QSDBBULK"); // magic
    want.extend_from_slice(&1u16.to_le_bytes()); // version
    want.extend_from_slice(&2u16.to_le_bytes()); // n_cols
    want.extend_from_slice(&2u64.to_le_bytes()); // n_rows
    // 列描述：id BIGINT
    want.extend_from_slice(&2u16.to_le_bytes());
    want.extend_from_slice(b"id");
    want.push(3);
    // 列描述：name VARCHAR
    want.extend_from_slice(&4u16.to_le_bytes());
    want.extend_from_slice(b"name");
    want.push(5);
    // 列 1（id）：位图 1 字节（第 1 行 NULL = 0b10），数据 2×8 字节
    want.extend_from_slice(&1u64.to_le_bytes());
    want.push(0b10);
    want.extend_from_slice(&16u64.to_le_bytes());
    want.extend_from_slice(&1i64.to_le_bytes());
    want.extend_from_slice(&0i64.to_le_bytes()); // NULL 行占位
    // 列 2（name）：位图同上；数据段 "a"；偏移 3 个 u32 = [0,1,1]
    want.extend_from_slice(&1u64.to_le_bytes());
    want.push(0b10);
    want.extend_from_slice(&1u64.to_le_bytes());
    for o in [0u32, 1, 1] {
        want.extend_from_slice(&o.to_le_bytes());
    }
    want.push(b'a');

    assert_eq!(
        got,
        want,
        "编码输出与契约手推字节不一致（左侧=实际 {} 字节，右侧=期望 {} 字节）",
        got.len(),
        want.len()
    );
}

#[test]
fn bulk_解码手推字节取值正确() {
    // 与上一条同一份字节：解码侧也要独立认它
    let cols = [
        ("id", vec![Some(1i64), None].into()),
        ("name", vec![Some("a".to_string()), None].into()),
    ];
    let bytes = encode_bulk(&cols).unwrap();
    let t = decode_bulk(bytes).unwrap();
    assert_eq!(t.num_rows(), 2);
    assert_eq!(t.num_columns(), 2);
    assert_eq!(t.schema()[0].name, "id");
    assert_eq!(t.schema()[0].tag, Tag::I64);
    assert_eq!(t.schema()[1].tag, Tag::Utf8);
    assert_eq!(t.value(0, 0).unwrap().to_value(), Value::I64(1));
    assert_eq!(
        t.value(1, 0).unwrap().to_value(),
        Value::Null,
        "NULL 由位图决定"
    );
    assert_eq!(t.value(0, 1).unwrap().to_value(), Value::Utf8("a".into()));
    assert_eq!(t.value(1, 1).unwrap().to_value(), Value::Null);
    // 空区间 ≠ NULL 的区分：本列的 NULL 由位图标示（行 1 的区间是 [1,1]）
    assert_eq!(
        t.column_by_name("name").unwrap().get(0).unwrap().as_str(),
        Some("a")
    );
}

#[test]
fn bulk_零行结果保留完整_schema() {
    // `LIMIT 0` 的姿势：零行 + 完整 schema（对接契约 §二之一）
    let cols = [("a", Column::I64(vec![])), ("b", Column::Utf8(vec![]))];
    let bytes = encode_bulk(&cols).unwrap();
    let t = decode_bulk(bytes).unwrap();
    assert_eq!(t.num_rows(), 0);
    assert!(t.is_empty());
    assert_eq!(t.num_columns(), 2);
    assert_eq!(t.schema()[1].name, "b");
}

#[test]
fn bulk_全类型往返() {
    let cols = [
        ("b", Column::Bool(vec![Some(true), Some(false), None])),
        ("i32", Column::I32(vec![Some(-1), None, Some(7)])),
        (
            "i64",
            Column::I64(vec![Some(i64::MIN), Some(i64::MAX), None]),
        ),
        ("f64", Column::F64(vec![Some(1.5), Some(f64::NAN), None])),
        ("s", Column::utf8([Some("中文"), Some(""), None])),
        ("d", Column::dates([Some(19000), None, Some(-1)])),
        (
            "ts",
            Column::timestamps([Some(1_700_000_000_000_000_000), None, Some(0)]),
        ),
    ];
    let t = decode_bulk(encode_bulk(&cols).unwrap()).unwrap();
    assert_eq!(t.num_rows(), 3);
    assert_eq!(t.value(0, 0).unwrap().to_value(), Value::Bool(true));
    assert_eq!(t.value(0, 1).unwrap().to_value(), Value::I32(-1));
    assert_eq!(t.value(0, 2).unwrap().to_value(), Value::I64(i64::MIN));
    assert_eq!(t.value(0, 3).unwrap().to_value(), Value::F64(1.5));
    assert_eq!(
        t.value(0, 4).unwrap().to_value(),
        Value::Utf8("中文".into())
    );
    assert_eq!(t.value(0, 5).unwrap().to_value(), Value::Date32(19000));
    assert_eq!(
        t.value(0, 6).unwrap().to_value(),
        Value::TimestampNs(1_700_000_000_000_000_000)
    );
    // 空串与 NULL 是两回事（位图 + 长度 0 的区间）
    assert_eq!(
        t.value(1, 4).unwrap().to_value(),
        Value::Utf8(String::new())
    );
    assert_eq!(t.value(2, 4).unwrap().to_value(), Value::Null);
    for r in 0..3 {
        for c in 0..7 {
            let _ = t.value(r, c).unwrap();
        }
    }
}

#[test]
fn bulk_拒绝参差不齐的列() {
    let cols = [
        ("a", Column::I64(vec![Some(1)])),
        ("b", Column::I64(vec![Some(1), Some(2)])),
    ];
    match encode_bulk(&cols) {
        Err(Error::Usage(m)) => assert!(m.contains("参差不齐") || m.contains("行"), "{m}"),
        other => panic!("应当报用法错，实为 {other:?}"),
    }
}

#[test]
fn bulk_坏字节一律显式报错() {
    let good = encode_bulk(&[("a", Column::I64(vec![Some(1), Some(2)]))]).unwrap();

    // 错 magic
    let mut b = good.clone();
    b[0] = b'X';
    assert!(decode_bulk(b).is_err(), "错 magic 必须报错");

    // 版本不认识
    let mut b = good.clone();
    b[8] = 9;
    assert!(decode_bulk(b).is_err(), "未知版本必须报错");

    // 截断
    let mut b = good.clone();
    b.truncate(good.len() - 3);
    assert!(decode_bulk(b).is_err(), "截断必须报错");

    // 尾巴多字节（两侧理解分叉的信号）
    let mut b = good.clone();
    b.push(0);
    assert!(decode_bulk(b).is_err(), "尾部多余字节必须报错");

    // 未知类型标签
    let mut b = good.clone();
    let tag_at = 8 + 2 + 2 + 8 + 2 + 1; // magic+ver+n_cols+n_rows+name_len+name
    b[tag_at] = 200;
    assert!(decode_bulk(b).is_err(), "未知标签必须报错");
}

#[test]
fn bulk_空列列表报用法错() {
    assert!(encode_bulk(&[]).is_err());
}

// ---------------------------------------------------------------------------
// rows：docs/rows-format.md §三.3 的示例向量
// ---------------------------------------------------------------------------

/// 契约文档里那段 85 字节示例（表：id BIGINT / name STRING / dt DATE；
/// 块 1 = 2 行、第 1 行整行 NULL；块 2 = 1 行）。
///
/// 文档给的是**写侧形态**（`flags = 0`，不带列描述）；读侧 `flags` 恒为 1，
/// 所以这里在 n_cols 之后**插入**三条列描述，其余字节与文档逐字相同。
fn doc_example_as_read_payload() -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(b"QSDBROWS");
    b.extend_from_slice(&1u16.to_le_bytes()); // version
    b.push(1); // flags = HAS_SCHEMA（读侧恒为 1）
    b.extend_from_slice(&3u16.to_le_bytes()); // n_cols
    // 列描述 ×3
    b.extend_from_slice(&2u16.to_le_bytes());
    b.extend_from_slice(b"id");
    b.push(3); // BIGINT
    b.push(1); // nullable
    b.extend_from_slice(&0u16.to_le_bytes()); // meta 空
    b.extend_from_slice(&4u16.to_le_bytes());
    b.extend_from_slice(b"name");
    b.push(5); // VARCHAR
    b.push(1);
    b.extend_from_slice(&4u16.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes()); // max_chars = 0（无上限）
    b.extend_from_slice(&2u16.to_le_bytes());
    b.extend_from_slice(b"dt");
    b.push(6); // DATE
    b.push(1);
    b.extend_from_slice(&0u16.to_le_bytes());
    // ---- 以下与 docs/rows-format.md §三.3 的偏移 13 起逐字相同 ----
    b.extend_from_slice(&2u32.to_le_bytes()); // chunk1 row_count = 2
    b.push(1); // HAS_NULLS
    b.extend_from_slice(&[0b10, 0b10, 0b10]); // 3 列位图
    b.extend_from_slice(&1i64.to_le_bytes()); // id = 1
    b.extend_from_slice(&5u32.to_le_bytes());
    b.extend_from_slice(b"alice"); // name = "alice"
    b.extend_from_slice(&19000i32.to_le_bytes()); // dt = 2022-01-08
    b.extend_from_slice(&[0u8; 8]); // NULL 行占位：id
    b.extend_from_slice(&[0u8; 4]); // NULL 行占位：name
    b.extend_from_slice(&[0u8; 4]); // NULL 行占位：dt
    b.extend_from_slice(&1u32.to_le_bytes()); // chunk2 row_count = 1
    b.push(0); // 无位图
    b.extend_from_slice(&3i64.to_le_bytes()); // id = 3
    let s = "中文".as_bytes();
    b.extend_from_slice(&(s.len() as u32).to_le_bytes());
    b.extend_from_slice(s); // name = "中文"
    b.extend_from_slice(&19001i32.to_le_bytes()); // dt = 2022-01-09
    b
}

#[test]
fn rows_解码契约文档示例向量() {
    let bytes = doc_example_as_read_payload();
    let d = decode_rows(&bytes).unwrap();
    assert_eq!(d.schema().len(), 3);
    assert_eq!(d.schema()[0].name, "id");
    assert_eq!(d.schema()[0].tag, Tag::I64);
    assert_eq!(d.schema()[1].tag, Tag::Utf8);
    assert_eq!(d.schema()[1].max_chars, Some(0));
    assert_eq!(d.schema()[2].tag, Tag::Date32);
    assert!(d.schema()[0].nullable, "列描述里 nullable 位应为 1");

    // 两块：2 行 + 1 行
    assert_eq!(d.num_chunks(), 2);
    assert_eq!(d.num_rows(), 3);

    // 块 1：位图 `0b10` ⇒ **第 2 行**（0 基下标 1）整行 NULL；行 0 有值。
    // （文档说"第 1 行整行 NULL"是**1 基**数法——这里按字节读出来的位图为准）
    assert_eq!(d.get(0, 0).unwrap().to_value(), Value::I64(1));
    assert_eq!(d.get(0, 1).unwrap().to_value(), Value::Utf8("alice".into()));
    assert_eq!(d.get(0, 2).unwrap().to_value(), Value::Date32(19000));
    assert_eq!(d.get(1, 0).unwrap().to_value(), Value::Null);
    assert_eq!(d.get(1, 1).unwrap().to_value(), Value::Null);
    assert_eq!(d.get(1, 2).unwrap().to_value(), Value::Null);
    // 块 2（跨块寻址）
    assert_eq!(d.get(2, 0).unwrap().to_value(), Value::I64(3));
    assert_eq!(d.get(2, 1).unwrap().to_value(), Value::Utf8("中文".into()));
    assert_eq!(d.get(2, 2).unwrap().to_value(), Value::Date32(19001));
    // 按名取
    assert_eq!(
        d.get_by_name(2, "name").unwrap().to_value(),
        Value::Utf8("中文".into())
    );
    // 越界必须报错（不是 panic、不是 NULL）
    assert!(d.get(3, 0).is_err());
    assert!(d.get(0, 3).is_err());
}

#[test]
fn rows_拒绝写侧形态() {
    let mut b = doc_example_as_read_payload();
    b[10] = 0; // flags bit0 = 0：写侧形态（schema 由使用方给）
    match decode_rows(&b) {
        Err(Error::Decode(m)) => assert!(m.contains("flags"), "{m}"),
        other => panic!("应当显式报错，实为 {other:?}"),
    }
}

#[test]
fn rows_坏字节一律显式报错() {
    let good = doc_example_as_read_payload();

    let mut b = good.clone();
    b[0] = b'X';
    assert!(decode_rows(&b).is_err(), "错 magic");

    let mut b = good.clone();
    b[8] = 7;
    assert!(decode_rows(&b).is_err(), "未知版本");

    let mut b = good.clone();
    b.truncate(good.len() - 2);
    assert!(decode_rows(&b).is_err(), "截断");

    // 未知类型标签（第一条列描述的 type 字节）
    let mut b = good.clone();
    b[13 + 2 + 2] = 99;
    assert!(decode_rows(&b).is_err(), "未知标签");

    // 定长列的 meta 多给了字节（规定必须为 0）
    let mut b = good.clone();
    let meta_len_at = 13 + 2 + 2 + 1 + 1; // name_len+name+type+col_flags
    b[meta_len_at] = 3;
    assert!(decode_rows(&b).is_err(), "无 meta 的类型多给字节");
}
