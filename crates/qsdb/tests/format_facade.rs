//! **format-spec 原语门面（F13）的编译期锚定**。
//!
//! qsdb-sync 的诉求原话：生态工具要**独立实现**合规布局，而不该去 fork 内核实现
//! （他们的 sink-parquet 初版就复刻过一份 `encode_partition_value`）；并且要求
//! "契约列表即导出列表，**可校验**"（F5）。
//!
//! 本文件就是那个"可校验"：**全部按 `qsdb::X` 的根路径引用**（不是
//! `qsdb::partition::X`）——这正是生态工具的写法。谁把某个名字从根门面删掉，
//! 这个文件**编译不过**，而不是等到某天对方的产出读了不对才发现。
//!
//! 同时锚住**行为**（编码/解码往返、目录名判定），因为门面存在的意义是"照它实现
//! 就能产出合规布局"，光有名字不够。

/// 编译期：所有 format-spec 原语必须能从 crate root 拿到。
#[test]
fn facade_is_reachable_from_crate_root() {
    // 分区路径原语
    let _ = qsdb::encode_partition_value;
    let _ = qsdb::decode_partition_value;
    let _ = qsdb::partition_value_string;
    let _ = qsdb::is_partition_dir_name;
    let _ = qsdb::partition_dir;
    let _ = qsdb::parse_partition_dir;
    let _ = qsdb::parse_partition_value;
    // 文件名常量（生态工具要按它找文件，不能各自硬编码字符串）
    let _ = qsdb::MANIFEST_FILE;
    let _ = qsdb::INDEX_FILE;
    let _ = qsdb::INDEX_LOCK;
    // 类型面（manifest 读写 + 分区索引）
    let _: &str = std::any::type_name::<qsdb::PartitionManifest>();
    let _: &str = std::any::type_name::<qsdb::SegmentEntry>();
    let _: &str = std::any::type_name::<qsdb::ColumnStats>();
    let _: &str = std::any::type_name::<qsdb::PartitionsIndex>();
    let _: &str = std::any::type_name::<qsdb::Literal>();
}

/// 运行时：门面给出的**行为**必须自洽（照它实现就能与内核互通）。
#[test]
fn facade_behaviour_roundtrips() {
    use qsdb::Literal;

    // ① 分区值编码：分隔符必须转义（否则目录名歧义——`a/b` 会被当成两级）
    assert_eq!(qsdb::encode_partition_value("plain"), "plain");
    assert_eq!(qsdb::encode_partition_value("a/b=c%d"), "a%2Fb%3Dc%25d");
    for raw in ["plain", "a/b=c%d", "带中文", "tab\there"] {
        let enc = qsdb::encode_partition_value(raw);
        let back = qsdb::decode_partition_value(&enc).expect("解码");
        assert_eq!(back, raw, "{raw:?} 编码后解不回原值（enc={enc:?}）");
    }

    // ② 目录名 → 分区值 → 目录名：生态工具重建布局就靠这条闭环
    let dir = qsdb::partition_dir(
        &["dt".to_string(), "k".to_string()],
        &[Literal::Date(19_000), Literal::Utf8("a/b".into())],
    )
    .expect("partition_dir");
    let text = dir.to_string_lossy().replace('\\', "/");
    assert_eq!(text, "dt=2022-01-08/k=a%2Fb", "目录布局即契约：{text}");
    let parsed = qsdb::parse_partition_dir(&dir, 2).expect("parse_partition_dir");
    // ⚠️ **契约**（工具最容易踩错的一处）：`parse_partition_dir` **已经解码**
    // （内部调 `decode_partition_value`），返回的是 `a/b` 而不是 `a%2Fb`；
    // 下游的 `parse_partition_value` **不再解码**。工具自己再解一次就是双重解码。
    assert_eq!(
        parsed,
        vec![
            ("dt".to_string(), "2022-01-08".to_string()),
            ("k".to_string(), "a/b".to_string()),
        ],
        "parse_partition_dir 返回**已解码**值（别再自己解一次）"
    );
    // 反过来说：解码归它，**编码**归 `encode_partition_value`（`partition_dir` 内部用）
    for (k, v) in &parsed {
        let rebuilt = qsdb::encode_partition_value(v);
        assert!(
            qsdb::decode_partition_value(&rebuilt).unwrap() == *v,
            "{k} 的编码/解码不自洽"
        );
    }

    // ③ 类型化解析：目录名 → Literal（工具把分区列重建成常量列就靠它）
    let lit = qsdb::parse_partition_value("2022-01-08", &qsdb::LogicalType::Date)
        .expect("parse_partition_value");
    assert_eq!(lit, Literal::Date(19_000));

    // ④ 目录名判定：`_` 前缀也是合法分区目录名（F22 修过，别再"优化"掉）
    assert!(qsdb::is_partition_dir_name("dt=1"));
    assert!(qsdb::is_partition_dir_name("_part=1"));
    assert!(!qsdb::is_partition_dir_name("partitions.idx"));
    assert!(!qsdb::is_partition_dir_name("dt"));

    // ⑤ 文件名常量与 format-spec 一致（工具据此找文件）
    assert_eq!(qsdb::MANIFEST_FILE, "manifest.postcard");
    assert_eq!(qsdb::INDEX_FILE, "partitions.idx");
    assert_eq!(qsdb::INDEX_LOCK, "_index.lock");
}
