//! 分区：目录名编解码（format-spec §2）与写入路由（batch → 分区组）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, Time64NanosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::catalog::TableDef;
use crate::error::{QsdbError, Result};
use crate::types::{Literal, LogicalType};

/// 目录名编码：`%`→`%25`、`/`→`%2F`、`=`→`%3D`、控制字符（<0x20、0x7F）→`%XX`。
///
/// 其余字节（含 UTF-8 多字节序列）原样透传。
pub fn encode_partition_value(raw: &str) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(raw.len());
    for b in raw.bytes() {
        match b {
            b'%' => out.extend_from_slice(b"%25"),
            b'/' => out.extend_from_slice(b"%2F"),
            b'=' => out.extend_from_slice(b"%3D"),
            0x00..=0x1F | 0x7F => {
                let hex = format!("%{b:02X}");
                out.extend_from_slice(hex.as_bytes());
            }
            _ => out.push(b),
        }
    }
    String::from_utf8(out).expect("non-escaped bytes keep UTF-8 validity")
}

/// 目录名解码（encode 的逆；非法转义报错）。
pub fn decode_partition_value(enc: &str) -> Result<String> {
    let bytes = enc.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 3 > bytes.len() {
                return Err(QsdbError::meta(format!(
                    "invalid percent-escape in partition dir '{enc}'"
                )));
            }
            let hex = &enc[i + 1..i + 3];
            let v = u8::from_str_radix(hex, 16)
                .map_err(|_| QsdbError::meta(format!("bad escape '%{hex}' in '{enc}'")))?;
            out.push(v);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| QsdbError::meta(format!("partition dir '{enc}' is not valid UTF-8")))
}

/// 分区值 → 目录显示文本：Utf8 原样，其余用 SQL 输出格式（无引号语义差异已消除）。
pub fn partition_value_string(lit: &Literal) -> String {
    match lit {
        Literal::Utf8(s) => s.clone(),
        other => other.to_sql_string(),
    }
}

/// 目录名是否是**分区目录段**：只认 hive 形态 `k=v`（F22）。
///
/// 历史判据是「含 `=` **且**不以 `_`/`.` 开头」，为的是别把内部目录当分区。
/// 代价是**写者能建、读者看不见**：分区列名叫 `__dt` 时目录形如
/// `__dt=2026-10-12`，`list_partitions` 直接跳过——行在盘上、count/scan 归零，
/// 而写路径毫无征兆（qsdb-sync F22 实测）。
///
/// 内部目录本就不长这样：`_catalog` / `_trash` / `_filecache` 在**库根**而非
/// 表目录下，表目录里唯一的内部物 `_index.lock` 是**文件**且不含 `=`。
/// 故 `=` 一项即足以判别，前缀不再是判据——分区列名不必再回避 `_` 开头。
pub fn is_partition_dir_name(name: &str) -> bool {
    name.contains('=')
}

/// 一组分区键值 → 相对目录路径 `k1=v1/k2=v2`。
pub fn partition_dir(keys: &[String], values: &[Literal]) -> Result<PathBuf> {
    if keys.len() != values.len() {
        return Err(QsdbError::invalid("partition keys/values length mismatch"));
    }
    let mut path = PathBuf::new();
    for (k, v) in keys.iter().zip(values) {
        path.push(format!(
            "{k}={}",
            encode_partition_value(&partition_value_string(v))
        ));
    }
    Ok(path)
}

/// 目录路径 → 分区值字符串（按 `=` 切分 + 反转义）。
pub fn parse_partition_dir(dir: &Path, depth: usize) -> Result<Vec<(String, String)>> {
    let all: Vec<_> = dir
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if all.len() < depth {
        return Err(QsdbError::meta(format!(
            "partition dir {:?} depth {} < {}",
            dir,
            all.len(),
            depth
        )));
    }
    // 取末尾 depth 层（调用方传入的是绝对路径）
    let comps = &all[all.len() - depth..];
    let mut out = Vec::with_capacity(depth);
    for comp in comps {
        let (k, v) = comp
            .split_once('=')
            .ok_or_else(|| QsdbError::meta(format!("partition component '{comp}' lacks '='")))?;
        if k.is_empty() {
            return Err(QsdbError::meta(format!(
                "partition component '{comp}' has empty key"
            )));
        }
        out.push((k.to_string(), decode_partition_value(v)?));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 写入路由：RecordBatch → 分区值提取 + 行分组
// ---------------------------------------------------------------------------

/// 从数组第 i 行取 Literal（NULL → Err：分区值不允许 NULL）。
///
/// `pub(crate)`：INSERT…SELECT 的 OVERWRITE 路径要按分区值切批，
/// 取值口径必须与写入端（`split_by_partition`）逐字一致——两处实现
/// 漂移会表现为"桶里混着别的分区的行"，是很难查的静默错写。
pub(crate) fn value_at(arr: &dyn Array, i: usize) -> Result<Literal> {
    if arr.is_null(i) {
        return Err(QsdbError::invalid("partition column value is NULL"));
    }
    let lit = match arr.data_type() {
        DataType::Int8 => Literal::Int8(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(i)),
        DataType::Int16 => {
            Literal::Int16(arr.as_any().downcast_ref::<Int16Array>().unwrap().value(i))
        }
        DataType::Int32 => {
            Literal::Int32(arr.as_any().downcast_ref::<Int32Array>().unwrap().value(i))
        }
        DataType::Int64 => {
            Literal::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
        }
        DataType::Boolean => Literal::Boolean(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(i),
        ),
        DataType::Utf8 => Literal::Utf8(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .to_string(),
        ),
        DataType::Date32 => {
            Literal::Date(arr.as_any().downcast_ref::<Date32Array>().unwrap().value(i))
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => Literal::Time(
            arr.as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap()
                .value(i),
        ),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => Literal::Timestamp(
            arr.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(i),
        ),
        DataType::Decimal128(p, s) => Literal::Decimal {
            unscaled: arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(i),
            meta: crate::types::DecimalMeta::try_new(*p, *s)?,
        },
        other => {
            return Err(QsdbError::unsupported(format!(
                "partition column physical type {other:?}"
            )));
        }
    };
    Ok(lit)
}

/// 按分区列把 batch 切成多组（每组同一分区值），保留非分区列。
///
/// 返回 (分区值, 子 batch) 列表；行序在组内保持稳定。
pub fn split_by_partition(
    batch: &RecordBatch,
    def: &TableDef,
) -> Result<Vec<(Vec<Literal>, RecordBatch)>> {
    let pkeys = &def.partition_keys;
    if pkeys.is_empty() {
        let data = strip_partition_columns(batch, def)?;
        return Ok(vec![(Vec::new(), data)]);
    }
    let pidx: Vec<usize> = pkeys
        .iter()
        .map(|k| {
            batch.schema().index_of(k).map_err(|_| {
                // 报出**实际收到的列**——只说"缺列"没法排障（实践：客户端列名/列序
                // 不对时，这句话是唯一线索）
                let sch = batch.schema();
                let have: Vec<&str> = sch.fields().iter().map(|f| f.name().as_str()).collect();
                QsdbError::invalid(format!(
                    "batch lacks partition column '{k}' (batch columns: {have:?})"
                ))
            })
        })
        .collect::<Result<_>>()?;

    // 行 → 分组 id（键用分区值字符串：同列类型下字符串一一对应）
    let mut groups: Vec<Vec<Literal>> = Vec::new();
    let mut group_of_row: Vec<usize> = Vec::with_capacity(batch.num_rows());
    let mut index: HashMap<Vec<String>, usize> = HashMap::new();
    for row in 0..batch.num_rows() {
        let key_lits: Vec<Literal> = pidx
            .iter()
            .map(|&c| value_at(batch.column(c), row))
            .collect::<Result<_>>()?;
        let key: Vec<String> = key_lits.iter().map(partition_value_string).collect();
        let id = match index.get(&key) {
            Some(&id) => id,
            None => {
                let id = groups.len();
                groups.push(key_lits);
                index.insert(key, id);
                id
            }
        };
        group_of_row.push(id);
    }

    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|&c| !pidx.contains(&c))
        .collect();
    let data_schema = projected_schema(&batch.schema(), &keep);
    let mut out = Vec::with_capacity(groups.len());
    for (gid, values) in groups.iter().enumerate() {
        let rows: Vec<u32> = group_of_row
            .iter()
            .enumerate()
            .filter(|(_, g)| **g == gid)
            .map(|(r, _)| r as u32)
            .collect();
        let n = rows.len();
        let take_idx = arrow::array::UInt32Array::from(rows);
        let cols: Vec<_> = keep
            .iter()
            .map(|&c| arrow::compute::take(batch.column(c), &take_idx, None).unwrap())
            .collect();
        let sub = RecordBatch::try_new_with_options(
            Arc::clone(&data_schema),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| QsdbError::invalid(format!("split batch failed: {e}")))?;
        out.push((values.clone(), sub));
    }
    Ok(out)
}

/// 去掉分区列，返回纯数据 batch（无分区列场景的批也走这里统一 schema）。
fn strip_partition_columns(batch: &RecordBatch, def: &TableDef) -> Result<RecordBatch> {
    if def.partition_keys.is_empty() {
        return Ok(batch.clone());
    }
    let keep: Vec<usize> = (0..batch.num_columns())
        .filter(|&c| {
            let schema = batch.schema();
            let name = schema.field(c).name();
            !def.partition_keys.iter().any(|k| k == name)
        })
        .collect();
    RecordBatch::project(batch, &keep)
        .map_err(|e| QsdbError::invalid(format!("strip partition columns failed: {e}")))
}

fn projected_schema(schema: &SchemaRef, indices: &[usize]) -> SchemaRef {
    let fields: Vec<_> = indices.iter().map(|&i| schema.field(i).clone()).collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// 把分区列（按 `values` 常量）**补回**数据批 —— [`split_by_partition`] 的逆操作。
///
/// 用途：`Qsdb::overwrite_partition` / `append` 的输入约定是**全列批**（库里自己
/// 按分区列路由并剥离）。已经自行按分区切过、只想写某一个分区的客户端，需要把批
/// 恢复成全列形状——**别把剥过的批直接喂进去**（库里会再剥一次 → 报
/// `batch lacks partition column`）。HTTP `/write?mode=overwrite` 就踩过这个坑
/// （长期不可用），修法即本函数。
///
/// 输出列序 = `def.columns` 序；`values` 是 `(列名, 值)` 对（与 `overwrite_partition`
/// 同形），必须覆盖该表全部分区键。
pub fn attach_partition_columns(
    def: &TableDef,
    values: &[(&str, crate::types::Literal)],
    data: &RecordBatch,
) -> Result<RecordBatch> {
    let n = data.num_rows();
    let mut cols: Vec<arrow::array::ArrayRef> = Vec::with_capacity(def.columns.len());
    let mut fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(def.columns.len());
    for c in &def.columns {
        match values.iter().find(|(k, _)| k.eq_ignore_ascii_case(&c.name)) {
            Some((_, lit)) => {
                let dt = c.logical_type.arrow_type();
                cols.push(constant_array(lit, &dt, n)?);
            }
            None => {
                let idx = data.schema().index_of(&c.name).map_err(|_| {
                    QsdbError::invalid(format!(
                        "data batch lacks column '{}' (needed to rebuild the full-column batch)",
                        c.name
                    ))
                })?;
                cols.push(Arc::clone(data.column(idx)));
            }
        }
        fields.push(arrow::datatypes::Field::new(
            c.name.clone(),
            c.logical_type.arrow_type(),
            true,
        ));
    }
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new_with_options(
        schema,
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("attach partition columns failed: {e}")))
}

/// `Literal` → 长度 `n` 的常量数组（分区列重建用）。
fn constant_array(
    lit: &crate::types::Literal,
    dt: &arrow::datatypes::DataType,
    n: usize,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, StringArray,
    };
    Ok(match (lit, dt) {
        (crate::types::Literal::Boolean(v), arrow::datatypes::DataType::Boolean) => {
            Arc::new(BooleanArray::from(vec![*v; n]))
        }
        (crate::types::Literal::Int8(v), arrow::datatypes::DataType::Int8) => {
            Arc::new(Int8Array::from(vec![*v; n]))
        }
        (crate::types::Literal::Int16(v), arrow::datatypes::DataType::Int16) => {
            Arc::new(Int16Array::from(vec![*v; n]))
        }
        (crate::types::Literal::Int32(v), arrow::datatypes::DataType::Int32) => {
            Arc::new(Int32Array::from(vec![*v; n]))
        }
        (crate::types::Literal::Int64(v), arrow::datatypes::DataType::Int64) => {
            Arc::new(Int64Array::from(vec![*v; n]))
        }
        (crate::types::Literal::Float32(v), arrow::datatypes::DataType::Float32) => {
            Arc::new(Float32Array::from(vec![*v; n]))
        }
        (crate::types::Literal::Float64(v), arrow::datatypes::DataType::Float64) => {
            Arc::new(Float64Array::from(vec![*v; n]))
        }
        (crate::types::Literal::Utf8(v), arrow::datatypes::DataType::Utf8) => {
            Arc::new(StringArray::from(vec![v.as_str(); n]))
        }
        (crate::types::Literal::Date(v), arrow::datatypes::DataType::Date32) => {
            Arc::new(Date32Array::from(vec![*v; n]))
        }
        (
            crate::types::Literal::Time(v),
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond),
        ) => Arc::new(arrow::array::Time64NanosecondArray::from(vec![*v; n])),
        (
            crate::types::Literal::Timestamp(v),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _),
        ) => Arc::new(
            arrow::array::TimestampNanosecondArray::from(vec![*v; n]).with_data_type(dt.clone()),
        ),
        (
            crate::types::Literal::Decimal { unscaled, .. },
            arrow::datatypes::DataType::Decimal128(_, _),
        ) => Arc::new(
            arrow::array::Decimal128Array::from(vec![*unscaled; n]).with_data_type(dt.clone()),
        ),
        (l, dt) => {
            return Err(QsdbError::type_err(format!(
                "cannot build scalar of {dt:?} from {l:?}"
            )));
        }
    })
}

/// 分区值字符串 → 该列逻辑类型的 Literal（扫描裁剪用）。
pub fn parse_partition_value(raw: &str, ty: &LogicalType) -> Result<Literal> {
    let lit = match ty {
        LogicalType::Varchar { .. } | LogicalType::Char { .. } => Literal::Utf8(raw.to_string()),
        _ => crate::types::cast::cast_literal(&Literal::Utf8(raw.to_string()), ty)?,
    };
    Ok(lit)
}

// 用于 Arc 引用
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;
    use arrow::array::{Date32Array, Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    fn def_with(part_keys: &[&str]) -> TableDef {
        let cols = vec![
            ColumnDef {
                name: "dt".into(),
                logical_type: LogicalType::Date,
                nullable: true,
                check: None,
                default: None,
                comment: None,
            },
            ColumnDef {
                name: "region".into(),
                logical_type: LogicalType::Varchar { max_chars: None },
                nullable: true,
                check: None,
                default: None,
                comment: None,
            },
            ColumnDef {
                name: "v".into(),
                logical_type: LogicalType::Int32,
                nullable: true,
                check: None,
                default: None,
                comment: None,
            },
        ];
        TableDef::new("t", cols, part_keys.iter().map(|s| s.to_string()).collect()).unwrap()
    }

    #[test]
    fn encode_decode_roundtrip() {
        for raw in [
            "2026-10-09",
            "cn=north",
            "a/b",
            "100%",
            "weird\x01\x7f",
            "你好",
            "=",
        ] {
            let enc = encode_partition_value(raw);
            assert!(
                !enc.contains('/') || raw.contains('%'),
                "slash escaped: {enc}"
            );
            assert_eq!(decode_partition_value(&enc).unwrap(), raw, "raw={raw:?}");
        }
        assert!(decode_partition_value("bad%2").is_err());
        assert!(decode_partition_value("trailing%ZZ").is_err());
    }

    #[test]
    fn dir_roundtrip() {
        let keys = vec!["dt".to_string(), "region".to_string()];
        let vals = vec![Literal::Date(20735), Literal::Utf8("cn=east".into())];
        let dir = partition_dir(&keys, &vals).unwrap();
        assert_eq!(dir, PathBuf::from("dt=2026-10-09/region=cn%3Deast"));
        let parsed = parse_partition_dir(&dir, 2).unwrap();
        assert_eq!(parsed[0], ("dt".into(), "2026-10-09".into()));
        assert_eq!(parsed[1], ("region".into(), "cn=east".into()));
    }

    #[test]
    fn split_groups_and_schema() {
        let def = def_with(&["dt", "region"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(1), Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let groups = split_by_partition(&batch, &def).unwrap();
        assert_eq!(groups.len(), 3);
        // 每组只剩数据列 v
        assert_eq!(groups[0].1.schema().fields().len(), 1);
        assert_eq!(groups[0].1.schema().field(0).name(), "v");
        // 组值
        assert_eq!(
            groups[0].0,
            vec![Literal::Date(1), Literal::Utf8("a".into())]
        );
        assert_eq!(
            groups[1].0,
            vec![Literal::Date(1), Literal::Utf8("b".into())]
        );
        assert_eq!(
            groups[2].0,
            vec![Literal::Date(2), Literal::Utf8("a".into())]
        );
        // 行内容
        let arr = groups[1]
            .1
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(arr.values(), &[20]);
    }

    #[test]
    fn null_partition_value_rejected() {
        let def = def_with(&["dt"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(1), None])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        assert!(split_by_partition(&batch, &def).is_err());
    }

    #[test]
    fn parse_partition_value_types() {
        let d = Literal::Date(20735);
        assert_eq!(
            parse_partition_value("2026-10-09", &LogicalType::Date).unwrap(),
            d
        );
        assert_eq!(
            parse_partition_value("42", &LogicalType::Int64).unwrap(),
            Literal::Int64(42)
        );
        // 输入是已解码的目录名值（decode 发生在 parse_partition_dir）
        assert_eq!(
            parse_partition_value("x=y", &LogicalType::Varchar { max_chars: None }).unwrap(),
            Literal::Utf8("x=y".into())
        );
    }

    #[test]
    fn timestamp_partition_display() {
        // Timestamp 分区目录名用其 SQL 显示格式
        let ns_day = crate::types::temporal::NANOS_PER_DAY;
        let ts = Literal::Timestamp(ns_day);
        let dir = partition_dir(&["ts".to_string()], &[ts]).unwrap();
        let (k, v) = parse_partition_dir(&dir, 1).unwrap().pop().unwrap();
        assert_eq!(k, "ts");
        assert_eq!(v, "1970-01-02 00:00:00");
        assert_eq!(
            parse_partition_value(&v, &LogicalType::Timestamp { timezone: None }).unwrap(),
            Literal::Timestamp(ns_day)
        );
    }
}
