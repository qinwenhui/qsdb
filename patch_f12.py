# -*- coding: utf-8 -*-
# F12: read_parquet hive directory semantics (k=v path segments -> partition columns)
p='crates/qsdb/src/sql/translate.rs'
s=open(p,encoding='utf-8').read()

anchor='''    let files = crate::scan::resolve_file_locs(&path)?;
    let Some(first) = files.first() else {
        return Err(QsdbError::not_found(format!("read_parquet: {path}"));
    };
    let schema = crate::scan::file_arrow_schema(first)?;'''
add='''    let files = crate::scan::resolve_file_locs(&path)?;
    let Some(first) = files.first() else {
        return Err(QsdbError::not_found(format!("read_parquet: {path}"));
    };
    let schema = crate::scan::file_arrow_schema(first)?;
    // F12：hive 目录语义——路径含 k=v 段 → 目录名组装分区列常量
    // （分区列不在文件内，由路径重建；与分区裁剪协同）
    let hive_parts = crate::scan::extract_hive_partitions(&files);
    let hive_col_names: Vec<String> = hive_parts
        .as_ref()
        .map(|h| h.keys().cloned().collect())
        .unwrap_or_default();'''
assert anchor in s
s=s.replace(anchor, add, 1)

# extend synthetic TableDef with hive partition columns
old='''    let def = Arc::new(TableDef {
        version: 1,
        name: "read_parquet".into(),
        columns,
        partition_keys: Vec::new(),
        created_at: 0,
        properties: std::collections::HashMap::new(),
    });'''
new='''    // F12：hive 分区列追加为 nullable 列 + partition_keys
    let mut all_columns = columns;
    let partition_keys = if hive_parts.is_some() {
        let hive = hive_parts.as_ref().unwrap();
        for (k, v) in hive {
            if let Some(lit) = v.first() {
                all_columns.push(crate::catalog::ColumnDef {
                    name: k.clone(),
                    logical_type: lit.logical_type(),
                    nullable: true,
                    check: None,
                    default: None,
                    comment: None,
                });
            }
        }
        hive.keys().cloned().collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let _ = &hive_col_names;
    let def = Arc::new(TableDef {
        version: 1,
        name: "read_parquet".into(),
        columns: all_columns,
        partition_keys,
        created_at: 0,
        properties: std::collections::HashMap::new(),
    });'''
assert old in s; s=s.replace(old,new)
open(p,'w',encoding='utf-8').write(s)
print('translate hive ok')
