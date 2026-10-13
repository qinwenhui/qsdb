use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
fn main() {
    let n = 500_000usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| format!("region-{:02}-{:040}", i % 50, i))
                    .collect::<Vec<String>>(),
            )),
        ],
    )
    .unwrap();
    let s = b.slice(0, 8192);
    println!("整批 500k: batch_bytes={}", qsdb::memory::batch_bytes(&b));
    println!(
        "切片 8192: batch_bytes={}  (逻辑约 {} 字节)",
        qsdb::memory::batch_bytes(&s),
        8192 * (8 + 54)
    );
    println!(
        "  切片列0 slice_mem={:?} 列1 slice_mem={:?}",
        s.column(0).to_data().get_slice_memory_size(),
        s.column(1).to_data().get_slice_memory_size()
    );
}
