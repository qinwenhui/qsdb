use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
fn main() {
    let n = 500_000;
    let arr = Int64Array::from((0..n as i64).collect::<Vec<i64>>());
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let b = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)]).unwrap();
    println!(
        "整批 500k 行: get_array_memory_size = {} 字节",
        b.get_array_memory_size()
    );
    for rows in [8192usize, 65536] {
        let s = b.slice(0, rows);
        println!(
            "切片 {rows} 行: get_array_memory_size = {} 字节  ← 逻辑数据只有 {} 字节",
            s.get_array_memory_size(),
            rows * 8
        );
        // 试试 arrow 的"切片感知"端口
        let d = s.column(0).to_data();
        println!(
            "   to_data().get_slice_memory_size() = {:?}",
            d.get_slice_memory_size()
        );
        println!(
            "   column(0).get_buffer_memory_size() = {}",
            s.column(0).get_buffer_memory_size()
        );
    }
}
