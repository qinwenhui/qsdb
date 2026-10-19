//! Arrow IPC 通道（`feature = "ipc"` 才编译）。
//!
//! 打开这个 feature 会引入 `arrow` 依赖，因而**与内核的 arrow 版本锁步**——
//! 这是本 crate 唯一想让你自己掏钱的地方（默认形态只有 `serde` + `serde_json`）。
//!
//! 什么时候值得开：
//!
//! - 你手上**已经是** Arrow 批（Rust 生态的列式计算），不想为了装载再转成
//!   [`Column`](crate::Column)；
//! - 你要把结果直接喂给 arrow 的算子（零拷贝、免转置）；
//! - 需要**嵌套类型**（`bulk`/`rows` 都**显式拒绝**嵌套：结构体/列表/Map 一律报错）。
//!
//! 否则用默认的 `bulk` 就够（同档速度、零 arrow 依赖）。
//!
//! ## 两个方向的约定不一样（别对称地想）
//!
//! - **读**：服务端**保留空批**——它正是"零行结果 + 完整 schema"的载体
//!   （见 `对接契约` §二之一），所以解码**不过滤**空批；
//! - **写**：空批是**无操作**（服务端写缓冲对空输入直接返回，见
//!   `write_buffer::push`），写进去也不会多一个段。

use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};

/// 解码一份 Arrow IPC **流**。
///
/// **空批照收**（零行结果的 schema 就在那个空批里）。
pub fn decode_all(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let mut r = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| Error::Decode(format!("ipc: 流头不合法: {e}")))?;
    let mut out = Vec::new();
    for b in r.by_ref() {
        out.push(b.map_err(|e| Error::Decode(format!("ipc: 批解码失败: {e}")))?);
    }
    Ok(out)
}

/// 编码若干 Arrow 批成一份 IPC 流（各批必须**同 schema**）。
pub fn encode(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let first = batches.first().ok_or_else(|| {
        Error::Usage("ipc: 没有批就没有 schema——零行写入本来也是无操作，别发这个请求".into())
    })?;
    let schema = first.schema();
    for b in batches {
        if b.schema() != schema {
            return Err(Error::Usage(
                "ipc: 各批 schema 不一致（服务端只按第一份 schema 理解整条流）".into(),
            ));
        }
    }
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| Error::Decode(format!("ipc: 建流失败: {e}")))?;
        for b in batches {
            w.write(b)
                .map_err(|e| Error::Decode(format!("ipc: 写批失败: {e}")))?;
        }
        w.finish()
            .map_err(|e| Error::Decode(format!("ipc: 收尾失败: {e}")))?;
    }
    Ok(buf)
}
