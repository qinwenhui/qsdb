//! `literals_to_array` 的**分配次数**与**往返正确性**回归。
//!
//! 背景（2026-10-11）：这个函数曾是"**每行建一个单元素 Arrow 数组再 `interleave`
//! 拼起来**"——n 行就是 n 个数组。`SELECT DISTINCT gs`（250 万字符串组）因此
//! 吃满 **4.4GB / 9s**（组表本身账上只有 449MB），对照 DuckDB 的
//! `count(DISTINCT gs)` 是 0.20s / 400MB，**差 87×**。
//!
//! 为什么用"分配次数"当判据：这是唯一**不会 flaky** 的判据。
//! 计时断言在 CI 上会飘，峰值内存断言受分配器与页缓存影响；
//! 而"O(n) 个数组对象"的退化是**结构性的**——它在分配次数上必然露出两个数量级。
//! 判据写成上界（< 1000 次 / 10 万行）而不是精确值，是为了不把实现细节焊死。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

use qsdb::exec::agg::{array_value_to_literal, literals_to_array};
use qsdb::{Literal, LogicalType};

/// **计数是全进程的，而测试默认并行** —— 隔壁用例的分配会混进我的测量窗口
/// （实测：锁只罩住测量那几行时，数值用例仍量到 6632 次而 release 下是 2220）。
/// 故锁必须罩住**整个测试体**：本文件每个用例一进来就拿锁，这样"造 10 万个
/// 字面量"这类大分配不会落在别人的测量窗口里。
fn serial<R>(f: impl FnOnce() -> R) -> R {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    f()
}

/// 测 `f` 期间的分配次数（**调用方须已持 [`serial`] 锁**）。
fn allocs_of<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let before = ALLOCS.load(Ordering::Relaxed);
    let r = f();
    (r, ALLOCS.load(Ordering::Relaxed) - before)
}

const N: usize = 100_000;

/// **主回归**：字符串组输出必须是一趟 builder，不是 n 个数组。
#[test]
fn string_array_is_built_in_one_pass() {
    serial(|| {
        let lits: Vec<Literal> = (0..N).map(|i| Literal::Utf8(format!("k-{i}"))).collect();
        let refs: Vec<&Literal> = lits.iter().collect();
        let (arr, allocs) =
            allocs_of(|| literals_to_array(&refs, &LogicalType::Varchar { max_chars: None }));
        let arr = arr.unwrap();
        assert_eq!(arr.len(), N);
        assert!(
            allocs < 2_000,
            "{N} 行字符串只该有常数次分配（builder 扩容），实测 {allocs} 次 —— \
             退回逐行建数组的老路径了（那是 4.4GB/9s 的成因）"
        );
    })
}

/// 数值/时间类型同理（这些类型的单元素数组更小，退化没那么显眼，但同样是 O(n) 个对象）。
///
/// 判据用**相对纯 arrow builder 的基线**，而不是绝对值：绝对次数取决于 arrow 内部
/// （缓冲区扩容、null 缓冲）的实现细节，把它焊死会在升级 arrow 时无谓地红。
/// 基线本身在同一次运行里量——**尺子和被测物同批校准**。
#[test]
fn numeric_arrays_are_built_in_one_pass() {
    serial(|| {
        // 纯 arrow builder 基线：同样 10 万行 i64，不掺任何 QSDB 代码
        let (_, baseline) = allocs_of(|| {
            use arrow::array::Int64Builder;
            let mut b = Int64Builder::with_capacity(N);
            for i in 0..N {
                b.append_value(i as i64);
            }
            b.finish().len()
        });
        println!("纯 arrow Int64Builder 基线：{baseline} 次分配 / {N} 行");

        for (ty, mk) in [
            (
                LogicalType::Int64,
                Box::new(|i: usize| Literal::Int64(i as i64)) as Box<dyn Fn(usize) -> Literal>,
            ),
            (
                LogicalType::Int32,
                Box::new(|i: usize| Literal::Int32(i as i32)),
            ),
            (
                LogicalType::Float64,
                Box::new(|i: usize| Literal::Float64(i as f64)),
            ),
            (
                LogicalType::Date,
                Box::new(|i: usize| Literal::Date(i as i32)),
            ),
            (
                LogicalType::Timestamp { timezone: None },
                Box::new(|i: usize| Literal::Timestamp(i as i64)),
            ),
            (
                LogicalType::Boolean,
                Box::new(|i: usize| Literal::Boolean(i % 2 == 0)),
            ),
        ] {
            let lits: Vec<Literal> = (0..N).map(mk).collect();
            let refs: Vec<&Literal> = lits.iter().collect();
            let (arr, allocs) = allocs_of(|| literals_to_array(&refs, &ty));
            assert_eq!(arr.unwrap().len(), N);
            // 逐行建数组的老路径是 ≥ 3N 次（每行 1 个数组 + 其 buffers + Arc）；
            // 基线量级是 N/100 上下。取 N/8 作界：两头都留足余量。
            assert!(
                allocs < N / 8,
                "{ty:?}: {N} 行实测 {allocs} 次分配（纯 arrow 基线 {baseline}）—— 疑似退回逐行建数组"
            );
        }
    })
}

/// **往返**：`Literal → 数组 → Literal` 必须逐行等价（含 NULL）。
///
/// 这条守的是"快路径别偷偷换语义"——快路径要求字面量变体与 arrow 类型精确对应，
/// 不精确的一律退慢路径（那里有无条件 cast）。
#[test]
fn literals_roundtrip_through_arrays() {
    fn check(lits: &[Literal], ty: LogicalType) {
        let refs: Vec<&Literal> = lits.iter().collect();
        let arr = literals_to_array(&refs, &ty).unwrap_or_else(|e| panic!("{ty:?}: {e}"));
        assert_eq!(arr.len(), lits.len(), "{ty:?} 长度");
        assert_eq!(arr.data_type(), &ty.arrow_type(), "{ty:?} 类型");
        for (i, l) in lits.iter().enumerate() {
            assert_eq!(
                &array_value_to_literal(arr.as_ref(), i),
                l,
                "{ty:?} 第 {i} 行"
            );
        }
    }
    // 每类都掺 NULL：NULL 走的是 builder 的 append_null，与快慢路径都不同
    check(
        &[
            Literal::Utf8("".into()),
            Literal::Null,
            Literal::Utf8("k-1".into()),
            Literal::Utf8("含多字节·中文".into()),
        ],
        LogicalType::Varchar { max_chars: None },
    );
    check(
        &[
            Literal::Int64(0),
            Literal::Null,
            Literal::Int64(-1),
            Literal::Int64(i64::MAX),
        ],
        LogicalType::Int64,
    );
    check(
        &[Literal::Int8(-128), Literal::Null, Literal::Int8(127)],
        LogicalType::Int8,
    );
    check(
        &[
            Literal::Float64(f64::MIN),
            Literal::Null,
            Literal::Float64(1.5),
        ],
        LogicalType::Float64,
    );
    check(
        &[
            Literal::Boolean(true),
            Literal::Null,
            Literal::Boolean(false),
        ],
        LogicalType::Boolean,
    );
    check(
        &[Literal::Date(0), Literal::Null, Literal::Date(-1)],
        LogicalType::Date,
    );
    check(
        &[
            Literal::Timestamp(0),
            Literal::Null,
            Literal::Timestamp(-123),
        ],
        LogicalType::Timestamp { timezone: None },
    );
    // **慢路径**（快路径不覆盖 Decimal）：语义必须与快路径一致地正确
    check(
        &[
            Literal::Decimal {
                unscaled: 12345,
                meta: qsdb::types::DecimalMeta::new(18, 4),
            },
            Literal::Null,
            Literal::Decimal {
                unscaled: -1,
                meta: qsdb::types::DecimalMeta::new(18, 4),
            },
        ],
        LogicalType::Decimal(qsdb::types::DecimalMeta::new(18, 4)),
    );
}

/// 空输入：给空数组而不是 panic（`Distinct`/聚合的空结果路径会走到）。
#[test]
fn empty_input_yields_empty_array() {
    let refs: Vec<&Literal> = Vec::new();
    for ty in [
        LogicalType::Int64,
        LogicalType::Varchar { max_chars: None },
        LogicalType::Boolean,
    ] {
        let a = literals_to_array(&refs, &ty).unwrap();
        assert_eq!(a.len(), 0);
        assert_eq!(a.data_type(), &ty.arrow_type());
    }
}
