//! LogicalPlan → ExecNode。聚合遇「Scan(/Filter) 无破坏算子」时做
//! 文件级 rayon 并行（分片 partial agg → merge → finalize）。

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::api::{Qsdb, ScanRequest};
use crate::error::Result;
use crate::exec::agg::GroupTable;
use crate::exec::{ExecNode, FilterExec, HashAggExec, LimitExec, ProjectExec, ScanExec, SortExec};
use crate::expr::{AggExpr, PhysExpr};

use super::translate::LogicalPlan;

/// 一次性批执行节点（并行聚合结果）。
struct OneShotExec {
    batch: Option<RecordBatch>,
    schema: Arc<arrow::datatypes::Schema>,
}

impl ExecNode for OneShotExec {
    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        self.batch.take().map(Ok)
    }
}

pub fn build(db: &Qsdb, plan: &LogicalPlan) -> Result<Box<dyn ExecNode>> {
    match plan {
        LogicalPlan::Scan { def, predicate } => {
            let scan = db.scan(ScanRequest {
                table: def.name.clone(),
                predicate: predicate.clone(),
                ..Default::default()
            })?;
            Ok(Box::new(ScanExec::new(scan)))
        }
        LogicalPlan::Filter { input, predicate } => Ok(Box::new(FilterExec::new(
            build(db, input)?,
            predicate.clone(),
        ))),
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => Ok(Box::new(ProjectExec::new(
            build(db, input)?,
            exprs.clone(),
            names.clone(),
        ))),
        LogicalPlan::Aggregate { input, group, aggs } => build_aggregate(db, input, group, aggs),
        LogicalPlan::Sort { input, keys } => Ok(Box::new(SortExec::new(
            build(db, input)?,
            keys.clone(),
            None,
        ))),
        LogicalPlan::Limit { input, n } => Ok(Box::new(LimitExec {
            input: build(db, input)?,
            remaining: *n,
        })),
        LogicalPlan::Distinct { input } => {
            Ok(Box::new(crate::exec::DistinctExec::new(build(db, input)?)))
        }
    }
}

/// 聚合构建：可分片源 → 并行 partial/merge；否则串行。
fn build_aggregate(
    db: &Qsdb,
    input: &LogicalPlan,
    group: &[PhysExpr],
    aggs: &[AggExpr],
) -> Result<Box<dyn ExecNode>> {
    let Some((shards, filters)) = try_shard_source(db, input) else {
        return Ok(Box::new(HashAggExec::new(
            build(db, input)?,
            group.to_vec(),
            aggs.to_vec(),
        )?));
    };

    let mut fields: Vec<arrow::datatypes::Field> = group
        .iter()
        .map(|g| arrow::datatypes::Field::new(g.display(), g.data_type().arrow_type(), true))
        .collect();
    for a in aggs {
        fields.push(arrow::datatypes::Field::new(
            &a.out_name,
            a.out_ty.arrow_type(),
            true,
        ));
    }
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));

    let group = group.to_vec();
    let aggs = aggs.to_vec();

    use rayon::prelude::*;
    let tables: Vec<GroupTable> = shards
        .into_par_iter()
        .map(|s| run_partial(s, &filters, &group, &aggs))
        .collect::<Result<Vec<_>>>()?;

    // merge（单线程）
    let mut merged = GroupTable::new();
    merged.set_arity(aggs.len());
    for t in tables {
        merged.merge(t, aggs.len());
    }
    if merged.is_empty() {
        return Ok(Box::new(OneShotExec {
            batch: None,
            schema,
        }));
    }
    let keys = merged.take_group_keys();
    let states = merged.take_states();
    let batch = crate::exec::agg::finalize(&keys, &states, &aggs, &group, &schema)?;
    Ok(Box::new(OneShotExec {
        batch: Some(batch),
        schema,
    }))
}

/// 单分片 partial 聚合（当前线程执行；由 rayon 并行调度调用方决定）。
fn run_partial(
    scan: crate::scan::Scan,
    filters: &[PhysExpr],
    group: &[PhysExpr],
    aggs: &[AggExpr],
) -> Result<GroupTable> {
    let mut node: Box<dyn ExecNode> = Box::new(ScanExec::new(scan));
    for f in filters {
        node = Box::new(FilterExec::new(node, f.clone()));
    }
    let mut exec = HashAggExec::new(node, group.to_vec(), aggs.to_vec())?;
    while exec.next().is_some() {
        // 驱动至耗尽；状态在 exec.state
    }
    Ok(std::mem::take(&mut exec.state))
}

/// 源分片：`(Scan | Filter)+` 链且 scan 可切多片 → Some((分片, 逆序 filters))。
fn try_shard_source(
    db: &Qsdb,
    plan: &LogicalPlan,
) -> Option<(Vec<crate::scan::Scan>, Vec<PhysExpr>)> {
    let mut filters: Vec<PhysExpr> = Vec::new();
    let mut cur = plan;
    loop {
        match cur {
            LogicalPlan::Filter { input, predicate } => {
                filters.push(predicate.clone());
                cur = input;
            }
            LogicalPlan::Scan { def, predicate } => {
                let scan = db
                    .scan(ScanRequest {
                        table: def.name.clone(),
                        predicate: predicate.clone(),
                        ..Default::default()
                    })
                    .ok()?;
                let threads = rayon::current_num_threads().clamp(1, 16);
                let shards = scan.shard(threads)?;
                return Some((shards, filters));
            }
            _ => return None,
        }
    }
}
