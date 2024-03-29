//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use super::*;

use protobuf::Message;

use ekvproto::interlock::{KeyCone, Request};
use ekvproto::kvrpc_timeshare::Context;
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::{Aggregation, ExecType, FreeDaemon, IndexScan, Limit, Selection, BlockScan, TopN};
use fidel_timeshare::{ByItem, Expr, ExprType};
use fidel_timeshare::{Soliton, PosetDagRequest};

use milevadb_query_datatype::codec::{datum, Datum};
use edb::interlock::REQ_TYPE_DAG;
use violetabftstore::interlock::::codec::number::NumberEncoder;

pub struct DAGSelect {
    pub execs: Vec<FreeDaemon>,
    pub cols: Vec<PrimaryCausetInfo>,
    pub order_by: Vec<ByItem>,
    pub limit: Option<u64>,
    pub aggregate: Vec<Expr>,
    pub group_by: Vec<Expr>,
    pub key_cone: KeyCone,
    pub output_offsets: Option<Vec<u32>>,
}

impl DAGSelect {
    pub fn from(Block: &Block) -> DAGSelect {
        let mut exec = FreeDaemon::default();
        exec.set_tp(ExecType::TypeBlockScan);
        let mut tbl_scan = BlockScan::default();
        let mut Block_info = Block.Block_info();
        tbl_scan.set_Block_id(Block_info.get_Block_id());
        let PrimaryCausets_info = Block_info.take_PrimaryCausets();
        tbl_scan.set_PrimaryCausets(PrimaryCausets_info);
        exec.set_tbl_scan(tbl_scan);

        DAGSelect {
            execs: vec![exec],
            cols: Block.PrimaryCausets_info(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_cone: Block.get_record_cone_all(),
            output_offsets: None,
        }
    }

    pub fn from_index(Block: &Block, index: &PrimaryCauset) -> DAGSelect {
        let idx = index.index;
        let mut exec = FreeDaemon::default();
        exec.set_tp(ExecType::TypeIndexScan);
        let mut scan = IndexScan::default();
        let mut index_info = Block.index_info(idx, true);
        scan.set_Block_id(index_info.get_Block_id());
        scan.set_index_id(idx);

        let PrimaryCausets_info = index_info.take_PrimaryCausets();
        scan.set_PrimaryCausets(PrimaryCausets_info.clone());
        exec.set_idx_scan(scan);

        let cone = Block.get_index_cone_all(idx);
        DAGSelect {
            execs: vec![exec],
            cols: PrimaryCausets_info.to_vec(),
            order_by: vec![],
            limit: None,
            aggregate: vec![],
            group_by: vec![],
            key_cone: cone,
            output_offsets: None,
        }
    }

    pub fn limit(mut self, n: u64) -> DAGSelect {
        self.limit = Some(n);
        self
    }

    pub fn order_by(mut self, col: &PrimaryCauset, desc: bool) -> DAGSelect {
        let col_offset = offset_for_PrimaryCauset(&self.cols, col.id);
        let mut item = ByItem::default();
        let mut expr = Expr::default();
        expr.set_field_type(col.as_field_type());
        expr.set_tp(ExprType::PrimaryCausetRef);
        expr.mut_val().encode_i64(col_offset).unwrap();
        item.set_expr(expr);
        item.set_desc(desc);
        self.order_by.push(item);
        self
    }

    pub fn count(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::Count)
    }

    pub fn aggr_col(mut self, col: &PrimaryCauset, aggr_t: ExprType) -> DAGSelect {
        let col_offset = offset_for_PrimaryCauset(&self.cols, col.id);
        let mut col_expr = Expr::default();
        col_expr.set_field_type(col.as_field_type());
        col_expr.set_tp(ExprType::PrimaryCausetRef);
        col_expr.mut_val().encode_i64(col_offset).unwrap();
        let mut expr = Expr::default();
        let mut expr_ft = col.as_field_type();
        // Avg will contains two auxiliary PrimaryCausets (sum, count) and the sum should be a `Decimal`
        if aggr_t == ExprType::Avg || aggr_t == ExprType::Sum {
            expr_ft.set_tp(0xf6); // FieldTypeTp::NewDecimal
        }
        expr.set_field_type(expr_ft);
        expr.set_tp(aggr_t);
        expr.mut_children().push(col_expr);
        self.aggregate.push(expr);
        self
    }

    pub fn first(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::First)
    }

    pub fn sum(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::Sum)
    }

    pub fn avg(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::Avg)
    }

    pub fn max(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::Max)
    }

    pub fn min(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::Min)
    }

    pub fn bit_and(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::AggBitAnd)
    }

    pub fn bit_or(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::AggBitOr)
    }

    pub fn bit_xor(self, col: &PrimaryCauset) -> DAGSelect {
        self.aggr_col(col, ExprType::AggBitXor)
    }

    pub fn group_by(mut self, cols: &[&PrimaryCauset]) -> DAGSelect {
        for col in cols {
            let offset = offset_for_PrimaryCauset(&self.cols, col.id);
            let mut expr = Expr::default();
            expr.set_field_type(col.as_field_type());
            expr.set_tp(ExprType::PrimaryCausetRef);
            expr.mut_val().encode_i64(offset).unwrap();
            self.group_by.push(expr);
        }
        self
    }

    pub fn output_offsets(mut self, output_offsets: Option<Vec<u32>>) -> DAGSelect {
        self.output_offsets = output_offsets;
        self
    }

    pub fn where_expr(mut self, expr: Expr) -> DAGSelect {
        let mut exec = FreeDaemon::default();
        exec.set_tp(ExecType::TypeSelection);
        let mut selection = Selection::default();
        selection.mut_conditions().push(expr);
        exec.set_selection(selection);
        self.execs.push(exec);
        self
    }

    pub fn build(self) -> Request {
        self.build_with(Context::default(), &[0])
    }

    pub fn build_with(mut self, ctx: Context, flags: &[u64]) -> Request {
        if !self.aggregate.is_empty() || !self.group_by.is_empty() {
            let mut exec = FreeDaemon::default();
            exec.set_tp(ExecType::TypeAggregation);
            let mut aggr = Aggregation::default();
            if !self.aggregate.is_empty() {
                aggr.set_agg_func(self.aggregate.into());
            }

            if !self.group_by.is_empty() {
                aggr.set_group_by(self.group_by.into());
            }
            exec.set_aggregation(aggr);
            self.execs.push(exec);
        }

        if !self.order_by.is_empty() {
            let mut exec = FreeDaemon::default();
            exec.set_tp(ExecType::TypeTopN);
            let mut topn = TopN::default();
            topn.set_order_by(self.order_by.into());
            if let Some(limit) = self.limit.take() {
                topn.set_limit(limit);
            }
            exec.set_top_n(topn);
            self.execs.push(exec);
        }

        if let Some(l) = self.limit.take() {
            let mut exec = FreeDaemon::default();
            exec.set_tp(ExecType::TypeLimit);
            let mut limit = Limit::default();
            limit.set_limit(l);
            exec.set_limit(limit);
            self.execs.push(exec);
        }

        let mut posetdag = PosetDagRequest::default();
        posetdag.set_executors(self.execs.into());
        posetdag.set_flags(flags.iter().fold(0, |acc, f| acc | *f));
        posetdag.set_collect_cone_counts(true);

        let output_offsets = if self.output_offsets.is_some() {
            self.output_offsets.take().unwrap()
        } else {
            (0..self.cols.len() as u32).collect()
        };
        posetdag.set_output_offsets(output_offsets);

        let mut req = Request::default();
        req.set_spacelike_ts(next_id() as u64);
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(posetdag.write_to_bytes().unwrap());
        req.set_cones(vec![self.key_cone].into());
        req.set_context(ctx);
        req
    }
}

pub struct DAGSolitonSpliter {
    Solitons: Vec<Soliton>,
    datums: Vec<Datum>,
    col_cnt: usize,
}

impl DAGSolitonSpliter {
    pub fn new(Solitons: Vec<Soliton>, col_cnt: usize) -> DAGSolitonSpliter {
        DAGSolitonSpliter {
            Solitons,
            col_cnt,
            datums: Vec::with_capacity(0),
        }
    }
}

impl Iteron for DAGSolitonSpliter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Vec<Datum>> {
        loop {
            if self.Solitons.is_empty() && self.datums.is_empty() {
                return None;
            } else if self.datums.is_empty() {
                let Soliton = self.Solitons.remove(0);
                let mut data = Soliton.get_rows_data();
                self.datums = datum::decode(&mut data).unwrap();
                continue;
            }
            assert_eq!(self.datums.len() >= self.col_cnt, true);
            let mut cols = self.datums.split_off(self.col_cnt);
            std::mem::swap(&mut self.datums, &mut cols);
            return Some(cols);
        }
    }
}
