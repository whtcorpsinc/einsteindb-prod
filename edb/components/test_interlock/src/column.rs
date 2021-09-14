//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use super::*;

use milevadb_query_datatype::codec::{datum, Datum};
use milevadb_query_datatype::expr::EvalContext;
use fidel_timeshare::{PrimaryCausetInfo, FieldType};

pub const TYPE_VAR_CHAR: i32 = 1;
pub const TYPE_LONG: i32 = 2;

#[derive(Clone)]
pub struct PrimaryCauset {
    pub id: i64,
    pub(crate) col_type: i32,
    // negative means not a index key, 0 means primary key, positive means normal index key.
    pub index: i64,
    pub(crate) default_val: Option<Datum>,
}

impl PrimaryCauset {
    pub fn as_PrimaryCauset_info(&self) -> PrimaryCausetInfo {
        let mut c_info = PrimaryCausetInfo::default();
        c_info.set_PrimaryCauset_id(self.id);
        c_info.set_tp(self.col_field_type());
        c_info.set_pk_handle(self.index == 0);
        if let Some(ref dv) = self.default_val {
            c_info.set_default_val(
                datum::encode_value(&mut EvalContext::default(), &[dv.clone()]).unwrap(),
            )
        }
        c_info
    }

    pub fn as_field_type(&self) -> FieldType {
        let mut ft = FieldType::default();
        ft.set_tp(self.col_field_type());
        ft
    }

    pub fn col_field_type(&self) -> i32 {
        match self.col_type {
            TYPE_LONG => 8,      // FieldTypeTp::LongLong
            TYPE_VAR_CHAR => 15, // FieldTypeTp::VarChar
            _ => unreachable!("col_type: {}", self.col_type),
        }
    }
}

pub struct PrimaryCausetBuilder {
    col_type: i32,
    index: i64,
    default_val: Option<Datum>,
}

impl PrimaryCausetBuilder {
    pub fn new() -> PrimaryCausetBuilder {
        PrimaryCausetBuilder {
            col_type: TYPE_LONG,
            index: -1,
            default_val: None,
        }
    }

    pub fn col_type(mut self, t: i32) -> PrimaryCausetBuilder {
        self.col_type = t;
        self
    }

    pub fn primary_key(mut self, b: bool) -> PrimaryCausetBuilder {
        if b {
            self.index = 0;
        } else {
            self.index = -1;
        }
        self
    }

    pub fn index_key(mut self, idx_id: i64) -> PrimaryCausetBuilder {
        self.index = idx_id;
        self
    }

    pub fn default(mut self, val: Datum) -> PrimaryCausetBuilder {
        self.default_val = Some(val);
        self
    }

    pub fn build(self) -> PrimaryCauset {
        PrimaryCauset {
            id: next_id(),
            col_type: self.col_type,
            index: self.index,
            default_val: self.default_val,
        }
    }
}
