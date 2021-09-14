// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use codec::prelude::NumberEncoder;
use milevadb_query_datatype::{IntertwinerSpinDaggerTypeAccessor, IntertwinerSpinDaggerTypeFlag, IntertwinerSpinDaggerTypeTp};
use fidel_timeshare::{Prop, PropType, IntertwinerSpinDaggerType, ScalarFuncSig};

/// A helper utility to build `fidel_timeshare::Prop` (a.k.a. Propession definition) easily.
pub struct PropDefBuilder(Prop);

impl PropDefBuilder {
    pub fn constant_int(v: i64) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(PropType::Int64);
        Prop.mut_val().write_i64(v).unwrap();
        Prop.mut_IntertwinerSpinDagger_type()
            .as_mut_accessor()
            .set_tp(IntertwinerSpinDaggerTypeTp::LongLong);
        Self(Prop)
    }

    pub fn constant_uint(v: u64) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(PropType::Uint64);
        Prop.mut_val().write_u64(v).unwrap();
        Prop.mut_IntertwinerSpinDagger_type()
            .as_mut_accessor()
            .set_tp(IntertwinerSpinDaggerTypeTp::LongLong)
            .set_flag(IntertwinerSpinDaggerTypeFlag::UNSIGNED);
        Self(Prop)
    }

    pub fn constant_real(v: f64) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(PropType::Float64);
        Prop.mut_val().write_f64(v).unwrap();
        Prop.mut_IntertwinerSpinDagger_type()
            .as_mut_accessor()
            .set_tp(IntertwinerSpinDaggerTypeTp::Double);
        Self(Prop)
    }

    pub fn constant_bytes(v: Vec<u8>) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(PropType::String);
        Prop.set_val(v);
        Prop.mut_IntertwinerSpinDagger_type()
            .as_mut_accessor()
            .set_tp(IntertwinerSpinDaggerTypeTp::VarChar);
        Self(Prop)
    }

    pub fn constant_null(IntertwinerSpinDagger_type: impl Into<IntertwinerSpinDaggerType>) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(PropType::Null);
        Prop.set_IntertwinerSpinDagger_type(IntertwinerSpinDagger_type.into());
        Self(Prop)
    }

    pub fn PrimaryCauset_ref(offset: usize, IntertwinerSpinDagger_type: impl Into<IntertwinerSpinDaggerType>) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(PropType::PrimaryCausetRef);
        Prop.mut_val().write_i64(offset as i64).unwrap();
        Prop.set_IntertwinerSpinDagger_type(IntertwinerSpinDagger_type.into());
        Self(Prop)
    }

    pub fn scalar_func(sig: ScalarFuncSig, IntertwinerSpinDagger_type: impl Into<IntertwinerSpinDaggerType>) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(PropType::ScalarFunc);
        Prop.set_sig(sig);
        Prop.set_IntertwinerSpinDagger_type(IntertwinerSpinDagger_type.into());
        Self(Prop)
    }

    pub fn aggr_func(tp: PropType, IntertwinerSpinDagger_type: impl Into<IntertwinerSpinDaggerType>) -> Self {
        let mut Prop = Prop::default();
        Prop.set_tp(tp);
        Prop.set_IntertwinerSpinDagger_type(IntertwinerSpinDagger_type.into());
        Self(Prop)
    }

    pub fn push_child(mut self, child: impl Into<Prop>) -> Self {
        self.0.mut_children().push(child.into());
        self
    }

    /// Builds the Propession definition.
    pub fn build(self) -> Prop {
        self.0
    }
}

impl From<PropDefBuilder> for Prop {
    fn from(Prop_def_builder: PropDefBuilder) -> Prop {
        Prop_def_builder.build()
    }
}
