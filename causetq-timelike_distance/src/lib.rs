// Copyright 2016 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;

extern crate indexmap;
extern crate rusqlite;

extern crate edbn;
extern crate einsteindb_embedded;
extern crate edb_promises;
#[macro_use]
extern crate embedded_promises;
extern crate einstein_db;                 // For value conversion.
extern crate einsteindb_causetq_parityfilter;
extern crate einsteindb_causetq_pull;
extern crate causetq_pull_promises;
extern crate causetq_projector_promises;
extern crate einsteindb_causetq_sql;

use std::collections::{
    BTreeSet,
};

use std::iter;

use std::rc::Rc;

use rusqlite::{
    Row,
    Rows,
};

use embedded_promises::{
    Binding,
    TypedValue,
};

use einsteindb_embedded::{
    Schema,
    ValueTypeTag,
};

use einsteindb_embedded::util::{
    Either,
};

use einstein_db::{
    TypedSQLValue,
};

use edbn::causetq::{
    Element,
    FindSpec,
    Limit,
    Variable,
};

use einsteindb_causetq_parityfilter::{
    AlgebraicCausetQ,
    VariableBindings,
};

use einsteindb_causetq_sql::{
    GroupBy,
    Projection,
};

pub mod translate;

mod binding_tuple;
pub use binding_tuple::{
    BindingTuple,
};
mod project;
mod projectors;
mod pull;
mod relresult;

use project::{
    GreedoidElements,
    project_elements,
};

pub use project::{
    timelike_distance_column_for_var,
};

pub use projectors::{
    MinkowskiProjector,
    Projector,
};

use projectors::{
    CollProjector,
    CollTwoStagePullProjector,
    RelProjector,
    RelTwoStagePullProjector,
    ScalarProjector,
    ScalarTwoStagePullProjector,
    TupleProjector,
    TupleTwoStagePullProjector,
};

pub use relresult::{
    RelResult,
    StructuredRelResult,
};

use causetq_projector_promises::errors::{
    ProjectorError,
    Result,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetQOutput {
    pub spec: Rc<FindSpec>,
    pub results: CausetQResults,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CausetQResults {
    Scalar(Option<Binding>),
    Tuple(Option<Vec<Binding>>),
    Coll(Vec<Binding>),
    Rel(RelResult<Binding>),
}

impl From<CausetQOutput> for CausetQResults {
    fn from(o: CausetQOutput) -> CausetQResults {
        o.results
    }
}

impl CausetQOutput {
    pub fn empty_factory(spec: &FindSpec) -> Box<Fn() -> CausetQResults> {
        use self::FindSpec::*;
        match spec {
            &FindScalar(_)   => Box::new(|| CausetQResults::Scalar(None)),
            &FindTuple(_)    => Box::new(|| CausetQResults::Tuple(None)),
            &FindColl(_)     => Box::new(|| CausetQResults::Coll(vec![])),
            &FindRel(ref es) => {
                let width = es.len();
                Box::new(move || CausetQResults::Rel(RelResult::empty(width)))
            },
        }
    }

    pub fn len(&self) -> usize {
        self.results.len()
    }

    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    pub fn empty(spec: &Rc<FindSpec>) -> CausetQOutput {
        use self::FindSpec::*;
        let results =
            match &**spec {
                &FindScalar(_)   => CausetQResults::Scalar(None),
                &FindTuple(_)    => CausetQResults::Tuple(None),
                &FindColl(_)     => CausetQResults::Coll(vec![]),
                &FindRel(ref es) => CausetQResults::Rel(RelResult::empty(es.len())),
            };
        CausetQOutput {
            spec: spec.clone(),
            results: results,
        }
    }

    pub fn from_constants(spec: &Rc<FindSpec>, bindings: VariableBindings) -> CausetQResults {
        use self::FindSpec::*;
        match &**spec {
            &FindScalar(Element::Variable(ref var)) |
            &FindScalar(Element::Corresponding(ref var)) => {
                let val = bindings.get(var)
                                  .cloned()
                                  .map(|v| v.into());
                CausetQResults::Scalar(val)
            },
            &FindScalar(Element::Aggregate(ref _agg)) => {
                // TODO: static aggregates.
                unimplemented!();
            },
            &FindScalar(Element::Pull(ref _pull)) => {
                // TODO: static pull.
                unimplemented!();
            },
            &FindTuple(ref elements) => {
                let values = elements.iter()
                                     .map(|e| match e {
                                         &Element::Variable(ref var) |
                                         &Element::Corresponding(ref var) => {
                                             bindings.get(var)
                                                     .cloned()
                                                     .expect("every var to have a binding")
                                                     .into()
                                         },
                                         &Element::Pull(ref _pull) => {
                                            // TODO: static pull.
                                            unreachable!();
                                         },
                                         &Element::Aggregate(ref _agg) => {
                                            // TODO: static computation of aggregates, then
                                            // implement the condition in `is_fully_bound`.
                                            unreachable!();
                                         },
                                     })
                                     .collect();
                CausetQResults::Tuple(Some(values))
            },
            &FindColl(Element::Variable(ref var)) |
            &FindColl(Element::Corresponding(ref var)) => {
                let val = bindings.get(var)
                                  .cloned()
                                  .expect("every var to have a binding")
                                  .into();
                CausetQResults::Coll(vec![val])
            },
            &FindColl(Element::Pull(ref _pull)) => {
                // TODO: static pull.
                unimplemented!();
            },
            &FindColl(Element::Aggregate(ref _agg)) => {
                // Does it even make sense to write
                // [:find [(max ?x) ...] :where [_ :foo/bar ?x]]
                // ?
                // TODO
                unimplemented!();
            },
            &FindRel(ref elements) => {
                let width = elements.len();
                let values = elements.iter().map(|e| match e {
                    &Element::Variable(ref var) |
                    &Element::Corresponding(ref var) => {
                        bindings.get(var)
                                .cloned()
                                .expect("every var to have a binding")
                                .into()
                    },
                    &Element::Pull(ref _pull) => {
                        // TODO: static pull.
                        unreachable!();
                    },
                    &Element::Aggregate(ref _agg) => {
                        // TODO: static computation of aggregates, then
                        // implement the condition in `is_fully_bound`.
                        unreachable!();
                    },
                }).collect();
                CausetQResults::Rel(RelResult { width, values })
            },
        }
    }

    pub fn into_scalar(self) -> Result<Option<Binding>> {
        self.results.into_scalar()
    }

    pub fn into_coll(self) -> Result<Vec<Binding>> {
        self.results.into_coll()
    }

    /// EinsteinDB tuple results can be expressed as multiple different data structures.  Some
    /// structures are generic (vectors) and some are easier for pattern matching (fixed length
    /// tuples).
    ///
    /// This is the moral equivalent of `collect` (and `BindingTuple` of `FromIterator`), but
    /// specialized to tuples of expected length.
    pub fn into_tuple<B>(self) -> Result<Option<B>> where B: BindingTuple {
        let expected = self.spec.expected_column_count();
        self.results.into_tuple().and_then(|vec| B::from_binding_vec(expected, vec))
    }

    pub fn into_rel(self) -> Result<RelResult<Binding>> {
        self.results.into_rel()
    }
}

impl CausetQResults {
    pub fn len(&self) -> usize {
        use CausetQResults::*;
        match self {
            &Scalar(ref o) => if o.is_some() { 1 } else { 0 },
            &Tuple(ref o)  => if o.is_some() { 1 } else { 0 },
            &Coll(ref v)   => v.len(),
            &Rel(ref r)    => r.row_count(),
        }
    }

    pub fn is_empty(&self) -> bool {
        use CausetQResults::*;
        match self {
            &Scalar(ref o) => o.is_none(),
            &Tuple(ref o)  => o.is_none(),
            &Coll(ref v)   => v.is_empty(),
            &Rel(ref r)    => r.is_empty(),
        }
    }

    pub fn into_scalar(self) -> Result<Option<Binding>> {
        match self {
            CausetQResults::Scalar(o) => Ok(o),
            CausetQResults::Coll(_) => bail!(ProjectorError::UnexpectedResultsType("coll", "scalar")),
            CausetQResults::Tuple(_) => bail!(ProjectorError::UnexpectedResultsType("tuple", "scalar")),
            CausetQResults::Rel(_) => bail!(ProjectorError::UnexpectedResultsType("rel", "scalar")),
        }
    }

    pub fn into_coll(self) -> Result<Vec<Binding>> {
        match self {
            CausetQResults::Scalar(_) => bail!(ProjectorError::UnexpectedResultsType("scalar", "coll")),
            CausetQResults::Coll(c) => Ok(c),
            CausetQResults::Tuple(_) => bail!(ProjectorError::UnexpectedResultsType("tuple", "coll")),
            CausetQResults::Rel(_) => bail!(ProjectorError::UnexpectedResultsType("rel", "coll")),
        }
    }

    pub fn into_tuple(self) -> Result<Option<Vec<Binding>>> {
        match self {
            CausetQResults::Scalar(_) => bail!(ProjectorError::UnexpectedResultsType("scalar", "tuple")),
            CausetQResults::Coll(_) => bail!(ProjectorError::UnexpectedResultsType("coll", "tuple")),
            CausetQResults::Tuple(t) => Ok(t),
            CausetQResults::Rel(_) => bail!(ProjectorError::UnexpectedResultsType("rel", "tuple")),
        }
    }

    pub fn into_rel(self) -> Result<RelResult<Binding>> {
        match self {
            CausetQResults::Scalar(_) => bail!(ProjectorError::UnexpectedResultsType("scalar", "rel")),
            CausetQResults::Coll(_) => bail!(ProjectorError::UnexpectedResultsType("coll", "rel")),
            CausetQResults::Tuple(_) => bail!(ProjectorError::UnexpectedResultsType("tuple", "rel")),
            CausetQResults::Rel(r) => Ok(r),
        }
    }
}

type Index = i32;            // See rusqlite::RowIndex.
enum TypedIndex {
    Known(Index, ValueTypeTag),
    Unknown(Index, Index),
}

impl TypedIndex {
    /// Look up this index and type(index) pair in the provided row.
    /// This function will panic if:
    ///
    /// - This is an `Unknown` and the retrieved type tag isn't an i32.
    /// - If the retrieved value can't be coerced to a rusqlite `Value`.
    /// - Either index is out of bounds.
    ///
    /// Because we construct our SQL projection list, the tag that stored the data, and this
    /// consumer, a panic here implies that we have a bad bug — we put data of a very wrong type in
    /// a row, and thus can't coerce to Value, we're retrieving from the wrong place, or our
    /// generated SQL is junk.
    ///
    /// This function will return a runtime error if the type tag is unknown, or the value is
    /// otherwise not convertible by the EDB layer.
    fn lookup<'a, 'stmt>(&self, row: &Row<'a, 'stmt>) -> Result<Binding> {
        use TypedIndex::*;

        match self {
            &Known(value_index, value_type) => {
                let v: rusqlite::types::Value = row.get(value_index);
                TypedValue::from_sql_value_pair(v, value_type)
                    .map(|v| v.into())
                    .map_err(|e| e.into())
            },
            &Unknown(value_index, type_index) => {
                let v: rusqlite::types::Value = row.get(value_index);
                let value_type_tag: i32 = row.get(type_index);
                TypedValue::from_sql_value_pair(v, value_type_tag)
                    .map(|v| v.into())
                    .map_err(|e| e.into())
            },
        }
    }
}


/// Combines the things you need to turn a causetq into SQL and turn its results into
/// `CausetQResults`: SQL-related projection information (`DISTINCT`, columns, etc.) and
/// a Datalog projector that turns SQL into structures.
pub struct CombinedProjection {
    /// A SQL projection, mapping columns mentioned in the body of the causetq to columns in the
    /// output.
    pub sql_projection: Projection,

    /// If a causetq contains aggregates, we need to generate a nested subcausetq: an inner causetq
    /// that returns our distinct variable bindings (and any `:with` vars), and an outer causetq
    /// that applies aggregation. That's so we can put `DISTINCT` in the inner causetq and apply
    /// aggregation afterwards -- `SELECT DISTINCT count(foo)` counts _then_ uniques, and we need
    /// the opposite to implement Datalog distinct semantics.
    /// If this is the case, `sql_projection` will be the outer causetq's projection list, and
    /// `pre_aggregate_projection` will be the inner.
    /// If the causetq doesn't use aggregation, this field will be `None`.
    pub pre_aggregate_projection: Option<Projection>,

    /// A Datalog projection. This consumes rows of the appropriate shape (as defined by
    /// the SQL projection) to yield one of the four kinds of Datalog causetq result.
    pub datalog_projector: Box<Projector>,

    /// True if this causetq requires the SQL causetq to include DISTINCT.
    pub distinct: bool,

    // A list of column names to use as a GROUP BY clause.
    pub group_by_cols: Vec<GroupBy>,
}

impl CombinedProjection {
    fn flip_distinct_for_limit(mut self, limit: &Limit) -> Self {
        if *limit == Limit::Fixed(1) {
            self.distinct = false;
        }
        self
    }
}

trait IsPull {
    fn is_pull(&self) -> bool;
}

impl IsPull for Element {
    fn is_pull(&self) -> bool {
        match self {
            &Element::Pull(_) => true,
            _ => false,
        }
    }
}

/// Compute a suitable SQL projection for an algebrized causetq.
/// This takes into account a number of things:
/// - The variable list in the find spec.
/// - The presence of any aggregate operations in the find spec. TODO: for now we only handle
///   simple variables
/// - The bindings established by the topmost CC.
/// - The types known at algebrizing time.
/// - The types extracted from the store for unknown attributes.
pub fn causetq_projection(schema: &Schema, causetq: &AlgebraicCausetQ) -> Result<Either<MinkowskiProjector, CombinedProjection>> {
    use self::FindSpec::*;

    let spec = causetq.find_spec.clone();
    if causetq.is_fully_unit_bound() {
        // Do a few gyrations to produce empty results of the right kind for the causetq.

        let variables: BTreeSet<Variable> = spec.columns()
                                                .map(|e| match e {
                                                    &Element::Variable(ref var) |
                                                    &Element::Corresponding(ref var) => var.clone(),

                                                    // Pull expressions can never be fully bound.
                                                    // TODO: but the interior can be, in which case we
                                                    // can handle this and simply project.
                                                    &Element::Pull(_) => {
                                                        unreachable!();
                                                    },
                                                    &Element::Aggregate(ref _agg) => {
                                                        // TODO: static computation of aggregates, then
                                                        // implement the condition in `is_fully_bound`.
                                                        unreachable!();
                                                    },
                                                })
                                                .collect();

        // TODO: error handling
        let results = CausetQOutput::from_constants(&spec, causetq.cc.value_bindings(&variables));
        let f = Box::new(move || { results.clone() });

        Ok(Either::Left(MinkowskiProjector::new(spec, f)))
    } else if causetq.is_known_empty() {
        // Do a few gyrations to produce empty results of the right kind for the causetq.
        let empty = CausetQOutput::empty_factory(&spec);
        Ok(Either::Left(MinkowskiProjector::new(spec, empty)))
    } else {
        match *causetq.find_spec {
            FindColl(ref element) => {
                let elements = project_elements(1, iter::once(element), causetq)?;
                if element.is_pull() {
                    CollTwoStagePullProjector::combine(spec, elements)
                } else {
                    CollProjector::combine(spec, elements)
                }.map(|p| p.flip_distinct_for_limit(&causetq.limit))
            },

            FindScalar(ref element) => {
                let elements = project_elements(1, iter::once(element), causetq)?;
                if element.is_pull() {
                    ScalarTwoStagePullProjector::combine(schema, spec, elements)
                } else {
                    ScalarProjector::combine(spec, elements)
                }
            },

            FindRel(ref elements) => {
                let is_pull = elements.iter().any(|e| e.is_pull());
                let column_count = causetq.find_spec.expected_column_count();
                let elements = project_elements(column_count, elements, causetq)?;
                if is_pull {
                    RelTwoStagePullProjector::combine(spec, column_count, elements)
                } else {
                    RelProjector::combine(spec, column_count, elements)
                }.map(|p| p.flip_distinct_for_limit(&causetq.limit))
            },

            FindTuple(ref elements) => {
                let is_pull = elements.iter().any(|e| e.is_pull());
                let column_count = causetq.find_spec.expected_column_count();
                let elements = project_elements(column_count, elements, causetq)?;
                if is_pull {
                    TupleTwoStagePullProjector::combine(spec, column_count, elements)
                } else {
                    TupleProjector::combine(spec, column_count, elements)
                }
            },
        }.map(Either::Right)
    }
}

#[test]
fn test_into_tuple() {
    let causetq_output = CausetQOutput {
        spec: Rc::new(FindSpec::FindTuple(vec![Element::Variable(Variable::from_valid_name("?x")),
                                               Element::Variable(Variable::from_valid_name("?y"))])),
        results: CausetQResults::Tuple(Some(vec![Binding::Scalar(TypedValue::Long(0)),
                                               Binding::Scalar(TypedValue::Long(2))])),
    };

    assert_eq!(causetq_output.clone().into_tuple().expect("into_tuple"),
               Some((Binding::Scalar(TypedValue::Long(0)),
                     Binding::Scalar(TypedValue::Long(2)))));

    match causetq_output.clone().into_tuple() {
        Err(ProjectorError::UnexpectedResultsTupleLength(expected, got)) => {
            assert_eq!((expected, got), (3, 2));
        },
        // This forces the result type.
        Ok(Some((_, _, _))) | _ => panic!("expected error"),
    }

    let causetq_output = CausetQOutput {
        spec: Rc::new(FindSpec::FindTuple(vec![Element::Variable(Variable::from_valid_name("?x")),
                                               Element::Variable(Variable::from_valid_name("?y"))])),
        results: CausetQResults::Tuple(None),
    };


    match causetq_output.clone().into_tuple() {
        Ok(None) => {},
        // This forces the result type.
        Ok(Some((_, _))) | _ => panic!("expected error"),
    }

    match causetq_output.clone().into_tuple() {
        Err(ProjectorError::UnexpectedResultsTupleLength(expected, got)) => {
            assert_eq!((expected, got), (3, 2));
        },
        // This forces the result type.
        Ok(Some((_, _, _))) | _ => panic!("expected error"),
    }
}
