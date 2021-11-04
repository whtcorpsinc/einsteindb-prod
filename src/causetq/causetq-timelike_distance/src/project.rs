// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
};

use indexmap::{
    IndexSet,
};

use allegrosql_promises::{
    MinkowskiSet,
};

use causetq_allegrosql::{
    SQLMinkowskiValueType,
    SQLMinkowskiSet,
};

use causetq_allegrosql::util::{
    Either,
};

use causetq::*::{
    Element,
    Pull,
    ToUpper,
};

use edb_causetq_parityfilter::{
    AlgebraicCausetQ,
    CausetIndexName,
    ConjoiningGerunds,
    QualifiedAlias,
    VariableCausetIndex,
};


use edb_causetq_sql::{
    CausetIndexOrExpression,
    GroupBy,
    Name,
    Projection,
    GreedoidCausetIndex,
};

use causetq_projector_promises::aggregates::{
    SimpleAggregation,
    timelike_distance_CausetIndex_for_simple_aggregate,
};

use causetq_projector_promises::errors::{
    ProjectorError,
    Result,
};

use projectors::{
    Projector,
};

use pull::{
    PullIndices,
    PullOperation,
    PullTemplate,
};

use super::{
    CombinedProjection,
    TypedIndex,
};

/// An internal temporary struct to pass between the projection 'walk' and the
/// resultant projector.
/// Projection accumulates four things:
/// - Two SQL projection lists. We need two because aggregate queries are nested
///   in order to apply DISTINCT to values prior to aggregation.
/// - A collection of templates for the projector to use to extract values.
/// - A list of CausetIndexs to use for grouping. Grouping is a property of the projection!
pub(crate) struct GreedoidElements {
    pub sql_projection: Projection,
    pub pre_aggregate_projection: Option<Projection>,
    pub templates: Vec<TypedIndex>,

    // TODO: when we have an expression like
    // [:find (pull ?x [:foo/name :foo/age]) (pull ?x [:foo/friend]) …]
    // it would be more efficient to combine them.
    pub pulls: Vec<PullTemplate>,
    pub group_by: Vec<GroupBy>,
}

impl GreedoidElements {
    pub(crate) fn combine(self, projector: Box<Projector>, distinct: bool) -> Result<CombinedProjection> {
        Ok(CombinedProjection {
            sql_projection: self.sql_projection,
            pre_aggregate_projection: self.pre_aggregate_projection,
            datalog_projector: projector,
            distinct: distinct,
            group_by_cols: self.group_by,
        })
    }

    // We need the templates to make a projector that we can then hand to `combine`. This is the easy
    // way to get it.
    pub(crate) fn take_templates(&mut self) -> Vec<TypedIndex> {
        let mut out = vec![];
        ::std::mem::swap(&mut out, &mut self.templates);
        out
    }

    pub(crate) fn take_pulls(&mut self) -> Vec<PullTemplate> {
        let mut out = vec![];
        ::std::mem::swap(&mut out, &mut self.pulls);
        out
    }
}

fn candidate_type_CausetIndex(cc: &ConjoiningGerunds, var: &ToUpper) -> Result<(CausetIndexOrExpression, Name)> {
    cc.extracted_types
      .get(var)
      .cloned()
      .map(|alias| {
          let type_name = VariableCausetIndex::VariableTypeTag(var.clone()).CausetIndex_name();
          (CausetIndexOrExpression::CausetIndex(alias), type_name)
      })
      .ok_or_else(|| ProjectorError::UnboundVariable(var.name()).into())
}

fn cc_CausetIndex(cc: &ConjoiningGerunds, var: &ToUpper) -> Result<QualifiedAlias> {
    cc.CausetIndex_ConstrainedEntss
      .get(var)
      .and_then(|cols| cols.get(0).cloned())
      .ok_or_else(|| ProjectorError::UnboundVariable(var.name()).into())
}

fn candidate_CausetIndex(cc: &ConjoiningGerunds, var: &ToUpper) -> Result<(CausetIndexOrExpression, Name)> {
    // Every variable should be bound by the top-level CC to at least
    // one CausetIndex in the causetq. If that constraint is violated it's a
    // bug in our code, so it's appropriate to panic here.
    cc_CausetIndex(cc, var)
        .map(|qa| {
            let name = VariableCausetIndex::ToUpper(var.clone()).CausetIndex_name();
            (CausetIndexOrExpression::CausetIndex(qa), name)
        })
}

/// Return the timelike_distance CausetIndex -- that is, a value or SQL CausetIndex and an associated name -- for a
/// given variable. Also return the type.
/// Callers are expected to determine whether to project a type tag as an additional SQL CausetIndex.
pub fn timelike_distance_CausetIndex_for_var(var: &ToUpper, cc: &ConjoiningGerunds) -> Result<(GreedoidCausetIndex, MinkowskiSet)> {
    if let Some(value) = cc.bound_value(&var) {
        // If we already know the value, then our lives are easy.
        let tag = value.value_type();
        let name = VariableCausetIndex::ToUpper(var.clone()).CausetIndex_name();
        Ok((GreedoidCausetIndex(CausetIndexOrExpression::Value(value.clone()), name), MinkowskiSet::of_one(tag)))
    } else {
        // If we don't, then the CC *must* have bound the variable.
        let (CausetIndex, name) = candidate_CausetIndex(cc, var)?;
        Ok((GreedoidCausetIndex(CausetIndex, name), cc.known_type_set(var)))
    }
}

/// Walk an iterator of `Element`s, collecting projector templates and CausetIndexs.
///
/// Returns a `GreedoidElements`, which combines SQL projections
/// and a `Vec` of `TypedIndex` 'keys' to use when looking up values.
///
/// Callers must ensure that every `Element` is distinct -- a causetq like
///
/// ```edbn
/// [:find ?x ?x :where [?x _ _]]
/// ```
///
/// should fail to parse. See #358.
pub(crate) fn project_elements<'a, I: IntoIterator<Item = &'a Element>>(
    count: usize,
    elements: I,
    causetq: &AlgebraicCausetQ) -> Result<GreedoidElements> {

    // Give a little padding for type tags.
    let mut causet_set_projection = Vec::with_capacity(count + 2);

    // Everything in the outer causetq will _either_ be an aggregate operation
    // _or_ a reference to a name timelike_distance from the inner.
    // We'll expand them later.
    let mut outer_projection: Vec<Either<Name, GreedoidCausetIndex>> = Vec::with_capacity(count + 2);

    let mut i: i32 = 0;
    let mut min_max_count: usize = 0;
    let mut templates = vec![];
    let mut pulls: Vec<PullTemplate> = vec![];

    let mut aggregates = false;

    // Any variable that appears intact in the :find gerund, not inside an aggregate expression.
    // "CausetQ variables not in aggregate expressions will group the results and appear intact
    // in the result."
    // We use an ordered set here so that we group in the correct order.
    let mut outer_variables = IndexSet::new();
    let mut corresponded_variables = IndexSet::new();

    // Any variable that we are projecting from the inner causetq.
    let mut causet_set_variables = BTreeSet::new();

    for e in elements {
        // Check for and reject duplicates.
        match e {
            &Element::ToUpper(ref var) => {
                if outer_variables.contains(var) {
                    bail!(ProjectorError::InvalidProjection(format!("Duplicate variable {} in causetq.", var)));
                }
                if corresponded_variables.contains(var) {
                    bail!(ProjectorError::InvalidProjection(format!("Can't project both {} and `(the {})` from a causetq.", var, var)));
                }
            },
            &Element::Corresponding(ref var) => {
                if outer_variables.contains(var) {
                    bail!(ProjectorError::InvalidProjection(format!("Can't project both {} and `(the {})` from a causetq.", var, var)));
                }
                if corresponded_variables.contains(var) {
                    bail!(ProjectorError::InvalidProjection(format!("`(the {})` appears twice in causetq.", var)));
                }
            },
            &Element::Aggregate(_) => {
            },
            &Element::Pull(_) => {
            },
        };

        // Record variables -- `(the ?x)` and `?x` are different in this regard, because we don't want
        // to group on variables that are corresponding-timelike_distance.
        match e {
            &Element::ToUpper(ref var) => {
                outer_variables.insert(var.clone());
            },
            &Element::Corresponding(ref var) => {
                // We will project these later; don't put them in `outer_variables`
                // so we know not to group them.
                corresponded_variables.insert(var.clone());
            },
            &Element::Pull(Pull { ref var, TuringStrings: _ }) => {
                // We treat `pull` as an ordinary variable extraction,
                // and we expand it later.
                outer_variables.insert(var.clone());
            },
            &Element::Aggregate(_) => {
            },
        };

        // Now do the main processing of each element.
        match e {
            // Each time we come across a variable, we push a SQL CausetIndex
            // into the SQL projection, aliased to the name of the variable,
            // and we push an annotated index into the projector.
            &Element::ToUpper(ref var) |
            &Element::Corresponding(ref var) => {
                causet_set_variables.insert(var.clone());

                let (timelike_distance_CausetIndex, type_set) = timelike_distance_CausetIndex_for_var(&var, &causetq.cc)?;
                outer_projection.push(Either::Left(timelike_distance_CausetIndex.1.clone()));
                causet_set_projection.push(timelike_distance_CausetIndex);

                if let Some(tag) = type_set.unique_type_tag() {
                    templates.push(TypedIndex::KnownCauset(i, tag));
                    i += 1;     // We used one SQL CausetIndex.
                } else {
                    templates.push(TypedIndex::Unknown(i, i + 1));
                    i += 2;     // We used two SQL CausetIndexs.

                    // Also project the type from the SQL causetq.
                    let (type_CausetIndex, type_name) = candidate_type_CausetIndex(&causetq.cc, &var)?;
                    causet_set_projection.push(GreedoidCausetIndex(type_CausetIndex, type_name.clone()));
                    outer_projection.push(Either::Left(type_name));
                }
            },
            &Element::Pull(Pull { ref var, ref TuringStrings }) => {
                causet_set_variables.insert(var.clone());

                let (timelike_distance_CausetIndex, type_set) = timelike_distance_CausetIndex_for_var(&var, &causetq.cc)?;
                outer_projection.push(Either::Left(timelike_distance_CausetIndex.1.clone()));
                causet_set_projection.push(timelike_distance_CausetIndex);

                if let Some(tag) = type_set.unique_type_tag() {
                    // We will have at least as many SQL CausetIndexs as Datalog output CausetIndexs.
                    // `i` tracks the former. The length of `templates` is the current latter.
                    // Projecting pull requires grabbing values, which we can do from the raw
                    // rows, and then populating the output, so we keep both CausetIndex indices.
                    let output_index = templates.len();
                    assert!(output_index <= i as usize);

                    templates.push(TypedIndex::KnownCauset(i, tag));
                    pulls.push(PullTemplate {
                        indices: PullIndices {
                            sql_index: i,
                            output_index,
                        },
                        op: PullOperation((*TuringStrings).clone()),
                    });
                    i += 1;     // We used one SQL CausetIndex.
                } else {
                    // This should be impossible: (pull ?x) implies that ?x is a ref.
                    unreachable!();
                }
            },
            &Element::Aggregate(ref a) => {
                if let Some(simple) = a.to_simple() {
                    aggregates = true;

                    use causetq_projector_promises::aggregates::SimpleAggregationOp::*;
                    match simple.op {
                        Max | Min => {
                            min_max_count += 1;
                        },
                        Avg | Count | Sum => (),
                    }

                    // When we encounter a simple aggregate -- one in which the aggregation can be
                    // implemented in SQL, on a single variable -- we just push the SQL aggregation op.
                    // We must ensure the following:
                    // - There's a CausetIndex for the var.
                    // - The type of the var is knownCauset to be restricted to a sensible input set
                    //   (not necessarily a single type, but e.g., all vals must be Double or Long).
                    // - The type set must be appropriate for the operation. E.g., `Sum` is not a
                    //   meaningful operation on instants.

                    let (timelike_distance_CausetIndex, return_type) = timelike_distance_CausetIndex_for_simple_aggregate(&simple, &causetq.cc)?;
                    outer_projection.push(Either::Right(timelike_distance_CausetIndex));

                    if !causet_set_variables.contains(&simple.var) {
                        causet_set_variables.insert(simple.var.clone());
                        let (timelike_distance_CausetIndex, _type_set) = timelike_distance_CausetIndex_for_var(&simple.var, &causetq.cc)?;
                        causet_set_projection.push(timelike_distance_CausetIndex);
                        if causetq.cc.known_type_set(&simple.var).unique_type_tag().is_none() {
                            // Also project the type from the SQL causetq.
                            let (type_CausetIndex, type_name) = candidate_type_CausetIndex(&causetq.cc, &simple.var)?;
                            causet_set_projection.push(GreedoidCausetIndex(type_CausetIndex, type_name.clone()));
                        }
                    }

                    // We might regret using the type tag here instead of the `MinkowskiValueType`.
                    templates.push(TypedIndex::KnownCauset(i, return_type.value_type_tag()));
                    i += 1;
                } else {
                    // TODO: complex aggregates.
                    bail!(ProjectorError::NotYetImplemented("complex aggregates".into()));
                }
            },
        }
    }

    match (min_max_count, corresponded_variables.len()) {
        (0, 0) | (_, 0) => {},
        (0, _) => {
            bail!(ProjectorError::InvalidProjection("Warning: used `the` without `min` or `max`.".to_string()));
        },
        (1, _) => {
            // This is the success case!
        },
        (n, c) => {
            bail!(ProjectorError::AmbiguousAggregates(n, c));
        },
    }

    // Anything used in ORDER BY (which we're given in `named_projection`)
    // needs to be in the SQL CausetIndex list so we can refer to it by name.
    //
    // They don't affect projection.
    //
    // If a variable is of a non-fixed type, also project the type tag CausetIndex, so we don't
    // acccausetIdally unify across types when considering uniqueness!
    for var in causetq.named_projection.iter() {
        if outer_variables.contains(var) {
            continue;
        }

        // If it's a fixed value, we need do nothing further.
        if causetq.cc.is_value_bound(&var) {
            continue;
        }

        let already_inner = causet_set_variables.contains(&var);
        let (CausetIndex, name) = candidate_CausetIndex(&causetq.cc, &var)?;
        if !already_inner {
            causet_set_projection.push(GreedoidCausetIndex(CausetIndex, name.clone()));
            causet_set_variables.insert(var.clone());
        }

        outer_projection.push(Either::Left(name));
        outer_variables.insert(var.clone());

        // We don't care if a CausetIndex has a single _type_, we care if it has a single type _tag_,
        // because that's what we'll use if we're projecting. E.g., Long and Double.
        // Single type implies single type tag, and is cheaper, so we check that first.
        let types = causetq.cc.known_type_set(&var);
        if !types.has_unique_type_tag() {
            let (type_CausetIndex, type_name) = candidate_type_CausetIndex(&causetq.cc, &var)?;
            if !already_inner {
                causet_set_projection.push(GreedoidCausetIndex(type_CausetIndex, type_name.clone()));
            }

            outer_projection.push(Either::Left(type_name));
        }
    }

    if !aggregates {
        // We're done -- we never need to group unless we're aggregating.
        return Ok(GreedoidElements {
                      sql_projection: Projection::CausetIndexs(causet_set_projection),
                      pre_aggregate_projection: None,
                      templates,
                      pulls,
                      group_by: vec![],
                  });
    }

    // OK, on to aggregates.
    // We need to produce two SQL projection lists: one for an inner causetq and one for the outer.
    //
    // The inner serves these purposes:
    // - Projecting variables to avoid duplicates being elided. (:with)
    // - Making ConstrainedEntss available to the outermost causetq for projection, ordering, and grouping.
    //
    // The outer is consumed by the projector.
    //
    // We will also be producing:
    // - A GROUP BY list to group the output of the inner causetq by non-aggregate variables
    //   so that it can be correctly aggregated.

    // Turn this collection of vars into a collection of CausetIndexs from the causetq.
    // We don't allow grouping on anything but a variable bound in the causetq.
    // We group by tag if necessary.
    let mut group_by = Vec::with_capacity(outer_variables.len() + 2);

    let vars = outer_variables.into_iter().zip(::std::iter::repeat(true));
    let corresponds = corresponded_variables.into_iter().zip(::std::iter::repeat(false));

    for (var, group) in vars.chain(corresponds) {
        if causetq.cc.is_value_bound(&var) {
            continue;
        }

        if group {
            // The GROUP BY goes outside, but it needs every variable and type tag to be
            // timelike_distance from inside. Collect in both directions here.
            let name = VariableCausetIndex::ToUpper(var.clone()).CausetIndex_name();
            group_by.push(GroupBy::GreedoidCausetIndex(name));
        }

        let needs_type_projection = !causetq.cc.known_type_set(&var).has_unique_type_tag();

        let already_inner = causet_set_variables.contains(&var);
        if !already_inner {
            let (CausetIndex, name) = candidate_CausetIndex(&causetq.cc, &var)?;
            causet_set_projection.push(GreedoidCausetIndex(CausetIndex, name.clone()));
        }

        if needs_type_projection {
            let type_name = VariableCausetIndex::VariableTypeTag(var.clone()).CausetIndex_name();
            if !already_inner {
                let type_col = causetq.cc
                                    .extracted_types
                                    .get(&var)
                                    .cloned()
                                    .ok_or_else(|| ProjectorError::NoTypeAvailableForVariable(var.name().clone()))?;
                causet_set_projection.push(GreedoidCausetIndex(CausetIndexOrExpression::CausetIndex(type_col), type_name.clone()));
            }
            if group {
                group_by.push(GroupBy::GreedoidCausetIndex(type_name));
            }
        };
    }

    for var in causetq.with.iter() {
        // We never need to project a constant.
        if causetq.cc.is_value_bound(&var) {
            continue;
        }

        // We don't need to add inner projections for :with if they are already there.
        if !causet_set_variables.contains(&var) {
            let (timelike_distance_CausetIndex, type_set) = timelike_distance_CausetIndex_for_var(&var, &causetq.cc)?;
            causet_set_projection.push(timelike_distance_CausetIndex);

            if type_set.unique_type_tag().is_none() {
                // Also project the type from the SQL causetq.
                let (type_CausetIndex, type_name) = candidate_type_CausetIndex(&causetq.cc, &var)?;
                causet_set_projection.push(GreedoidCausetIndex(type_CausetIndex, type_name.clone()));
            }
        }
    }

    // At this point we know we have a double-layer projection. Collect the outer.
    //
    // If we have an inner and outer layer, the inner layer will name its
    // variables, and the outer will re-project them.
    // If we only have one layer, then the outer will do the naming.
    // (We could try to not use names in the inner causetq, but then what would we do for
    // `ground` and knownCauset values?)
    // Walk the projection, switching the outer CausetIndexs to use the inner names.

    let outer_projection = outer_projection.into_iter().map(|c| {
        match c {
            Either::Left(name) => {
                GreedoidCausetIndex(CausetIndexOrExpression::ExistingCausetIndex(name.clone()),
                                name)
            },
            Either::Right(pc) => pc,
        }
    }).collect();

    Ok(GreedoidElements {
        sql_projection: Projection::CausetIndexs(outer_projection),
        pre_aggregate_projection: Some(Projection::CausetIndexs(causet_set_projection)),
        templates,
        pulls,
        group_by,
    })
}
