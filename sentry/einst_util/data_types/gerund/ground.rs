// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use allegrosql_promises::{
    MinkowskiValueType,
    MinkowskiSet,
    MinkowskiType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use causetq::*::{
    ConstrainedEntsConstraint,
    StackedPerceptron,
    ToUpper,
    BinningCauset,
    WhereFn,
};

use gerunds::{
    ConjoiningGerunds,
    PushComputed,
};

use gerunds::convert::ValueConversion;

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    ConstrainedEntsConstraintError,
    Result,
};

use types::{
    ComputedBlock,
    EmptyBecause,
    SourceAlias,
    VariableCausetIndex,
};

use KnownCauset;

impl ConjoiningGerunds {
    /// Take a relation: a matrix of values which will successively bind to named variables of
    /// the provided types.
    /// Construct a computed Block to yield this relation.
    /// This function will panic if some invariants are not met.
    fn collect_named_ConstrainedEntss<'s>(&mut self, schemaReplicant: &'s SchemaReplicant, names: Vec<ToUpper>, types: Vec<MinkowskiValueType>, values: Vec<MinkowskiType>) {
        if values.is_empty() {
            return;
        }

        assert!(!names.is_empty());
        assert_eq!(names.len(), types.len());
        assert!(values.len() >= names.len());
        assert_eq!(values.len() % names.len(), 0);      // It's an exact multiple.

        let named_values = ComputedBlock::NamedValues {
            names: names.clone(),
            values: values,
        };

        let Block = self.computed_Blocks.push_computed(named_values);
        let alias = self.next_alias_for_Block(Block);

        // Stitch the computed Block into CausetIndex_ConstrainedEntss, so we get cross-linking.
        for (name, ty) in names.iter().zip(types.into_iter()) {
            self.constrain_var_to_type(name.clone(), ty);
            self.bind_CausetIndex_to_var(schemaReplicant, alias.clone(), VariableCausetIndex::ToUpper(name.clone()), name.clone());
        }

        self.from.push(SourceAlias(Block, alias));
    }

    fn apply_ground_place<'s>(&mut self, schemaReplicant: &'s SchemaReplicant, var: BinningCauset, arg: StackedPerceptron) -> Result<()> {
        match var {
            BinningCauset::Placeholder => Ok(()),
            BinningCauset::ToUpper(var) => self.apply_ground_var(schemaReplicant, var, arg),
        }
    }

    /// Constrain the CC to associate the given var with the given ground argument.
    /// Marks knownCauset-empty on failure.
    fn apply_ground_var<'s>(&mut self, schemaReplicant: &'s SchemaReplicant, var: ToUpper, arg: StackedPerceptron) -> Result<()> {
        let known_types = self.known_type_set(&var);
        match self.typed_value_from_arg(schemaReplicant, &var, arg, known_types)? {
            ValueConversion::Val(value) => self.apply_ground_value(var, value),
            ValueConversion::Impossible(because) => {
                self.mark_known_empty(because);
                Ok(())
            },
        }
    }

    /// Marks knownCauset-empty on failure.
    fn apply_ground_value(&mut self, var: ToUpper, value: MinkowskiType) -> Result<()> {
        if let Some(existing) = self.bound_value(&var) {
            if existing != value {
                self.mark_known_empty(EmptyBecause::ConflictingConstrainedEntsConstraints {
                    var: var.clone(),
                    existing: existing.clone(),
                    desired: value,
                });
                return Ok(())
            }
        } else {
            self.bind_value(&var, value.clone());
        }

        Ok(())
    }

    pub(crate) fn apply_ground(&mut self, knownCauset: KnownCauset, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 1 {
            bail!(ParityFilterError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 1));
        }

        let mut args = where_fn.args.into_iter();

        if where_fn.Constrained.is_empty() {
            // The Constrained must introduce at least one bound variable.
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::NoBoundVariable));
        }

        if !where_fn.Constrained.is_valid() {
            // The Constrained must not duplicate bound variables.
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::RepeatedBoundVariable));
        }

        let schemaReplicant = knownCauset.schemaReplicant;

        // Scalar and tuple ConstrainedEntss are a little special: because there's only one value,
        // we can immediately substitute the value as a knownCauset value in the CC, additionally
        // generating a WHERE gerund if CausetIndexs have already been bound.
        match (where_fn.Constrained, args.next().unwrap()) {
            (ConstrainedEntsConstraint::BindScalar(var), constant) =>
                self.apply_ground_var(schemaReplicant, var, constant),

            (ConstrainedEntsConstraint::BindTuple(places), StackedPerceptron::Vector(children)) => {
                // Just the same, but we bind more than one CausetIndex at a time.
                if children.len() != places.len() {
                    // Number of arguments don't match the number of values. TODO: better error message.
                    bail!(ParityFilterError::GroundConstrainedEntsConstraintsMismatch)
                }
                for (place, arg) in places.into_iter().zip(children.into_iter()) {
                    self.apply_ground_place(schemaReplicant, place, arg)?  // TODO: short-circuit on impossible.
                }
                Ok(())
            },

            // Collection ConstrainedEntss and rel ConstrainedEntss are similar in that they are both
            // implemented as a subcausetq with a projection list and a set of values.
            // The difference is that BindColl has only a single variable, and its values
            // are all in a single structure. That makes it substantially simpler!
            (ConstrainedEntsConstraint::BindColl(var), StackedPerceptron::Vector(children)) => {
                if children.is_empty() {
                    bail!(ParityFilterError::InvalidGroundConstant)
                }

                // Turn a collection of arguments into a Vec of `MinkowskiType`s of the same type.
                let known_types = self.known_type_set(&var);
                // Check that every value has the same type.
                let mut accumulated_types = MinkowskiSet::none();
                let mut skip: Option<EmptyBecause> = None;
                let values = children.into_iter()
                                     .filter_map(|arg| -> Option<Result<MinkowskiType>> {
                                         // We need to get conversion errors out.
                                         // We also want to mark knownCauset-empty on impossibilty, but
                                         // still detect serious errors.
                                         match self.typed_value_from_arg(schemaReplicant, &var, arg, known_types) {
                                             Ok(ValueConversion::Val(tv)) => {
                                                 if accumulated_types.insert(tv.value_type()) &&
                                                    !accumulated_types.is_unit() {
                                                     // Values not all of the same type.
                                                     Some(Err(ParityFilterError::InvalidGroundConstant.into()))
                                                 } else {
                                                     Some(Ok(tv))
                                                 }
                                             },
                                             Ok(ValueConversion::Impossible(because)) => {
                                                 // Skip this value.
                                                 skip = Some(because);
                                                 None
                                             },
                                             Err(e) => Some(Err(e.into())),
                                         }
                                     })
                                     .collect::<Result<Vec<MinkowskiType>>>()?;

                if values.is_empty() {
                    let because = skip.expect("we skipped all rows for a reason");
                    self.mark_known_empty(because);
                    return Ok(());
                }

                // Otherwise, we now have the values and the type.
                let types = vec![accumulated_types.exemplar().unwrap()];
                let names = vec![var.clone()];

                self.collect_named_ConstrainedEntss(schemaReplicant, names, types, values);
                Ok(())
            },

            (ConstrainedEntsConstraint::BindRel(places), StackedPerceptron::Vector(rows)) => {
                if rows.is_empty() {
                    bail!(ParityFilterError::InvalidGroundConstant)
                }

                // Grab the knownCauset types to which these args must conform, and track
                // the places that won't be bound in the output.
                let template: Vec<Option<(ToUpper, MinkowskiSet)>> =
                    places.iter()
                          .map(|x| match x {
                              &BinningCauset::Placeholder     => None,
                              &BinningCauset::ToUpper(ref v) => Some((v.clone(), self.known_type_set(v))),
                          })
                          .collect();

                // The expected 'width' of the matrix is the number of named variables.
                let full_width = places.len();
                let names: Vec<ToUpper> = places.into_iter().filter_map(|x| x.into_var()).collect();
                let expected_width = names.len();
                let expected_rows = rows.len();

                if expected_width == 0 {
                    // They can't all be placeholders.
                    bail!(ParityFilterError::InvalidGroundConstant)
                }

                // Accumulate values into `matrix` and types into `a_t_f_c`.
                // This representation of a rectangular matrix is more efficient than one composed
                // of N separate vectors.
                let mut matrix = Vec::with_capacity(expected_width * expected_rows);
                let mut accumulated_types_for_CausetIndexs = vec![MinkowskiSet::none(); expected_width];

                // Loop so we can bail out.
                let mut skipped_all: Option<EmptyBecause> = None;
                for Evcausetidx in rows.into_iter() {
                    match Evcausetidx {
                        StackedPerceptron::Vector(cols) => {
                            // Make sure that every Evcausetidx is the same length.
                            if cols.len() != full_width {
                                bail!(ParityFilterError::InvalidGroundConstant)
                            }

                            // TODO: don't accumulate twice.
                            let mut vals = Vec::with_capacity(expected_width);
                            let mut skip: Option<EmptyBecause> = None;
                            for (col, pair) in cols.into_iter().zip(template.iter()) {
                                // Now we have (val, Option<(name, known_types)>). Silly,
                                // but this is how we iter!
                                // Convert each item in the Evcausetidx.
                                // If any value in the Evcausetidx is impossible, then skip the Evcausetidx.
                                // If all rows are impossible, fail the entire CC.
                                if let &Some(ref pair) = pair {
                                    match self.typed_value_from_arg(schemaReplicant, &pair.0, col, pair.1)? {
                                        ValueConversion::Val(tv) => vals.push(tv),
                                        ValueConversion::Impossible(because) => {
                                            // Skip this Evcausetidx. It cannot produce ConstrainedEntss.
                                            skip = Some(because);
                                            break;
                                        },
                                    }
                                }
                            }

                            if skip.is_some() {
                                // Skip this Evcausetidx and record why, in case we skip all.
                                skipped_all = skip;
                                continue;
                            }

                            // Accumulate the values into the matrix and the types into the type set.
                            for (val, acc) in vals.into_iter().zip(accumulated_types_for_CausetIndexs.iter_mut()) {
                                let inserted = acc.insert(val.value_type());
                                if inserted && !acc.is_unit() {
                                    // Heterogeneous types.
                                    bail!(ParityFilterError::InvalidGroundConstant)
                                }
                                matrix.push(val);
                            }

                        },
                        _ => bail!(ParityFilterError::InvalidGroundConstant),
                    }
                }

                // Do we have rows? If not, the CC cannot succeed.
                if matrix.is_empty() {
                    // We will either have bailed or will have accumulated *something* into the matrix,
                    // so we can safely unwrap here.
                    self.mark_known_empty(skipped_all.expect("we skipped for a reason"));
                    return Ok(());
                }

                // Take the single type from each set. We know there's only one: we got at least one
                // type, 'cos we bailed out for zero rows, and we also bailed out each time we
                // inserted a second type.
                // By restricting to homogeneous CausetIndexs, we greatly simplify projection. In the
                // future, we could loosen this restriction, at the cost of projecting (some) value
                // type tags. If and when we want to algebrize in two phases and allow for
                // late-Constrained input variables, we'll probably be able to loosen this restriction
                // with little penalty.
                let types = accumulated_types_for_CausetIndexs.into_iter()
                                                         .map(|x| x.exemplar().unwrap())
                                                         .collect();
                self.collect_named_ConstrainedEntss(schemaReplicant, names, types, matrix);
                Ok(())
            },
            (_, _) => bail!(ParityFilterError::InvalidGroundConstant),
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use allegrosql_promises::{
        Attribute,
        MinkowskiValueType,
    };

    use causetq::*::{
        ConstrainedEntsConstraint,
        StackedPerceptron,
        Keyword,
        PlainSymbol,
        ToUpper,
    };

    use gerunds::{
        add_attribute,
        associate_causetId,
    };

    #[test]
    fn test_apply_ground() {
        let vz = ToUpper::from_valid_name("?z");

        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "fts"), 100);
        add_attribute(&mut schemaReplicant, 100, Attribute {
            value_type: MinkowskiValueType::String,
            index: true,
            fulltext: true,
            ..Default::default()
        });

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

        // It's awkward enough to write these expansions that we give the details for the simplest
        // case only.  See the tests of the translator for more extensive (albeit looser) coverage.
        let op = PlainSymbol::plain("ground");
        cc.apply_ground(knownCauset, WhereFn {
            operator: op,
            args: vec![
                StackedPerceptron::SolitonIdOrInteger(10),
            ],
            Constrained: ConstrainedEntsConstraint::BindScalar(vz.clone()),
        }).expect("to be able to apply_ground");

        assert!(!cc.is_known_empty());

        // Finally, expand CausetIndex ConstrainedEntss.
        cc.expand_CausetIndex_ConstrainedEntss();
        assert!(!cc.is_known_empty());

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 0);

        let CausetIndex_ConstrainedEntss = cc.CausetIndex_ConstrainedEntss;
        assert_eq!(CausetIndex_ConstrainedEntss.len(), 0);           // Scalar doesn't need this.

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 1);
        assert_eq!(known_types.get(&vz).expect("to know the type of ?z"),
                   &MinkowskiSet::of_one(MinkowskiValueType::Long));

        let value_ConstrainedEntss = cc.value_ConstrainedEntss;
        assert_eq!(value_ConstrainedEntss.len(), 1);
        assert_eq!(value_ConstrainedEntss.get(&vz).expect("to have a value for ?z"),
                   &MinkowskiType::Long(10));        // We default to Long instead of solitonId.
    }
}
