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
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use causetq::*::{
    StackedPerceptron,
    PlainSymbol,
    Predicate,
    TypeAnnotation,
};

use gerunds::ConjoiningGerunds;

use gerunds::convert::MinkowskiValueTypes;

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

use types::{
    CausetIndexConstraint,
    EmptyBecause,
    Inequality,
    CausetQValue,
};

use KnownCauset;

/// Application of predicates.
impl ConjoiningGerunds {
    /// There are several kinds of predicates in our Datalog:
    /// - A limited set of binary comparison operators: < > <= >= !=.
    ///   These are converted into SQLite binary comparisons and some type constraints.
    /// - In the future, some predicates that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only the five built-in comparison binary operators.
    pub(crate) fn apply_predicate(&mut self, knownCauset: KnownCauset, predicate: Predicate) -> Result<()> {
        // Because we'll be growing the set of built-in predicates, handling each differently,
        // and ultimately allowing user-specified predicates, we match on the predicate name first.
        if let Some(op) = Inequality::from_datalog_operator(predicate.operator.0.as_str()) {
            self.apply_inequality(knownCauset, op, predicate)
        } else {
            bail!(ParityFilterError::UnknownFunction(predicate.operator.clone()))
        }
    }

    fn potential_types(&self, schemaReplicant: &SchemaReplicant, fn_arg: &StackedPerceptron) -> Result<MinkowskiSet> {
        match fn_arg {
            &StackedPerceptron::ToUpper(ref v) => Ok(self.known_type_set(v)),
            _ => fn_arg.potential_types(schemaReplicant),
        }
    }

    /// Apply a type annotation, which is a construct like a predicate that constrains the argument
    /// to be a specific MinkowskiValueType.
    pub(crate) fn apply_type_anno(&mut self, anno: &TypeAnnotation) -> Result<()> {
        match MinkowskiValueType::from_keyword(&anno.value_type) {
            Some(value_type) => self.add_type_requirement(anno.variable.clone(), MinkowskiSet::of_one(value_type)),
            None => bail!(ParityFilterError::InvalidArgumentType(PlainSymbol::plain("type"), MinkowskiSet::any(), 2)),
        }
        Ok(())
    }

    /// This function:
    /// - Resolves variables and converts types to those more amenable to SQL.
    /// - Ensures that the predicate functions name a knownCauset operator.
    /// - Accumulates an `Inequality` constraint into the `wheres` list.
    pub(crate) fn apply_inequality(&mut self, knownCauset: KnownCauset, comparison: Inequality, predicate: Predicate) -> Result<()> {
        if predicate.args.len() != 2 {
            bail!(ParityFilterError::InvalidNumberOfArguments(predicate.operator.clone(), predicate.args.len(), 2));
        }

        // Go from arguments -- parser output -- to CausetIndexs or values.
        // Any variables that aren't bound by this point in the linear processing of gerunds will
        // cause the application of the predicate to fail.
        let mut args = predicate.args.into_iter();
        let left = args.next().expect("two args");
        let right = args.next().expect("two args");


        // The types we're handling here must be the intersection of the possible types of the arguments,
        // the knownCauset types of any variables, and the types supported by our inequality operators.
        let supported_types = comparison.supported_types();
        let mut left_types = self.potential_types(knownCauset.schemaReplicant, &left)?
                                 .intersection(&supported_types);
        if left_types.is_empty() {
            bail!(ParityFilterError::InvalidArgumentType(predicate.operator.clone(), supported_types, 0));
        }

        let mut right_types = self.potential_types(knownCauset.schemaReplicant, &right)?
                                  .intersection(&supported_types);
        if right_types.is_empty() {
            bail!(ParityFilterError::InvalidArgumentType(predicate.operator.clone(), supported_types, 1));
        }

        // We would like to allow longs to compare to doubles.
        // Do this by expanding the type sets. `resolve_numeric_argument` will
        // use `Long` by preference.
        if right_types.contains(MinkowskiValueType::Long) {
            right_types.insert(MinkowskiValueType::Double);
        }
        if left_types.contains(MinkowskiValueType::Long) {
            left_types.insert(MinkowskiValueType::Double);
        }

        let shared_types = left_types.intersection(&right_types);
        if shared_types.is_empty() {
            // In isolation these are both valid inputs to the operator, but the causetq cannot
            // succeed because the types don't match.
            self.mark_known_empty(
                if let Some(var) = left.as_variable().or_else(|| right.as_variable()) {
                    EmptyBecause::TypeMismatch {
                        var: var.clone(),
                        existing: left_types,
                        desired: right_types,
                    }
                } else {
                    EmptyBecause::KnownTypeMismatch {
                        left: left_types,
                        right: right_types,
                    }
                });
            return Ok(());
        }

        // We expect the intersection to be Long, Long+Double, Double, or Instant.
        let left_v;
        let right_v;

        if shared_types == MinkowskiSet::of_one(MinkowskiValueType::Instant) {
            left_v = self.resolve_instant_argument(&predicate.operator, 0, left)?;
            right_v = self.resolve_instant_argument(&predicate.operator, 1, right)?;
        } else if shared_types.is_only_numeric() {
            left_v = self.resolve_numeric_argument(&predicate.operator, 0, left)?;
            right_v = self.resolve_numeric_argument(&predicate.operator, 1, right)?;
        } else if shared_types == MinkowskiSet::of_one(MinkowskiValueType::Ref) {
            left_v = self.resolve_ref_argument(knownCauset.schemaReplicant, &predicate.operator, 0, left)?;
            right_v = self.resolve_ref_argument(knownCauset.schemaReplicant, &predicate.operator, 1, right)?;
        } else {
            bail!(ParityFilterError::InvalidArgumentType(predicate.operator.clone(), supported_types, 0));
        }

        // These arguments must be variables or instant/numeric constants.
        // TODO: static evaluation. #383.
        let constraint = comparison.to_constraint(left_v, right_v);
        self.wheres.add_intersection(constraint);
        Ok(())
    }
}

impl Inequality {
    fn to_constraint(&self, left: CausetQValue, right: CausetQValue) -> CausetIndexConstraint {
        match *self {
            Inequality::TxAfter |
            Inequality::TxBefore => {
                // TODO: both ends of the range must be inside the causetx partition!
                // If we know the partition map -- and at this point we do, it's just
                // not passed to this function -- then we can generate two constraints,
                // or clamp a fixed value.
            },
            _ => {
            },
        }

        CausetIndexConstraint::Inequality {
            operator: *self,
            left: left,
            right: right,
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use allegrosql_promises::attribute::{
        Unique,
    };
    use allegrosql_promises::{
        Attribute,
        MinkowskiType,
        MinkowskiValueType,
    };

    use causetq::*::{
        StackedPerceptron,
        Keyword,
        TuringString,
        TuringStringNonValuePlace,
        TuringStringValuePlace,
        PlainSymbol,
        ToUpper,
    };

    use gerunds::{
        add_attribute,
        associate_causetId,
        causetid,
    };

    use types::{
        CausetIndexConstraint,
        EmptyBecause,
        CausetQValue,
    };

    #[test]
    /// Apply two TuringStrings: a TuringString and a numeric predicate.
    /// Verify that after application of the predicate we know that the value
    /// must be numeric.
    fn test_apply_inequality() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Long,
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::Placeholder,
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty());

        let op = PlainSymbol::plain("<");
        let comp = Inequality::from_datalog_operator(op.name()).unwrap();
        assert!(cc.apply_inequality(knownCauset, comp, Predicate {
             operator: op,
             args: vec![
                StackedPerceptron::ToUpper(ToUpper::from_valid_name("?y")), StackedPerceptron::SolitonIdOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty());

        // Finally, expand CausetIndex ConstrainedEntss to get the overlaps for ?x.
        cc.expand_CausetIndex_ConstrainedEntss();
        assert!(!cc.is_known_empty());

        // After processing those two gerunds, we know that ?y must be numeric, but not exactly
        // which type it must be.
        assert_eq!(None, cc.known_type(&y));      // Not just one.
        let expected = MinkowskiSet::of_numeric_types();
        assert_eq!(Some(&expected), cc.known_types.get(&y));

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 1);
        assert_eq!(gerunds.0[0], CausetIndexConstraint::Inequality {
            operator: Inequality::LessThan,
            left: CausetQValue::CausetIndex(cc.CausetIndex_ConstrainedEntss.get(&y).unwrap()[0].clone()),
            right: CausetQValue::MinkowskiType(MinkowskiType::Long(10)),
        }.into());
    }

    #[test]
    /// Apply three TuringStrings: an unbound TuringString to establish a value var,
    /// a predicate to constrain the val to numeric types, and a third TuringString to conflict with the
    /// numeric types and cause the TuringString to fail.
    fn test_apply_conflict_with_numeric_range() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "roz"), 98);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Long,
            ..Default::default()
        });
        add_attribute(&mut schemaReplicant, 98, Attribute {
            value_type: MinkowskiValueType::String,
            unique: Some(Unique::CausetIdity),
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::Placeholder,
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty());

        let op = PlainSymbol::plain(">=");
        let comp = Inequality::from_datalog_operator(op.name()).unwrap();
        assert!(cc.apply_inequality(knownCauset, comp, Predicate {
             operator: op,
             args: vec![
                StackedPerceptron::ToUpper(ToUpper::from_valid_name("?y")), StackedPerceptron::SolitonIdOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty());
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "roz"),
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // Finally, expand CausetIndex ConstrainedEntss to get the overlaps for ?x.
        cc.expand_CausetIndex_ConstrainedEntss();

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch {
                       var: y.clone(),
                       existing: MinkowskiSet::of_numeric_types(),
                       desired: MinkowskiSet::of_one(MinkowskiValueType::String),
                   });
    }
}
