// Copyright 2021 WHTCORPS INC
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
};

use causetq::*::{
    ConstrainedEntsConstraint,
    StackedPerceptron,
    SrcVar,
    BinningCauset,
    WhereFn,
};

use gerunds::{
    ConjoiningGerunds,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    ConstrainedEntsConstraintError,
    Result,
};

use types::{
    CausetIndex,
    CausetIndexConstraint,
    CausetsBlock,
    Inequality,
    QualifiedAlias,
    CausetQValue,
    SourceAlias,
    bundlesCausetIndex,
};

use KnownCauset;

impl ConjoiningGerunds {
    // Log in CausetQ: causetx-ids and causetx-data
    //
    // The log API includes two convenience functions that are available within causetq. The causetx-ids
    // function takes arguments similar to causecausetxRange above, but returns a collection of transaction
    // instanton ids. You will typically use the collection Constrained form [?causetx …] to capture the
    // results.
    //
    // [(causetx-ids ?log ?causecausetx1 ?causecausetx2) [?causetx ...]]
    //
    // TODO: handle causecausetx1 == 0 (more generally, causecausetx1 < bootstrap::TX0) specially (no after constraint).
    // TODO: allow causecausetx2 to be missing (no before constraint).
    // TODO: allow causecausetxK arguments to be instants.
    // TODO: allow arbitrary additional attribute arguments that restrict the causetx-ids to those
    // bundles that impact one of the given attributes.
    pub(crate) fn apply_causecausetx_ids(&mut self, knownCauset: KnownCauset, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 3 {
            bail!(ParityFilterError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 3));
        }

        if where_fn.Constrained.is_empty() {
            // The Constrained must introduce at least one bound variable.
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::NoBoundVariable));
        }

        if !where_fn.Constrained.is_valid() {
            // The Constrained must not duplicate bound variables.
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::RepeatedBoundVariable));
        }

        // We should have exactly one Constrained. Destructure it now.
        let causecausetx_var = match where_fn.Constrained {
            ConstrainedEntsConstraint::BindRel(ConstrainedEntss) => {
                let ConstrainedEntss_count = ConstrainedEntss.len();
                if ConstrainedEntss_count != 1 {
                    bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(),
                                                    ConstrainedEntsConstraintError::InvalidNumberOfConstrainedEntsConstraints {
                                                        number: ConstrainedEntss_count,
                                                        expected: 1,
                                                    }));
                }
                match ConstrainedEntss.into_iter().next().unwrap() {
                    BinningCauset::Placeholder => unreachable!("Constrained.is_empty()!"),
                    BinningCauset::ToUpper(v) => v,
                }
            },
            ConstrainedEntsConstraint::BindColl(v) => v,
            ConstrainedEntsConstraint::BindScalar(_) |
            ConstrainedEntsConstraint::BindTuple(_) => {
                bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::ExpectedBindRelOrBindColl))
            },
        };

        let mut args = where_fn.args.into_iter();

        match args.next().unwrap() {
            StackedPerceptron::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "source variable", 0)),
        }

        let causecausetx1 = self.resolve_causecausetx_argument(&knownCauset.schemaReplicant, &where_fn.operator, 1, args.next().unwrap())?;
        let causecausetx2 = self.resolve_causecausetx_argument(&knownCauset.schemaReplicant, &where_fn.operator, 2, args.next().unwrap())?;

        let bundles = self.next_alias_for_Block(CausetsBlock::bundles);

        self.from.push(SourceAlias(CausetsBlock::bundles, bundles.clone()));

        // Bound variable must be a ref.
        self.constrain_var_to_type(causecausetx_var.clone(), MinkowskiValueType::Ref);
        if self.is_known_empty() {
            return Ok(());
        }

        self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, bundles.clone(), bundlesCausetIndex::Tx, causecausetx_var.clone());

        let after_constraint = CausetIndexConstraint::Inequality {
            operator: Inequality::LessThanOrEquals,
            left: causecausetx1,
            right: CausetQValue::CausetIndex(QualifiedAlias(bundles.clone(), CausetIndex::bundles(bundlesCausetIndex::Tx))),
        };
        self.wheres.add_intersection(after_constraint);

        let before_constraint = CausetIndexConstraint::Inequality {
            operator: Inequality::LessThan,
            left: CausetQValue::CausetIndex(QualifiedAlias(bundles.clone(), CausetIndex::bundles(bundlesCausetIndex::Tx))),
            right: causecausetx2,
        };
        self.wheres.add_intersection(before_constraint);

        Ok(())
    }

    pub(crate) fn apply_causecausetx_data(&mut self, knownCauset: KnownCauset, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 2 {
            bail!(ParityFilterError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 2));
        }

        if where_fn.Constrained.is_empty() {
    
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::NoBoundVariable));
        }

        if !where_fn.Constrained.is_valid() {
            // The Constrained must not duplicate bound variables.
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::RepeatedBoundVariable));
        }

        let ConstrainedEntss = match where_fn.Constrained {
            ConstrainedEntsConstraint::BindRel(ConstrainedEntss) => {
                let ConstrainedEntss_count = ConstrainedEntss.len();
                if ConstrainedEntss_count < 1 || ConstrainedEntss_count > 5 {
                    bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(),
                                                    ConstrainedEntsConstraintError::InvalidNumberOfConstrainedEntsConstraints {
                                                        number: ConstrainedEntss.len(),
                                                        expected: 5,
                                                    }));
                }
                ConstrainedEntss
            },
            ConstrainedEntsConstraint::BindScalar(_) |
            ConstrainedEntsConstraint::BindTuple(_) |
            ConstrainedEntsConstraint::BindColl(_) => bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::ExpectedBindRel)),
        };
        let mut ConstrainedEntss = ConstrainedEntss.into_iter();
        let b_e = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);
        let b_a = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);
        let b_v = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);
        let b_causecausetx = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);
        let b_op = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);

        let mut args = where_fn.args.into_iter();

        // TODO: process source variables.
        match args.next().unwrap() {
            StackedPerceptron::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "source variable", 0)),
        }

        let causetx = self.resolve_causecausetx_argument(&knownCauset.schemaReplicant, &where_fn.operator, 1, args.next().unwrap())?;

        let bundles = self.next_alias_for_Block(CausetsBlock::bundles);

        self.from.push(SourceAlias(CausetsBlock::bundles, bundles.clone()));

        let causecausetx_constraint = CausetIndexConstraint::Equals(
            QualifiedAlias(bundles.clone(), CausetIndex::bundles(bundlesCausetIndex::Tx)),
            causetx);
        self.wheres.add_intersection(causecausetx_constraint);

        if let BinningCauset::ToUpper(ref var) = b_e {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, bundles.clone(), bundlesCausetIndex::Instanton, var.clone());
        }

        if let BinningCauset::ToUpper(ref var) = b_a {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, bundles.clone(), bundlesCausetIndex::Attribute, var.clone());
        }

        if let BinningCauset::ToUpper(ref var) = b_v {
            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, bundles.clone(), bundlesCausetIndex::Value, var.clone());
        }

        if let BinningCauset::ToUpper(ref var) = b_causecausetx {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            // TODO: this might be a programming error if var is our causetx argument.  Perhaps we can be
            // helpful in that case.
            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, bundles.clone(), bundlesCausetIndex::Tx, var.clone());
        }

        if let BinningCauset::ToUpper(ref var) = b_op {
            // It must be a boolean.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Boolean);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, bundles.clone(), bundlesCausetIndex::Added, var.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use allegrosql_promises::{
        MinkowskiType,
        MinkowskiValueType,
    };

    use causetq_allegrosql::{
        SchemaReplicant,
    };

    use causetq::*::{
        ConstrainedEntsConstraint,
        StackedPerceptron,
        PlainSymbol,
        ToUpper,
    };

    #[test]
    fn test_apply_causecausetx_ids() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

        let op = PlainSymbol::plain("causetx-ids");
        cc.apply_causecausetx_ids(knownCauset, WhereFn {
            operator: op,
            args: vec![
                StackedPerceptron::SrcVar(SrcVar::DefaultSrc),
                StackedPerceptron::SolitonIdOrInteger(1000),
                StackedPerceptron::SolitonIdOrInteger(2000),
            ],
            Constrained: ConstrainedEntsConstraint::BindRel(vec![BinningCauset::ToUpper(ToUpper::from_valid_name("?causetx")),
            ]),
        }).expect("to be able to apply_causecausetx_ids");

        assert!(!cc.is_known_empty());

        // Finally, expand CausetIndex ConstrainedEntss.
        cc.expand_CausetIndex_ConstrainedEntss();
        assert!(!cc.is_known_empty());

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 2);

        assert_eq!(gerunds.0[0],
                   CausetIndexConstraint::Inequality {
                       operator: Inequality::LessThanOrEquals,
                       left: CausetQValue::MinkowskiType(MinkowskiType::Ref(1000)),
                       right: CausetQValue::CausetIndex(QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Tx))),
                   }.into());

        assert_eq!(gerunds.0[1],
                   CausetIndexConstraint::Inequality {
                       operator: Inequality::LessThan,
                       left: CausetQValue::CausetIndex(QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Tx))),
                       right: CausetQValue::MinkowskiType(MinkowskiType::Ref(2000)),
                   }.into());

        let ConstrainedEntss = cc.CausetIndex_ConstrainedEntss;
        assert_eq!(ConstrainedEntss.len(), 1);

        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?causetx")).expect("CausetIndex Constrained for ?causetx").clone(),
                   vec![QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Tx))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 1);

        assert_eq!(known_types.get(&ToUpper::from_valid_name("?causetx")).expect("knownCauset types for ?causetx").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());
    }

    #[test]
    fn test_apply_causecausetx_data() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

        let op = PlainSymbol::plain("causetx-data");
        cc.apply_causecausetx_data(knownCauset, WhereFn {
            operator: op,
            args: vec![
                StackedPerceptron::SrcVar(SrcVar::DefaultSrc),
                StackedPerceptron::SolitonIdOrInteger(1000),
            ],
            Constrained: ConstrainedEntsConstraint::BindRel(vec![
                BinningCauset::ToUpper(ToUpper::from_valid_name("?e")),
                BinningCauset::ToUpper(ToUpper::from_valid_name("?a")),
                BinningCauset::ToUpper(ToUpper::from_valid_name("?v")),
                BinningCauset::ToUpper(ToUpper::from_valid_name("?causetx")),
                BinningCauset::ToUpper(ToUpper::from_valid_name("?added")),
            ]),
        }).expect("to be able to apply_causecausetx_data");

        assert!(!cc.is_known_empty());

        // Finally, expand CausetIndex ConstrainedEntss.
        cc.expand_CausetIndex_ConstrainedEntss();
        assert!(!cc.is_known_empty());

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 1);

        assert_eq!(gerunds.0[0],
                   CausetIndexConstraint::Equals(QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Tx)),
                                            CausetQValue::MinkowskiType(MinkowskiType::Ref(1000))).into());

        let ConstrainedEntss = cc.CausetIndex_ConstrainedEntss;
        assert_eq!(ConstrainedEntss.len(), 5);

        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?e")).expect("CausetIndex Constrained for ?e").clone(),
                   vec![QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Instanton))]);

        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?a")).expect("CausetIndex Constrained for ?a").clone(),
                   vec![QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Attribute))]);

        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?v")).expect("CausetIndex Constrained for ?v").clone(),
                   vec![QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Value))]);

        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?causetx")).expect("CausetIndex Constrained for ?causetx").clone(),
                   vec![QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Tx))]);

        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?added")).expect("CausetIndex Constrained for ?added").clone(),
                   vec![QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::Added))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 4);

        assert_eq!(known_types.get(&ToUpper::from_valid_name("?e")).expect("knownCauset types for ?e").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&ToUpper::from_valid_name("?a")).expect("knownCauset types for ?a").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&ToUpper::from_valid_name("?causetx")).expect("knownCauset types for ?causetx").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&ToUpper::from_valid_name("?added")).expect("knownCauset types for ?added").clone(),
                   vec![MinkowskiValueType::Boolean].into_iter().collect());

        let extracted_types = cc.extracted_types;
        assert_eq!(extracted_types.len(), 1);

        assert_eq!(extracted_types.get(&ToUpper::from_valid_name("?v")).expect("extracted types for ?v").clone(),
                   QualifiedAlias("bundles00".to_string(), CausetIndex::bundles(bundlesCausetIndex::MinkowskiValueTypeTag)));
    }
}
