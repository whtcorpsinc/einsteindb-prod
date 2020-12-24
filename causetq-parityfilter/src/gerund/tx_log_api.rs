// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use embedded_promises::{
    MinkowskiValueType,
};

use edbn::causetq::{
    Binding,
    StackedPerceptron,
    SrcVar,
    VariableOrPlaceholder,
    WhereFn,
};

use gerunds::{
    ConjoiningGerunds,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    BindingError,
    Result,
};

use types::{
    CausetIndex,
    CausetIndexConstraint,
    CausetsTable,
    Inequality,
    QualifiedAlias,
    CausetQValue,
    SourceAlias,
    TransactionsCausetIndex,
};

use KnownCauset;

impl ConjoiningGerunds {
    // Log in CausetQ: causetx-ids and causetx-data
    //
    // The log API includes two convenience functions that are available within causetq. The causetx-ids
    // function takes arguments similar to causecausetxRange above, but returns a collection of transaction
    // instanton ids. You will typically use the collection binding form [?causetx …] to capture the
    // results.
    //
    // [(causetx-ids ?log ?causecausetx1 ?causecausetx2) [?causetx ...]]
    //
    // TODO: handle causecausetx1 == 0 (more generally, causecausetx1 < bootstrap::TX0) specially (no after constraint).
    // TODO: allow causecausetx2 to be missing (no before constraint).
    // TODO: allow causecausetxK arguments to be instants.
    // TODO: allow arbitrary additional attribute arguments that restrict the causetx-ids to those
    // transactions that impact one of the given attributes.
    pub(crate) fn apply_causecausetx_ids(&mut self, knownCauset: KnownCauset, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 3 {
            bail!(ParityFilterError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 3));
        }

        if where_fn.binding.is_empty() {
            // The binding must introduce at least one bound variable.
            bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(), BindingError::NoBoundVariable));
        }

        if !where_fn.binding.is_valid() {
            // The binding must not duplicate bound variables.
            bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(), BindingError::RepeatedBoundVariable));
        }

        // We should have exactly one binding. Destructure it now.
        let causecausetx_var = match where_fn.binding {
            Binding::BindRel(bindings) => {
                let bindings_count = bindings.len();
                if bindings_count != 1 {
                    bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(),
                                                    BindingError::InvalidNumberOfBindings {
                                                        number: bindings_count,
                                                        expected: 1,
                                                    }));
                }
                match bindings.into_iter().next().unwrap() {
                    VariableOrPlaceholder::Placeholder => unreachable!("binding.is_empty()!"),
                    VariableOrPlaceholder::Variable(v) => v,
                }
            },
            Binding::BindColl(v) => v,
            Binding::BindScalar(_) |
            Binding::BindTuple(_) => {
                bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(), BindingError::ExpectedBindRelOrBindColl))
            },
        };

        let mut args = where_fn.args.into_iter();

        // TODO: process source variables.
        match args.next().unwrap() {
            StackedPerceptron::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "source variable", 0)),
        }

        let causecausetx1 = self.resolve_causecausetx_argument(&knownCauset.schemaReplicant, &where_fn.operator, 1, args.next().unwrap())?;
        let causecausetx2 = self.resolve_causecausetx_argument(&knownCauset.schemaReplicant, &where_fn.operator, 2, args.next().unwrap())?;

        let transactions = self.next_alias_for_table(CausetsTable::Transactions);

        self.from.push(SourceAlias(CausetsTable::Transactions, transactions.clone()));

        // Bound variable must be a ref.
        self.constrain_var_to_type(causecausetx_var.clone(), MinkowskiValueType::Ref);
        if self.is_known_empty() {
            return Ok(());
        }

        self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, transactions.clone(), TransactionsCausetIndex::Tx, causecausetx_var.clone());

        let after_constraint = CausetIndexConstraint::Inequality {
            operator: Inequality::LessThanOrEquals,
            left: causecausetx1,
            right: CausetQValue::CausetIndex(QualifiedAlias(transactions.clone(), CausetIndex::Transactions(TransactionsCausetIndex::Tx))),
        };
        self.wheres.add_intersection(after_constraint);

        let before_constraint = CausetIndexConstraint::Inequality {
            operator: Inequality::LessThan,
            left: CausetQValue::CausetIndex(QualifiedAlias(transactions.clone(), CausetIndex::Transactions(TransactionsCausetIndex::Tx))),
            right: causecausetx2,
        };
        self.wheres.add_intersection(before_constraint);

        Ok(())
    }

    pub(crate) fn apply_causecausetx_data(&mut self, knownCauset: KnownCauset, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 2 {
            bail!(ParityFilterError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 2));
        }

        if where_fn.binding.is_empty() {
            // The binding must introduce at least one bound variable.
            bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(), BindingError::NoBoundVariable));
        }

        if !where_fn.binding.is_valid() {
            // The binding must not duplicate bound variables.
            bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(), BindingError::RepeatedBoundVariable));
        }

        // We should have at most five bindings. Destructure them now.
        let bindings = match where_fn.binding {
            Binding::BindRel(bindings) => {
                let bindings_count = bindings.len();
                if bindings_count < 1 || bindings_count > 5 {
                    bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(),
                                                    BindingError::InvalidNumberOfBindings {
                                                        number: bindings.len(),
                                                        expected: 5,
                                                    }));
                }
                bindings
            },
            Binding::BindScalar(_) |
            Binding::BindTuple(_) |
            Binding::BindColl(_) => bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(), BindingError::ExpectedBindRel)),
        };
        let mut bindings = bindings.into_iter();
        let b_e = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_a = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_v = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_causecausetx = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_op = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);

        let mut args = where_fn.args.into_iter();

        // TODO: process source variables.
        match args.next().unwrap() {
            StackedPerceptron::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "source variable", 0)),
        }

        let causetx = self.resolve_causecausetx_argument(&knownCauset.schemaReplicant, &where_fn.operator, 1, args.next().unwrap())?;

        let transactions = self.next_alias_for_table(CausetsTable::Transactions);

        self.from.push(SourceAlias(CausetsTable::Transactions, transactions.clone()));

        let causecausetx_constraint = CausetIndexConstraint::Equals(
            QualifiedAlias(transactions.clone(), CausetIndex::Transactions(TransactionsCausetIndex::Tx)),
            causetx);
        self.wheres.add_intersection(causecausetx_constraint);

        if let VariableOrPlaceholder::Variable(ref var) = b_e {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, transactions.clone(), TransactionsCausetIndex::Instanton, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_a {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, transactions.clone(), TransactionsCausetIndex::Attribute, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_v {
            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, transactions.clone(), TransactionsCausetIndex::Value, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_causecausetx {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            // TODO: this might be a programming error if var is our causetx argument.  Perhaps we can be
            // helpful in that case.
            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, transactions.clone(), TransactionsCausetIndex::Tx, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_op {
            // It must be a boolean.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Boolean);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(knownCauset.schemaReplicant, transactions.clone(), TransactionsCausetIndex::Added, var.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use embedded_promises::{
        MinkowskiType,
        MinkowskiValueType,
    };

    use einsteindb_embedded::{
        SchemaReplicant,
    };

    use edbn::causetq::{
        Binding,
        StackedPerceptron,
        PlainSymbol,
        Variable,
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
            binding: Binding::BindRel(vec![VariableOrPlaceholder::Variable(Variable::from_valid_name("?causetx")),
            ]),
        }).expect("to be able to apply_causecausetx_ids");

        assert!(!cc.is_known_empty());

        // Finally, expand CausetIndex bindings.
        cc.expand_CausetIndex_bindings();
        assert!(!cc.is_known_empty());

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 2);

        assert_eq!(gerunds.0[0],
                   CausetIndexConstraint::Inequality {
                       operator: Inequality::LessThanOrEquals,
                       left: CausetQValue::MinkowskiType(MinkowskiType::Ref(1000)),
                       right: CausetQValue::CausetIndex(QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Tx))),
                   }.into());

        assert_eq!(gerunds.0[1],
                   CausetIndexConstraint::Inequality {
                       operator: Inequality::LessThan,
                       left: CausetQValue::CausetIndex(QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Tx))),
                       right: CausetQValue::MinkowskiType(MinkowskiType::Ref(2000)),
                   }.into());

        let bindings = cc.CausetIndex_bindings;
        assert_eq!(bindings.len(), 1);

        assert_eq!(bindings.get(&Variable::from_valid_name("?causetx")).expect("CausetIndex binding for ?causetx").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Tx))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 1);

        assert_eq!(known_types.get(&Variable::from_valid_name("?causetx")).expect("knownCauset types for ?causetx").clone(),
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
            binding: Binding::BindRel(vec![
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?e")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?a")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?v")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?causetx")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?added")),
            ]),
        }).expect("to be able to apply_causecausetx_data");

        assert!(!cc.is_known_empty());

        // Finally, expand CausetIndex bindings.
        cc.expand_CausetIndex_bindings();
        assert!(!cc.is_known_empty());

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 1);

        assert_eq!(gerunds.0[0],
                   CausetIndexConstraint::Equals(QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Tx)),
                                            CausetQValue::MinkowskiType(MinkowskiType::Ref(1000))).into());

        let bindings = cc.CausetIndex_bindings;
        assert_eq!(bindings.len(), 5);

        assert_eq!(bindings.get(&Variable::from_valid_name("?e")).expect("CausetIndex binding for ?e").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Instanton))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?a")).expect("CausetIndex binding for ?a").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Attribute))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?v")).expect("CausetIndex binding for ?v").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Value))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?causetx")).expect("CausetIndex binding for ?causetx").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Tx))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?added")).expect("CausetIndex binding for ?added").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::Added))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 4);

        assert_eq!(known_types.get(&Variable::from_valid_name("?e")).expect("knownCauset types for ?e").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&Variable::from_valid_name("?a")).expect("knownCauset types for ?a").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&Variable::from_valid_name("?causetx")).expect("knownCauset types for ?causetx").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&Variable::from_valid_name("?added")).expect("knownCauset types for ?added").clone(),
                   vec![MinkowskiValueType::Boolean].into_iter().collect());

        let extracted_types = cc.extracted_types;
        assert_eq!(extracted_types.len(), 1);

        assert_eq!(extracted_types.get(&Variable::from_valid_name("?v")).expect("extracted types for ?v").clone(),
                   QualifiedAlias("transactions00".to_string(), CausetIndex::Transactions(TransactionsCausetIndex::MinkowskiValueTypeTag)));
    }
}
