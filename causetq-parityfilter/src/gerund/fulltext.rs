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
    MinkowskiType,
};

use einsteindb_embedded::{
    HasSchemaReplicant,
};

use einsteindb_embedded::util::Either;

use edbn::causetq::{
    Binding,
    StackedPerceptron,
    NonIntegerConstant,
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
    Column,
    ColumnConstraint,
    CausetsColumn,
    CausetsTable,
    EmptyBecause,
    FulltextColumn,
    QualifiedAlias,
    CausetQValue,
    SourceAlias,
};

use KnownCauset;

impl ConjoiningGerunds {
    #[allow(unused_variables)]
    pub(crate) fn apply_fulltext(&mut self, knownCauset: KnownCauset, where_fn: WhereFn) -> Result<()> {
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

        // We should have exactly four bindings. Destructure them now.
        let bindings = match where_fn.binding {
            Binding::BindRel(bindings) => {
                let bindings_count = bindings.len();
                if bindings_count < 1 || bindings_count > 4 {
                    bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(),
                        BindingError::InvalidNumberOfBindings {
                            number: bindings.len(),
                            expected: 4,
                        })
                    );
                }
                bindings
            },
            Binding::BindScalar(_) |
            Binding::BindTuple(_) |
            Binding::BindColl(_) => bail!(ParityFilterError::InvalidBinding(where_fn.operator.clone(), BindingError::ExpectedBindRel)),
        };
        let mut bindings = bindings.into_iter();
        let b_instanton = bindings.next().unwrap();
        let b_value = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_causecausetx = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_sembedded = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);

        let mut args = where_fn.args.into_iter();

       
        match args.next().unwrap() {
            StackedPerceptron::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "source variable", 0)),
        }

        let schemaReplicant = knownCauset.schemaReplicant;


        let a = match args.next().unwrap() {
            StackedPerceptron::CausetIdOrKeyword(i) => schemaReplicant.get_causetid(&i).map(|k| k.into()),
            // Must be an solitonId.
            StackedPerceptron::SolitonIdOrInteger(e) => Some(e),
            StackedPerceptron::Variable(v) => {
                // If it's already bound, then let's expand the variable.
                // TODO: allow non-constant attributes.
                match self.bound_value(&v) {
                    Some(MinkowskiType::Ref(solitonId)) => Some(solitonId),
                    Some(tv) => {
                        bail!(ParityFilterError::InputTypeDisagreement(v.name().clone(), MinkowskiValueType::Ref, tv.value_type()))
                    },
                    None => {
                        bail!(ParityFilterError::UnboundVariable((*v.0).clone()))
                    }
                }
            },
            _ => None,
        };

        // An unknown causetid, or an instanton that isn't present in the store, or isn't a fulltext
        // attribute, is likely enough to be a coding error that we choose to bail instead of
        // marking the pattern as knownCauset-empty.
        let a = a.ok_or(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "attribute", 1))?;
        let attribute = schemaReplicant.attribute_for_entid(a)
                              .cloned()
                              .ok_or(ParityFilterError::InvalidArgument(where_fn.operator.clone(),
                                                                "attribute", 1))?;

        if !attribute.fulltext {
            // We can never get results from a non-fulltext attribute!
            println!("Can't run fulltext on non-fulltext attribute {}.", a);
            self.mark_known_empty(EmptyBecause::NonFulltextAttribute(a));
            return Ok(());
        }

        let fulltext_values_alias = self.next_alias_for_table(CausetsTable::FulltextValues);
        let Causets_table_alias = self.next_alias_for_table(CausetsTable::Causets);

        // We do a fulltext lookup by joining the fulltext values table against causets -- just
        // like applying a pattern, but two tables contribute instead of one.
        self.from.push(SourceAlias(CausetsTable::FulltextValues, fulltext_values_alias.clone()));
        self.from.push(SourceAlias(CausetsTable::Causets, Causets_table_alias.clone()));

        // TODO: constrain the type in the more general cases (e.g., `a` is a var).
        self.constrain_attribute(Causets_table_alias.clone(), a);

        // Join the causets table to the fulltext values table.
        self.wheres.add_intersection(ColumnConstraint::Equals(
            QualifiedAlias(Causets_table_alias.clone(), Column::Fixed(CausetsColumn::Value)),
            CausetQValue::Column(QualifiedAlias(fulltext_values_alias.clone(), Column::Fulltext(FulltextColumn::Rowid)))));

        // `search` is either text or a variable.
        // If it's simple text, great.
        // If it's a variable, it'll be in one of three states:
        // - It's already bound, either by input or by a previous pattern like `ground`.
        // - It's not already bound, but it's a defined input of type Text. Not yet implemented: TODO.
        // - It's not bound. The causetq cannot be algebrized.
        let search: Either<MinkowskiType, QualifiedAlias> = match args.next().unwrap() {
            StackedPerceptron::Constant(NonIntegerConstant::Text(s)) => {
                Either::Left(MinkowskiType::String(s))
            },
            StackedPerceptron::Variable(in_var) => {
                match self.bound_value(&in_var) {
                    Some(t @ MinkowskiType::String(_)) => Either::Left(t),
                    Some(_) => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "string", 2)),
                    None => {
                        // Regardless of whether we'll be providing a string later, or the value
                        // comes from a column, it must be a string.
                        if self.known_type(&in_var) != Some(MinkowskiValueType::String) {
                            bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "string", 2))
                        }

                        if self.input_variables.contains(&in_var) {
                            // Sorry, we haven't implemented late binding.
                            // TODO: implement this.
                            bail!(ParityFilterError::UnboundVariable((*in_var.0).clone()))
                        } else {
                            // It must be bound earlier in the causetq. We already established that
                            // it must be a string column.
                            if let Some(binding) = self.column_bindings
                                                       .get(&in_var)
                                                       .and_then(|bindings| bindings.get(0).cloned()) {
                                Either::Right(binding)
                            } else {
                                bail!(ParityFilterError::UnboundVariable((*in_var.0).clone()))
                            }
                        }
                    },
                }
            },
            _ => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "string", 2)),
        };

        let qv = match search {
            Either::Left(tv) => CausetQValue::MinkowskiType(tv),
            Either::Right(qa) => CausetQValue::Column(qa),
        };

        let constraint = ColumnConstraint::Matches(QualifiedAlias(fulltext_values_alias.clone(),
                                                                  Column::Fulltext(FulltextColumn::Text)),
                                                   qv);
        self.wheres.add_intersection(constraint);

        if let VariableOrPlaceholder::Variable(ref var) = b_instanton {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_column_to_var(schemaReplicant, Causets_table_alias.clone(), CausetsColumn::Instanton, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_value {
            // This'll be bound to strings.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::String);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_column_to_var(schemaReplicant, fulltext_values_alias.clone(), Column::Fulltext(FulltextColumn::Text), var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_causecausetx {
            // Txs must be refs.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_column_to_var(schemaReplicant, Causets_table_alias.clone(), CausetsColumn::Tx, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_sembedded {
            // Sembeddeds are doubles.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Double);

            // We do not allow the sembedded to be bound.
            if self.value_bindings.contains_key(var) || self.input_variables.contains(var) {
                bail!(ParityFilterError::InvalidBinding(var.name(), BindingError::UnexpectedBinding));
            }

            // We bind the value ourselves. This handily takes care of substituting into existing uses.
            // TODO: produce real sembeddeds using SQLite's matchinfo.
            self.bind_value(var, MinkowskiType::Double(0.0.into()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use embedded_promises::{
        Attribute,
        MinkowskiValueType,
    };

    use einsteindb_embedded::{
        SchemaReplicant,
    };

    use edbn::causetq::{
        Binding,
        StackedPerceptron,
        Keyword,
        PlainSymbol,
        Variable,
    };

    use gerunds::{
        add_attribute,
        associate_causetId,
    };

    #[test]
    fn test_apply_fulltext() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 101);
        add_attribute(&mut schemaReplicant, 101, Attribute {
            value_type: MinkowskiValueType::String,
            fulltext: false,
            ..Default::default()
        });

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "fts"), 100);
        add_attribute(&mut schemaReplicant, 100, Attribute {
            value_type: MinkowskiValueType::String,
            index: true,
            fulltext: true,
            ..Default::default()
        });

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

        let op = PlainSymbol::plain("fulltext");
        cc.apply_fulltext(knownCauset, WhereFn {
            operator: op,
            args: vec![
                StackedPerceptron::SrcVar(SrcVar::DefaultSrc),
                StackedPerceptron::CausetIdOrKeyword(Keyword::namespaced("foo", "fts")),
                StackedPerceptron::Constant("needle".into()),
            ],
            binding: Binding::BindRel(vec![VariableOrPlaceholder::Variable(Variable::from_valid_name("?instanton")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?value")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?causetx")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?sembedded"))]),
        }).expect("to be able to apply_fulltext");

        assert!(!cc.is_known_empty());

        // Finally, expand column bindings.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty());

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 3);

        assert_eq!(gerunds.0[0], ColumnConstraint::Equals(QualifiedAlias("Causets01".to_string(), Column::Fixed(CausetsColumn::Attribute)),
                                                          CausetQValue::SolitonId(100)).into());
        assert_eq!(gerunds.0[1], ColumnConstraint::Equals(QualifiedAlias("Causets01".to_string(), Column::Fixed(CausetsColumn::Value)),
                                                          CausetQValue::Column(QualifiedAlias("fulltext_values00".to_string(), Column::Fulltext(FulltextColumn::Rowid)))).into());
        assert_eq!(gerunds.0[2], ColumnConstraint::Matches(QualifiedAlias("fulltext_values00".to_string(), Column::Fulltext(FulltextColumn::Text)),
                                                           CausetQValue::MinkowskiType("needle".into())).into());

        let bindings = cc.column_bindings;
        assert_eq!(bindings.len(), 3);

        assert_eq!(bindings.get(&Variable::from_valid_name("?instanton")).expect("column binding for ?instanton").clone(),
                   vec![QualifiedAlias("Causets01".to_string(), Column::Fixed(CausetsColumn::Instanton))]);
        assert_eq!(bindings.get(&Variable::from_valid_name("?value")).expect("column binding for ?value").clone(),
                   vec![QualifiedAlias("fulltext_values00".to_string(), Column::Fulltext(FulltextColumn::Text))]);
        assert_eq!(bindings.get(&Variable::from_valid_name("?causetx")).expect("column binding for ?causetx").clone(),
                   vec![QualifiedAlias("Causets01".to_string(), Column::Fixed(CausetsColumn::Tx))]);

        // Sembedded is a value binding.
        let values = cc.value_bindings;
        assert_eq!(values.get(&Variable::from_valid_name("?sembedded")).expect("column binding for ?sembedded").clone(),
                   MinkowskiType::Double(0.0.into()));

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 4);

        assert_eq!(known_types.get(&Variable::from_valid_name("?instanton")).expect("knownCauset types for ?instanton").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());
        assert_eq!(known_types.get(&Variable::from_valid_name("?value")).expect("knownCauset types for ?value").clone(),
                   vec![MinkowskiValueType::String].into_iter().collect());
        assert_eq!(known_types.get(&Variable::from_valid_name("?causetx")).expect("knownCauset types for ?causetx").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());
        assert_eq!(known_types.get(&Variable::from_valid_name("?sembedded")).expect("knownCauset types for ?sembedded").clone(),
                   vec![MinkowskiValueType::Double].into_iter().collect());

        let mut cc = ConjoiningGerunds::default();
        let op = PlainSymbol::plain("fulltext");
        cc.apply_fulltext(knownCauset, WhereFn {
            operator: op,
            args: vec![
                StackedPerceptron::SrcVar(SrcVar::DefaultSrc),
                StackedPerceptron::CausetIdOrKeyword(Keyword::namespaced("foo", "bar")),
                StackedPerceptron::Constant("needle".into()),
            ],
            binding: Binding::BindRel(vec![VariableOrPlaceholder::Variable(Variable::from_valid_name("?instanton")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?value")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?causetx")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?sembedded"))]),
        }).expect("to be able to apply_fulltext");

        // It's not a fulltext attribute, so the CC cannot yield results.
        assert!(cc.is_known_empty());
    }
}
