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
    MinkowskiType,
};

use causetq_allegrosql::{
    HasSchemaReplicant,
};

use causetq_allegrosql::util::Either;

use causetq::*::{
    ConstrainedEntsConstraint,
    StackedPerceptron,
    NonIntegerConstant,
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
    CausetsCausetIndex,
    CausetsBlock,
    EmptyBecause,
    FulltextCausetIndex,
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

        if where_fn.Constrained.is_empty() {
            // The Constrained must introduce at least one bound variable.
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::NoBoundVariable));
        }

        if !where_fn.Constrained.is_valid() {
            // The Constrained must not duplicate bound variables.
            bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::RepeatedBoundVariable));
        }

        // We should have exactly four ConstrainedEntss. Destructure them now.
        let ConstrainedEntss = match where_fn.Constrained {
            ConstrainedEntsConstraint::BindRel(ConstrainedEntss) => {
                let ConstrainedEntss_count = ConstrainedEntss.len();
                if ConstrainedEntss_count < 1 || ConstrainedEntss_count > 4 {
                    bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(),
                        ConstrainedEntsConstraintError::InvalidNumberOfConstrainedEntsConstraints {
                            number: ConstrainedEntss.len(),
                            expected: 4,
                        })
                    );
                }
                ConstrainedEntss
            },
            ConstrainedEntsConstraint::BindScalar(_) |
            ConstrainedEntsConstraint::BindTuple(_) |
            ConstrainedEntsConstraint::BindColl(_) => bail!(ParityFilterError::InvalidConstrainedEntsConstraint(where_fn.operator.clone(), ConstrainedEntsConstraintError::ExpectedBindRel)),
        };
        let mut ConstrainedEntss = ConstrainedEntss.into_iter();
        let b_instanton = ConstrainedEntss.next().unwrap();
        let b_value = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);
        let b_causecausetx = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);
        let b_sallegro = ConstrainedEntss.next().unwrap_or(BinningCauset::Placeholder);

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
            StackedPerceptron::ToUpper(v) => {
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


        let a = a.ok_or(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "attribute", 1))?;
        let attribute = schemaReplicant.attribute_for_causetid(a)
                              .cloned()
                              .ok_or(ParityFilterError::InvalidArgument(where_fn.operator.clone(),
                                                                "attribute", 1))?;

        if !attribute.fulltext {
            // We can never get results from a non-fulltext attribute!
            println!("Can't run fulltext on non-fulltext attribute {}.", a);
            self.mark_known_empty(EmptyBecause::NonFulltextAttribute(a));
            return Ok(());
        }

        let fulltext_values_alias = self.next_alias_for_Block(CausetsBlock::FulltextValues);
        let Causets_Block_alias = self.next_alias_for_Block(CausetsBlock::Causets);

    
        self.from.push(SourceAlias(CausetsBlock::FulltextValues, fulltext_values_alias.clone()));
        self.from.push(SourceAlias(CausetsBlock::Causets, Causets_Block_alias.clone()));

        // TODO: constrain the type in the more general cases (e.g., `a` is a var).
        self.constrain_attribute(Causets_Block_alias.clone(), a);


        self.wheres.add_intersection(CausetIndexConstraint::Equals(
            QualifiedAlias(Causets_Block_alias.clone(), CausetIndex::Fixed(CausetsCausetIndex::Value)),
            CausetQValue::CausetIndex(QualifiedAlias(fulltext_values_alias.clone(), CausetIndex::Fulltext(FulltextCausetIndex::Evcausetid)))));

   
        let search: Either<MinkowskiType, QualifiedAlias> = match args.next().unwrap() {
            StackedPerceptron::Constant(NonIntegerConstant::Text(s)) => {
                Either::Left(MinkowskiType::String(s))
            },
            StackedPerceptron::ToUpper(in_var) => {
                match self.bound_value(&in_var) {
                    Some(t @ MinkowskiType::String(_)) => Either::Left(t),
                    Some(_) => bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "string", 2)),
                    None => {
                        // Regardless of whether we'll be providing a string later, or the value
                        // comes from a CausetIndex, it must be a string.
                        if self.known_type(&in_var) != Some(MinkowskiValueType::String) {
                            bail!(ParityFilterError::InvalidArgument(where_fn.operator.clone(), "string", 2))
                        }

                        if self.input_variables.contains(&in_var) {
                           
                            bail!(ParityFilterError::UnboundVariable((*in_var.0).clone()))
                        } else {
                         
                            if let Some(Constrained) = self.CausetIndex_ConstrainedEntss
                                                       .get(&in_var)
                                                       .and_then(|ConstrainedEntss| ConstrainedEntss.get(0).cloned()) {
                                Either::Right(Constrained)
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
            Either::Right(qa) => CausetQValue::CausetIndex(qa),
        };

        let constraint = CausetIndexConstraint::Matches(QualifiedAlias(fulltext_values_alias.clone(),
                                                                  CausetIndex::Fulltext(FulltextCausetIndex::Text)),
                                                   qv);
        self.wheres.add_intersection(constraint);

        if let BinningCauset::ToUpper(ref var) = b_instanton {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(schemaReplicant, Causets_Block_alias.clone(), CausetsCausetIndex::Instanton, var.clone());
        }

        if let BinningCauset::ToUpper(ref var) = b_value {
            // This'll be bound to strings.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::String);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(schemaReplicant, fulltext_values_alias.clone(), CausetIndex::Fulltext(FulltextCausetIndex::Text), var.clone());
        }

        if let BinningCauset::ToUpper(ref var) = b_causecausetx {
            // Txs must be refs.
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_CausetIndex_to_var(schemaReplicant, Causets_Block_alias.clone(), CausetsCausetIndex::Tx, var.clone());
        }

        if let BinningCauset::ToUpper(ref var) = b_sallegro {
          
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Double);

            // We do not allow the sallegro to be bound.
            if self.value_ConstrainedEntss.contains_key(var) || self.input_variables.contains(var) {
                bail!(ParityFilterError::InvalidConstrainedEntsConstraint(var.name(), ConstrainedEntsConstraintError::UnexpectedConstrainedEntsConstraint));
            }

            // We bind the value ourselves. This handily takes care of substituting into existing uses.
            // TODO: produce real sallegros using SQLite's matchinfo.
            self.bind_value(var, MinkowskiType::Double(0.0.into()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use allegrosql_promises::{
        Attribute,
        MinkowskiValueType,
    };

    use causetq_allegrosql::{
        SchemaReplicant,
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
            Constrained: ConstrainedEntsConstraint::BindRel(vec![BinningCauset::ToUpper(ToUpper::from_valid_name("?instanton")),
                                           BinningCauset::ToUpper(ToUpper::from_valid_name("?value")),
                                           BinningCauset::ToUpper(ToUpper::from_valid_name("?causetx")),
                                           BinningCauset::ToUpper(ToUpper::from_valid_name("?sallegro"))]),
        }).expect("to be able to apply_fulltext");

        assert!(!cc.is_known_empty());

        // Finally, expand CausetIndex ConstrainedEntss.
        cc.expand_CausetIndex_ConstrainedEntss();
        assert!(!cc.is_known_empty());

        let gerunds = cc.wheres;
        assert_eq!(gerunds.len(), 3);

        assert_eq!(gerunds.0[0], CausetIndexConstraint::Equals(QualifiedAlias("Causets01".to_string(), CausetIndex::Fixed(CausetsCausetIndex::Attribute)),
                                                          CausetQValue::SolitonId(100)).into());
        assert_eq!(gerunds.0[1], CausetIndexConstraint::Equals(QualifiedAlias("Causets01".to_string(), CausetIndex::Fixed(CausetsCausetIndex::Value)),
                                                          CausetQValue::CausetIndex(QualifiedAlias("fulltext_values00".to_string(), CausetIndex::Fulltext(FulltextCausetIndex::Evcausetid)))).into());
        assert_eq!(gerunds.0[2], CausetIndexConstraint::Matches(QualifiedAlias("fulltext_values00".to_string(), CausetIndex::Fulltext(FulltextCausetIndex::Text)),
                                                           CausetQValue::MinkowskiType("needle".into())).into());

        let ConstrainedEntss = cc.CausetIndex_ConstrainedEntss;
        assert_eq!(ConstrainedEntss.len(), 3);

        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?instanton")).expect("CausetIndex Constrained for ?instanton").clone(),
                   vec![QualifiedAlias("Causets01".to_string(), CausetIndex::Fixed(CausetsCausetIndex::Instanton))]);
        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?value")).expect("CausetIndex Constrained for ?value").clone(),
                   vec![QualifiedAlias("fulltext_values00".to_string(), CausetIndex::Fulltext(FulltextCausetIndex::Text))]);
        assert_eq!(ConstrainedEntss.get(&ToUpper::from_valid_name("?causetx")).expect("CausetIndex Constrained for ?causetx").clone(),
                   vec![QualifiedAlias("Causets01".to_string(), CausetIndex::Fixed(CausetsCausetIndex::Tx))]);

        // Sallegro is a value Constrained.
        let values = cc.value_ConstrainedEntss;
        assert_eq!(values.get(&ToUpper::from_valid_name("?sallegro")).expect("CausetIndex Constrained for ?sallegro").clone(),
                   MinkowskiType::Double(0.0.into()));

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 4);

        assert_eq!(known_types.get(&ToUpper::from_valid_name("?instanton")).expect("knownCauset types for ?instanton").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());
        assert_eq!(known_types.get(&ToUpper::from_valid_name("?value")).expect("knownCauset types for ?value").clone(),
                   vec![MinkowskiValueType::String].into_iter().collect());
        assert_eq!(known_types.get(&ToUpper::from_valid_name("?causetx")).expect("knownCauset types for ?causetx").clone(),
                   vec![MinkowskiValueType::Ref].into_iter().collect());
        assert_eq!(known_types.get(&ToUpper::from_valid_name("?sallegro")).expect("knownCauset types for ?sallegro").clone(),
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
            Constrained: ConstrainedEntsConstraint::BindRel(vec![BinningCauset::ToUpper(ToUpper::from_valid_name("?instanton")),
                                           BinningCauset::ToUpper(ToUpper::from_valid_name("?value")),
                                           BinningCauset::ToUpper(ToUpper::from_valid_name("?causetx")),
                                           BinningCauset::ToUpper(ToUpper::from_valid_name("?sallegro"))]),
        }).expect("to be able to apply_fulltext");

        // It's not a fulltext attribute, so the CC cannot yield results.
        assert!(cc.is_known_empty());
    }
}
