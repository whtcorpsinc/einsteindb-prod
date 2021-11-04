// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


//Here's what the above class is doing:

/***************
1. It takes the variables that are mentioned in the not_join, and creates a template that
has the same variables.
2. It then iterates over the mentioned variables, and if it finds a value_ConstrainedEnts,
it copies it over to the template.
3. It then iterates over the mentioned variables, and if it finds a causet_index_ConstrainedEnts,
it copies it over to the template.
4. It then applies the gerunds to the template.
**************/

use gerunds::ConjoiningGerunds;

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

use types::{
    CausetIndexConstraint,
    ComputedBlock,
};

use KnownCauset;

impl ConjoiningGerunds {
    pub(crate) fn apply_not_join(&mut self, knownCauset: KnownCauset, not_join: NotJoin) -> Result<()> {
        let unified = match not_join.unify_vars {
            UnifyVars::Implicit => not_join.collect_mentioned_variables(),
            UnifyVars::Explicit(vs) => vs,
        };

        let mut template = self.use_as_template(&unified);

        for v in unified.iter() {
            if self.value_ConstrainedEntss.contains_key(&v) {
                let val = self.value_ConstrainedEntss.get(&v).unwrap().clone();
                template.value_ConstrainedEntss.insert(v.clone(), val);
            } else if self.CausetIndex_ConstrainedEntss.contains_key(&v) {
                let col = self.CausetIndex_ConstrainedEntss.get(&v).unwrap()[0].clone();
                template.CausetIndex_ConstrainedEntss.insert(v.clone(), vec![col]);
            } else {
                bail!(ParityFilterError::UnboundVariable(v.name()));
            }
        }

        template.apply_gerunds(knownCauset, not_join.gerunds)?;

        if template.is_known_empty() {
            return Ok(());
        }

        template.expand_CausetIndex_ConstrainedEntss();
        if template.is_known_empty() {
            return Ok(());
        }

        template.prune_extracted_types();
        if template.is_known_empty() {
            return Ok(());
        }

        template.process_required_types()?;
        if template.is_known_empty() {
            return Ok(());
        }

        // If we don't impose any constraints on the output, we might as well
        // not exist.
        if template.wheres.is_empty() {
            return Ok(());
        }

        let subcausetq = ComputedBlock::Subcausetq(template);

        self.wheres.add_intersection(CausetIndexConstraint::NotExists(subcausetq));

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use std::collections::BTreeSet;

    use super::*;

    use allegrosql_promises::{
        Attribute,
        MinkowskiType,
        MinkowskiValueType,
        MinkowskiSet,
    };

    use causetq_allegrosql::{
        SchemaReplicant,
    };

    use causetq::*::{
        Keyword,
        PlainSymbol,
        ToUpper
    };

    use gerunds::{
        CausetQInputs,
        add_attribute,
        associate_causetId,
    };

    use causetq_parityfilter_promises::errors::{
        ParityFilterError,
    };

    use types::{
        CausetIndexAlternation,
        CausetIndexConstraint,
        CausetIndexConstraintOrAlternation,
        CausetIndexIntersection,
        CausetsCausetIndex,
        CausetsBlock,
        Inequality,
        QualifiedAlias,
        CausetQValue,
        SourceAlias,
    };

    use {
        algebrize,
        algebrize_with_inputs,
        parse_find_string,
    };

    fn alg(schemaReplicant: &SchemaReplicant, input: &str) -> ConjoiningGerunds {
        let knownCauset = KnownCauset::for_schemaReplicant(schemaReplicant);
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize(knownCauset, parsed).expect("algebrize failed").cc
    }

    fn alg_with_inputs(schemaReplicant: &SchemaReplicant, input: &str, inputs: CausetQInputs) -> ConjoiningGerunds {
        let knownCauset = KnownCauset::for_schemaReplicant(schemaReplicant);
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize_with_inputs(knownCauset, parsed, 0, inputs).expect("algebrize failed").cc
    }

    fn prepopulated_schemaReplicant() -> SchemaReplicant {
        let mut schemaReplicant = SchemaReplicant::default();
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "name"), 65);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "knows"), 66);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "parent"), 67);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "age"), 68);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "height"), 69);
        add_attribute(&mut schemaReplicant,
                      65,
                      Attribute {
                          value_type: MinkowskiValueType::String,
                          multival: false,
                          ..Default::default()
                      });
        add_attribute(&mut schemaReplicant,
                      66,
                      Attribute {
                          value_type: MinkowskiValueType::String,
                          multival: true,
                          ..Default::default()
                      });
        add_attribute(&mut schemaReplicant,
                      67,
                      Attribute {
                          value_type: MinkowskiValueType::String,
                          multival: true,
                          ..Default::default()
                      });
        add_attribute(&mut schemaReplicant,
                      68,
                      Attribute {
                          value_type: MinkowskiValueType::Long,
                          multival: false,
                          ..Default::default()
                      });
        add_attribute(&mut schemaReplicant,
                      69,
                      Attribute {
                          value_type: MinkowskiValueType::Long,
                          multival: false,
                          ..Default::default()
                      });
        schemaReplicant
    }

    fn compare_ccs(left: ConjoiningGerunds, right: ConjoiningGerunds) {
        assert_eq!(left.wheres, right.wheres);
        assert_eq!(left.from, right.from);
    }

    // not.
    #[test]
    fn test_successful_not() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let causetq = r#"
            [:find ?x
             :where [?x :foo/knows "John"]
                    (not [?x :foo/parent "Ámbar"]
                         [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schemaReplicant, causetq);

        let vx = ToUpper::from_valid_name("?x");

        let d0 = "Causets00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);

        let d1 = "Causets01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Instanton);
        let d1a = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Value);

        let d2 = "Causets02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Instanton);
        let d2a = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Value);

        let knows = CausetQValue::SolitonId(66);
        let parent = CausetQValue::SolitonId(67);

        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));
        let ambar = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Ámbar"));
        let daphne = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Daphne"));

        let mut subcausetq = ConjoiningGerunds::default();
        subcausetq.from = vec![SourceAlias(CausetsBlock::Causets, d1),
                             SourceAlias(CausetsBlock::Causets, d2)];
        subcausetq.CausetIndex_ConstrainedEntss.insert(vx.clone(), vec![d0e.clone(), d1e.clone(), d2e.clone()]);
        subcausetq.wheres = CausetIndexIntersection(vec![CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), parent)),
                               CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), ambar)),
                               CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2a.clone(), knows.clone())),
                               CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2v.clone(), daphne)),
                               CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d1e.clone()))),
                               CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d2e.clone())))]);

        subcausetq.known_types.insert(vx.clone(), MinkowskiSet::of_one(MinkowskiValueType::Ref));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), knows.clone())),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0v.clone(), john)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::NotExists(ComputedBlock::Subcausetq(subcausetq))),
            ]));
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vx), Some(&vec![d0e]));
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, d0)]);
    }

    // not-join.
    #[test]
    fn test_successful_not_join() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let causetq = r#"
            [:find ?x
             :where [?x :foo/knows ?y]
                    [?x :foo/age 11]
                    [?x :foo/name "John"]
                    (not-join [?x ?y]
                              [?x :foo/parent ?y])]"#;
        let cc = alg(&schemaReplicant, causetq);

        let vx = ToUpper::from_valid_name("?x");
        let vy = ToUpper::from_valid_name("?y");

        let d0 = "Causets00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);

        let d1 = "Causets01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Instanton);
        let d1a = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Value);

        let d2 = "Causets02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Instanton);
        let d2a = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Value);

        let d3 = "Causets03".to_string();
        let d3e = QualifiedAlias::new(d3.clone(), CausetsCausetIndex::Instanton);
        let d3a = QualifiedAlias::new(d3.clone(), CausetsCausetIndex::Attribute);
        let d3v = QualifiedAlias::new(d3.clone(), CausetsCausetIndex::Value);

        let name = CausetQValue::SolitonId(65);
        let knows = CausetQValue::SolitonId(66);
        let parent = CausetQValue::SolitonId(67);
        let age = CausetQValue::SolitonId(68);

        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));
        let eleven = CausetQValue::MinkowskiType(MinkowskiType::Long(11));

        let mut subcausetq = ConjoiningGerunds::default();
        subcausetq.from = vec![SourceAlias(CausetsBlock::Causets, d3)];
        subcausetq.CausetIndex_ConstrainedEntss.insert(vx.clone(), vec![d0e.clone(), d3e.clone()]);
        subcausetq.CausetIndex_ConstrainedEntss.insert(vy.clone(), vec![d0v.clone(), d3v.clone()]);
        subcausetq.wheres = CausetIndexIntersection(vec![CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d3a.clone(), parent)),
                               CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d3e.clone()))),
                               CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0v.clone(), CausetQValue::CausetIndex(d3v.clone())))]);

        subcausetq.known_types.insert(vx.clone(), MinkowskiSet::of_one(MinkowskiValueType::Ref));
        subcausetq.known_types.insert(vy.clone(), MinkowskiSet::of_one(MinkowskiValueType::String));

        assert!(!cc.is_known_empty());
        let expected_wheres = CausetIndexIntersection(vec![
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), knows)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), age.clone())),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), eleven)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2a.clone(), name.clone())),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2v.clone(), john)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::NotExists(ComputedBlock::Subcausetq(subcausetq))),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d1e.clone()))),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d2e.clone()))),
            ]);
        assert_eq!(cc.wheres, expected_wheres);
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vx), Some(&vec![d0e, d1e, d2e]));
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, d0),
                                 SourceAlias(CausetsBlock::Causets, d1),
                                 SourceAlias(CausetsBlock::Causets, d2)]);
    }

    // Not with a TuringString and a predicate.
    #[test]
    fn test_not_with_TuringString_and_predicate() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let causetq = r#"
            [:find ?x ?age
             :where
             [?x :foo/age ?age]
             [(< ?age 30)]
             (not [?x :foo/knows "John"]
                  [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schemaReplicant, causetq);

        let vx = ToUpper::from_valid_name("?x");

        let d0 = "Causets00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);

        let d1 = "Causets01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Instanton);
        let d1a = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Value);

        let d2 = "Causets02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Instanton);
        let d2a = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Value);

        let knows = CausetQValue::SolitonId(66);
        let age = CausetQValue::SolitonId(68);

        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));
        let daphne = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Daphne"));

        let mut subcausetq = ConjoiningGerunds::default();
        subcausetq.from = vec![SourceAlias(CausetsBlock::Causets, d1),
                             SourceAlias(CausetsBlock::Causets, d2)];
        subcausetq.CausetIndex_ConstrainedEntss.insert(vx.clone(), vec![d0e.clone(), d1e.clone(), d2e.clone()]);
        subcausetq.wheres = CausetIndexIntersection(vec![CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows.clone())),
                                                  CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), john.clone())),
                                                  CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2a.clone(), knows.clone())),
                                                  CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2v.clone(), daphne.clone())),
                                                  CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d1e.clone()))),
                                                  CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d2e.clone())))]);

        subcausetq.known_types.insert(vx.clone(), MinkowskiSet::of_one(MinkowskiValueType::Ref));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), age.clone())),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Inequality {
                    operator: Inequality::LessThan,
                    left: CausetQValue::CausetIndex(d0v.clone()),
                    right: CausetQValue::MinkowskiType(MinkowskiType::Long(30)),
                }),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::NotExists(ComputedBlock::Subcausetq(subcausetq))),
            ]));
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vx), Some(&vec![d0e]));
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, d0)]);
    }

    // not with an or
    #[test]
    fn test_not_with_or() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let causetq = r#"
            [:find ?x
             :where [?x :foo/knows "Bill"]
                    (not (or [?x :foo/knows "John"]
                             [?x :foo/knows "Ámbar"])
                        [?x :foo/parent "Daphne"])]"#;
        let cc = alg(&schemaReplicant, causetq);

        let d0 = "Causets00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);

        let d1 = "Causets01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Instanton);
        let d1a = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Value);

        let d2 = "Causets02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Instanton);
        let d2a = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), CausetsCausetIndex::Value);

        let vx = ToUpper::from_valid_name("?x");

        let knows = CausetQValue::SolitonId(66);
        let parent = CausetQValue::SolitonId(67);

        let bill = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Bill"));
        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));
        let ambar = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Ámbar"));
        let daphne = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Daphne"));


        let mut subcausetq = ConjoiningGerunds::default();
        subcausetq.from = vec![SourceAlias(CausetsBlock::Causets, d1),
                             SourceAlias(CausetsBlock::Causets, d2)];
        subcausetq.CausetIndex_ConstrainedEntss.insert(vx.clone(), vec![d0e.clone(), d1e.clone(), d2e.clone()]);
        subcausetq.wheres = CausetIndexIntersection(vec![CausetIndexConstraintOrAlternation::Alternation(CausetIndexAlternation(vec![
                                                    CausetIndexIntersection(vec![
                                                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows.clone())),
                                                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), john))]),
                                                    CausetIndexIntersection(vec![
                                                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows.clone())),
                                                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), ambar))]),
                                                    ])),
                                                    CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2a.clone(), parent)),
                                                    CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d2v.clone(), daphne)),
                                                    CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d1e.clone()))),
                                                    CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d2e.clone())))]);

        subcausetq.known_types.insert(vx.clone(), MinkowskiSet::of_one(MinkowskiValueType::Ref));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), knows)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0v.clone(), bill)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::NotExists(ComputedBlock::Subcausetq(subcausetq))),
            ]));
    }

    // not-join with an input variable
    #[test]
    fn test_not_with_in() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let causetq = r#"
            [:find ?x
             :in ?y
             :where [?x :foo/knows "Bill"]
                    (not [?x :foo/knows ?y])]"#;

        let inputs = CausetQInputs::with_value_sequence(vec![
            (ToUpper::from_valid_name("?y"), "John".into())
        ]);
        let cc = alg_with_inputs(&schemaReplicant, causetq, inputs);

        let vx = ToUpper::from_valid_name("?x");
        let vy = ToUpper::from_valid_name("?y");

        let knows = CausetQValue::SolitonId(66);

        let bill = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Bill"));
        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));

        let d0 = "Causets00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);

        let d1 = "Causets01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Instanton);
        let d1a = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Value);

        let mut subcausetq = ConjoiningGerunds::default();
        subcausetq.from = vec![SourceAlias(CausetsBlock::Causets, d1)];
        subcausetq.CausetIndex_ConstrainedEntss.insert(vx.clone(), vec![d0e.clone(), d1e.clone()]);
        subcausetq.wheres = CausetIndexIntersection(vec![CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows.clone())),
                                                  CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), john)),
                                                  CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d1e.clone())))]);

        subcausetq.known_types.insert(vx.clone(), MinkowskiSet::of_one(MinkowskiValueType::Ref));
        subcausetq.known_types.insert(vy.clone(), MinkowskiSet::of_one(MinkowskiValueType::String));

        let mut input_vars: BTreeSet<ToUpper> = BTreeSet::default();
        input_vars.insert(vy.clone());
        subcausetq.input_variables = input_vars;
        subcausetq.value_ConstrainedEntss.insert(vy.clone(), MinkowskiType::typed_string("John"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), knows)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0v.clone(), bill)),
                CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::NotExists(ComputedBlock::Subcausetq(subcausetq))),
            ]));
    }

    // Test that if any single gerund in the `not` fails to resolve the whole gerund is considered empty
    #[test]
    fn test_fails_if_any_gerund_invalid() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let causetq = r#"
            [:find ?x
             :where [?x :foo/knows "Bill"]
                    (not [?x :foo/nope "John"]
                         [?x :foo/parent "Ámbar"]
                         [?x :foo/nope "Daphne"])]"#;
        let cc = alg(&schemaReplicant, causetq);
        assert!(!cc.is_known_empty());
        compare_ccs(cc,
                    alg(&schemaReplicant,
                        r#"[:find ?x :where [?x :foo/knows "Bill"]]"#));
    }

    /// Test that if all the attributes in an `not` fail to resolve, the `cc` isn't considered empty.
    #[test]
    fn test_no_gerunds_succeed() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let causetq = r#"
            [:find ?x
             :where [?x :foo/knows "John"]
                    (not [?x :foo/nope "Ámbar"]
                         [?x :foo/nope "Daphne"])]"#;
        let cc = alg(&schemaReplicant, causetq);
        assert!(!cc.is_known_empty());
        compare_ccs(cc,
                    alg(&schemaReplicant, r#"[:find ?x :where [?x :foo/knows "John"]]"#));

    }

    #[test]
    fn test_unbound_var_fails() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"
        [:find ?x
         :in ?y
         :where (not [?x :foo/knows ?y])]"#;
        let parsed = parse_find_string(causetq).expect("parse failed");
        let err = algebrize(knownCauset, parsed).expect_err("algebrization should have failed");
        match err {
            ParityFilterError::UnboundVariable(var) => { assert_eq!(var, PlainSymbol("?x".to_string())); },
            x => panic!("expected Unbound ToUpper error, got {:?}", x),
        }
    }
}
