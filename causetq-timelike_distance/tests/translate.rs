// Copyright 2016 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edbn;
extern crate einsteindb_embedded;
extern crate embedded_promises;
extern crate einsteindb_causetq_parityfilter;
extern crate einsteindb_causetq_projector;
extern crate einsteindb_sql;

use std::collections::BTreeMap;

use std::rc::Rc;

use edbn::causetq::{
    FindSpec,
    Keyword,
    Variable,
};

use embedded_promises::{
    Attribute,
    SolitonId,
    TypedValue,
    ValueType,
};

use einsteindb_embedded::{
    Schema,
};

use einsteindb_causetq_parityfilter::{
    Known,
    CausetQInputs,
    algebrize,
    algebrize_with_inputs,
    parse_find_string,
};

use einsteindb_causetq_projector::{
    MinkowskiProjector,
};

use einsteindb_causetq_projector::translate::{
    GreedoidSelect,
    causetq_to_select,
};

use einsteindb_sql::SQLCausetQ;

/// Produce the appropriate `Variable` for the provided valid ?-prefixed name.
/// This lives here because we can't re-export macros:
/// https://github.com/rust-lang/rust/issues/29638.
macro_rules! var {
    ( ? $var:causetid ) => {
        $crate::Variable::from_valid_name(concat!("?", stringify!($var)))
    };
}

fn associate_causetId(schema: &mut Schema, i: Keyword, e: SolitonId) {
    schema.entid_map.insert(e, i.clone());
    schema.causetId_map.insert(i.clone(), e);
}

fn add_attribute(schema: &mut Schema, e: SolitonId, a: Attribute) {
    schema.attribute_map.insert(e, a);
}

fn causetq_to_sql(causetq: GreedoidSelect) -> SQLCausetQ {
    match causetq {
        GreedoidSelect::CausetQ { causetq, projector: _projector } => {
            causetq.to_sql_causetq().expect("to_sql_causetq to succeed")
        },
        GreedoidSelect::Constant(constant) => {
            panic!("GreedoidSelect wasn't ::CausetQ! Got constant {:#?}", constant.project_without_rows());
        },
    }
}

fn causetq_to_constant(causetq: GreedoidSelect) -> MinkowskiProjector {
    match causetq {
        GreedoidSelect::Constant(constant) => {
            constant
        },
        _ => panic!("GreedoidSelect wasn't ::Constant!"),
    }
}

fn assert_causetq_is_empty(causetq: GreedoidSelect, expected_spec: FindSpec) {
    let constant = causetq_to_constant(causetq).project_without_rows().expect("constant run");
    assert_eq!(*constant.spec, expected_spec);
    assert!(constant.results.is_empty());
}

fn inner_translate_with_inputs(schema: &Schema, causetq: &'static str, inputs: CausetQInputs) -> GreedoidSelect {
    let known = Known::for_schema(schema);
    let parsed = parse_find_string(causetq).expect("parse to succeed");
    let algebrized = algebrize_with_inputs(known, parsed, 0, inputs).expect("algebrize to succeed");
    causetq_to_select(schema, algebrized).expect("translate to succeed")
}

fn translate_with_inputs(schema: &Schema, causetq: &'static str, inputs: CausetQInputs) -> SQLCausetQ {
    causetq_to_sql(inner_translate_with_inputs(schema, causetq, inputs))
}

fn translate(schema: &Schema, causetq: &'static str) -> SQLCausetQ {
    translate_with_inputs(schema, causetq, CausetQInputs::default())
}

fn translate_with_inputs_to_constant(schema: &Schema, causetq: &'static str, inputs: CausetQInputs) -> MinkowskiProjector {
    causetq_to_constant(inner_translate_with_inputs(schema, causetq, inputs))
}

fn translate_to_constant(schema: &Schema, causetq: &'static str) -> MinkowskiProjector {
    translate_with_inputs_to_constant(schema, causetq, CausetQInputs::default())
}


fn prepopulated_typed_schema(foo_type: ValueType) -> Schema {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("foo", "bar"), 99);
    add_attribute(&mut schema, 99, Attribute {
        value_type: foo_type,
        ..Default::default()
    });
    associate_causetId(&mut schema, Keyword::namespaced("foo", "fts"), 100);
    add_attribute(&mut schema, 100, Attribute {
        value_type: ValueType::String,
        index: true,
        fulltext: true,
        ..Default::default()
    });
    schema
}

fn prepopulated_schema() -> Schema {
    prepopulated_typed_schema(ValueType::String)
}

fn make_arg(name: &'static str, value: &'static str) -> (String, Rc<einsteindb_sql::Value>) {
    (name.to_string(), Rc::new(einsteindb_sql::Value::Text(value.to_string())))
}

#[test]
fn test_scalar() {
    let schema = prepopulated_schema();

    let causetq = r#"[:find ?x . :where [?x :foo/bar "yyy"]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_tuple() {
    let schema = prepopulated_schema();

    let causetq = r#"[:find [?x] :where [?x :foo/bar "yyy"]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_coll() {
    let schema = prepopulated_schema();

    let causetq = r#"[:find [?x ...] :where [?x :foo/bar "yyy"]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_rel() {
    let schema = prepopulated_schema();

    let causetq = r#"[:find ?x :where [?x :foo/bar "yyy"]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_limit() {
    let schema = prepopulated_schema();

    let causetq = r#"[:find ?x :where [?x :foo/bar "yyy"] :limit 5]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 5");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_unbound_variable_limit() {
    let schema = prepopulated_schema();

    // We don't know the value of the limit var, so we produce an escaped SQL variable to handle
    // later input.
    let causetq = r#"[:find ?x :in ?limit-is-9-great :where [?x :foo/bar "yyy"] :limit ?limit-is-9-great]"#;
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, CausetQInputs::default());
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` \
                     FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 \
                     LIMIT $ilimit_is_9_great");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_bound_variable_limit() {
    let schema = prepopulated_schema();

    // We know the value of `?limit` at algebrizing time, so we substitute directly.
    let causetq = r#"[:find ?x :in ?limit :where [?x :foo/bar "yyy"] :limit ?limit]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?limit"), TypedValue::Long(92))]);
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 92");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_bound_variable_limit_affects_distinct() {
    let schema = prepopulated_schema();

    // We know the value of `?limit` at algebrizing time, so we substitute directly.
    // As it's `1`, we know we don't need `DISTINCT`!
    let causetq = r#"[:find ?x :in ?limit :where [?x :foo/bar "yyy"] :limit ?limit]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?limit"), TypedValue::Long(1))]);
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    assert_eq!(allegrosql, "SELECT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_bound_variable_limit_affects_types() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);

    let causetq = r#"[:find ?x ?limit :in ?limit :where [?x _ ?limit] :limit ?limit]"#;
    let parsed = parse_find_string(causetq).expect("parse failed");
    let algebrized = algebrize(known, parsed).expect("algebrize failed");

    // The type is known.
    assert_eq!(Some(ValueType::Long),
               algebrized.cc.known_type(&Variable::from_valid_name("?limit")));

    let select = causetq_to_select(&schema, algebrized).expect("causetq to translate");
    let SQLCausetQ { allegrosql, args } = causetq_to_sql(select);

    // TODO: this causetq isn't actually correct -- we don't yet algebrize for variables that are
    // specified in `:in` but not provided at algebrizing time. But it shows what we care about
    // at the moment: we don't project a type column, because we know it's a Long.
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x`, `datoms00`.v AS `?limit` FROM `causets` AS `datoms00` LIMIT $ilimit");
    assert_eq!(args, vec![]);
}

#[test]
fn test_unknown_attribute_keyword_value() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ :ab/yyy]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // Only match keywords, not strings: tag = 13.
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.v = $v0 AND (`datoms00`.value_type_tag = 13)");
    assert_eq!(args, vec![make_arg("$v0", ":ab/yyy")]);
}

#[test]
fn test_unknown_attribute_string_value() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ "horses"]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // We expect all_datoms because we're causetqing for a string. Magic, that.
    // We don't want keywords etc., so tag = 10.
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms00`.e AS `?x` FROM `all_datoms` AS `all_datoms00` WHERE `all_datoms00`.v = $v0 AND (`all_datoms00`.value_type_tag = 10)");
    assert_eq!(args, vec![make_arg("$v0", "horses")]);
}

#[test]
fn test_unknown_attribute_double_value() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ 9.95]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // In general, doubles _could_ be 1.0, which might match a boolean or a ref. Set tag = 5 to
    // make sure we only match numbers.
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.v = 9.95e0 AND (`datoms00`.value_type_tag = 5)");
    assert_eq!(args, vec![]);
}

#[test]
fn test_unknown_attribute_integer_value() {
    let schema = Schema::default();

    let negative = r#"[:find ?x :where [?x _ -1]]"#;
    let zero = r#"[:find ?x :where [?x _ 0]]"#;
    let one = r#"[:find ?x :where [?x _ 1]]"#;
    let two = r#"[:find ?x :where [?x _ 2]]"#;

    // Can't match boolean; no need to filter it out.
    let SQLCausetQ { allegrosql, args } = translate(&schema, negative);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.v = -1");
    assert_eq!(args, vec![]);

    // Excludes booleans.
    let SQLCausetQ { allegrosql, args } = translate(&schema, zero);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE (`datoms00`.v = 0 AND `datoms00`.value_type_tag <> 1)");
    assert_eq!(args, vec![]);

    // Excludes booleans.
    let SQLCausetQ { allegrosql, args } = translate(&schema, one);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE (`datoms00`.v = 1 AND `datoms00`.value_type_tag <> 1)");
    assert_eq!(args, vec![]);

    // Can't match boolean; no need to filter it out.
    let SQLCausetQ { allegrosql, args } = translate(&schema, two);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.v = 2");
    assert_eq!(args, vec![]);
}

#[test]
fn test_unknown_causetId() {
    let schema = Schema::default();
    let known = Known::for_schema(&schema);

    let impossible = r#"[:find ?x :where [?x :edb/causetid :no/exist]]"#;
    let parsed = parse_find_string(impossible).expect("parse failed");
    let algebrized = algebrize(known, parsed).expect("algebrize failed");

    // This causetq cannot return results: the causetid doesn't resolve for a ref-typed attribute.
    assert!(algebrized.is_known_empty());

    // If you insist…
    let select = causetq_to_select(&schema, algebrized).expect("causetq to translate");
    assert_causetq_is_empty(select, FindSpec::FindRel(vec![var!(?x).into()]));
}

#[test]
fn test_type_required_long() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ ?e] [(type ?e :edb.type/long)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` \
                     FROM `causets` AS `datoms00` \
                     WHERE ((`datoms00`.value_type_tag = 5 AND \
                             (typeof(`datoms00`.v) = 'integer')))");

    assert_eq!(args, vec![]);
}

#[test]
fn test_type_required_double() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ ?e] [(type ?e :edb.type/double)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` \
                     FROM `causets` AS `datoms00` \
                     WHERE ((`datoms00`.value_type_tag = 5 AND \
                             (typeof(`datoms00`.v) = 'real')))");

    assert_eq!(args, vec![]);
}

#[test]
fn test_type_required_boolean() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ ?e] [(type ?e :edb.type/boolean)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` \
                     FROM `causets` AS `datoms00` \
                     WHERE (`datoms00`.value_type_tag = 1)");

    assert_eq!(args, vec![]);
}

#[test]
fn test_type_required_string() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ ?e] [(type ?e :edb.type/string)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // Note: strings should use `all_datoms` and not `causets`.
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms00`.e AS `?x` \
                     FROM `all_datoms` AS `all_datoms00` \
                     WHERE (`all_datoms00`.value_type_tag = 10)");
    assert_eq!(args, vec![]);
}

#[test]
fn test_numeric_less_than_unknown_attribute() {
    let schema = Schema::default();

    let causetq = r#"[:find ?x :where [?x _ ?y] [(< ?y 10)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // Although we infer numericness from numeric predicates, we've already assigned a table to the
    // first pattern, and so this is _still_ `all_datoms`.
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms00`.e AS `?x` FROM `all_datoms` AS `all_datoms00` WHERE `all_datoms00`.v < 10");
    assert_eq!(args, vec![]);
}

#[test]
fn test_numeric_gte_known_attribute() {
    let schema = prepopulated_typed_schema(ValueType::Double);
    let causetq = r#"[:find ?x :where [?x :foo/bar ?y] [(>= ?y 12.9)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v >= 1.29e1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_numeric_not_equals_known_attribute() {
    let schema = prepopulated_typed_schema(ValueType::Long);
    let causetq = r#"[:find ?x . :where [?x :foo/bar ?y] [(!= ?y 12)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v <> 12 LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_compare_long_to_double_constants() {
    let schema = prepopulated_typed_schema(ValueType::Double);

    let causetq = r#"[:find ?e .
                    :where
                    [?e :foo/bar ?v]
                    [(< 99.0 1234512345)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms00`.e AS `?e` FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 \
                       AND 9.9e1 < 1234512345 \
                     LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_compare_long_to_double() {
    let schema = prepopulated_typed_schema(ValueType::Double);

    // You can compare longs to doubles.
    let causetq = r#"[:find ?e .
                    :where
                    [?e :foo/bar ?t]
                    [(< ?t 1234512345)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms00`.e AS `?e` FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 \
                       AND `datoms00`.v < 1234512345 \
                     LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_compare_double_to_long() {
    let schema = prepopulated_typed_schema(ValueType::Long);

    // You can compare doubles to longs.
    let causetq = r#"[:find ?e .
                    :where
                    [?e :foo/bar ?t]
                    [(< ?t 1234512345.0)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms00`.e AS `?e` FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 \
                       AND `datoms00`.v < 1.234512345e9 \
                     LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_simple_or_join() {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("page", "url"), 97);
    associate_causetId(&mut schema, Keyword::namespaced("page", "title"), 98);
    associate_causetId(&mut schema, Keyword::namespaced("page", "description"), 99);
    for x in 97..100 {
        add_attribute(&mut schema, x, Attribute {
            value_type: ValueType::String,
            ..Default::default()
        });
    }

    let causetq = r#"[:find [?url ?description]
                    :where
                    (or-join [?page]
                      [?page :page/url "http://foo.com/"]
                      [?page :page/title "Foo"])
                    [?page :page/url ?url]
                    [?page :page/description ?description]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms01`.v AS `?url`, `datoms02`.v AS `?description` FROM `causets` AS `datoms00`, `causets` AS `datoms01`, `causets` AS `datoms02` WHERE ((`datoms00`.a = 97 AND `datoms00`.v = $v0) OR (`datoms00`.a = 98 AND `datoms00`.v = $v1)) AND `datoms01`.a = 97 AND `datoms02`.a = 99 AND `datoms00`.e = `datoms01`.e AND `datoms00`.e = `datoms02`.e LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "http://foo.com/"), make_arg("$v1", "Foo")]);
}

#[test]
fn test_complex_or_join() {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("page", "save"), 95);
    add_attribute(&mut schema, 95, Attribute {
        value_type: ValueType::Ref,
        ..Default::default()
    });

    associate_causetId(&mut schema, Keyword::namespaced("save", "title"), 96);
    associate_causetId(&mut schema, Keyword::namespaced("page", "url"), 97);
    associate_causetId(&mut schema, Keyword::namespaced("page", "title"), 98);
    associate_causetId(&mut schema, Keyword::namespaced("page", "description"), 99);
    for x in 96..100 {
        add_attribute(&mut schema, x, Attribute {
            value_type: ValueType::String,
            ..Default::default()
        });
    }

    let causetq = r#"[:find [?url ?description]
                    :where
                    (or-join [?page]
                      [?page :page/url "http://foo.com/"]
                      [?page :page/title "Foo"]
                      (and
                        [?page :page/save ?save]
                        [?save :save/title "Foo"]))
                    [?page :page/url ?url]
                    [?page :page/description ?description]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `datoms04`.v AS `?url`, \
                            `datoms05`.v AS `?description` \
                     FROM (SELECT `datoms00`.e AS `?page` \
                           FROM `causets` AS `datoms00` \
                           WHERE `datoms00`.a = 97 \
                           AND `datoms00`.v = $v0 \
                           UNION \
                           SELECT `datoms01`.e AS `?page` \
                               FROM `causets` AS `datoms01` \
                               WHERE `datoms01`.a = 98 \
                               AND `datoms01`.v = $v1 \
                           UNION \
                           SELECT `datoms02`.e AS `?page` \
                               FROM `causets` AS `datoms02`, \
                                   `causets` AS `datoms03` \
                               WHERE `datoms02`.a = 95 \
                               AND `datoms03`.a = 96 \
                               AND `datoms03`.v = $v1 \
                               AND `datoms02`.v = `datoms03`.e) AS `c00`, \
                           `causets` AS `datoms04`, \
                           `causets` AS `datoms05` \
                    WHERE `datoms04`.a = 97 \
                    AND `datoms05`.a = 99 \
                    AND `c00`.`?page` = `datoms04`.e \
                    AND `c00`.`?page` = `datoms05`.e \
                    LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "http://foo.com/"),
                          make_arg("$v1", "Foo")]);
}

#[test]
fn test_complex_or_join_type_projection() {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("page", "title"), 98);
    add_attribute(&mut schema, 98, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let causetq = r#"[:find [?y]
                    :where
                    (or
                      [6 :page/title ?y]
                      [5 _ ?y])]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `c00`.`?y` AS `?y`, \
                            `c00`.`?y_value_type_tag` AS `?y_value_type_tag` \
                       FROM (SELECT `datoms00`.v AS `?y`, \
                                    10 AS `?y_value_type_tag` \
                            FROM `causets` AS `datoms00` \
                            WHERE `datoms00`.e = 6 \
                            AND `datoms00`.a = 98 \
                            UNION \
                            SELECT `all_datoms01`.v AS `?y`, \
                                `all_datoms01`.value_type_tag AS `?y_value_type_tag` \
                            FROM `all_datoms` AS `all_datoms01` \
                            WHERE `all_datoms01`.e = 5) AS `c00` \
                    LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_not() {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("page", "url"), 97);
    associate_causetId(&mut schema, Keyword::namespaced("page", "title"), 98);
    associate_causetId(&mut schema, Keyword::namespaced("page", "bookmarked"), 99);
    for x in 97..99 {
        add_attribute(&mut schema, x, Attribute {
            value_type: ValueType::String,
            ..Default::default()
        });
    }
    add_attribute(&mut schema, 99, Attribute {
        value_type: ValueType::Boolean,
        ..Default::default()
    });

    let causetq = r#"[:find ?title
                    :where [?page :page/title ?title]
                           (not [?page :page/url "http://foo.com/"]
                                [?page :page/bookmarked true])]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.v AS `?title` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 98 AND NOT EXISTS (SELECT 1 FROM `causets` AS `datoms01`, `causets` AS `datoms02` WHERE `datoms01`.a = 97 AND `datoms01`.v = $v0 AND `datoms02`.a = 99 AND `datoms02`.v = 1 AND `datoms00`.e = `datoms01`.e AND `datoms00`.e = `datoms02`.e)");
    assert_eq!(args, vec![make_arg("$v0", "http://foo.com/")]);
}

#[test]
fn test_not_join() {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("page", "url"), 97);
    associate_causetId(&mut schema, Keyword::namespaced("bookmarks", "page"), 98);
    associate_causetId(&mut schema, Keyword::namespaced("bookmarks", "date_created"), 99);
    add_attribute(&mut schema, 97, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });
    add_attribute(&mut schema, 98, Attribute {
        value_type: ValueType::Ref,
        ..Default::default()
    });
    add_attribute(&mut schema, 99, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let causetq = r#"[:find ?url
                        :where [?url :page/url]
                               (not-join [?url]
                                   [?page :bookmarks/page ?url]
                                   [?page :bookmarks/date_created "4/4/2017"])]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?url` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 97 AND NOT EXISTS (SELECT 1 FROM `causets` AS `datoms01`, `causets` AS `datoms02` WHERE `datoms01`.a = 98 AND `datoms02`.a = 99 AND `datoms02`.v = $v0 AND `datoms01`.e = `datoms02`.e AND `datoms00`.e = `datoms01`.v)");
    assert_eq!(args, vec![make_arg("$v0", "4/4/2017")]);
}

#[test]
fn test_with_without_aggregate() {
    let schema = prepopulated_schema();

    // Known type.
    let causetq = r#"[:find ?x :with ?y :where [?x :foo/bar ?y]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 99");
    assert_eq!(args, vec![]);

    // Unknown type.
    let causetq = r#"[:find ?x :with ?y :where [?x _ ?y]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms00`.e AS `?x` FROM `all_datoms` AS `all_datoms00`");
    assert_eq!(args, vec![]);
}

#[test]
fn test_order_by() {
    let schema = prepopulated_schema();

    // Known type.
    let causetq = r#"[:find ?x :where [?x :foo/bar ?y] :order (desc ?y)]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?x`, `datoms00`.v AS `?y` \
                     FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 \
                     ORDER BY `?y` DESC");
    assert_eq!(args, vec![]);

    // Unknown type.
    let causetq = r#"[:find ?x :with ?y :where [?x _ ?y] :order ?y ?x]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, `all_datoms00`.v AS `?y`, \
                                     `all_datoms00`.value_type_tag AS `?y_value_type_tag` \
                     FROM `all_datoms` AS `all_datoms00` \
                     ORDER BY `?y_value_type_tag` ASC, `?y` ASC, `?x` ASC");
    assert_eq!(args, vec![]);
}

#[test]
fn test_complex_nested_or_join_type_projection() {
    let mut schema = Schema::default();
    associate_causetId(&mut schema, Keyword::namespaced("page", "title"), 98);
    add_attribute(&mut schema, 98, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let input = r#"[:find [?y]
                    :where
                    (or
                      (or
                        [_ :page/title ?y])
                      (or
                        [_ :page/title ?y]))]"#;

    let SQLCausetQ { allegrosql, args } = translate(&schema, input);
    assert_eq!(allegrosql, "SELECT `c00`.`?y` AS `?y` \
                     FROM (SELECT `datoms00`.v AS `?y` \
                           FROM `causets` AS `datoms00` \
                           WHERE `datoms00`.a = 98 \
                           UNION \
                           SELECT `datoms01`.v AS `?y` \
                           FROM `causets` AS `datoms01` \
                           WHERE `datoms01`.a = 98) \
                           AS `c00` \
                     LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_ground_scalar() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let causetq = r#"[:find ?x . :where [(ground "yyy") ?x]]"#;
    let constant = translate_to_constant(&schema, causetq);
    assert_eq!(constant.project_without_rows().unwrap()
                       .into_scalar().unwrap(),
               Some(TypedValue::typed_string("yyy").into()));

    // Verify that we accept bound input constants.
    let causetq = r#"[:find ?x . :in ?v :where [(ground ?v) ?x]]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?v"), "aaa".into())]);
    let constant = translate_with_inputs_to_constant(&schema, causetq, inputs);
    assert_eq!(constant.project_without_rows().unwrap()
                       .into_scalar().unwrap(),
               Some(TypedValue::typed_string("aaa").into()));
}

#[test]
fn test_ground_tuple() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let causetq = r#"[:find ?x ?y :where [(ground [1 "yyy"]) [?x ?y]]]"#;
    let constant = translate_to_constant(&schema, causetq);
    assert_eq!(constant.project_without_rows().unwrap()
                       .into_rel().unwrap(),
               vec![vec![TypedValue::Long(1), TypedValue::typed_string("yyy")]].into());

    // Verify that we accept bound input constants.
    let causetq = r#"[:find [?x ?y] :in ?u ?v :where [(ground [?u ?v]) [?x ?y]]]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?u"), TypedValue::Long(2)),
                                                       (Variable::from_valid_name("?v"), "aaa".into()),]);

    let constant = translate_with_inputs_to_constant(&schema, causetq, inputs);
    assert_eq!(constant.project_without_rows().unwrap()
                       .into_tuple().unwrap(),
               Some(vec![TypedValue::Long(2).into(), TypedValue::typed_string("aaa").into()]));

    // TODO: treat 2 as an input variable that could be bound late, rather than eagerly binding it.
    // In that case the causetq wouldn't be constant, and would look more like:
    // let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    // assert_eq!(allegrosql, "SELECT 2 AS `?x`, $v0 AS `?y` LIMIT 1");
    // assert_eq!(args, vec![make_arg("$v0", "aaa"),]);
}

#[test]
fn test_ground_coll() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let causetq = r#"[:find ?x :where [(ground ["xxx" "yyy"]) [?x ...]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `c00`.`?x` AS `?x` FROM \
                         (SELECT 0 AS `?x` WHERE 0 UNION ALL VALUES ($v0), ($v1)) AS `c00`");
    assert_eq!(args, vec![make_arg("$v0", "xxx"),
                          make_arg("$v1", "yyy")]);

    // Verify that we accept bound input constants.
    let causetq = r#"[:find ?x :in ?u ?v :where [(ground [?u ?v]) [?x ...]]]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?u"), TypedValue::Long(2)),
                                                       (Variable::from_valid_name("?v"), TypedValue::Long(3)),]);
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    // TODO: treat 2 and 3 as input variables that could be bound late, rather than eagerly binding.
    assert_eq!(allegrosql, "SELECT DISTINCT `c00`.`?x` AS `?x` FROM \
                         (SELECT 0 AS `?x` WHERE 0 UNION ALL VALUES (2), (3)) AS `c00`");
    assert_eq!(args, vec![]);
}

#[test]
fn test_ground_rel() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let causetq = r#"[:find ?x ?y :where [(ground [[1 "xxx"] [2 "yyy"]]) [[?x ?y]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `c00`.`?x` AS `?x`, `c00`.`?y` AS `?y` FROM \
                         (SELECT 0 AS `?x`, 0 AS `?y` WHERE 0 UNION ALL VALUES (1, $v0), (2, $v1)) AS `c00`");
    assert_eq!(args, vec![make_arg("$v0", "xxx"),
                          make_arg("$v1", "yyy")]);

    // Verify that we accept bound input constants.
    let causetq = r#"[:find ?x ?y :in ?u ?v :where [(ground [[?u 1] [?v 2]]) [[?x ?y]]]]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?u"), TypedValue::Long(3)),
                                                       (Variable::from_valid_name("?v"), TypedValue::Long(4)),]);
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    // TODO: treat 3 and 4 as input variables that could be bound late, rather than eagerly binding.
    assert_eq!(allegrosql, "SELECT DISTINCT `c00`.`?x` AS `?x`, `c00`.`?y` AS `?y` FROM \
                         (SELECT 0 AS `?x`, 0 AS `?y` WHERE 0 UNION ALL VALUES (3, 1), (4, 2)) AS `c00`");
    assert_eq!(args, vec![]);
}

#[test]
fn test_compound_with_ground() {
    let schema = prepopulated_schema();

    // Verify that we can use the resulting CCs as children in compound CCs.
    let causetq = r#"[:find ?x :where (or [(ground "yyy") ?x]
                                        [(ground "zzz") ?x])]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // This is confusing because the computed tables (like `c00`) are numbered sequentially in each
    // arm of the `or` rather than numbered globally.  But SQLite scopes the names correctly, so it
    // works.  In the future, we might number the computed tables globally to make this more clear.
    assert_eq!(allegrosql, "SELECT DISTINCT `c00`.`?x` AS `?x` FROM (\
                         SELECT $v0 AS `?x` UNION \
                         SELECT $v1 AS `?x`) AS `c00`");
    assert_eq!(args, vec![make_arg("$v0", "yyy"),
                          make_arg("$v1", "zzz"),]);

    // Verify that we can use ground to constrain the bindings produced by earlier clauses.
    let causetq = r#"[:find ?x . :where [_ :foo/bar ?x] [(ground "yyy") ?x]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT $v0 AS `?x` FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");

    assert_eq!(args, vec![make_arg("$v0", "yyy")]);

    // Verify that we can further constrain the bindings produced by our clause.
    let causetq = r#"[:find ?x . :where [(ground "yyy") ?x] [_ :foo/bar ?x]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT $v0 AS `?x` FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");

    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_unbound_attribute_with_ground_instanton() {
    let causetq = r#"[:find ?x ?v :where [?x _ ?v] (not [(ground 17) ?x])]"#;
    let schema = prepopulated_schema();
    let SQLCausetQ { allegrosql, .. } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, \
                                     `all_datoms00`.v AS `?v`, \
                                     `all_datoms00`.value_type_tag AS `?v_value_type_tag` \
                     FROM `all_datoms` AS `all_datoms00` \
                     WHERE NOT EXISTS (SELECT 1 WHERE `all_datoms00`.e = 17)");
}

#[test]
fn test_unbound_attribute_with_ground() {
    let causetq = r#"[:find ?x ?v :where [?x _ ?v] (not [(ground 17) ?v])]"#;
    let schema = prepopulated_schema();
    let SQLCausetQ { allegrosql, .. } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, \
                                     `all_datoms00`.v AS `?v`, \
                                     `all_datoms00`.value_type_tag AS `?v_value_type_tag` \
                     FROM `all_datoms` AS `all_datoms00` \
                     WHERE NOT EXISTS (SELECT 1 WHERE `all_datoms00`.v = 17 AND \
                                                     (`all_datoms00`.value_type_tag = 5))");
}


#[test]
fn test_not_with_ground() {
    let mut schema = prepopulated_schema();
    associate_causetId(&mut schema, Keyword::namespaced("edb", "valueType"), 7);
    associate_causetId(&mut schema, Keyword::namespaced("edb.type", "ref"), 23);
    associate_causetId(&mut schema, Keyword::namespaced("edb.type", "bool"), 28);
    associate_causetId(&mut schema, Keyword::namespaced("edb.type", "instant"), 29);
    add_attribute(&mut schema, 7, Attribute {
        value_type: ValueType::Ref,
        multival: false,
        ..Default::default()
    });

    // Scalar.
    // TODO: this kind of simple `not` should be implemented without the subcausetq. #476.
    let causetq = r#"[:find ?x :where [?x :edb/valueType ?v] (not [(ground :edb.type/instant) ?v])]"#;
    let SQLCausetQ { allegrosql, .. } = translate(&schema, causetq);
    assert_eq!(allegrosql,
               "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` WHERE `datoms00`.a = 7 AND NOT \
                EXISTS (SELECT 1 WHERE `datoms00`.v = 29)");

    // Coll.
    // TODO: we can generate better SQL for this, too. #476.
    let causetq = r#"[:find ?x :where [?x :edb/valueType ?v] (not [(ground [:edb.type/bool :edb.type/instant]) [?v ...]])]"#;
    let SQLCausetQ { allegrosql, .. } = translate(&schema, causetq);
    assert_eq!(allegrosql,
               "SELECT DISTINCT `datoms00`.e AS `?x` FROM `causets` AS `datoms00` \
                WHERE `datoms00`.a = 7 AND NOT EXISTS \
                (SELECT 1 FROM (SELECT 0 AS `?v` WHERE 0 UNION ALL VALUES (28), (29)) AS `c00` \
                 WHERE `datoms00`.v = `c00`.`?v`)");
}

#[test]
fn test_fulltext() {
    let schema = prepopulated_typed_schema(ValueType::Double);

    let causetq = r#"[:find ?instanton ?value ?causetx ?sembedded :where [(fulltext $ :foo/fts "needle") [[?instanton ?value ?causetx ?sembedded]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms01`.e AS `?instanton`, \
                                     `fulltext_values00`.text AS `?value`, \
                                     `datoms01`.causetx AS `?causetx`, \
                                     0e0 AS `?sembedded` \
                     FROM `fulltext_values` AS `fulltext_values00`, \
                          `causets` AS `datoms01` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let causetq = r#"[:find ?instanton ?value ?causetx :where [(fulltext $ :foo/fts "needle") [[?instanton ?value ?causetx ?sembedded]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    // Observe that the computed table isn't dropped, even though `?sembedded` isn't bound in the final conjoining clause.
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms01`.e AS `?instanton`, \
                                     `fulltext_values00`.text AS `?value`, \
                                     `datoms01`.causetx AS `?causetx` \
                     FROM `fulltext_values` AS `fulltext_values00`, \
                          `causets` AS `datoms01` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let causetq = r#"[:find ?instanton ?value ?causetx :where [(fulltext $ :foo/fts "needle") [[?instanton ?value ?causetx _]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    // Observe that the computed table isn't included at all when `?sembedded` isn't bound.
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms01`.e AS `?instanton`, \
                                     `fulltext_values00`.text AS `?value`, \
                                     `datoms01`.causetx AS `?causetx` \
                     FROM `fulltext_values` AS `fulltext_values00`, \
                          `causets` AS `datoms01` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let causetq = r#"[:find ?instanton ?value ?causetx :where [(fulltext $ :foo/fts "needle") [[?instanton ?value ?causetx ?sembedded]]] [?instanton :foo/bar ?sembedded]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms01`.e AS `?instanton`, \
                                     `fulltext_values00`.text AS `?value`, \
                                     `datoms01`.causetx AS `?causetx` \
                     FROM `fulltext_values` AS `fulltext_values00`, \
                          `causets` AS `datoms01`, \
                          `causets` AS `datoms02` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0 \
                       AND `datoms02`.a = 99 \
                       AND `datoms02`.v = 0e0 \
                       AND `datoms01`.e = `datoms02`.e");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let causetq = r#"[:find ?instanton ?value ?causetx :where [?instanton :foo/bar ?sembedded] [(fulltext $ :foo/fts "needle") [[?instanton ?value ?causetx ?sembedded]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?instanton`, \
                                     `fulltext_values01`.text AS `?value`, \
                                     `datoms02`.causetx AS `?causetx` \
                     FROM `causets` AS `datoms00`, \
                          `fulltext_values` AS `fulltext_values01`, \
                          `causets` AS `datoms02` \
                     WHERE `datoms00`.a = 99 \
                       AND `datoms02`.a = 100 \
                       AND `datoms02`.v = `fulltext_values01`.rowid \
                       AND `fulltext_values01`.text MATCH $v0 \
                       AND `datoms00`.v = 0e0 \
                       AND `datoms00`.e = `datoms02`.e");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);
}

#[test]
fn test_fulltext_inputs() {
    let schema = prepopulated_typed_schema(ValueType::String);

    // Bind ?instanton. We expect the output to collide.
    let causetq = r#"[:find ?val
                    :in ?instanton
                    :where [(fulltext $ :foo/fts "hello") [[?instanton ?val _ _]]]]"#;
    let mut types = BTreeMap::default();
    types.insert(Variable::from_valid_name("?instanton"), ValueType::Ref);
    let inputs = CausetQInputs::new(types, BTreeMap::default()).expect("valid inputs");

    // Without binding the value. q_once will err if you try this!
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    assert_eq!(allegrosql, "SELECT DISTINCT `fulltext_values00`.text AS `?val` \
                     FROM \
                     `fulltext_values` AS `fulltext_values00`, \
                     `causets` AS `datoms01` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0");
    assert_eq!(args, vec![make_arg("$v0", "hello"),]);

    // With the value bound.
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?instanton"), TypedValue::Ref(111))]);
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    assert_eq!(allegrosql, "SELECT DISTINCT `fulltext_values00`.text AS `?val` \
                     FROM \
                     `fulltext_values` AS `fulltext_values00`, \
                     `causets` AS `datoms01` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0 \
                       AND `datoms01`.e = 111");
    assert_eq!(args, vec![make_arg("$v0", "hello"),]);

    // Same again, but retrieving the instanton.
    let causetq = r#"[:find ?instanton .
                    :in ?instanton
                    :where [(fulltext $ :foo/fts "hello") [[?instanton _ _]]]]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?instanton"), TypedValue::Ref(111))]);
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    assert_eq!(allegrosql, "SELECT 111 AS `?instanton` FROM \
                     `fulltext_values` AS `fulltext_values00`, \
                     `causets` AS `datoms01` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0 \
                       AND `datoms01`.e = 111 \
                     LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "hello"),]);

    // A larger pattern.
    let causetq = r#"[:find ?instanton ?value ?friend
                    :in ?instanton
                    :where
                    [(fulltext $ :foo/fts "hello") [[?instanton ?value]]]
                    [?instanton :foo/bar ?friend]]"#;
    let inputs = CausetQInputs::with_value_sequence(vec![(Variable::from_valid_name("?instanton"), TypedValue::Ref(121))]);
    let SQLCausetQ { allegrosql, args } = translate_with_inputs(&schema, causetq, inputs);
    assert_eq!(allegrosql, "SELECT DISTINCT 121 AS `?instanton`, \
                                     `fulltext_values00`.text AS `?value`, \
                                     `datoms02`.v AS `?friend` \
                     FROM \
                     `fulltext_values` AS `fulltext_values00`, \
                     `causets` AS `datoms01`, \
                     `causets` AS `datoms02` \
                     WHERE `datoms01`.a = 100 \
                       AND `datoms01`.v = `fulltext_values00`.rowid \
                       AND `fulltext_values00`.text MATCH $v0 \
                       AND `datoms01`.e = 121 \
                       AND `datoms02`.e = 121 \
                       AND `datoms02`.a = 99");
    assert_eq!(args, vec![make_arg("$v0", "hello"),]);
}

#[test]
fn test_instant_range() {
    let schema = prepopulated_typed_schema(ValueType::Instant);
    let causetq = r#"[:find ?e
                    :where
                    [?e :foo/bar ?t]
                    [(> ?t #inst "2017-06-16T00:56:41.257Z")]]"#;

    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `datoms00`.e AS `?e` \
                     FROM \
                     `causets` AS `datoms00` \
                     WHERE `datoms00`.a = 99 \
                       AND `datoms00`.v > 1497574601257000");
    assert_eq!(args, vec![]);
}

#[test]
fn test_project_aggregates() {
    let schema = prepopulated_typed_schema(ValueType::Long);
    let causetq = r#"[:find ?e (max ?t)
                    :where
                    [?e :foo/bar ?t]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // No outer DISTINCT: we aggregate or group by every variable.
    assert_eq!(allegrosql, "SELECT * \
                     FROM \
                     (SELECT `?e` AS `?e`, max(`?t`) AS `(max ?t)` \
                      FROM \
                      (SELECT DISTINCT \
                       `datoms00`.e AS `?e`, \
                       `datoms00`.v AS `?t` \
                       FROM `causets` AS `datoms00` \
                       WHERE `datoms00`.a = 99) \
                      GROUP BY `?e`) \
                     WHERE `(max ?t)` IS NOT NULL");
    assert_eq!(args, vec![]);

    let causetq = r#"[:find (max ?t)
                    :with ?e
                    :where
                    [?e :foo/bar ?t]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT * \
                     FROM \
                     (SELECT max(`?t`) AS `(max ?t)` \
                      FROM \
                      (SELECT DISTINCT \
                       `datoms00`.v AS `?t`, \
                       `datoms00`.e AS `?e` \
                       FROM `causets` AS `datoms00` \
                       WHERE `datoms00`.a = 99)\
                      ) \
                     WHERE `(max ?t)` IS NOT NULL");
    assert_eq!(args, vec![]);

    // ORDER BY lifted to outer causetq if there is no LIMIT.
    let causetq = r#"[:find (max ?x)
                    :with ?e
                    :where
                    [?e ?a ?t]
                    [?t :foo/bar ?x]
                    :order ?a]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT * \
	                   FROM \
                     (SELECT max(`?x`) AS `(max ?x)`, `?a` AS `?a` \
                      FROM \
                      (SELECT DISTINCT \
                       `datoms01`.v AS `?x`, \
                       `datoms00`.a AS `?a`, \
                       `datoms00`.e AS `?e` \
                       FROM `causets` AS `datoms00`, `causets` AS `datoms01` \
                       WHERE `datoms01`.a = 99 AND `datoms00`.v = `datoms01`.e) \
                      GROUP BY `?a`) \
                     WHERE `(max ?x)` IS NOT NULL \
                     ORDER BY `?a` ASC");
    assert_eq!(args, vec![]);

    // ORDER BY duplicated in outer causetq if there is a LIMIT.
    let causetq = r#"[:find (max ?x)
                    :with ?e
                    :where
                    [?e ?a ?t]
                    [?t :foo/bar ?x]
                    :order (desc ?a)
                    :limit 10]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT * \
	                   FROM \
                     (SELECT max(`?x`) AS `(max ?x)`, `?a` AS `?a` \
                      FROM \
                      (SELECT DISTINCT \
                       `datoms01`.v AS `?x`, \
                       `datoms00`.a AS `?a`, \
                       `datoms00`.e AS `?e` \
                       FROM `causets` AS `datoms00`, `causets` AS `datoms01` \
                       WHERE `datoms01`.a = 99 AND `datoms00`.v = `datoms01`.e) \
                      GROUP BY `?a` \
                      ORDER BY `?a` DESC \
                      LIMIT 10) \
                     WHERE `(max ?x)` IS NOT NULL \
                     ORDER BY `?a` DESC");
    assert_eq!(args, vec![]);

    // No outer SELECT * for non-nullable aggregates.
    let causetq = r#"[:find (count ?t)
                    :with ?e
                    :where
                    [?e :foo/bar ?t]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT count(`?t`) AS `(count ?t)` \
                     FROM \
                     (SELECT DISTINCT \
                      `datoms00`.v AS `?t`, \
                      `datoms00`.e AS `?e` \
                      FROM `causets` AS `datoms00` \
                      WHERE `datoms00`.a = 99)");
    assert_eq!(args, vec![]);
}

#[test]
fn test_project_the() {
    let schema = prepopulated_typed_schema(ValueType::Long);
    let causetq = r#"[:find (the ?e) (max ?t)
                    :where
                    [?e :foo/bar ?t]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);

    // We shouldn't NULL-check (the).
    assert_eq!(allegrosql, "SELECT * \
                     FROM \
                     (SELECT `?e` AS `?e`, max(`?t`) AS `(max ?t)` \
                      FROM \
                      (SELECT DISTINCT \
                       `datoms00`.e AS `?e`, \
                       `datoms00`.v AS `?t` \
                       FROM `causets` AS `datoms00` \
                       WHERE `datoms00`.a = 99)) \
                     WHERE `(max ?t)` IS NOT NULL");
    assert_eq!(args, vec![]);
}

#[test]
fn test_causecausetx_before_and_after() {
    let schema = prepopulated_typed_schema(ValueType::Long);
    let causetq = r#"[:find ?x :where [?x _ _ ?causetx] [(causetx-after ?causetx 12345)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT \
                     `datoms00`.e AS `?x` \
                     FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.causetx > 12345");
    assert_eq!(args, vec![]);
    let causetq = r#"[:find ?x :where [?x _ _ ?causetx] [(causetx-before ?causetx 12345)]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT \
                     `datoms00`.e AS `?x` \
                     FROM `causets` AS `datoms00` \
                     WHERE `datoms00`.causetx < 12345");
    assert_eq!(args, vec![]);
}


#[test]
fn test_causecausetx_ids() {
    let mut schema = prepopulated_typed_schema(ValueType::Double);
    associate_causetId(&mut schema, Keyword::namespaced("edb", "causecausetxInstant"), 101);
    add_attribute(&mut schema, 101, Attribute {
        value_type: ValueType::Instant,
        multival: false,
        index: true,
        ..Default::default()
    });

    let causetq = r#"[:find ?causetx :where [(causetx-ids $ 1000 2000) [[?causetx]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `transactions00`.causetx AS `?causetx` \
                     FROM `transactions` AS `transactions00` \
                     WHERE 1000 <= `transactions00`.causetx \
                     AND `transactions00`.causetx < 2000");
    assert_eq!(args, vec![]);

    // This is rather artificial but verifies that binding the arguments to (causetx-ids) works.
    let causetq = r#"[:find ?causetx :where [?first :edb/causecausetxInstant #inst "2016-01-01T11:00:00.000Z"] [?last :edb/causecausetxInstant #inst "2017-01-01T11:00:00.000Z"] [(causetx-ids $ ?first ?last) [?causetx ...]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `transactions02`.causetx AS `?causetx` \
                     FROM `causets` AS `datoms00`, \
                     `causets` AS `datoms01`, \
                     `transactions` AS `transactions02` \
                     WHERE `datoms00`.a = 101 \
                     AND `datoms00`.v = 1451646000000000 \
                     AND `datoms01`.a = 101 \
                     AND `datoms01`.v = 1483268400000000 \
                     AND `datoms00`.e <= `transactions02`.causetx \
                     AND `transactions02`.causetx < `datoms01`.e");
    assert_eq!(args, vec![]);

    // In practice the following causetq would be inefficient because of the filter on all_datoms.causetx,
    // but that is what (causetx-data) is for.
    let causetq = r#"[:find ?e ?a ?v ?causetx :where [(causetx-ids $ 1000 2000) [[?causetx]]] [?e ?a ?v ?causetx]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `all_datoms01`.e AS `?e`, \
	                   `all_datoms01`.a AS `?a`, \
                     `all_datoms01`.v AS `?v`, \
                     `all_datoms01`.value_type_tag AS `?v_value_type_tag`, \
                     `transactions00`.causetx AS `?causetx` \
                     FROM `transactions` AS `transactions00`, \
                     `all_datoms` AS `all_datoms01` \
                     WHERE 1000 <= `transactions00`.causetx \
                     AND `transactions00`.causetx < 2000 \
                     AND `transactions00`.causetx = `all_datoms01`.causetx");
    assert_eq!(args, vec![]);
}

#[test]
fn test_causecausetx_data() {
    let schema = prepopulated_typed_schema(ValueType::Double);

    let causetq = r#"[:find ?e ?a ?v ?causetx ?added :where [(causetx-data $ 1000) [[?e ?a ?v ?causetx ?added]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `transactions00`.e AS `?e`, \
                     `transactions00`.a AS `?a`, \
                     `transactions00`.v AS `?v`, \
                     `transactions00`.value_type_tag AS `?v_value_type_tag`, \
                     `transactions00`.causetx AS `?causetx`, \
                     `transactions00`.added AS `?added` \
                     FROM `transactions` AS `transactions00` \
                     WHERE `transactions00`.causetx = 1000");
    assert_eq!(args, vec![]);

    // Ensure that we don't project columns that we don't need, even if they are bound to named
    // variables or to placeholders.
    let causetq = r#"[:find [?a ?v ?added] :where [(causetx-data $ 1000) [[?e ?a ?v _ ?added]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT `transactions00`.a AS `?a`, \
                     `transactions00`.v AS `?v`, \
                     `transactions00`.value_type_tag AS `?v_value_type_tag`, \
                     `transactions00`.added AS `?added` \
                     FROM `transactions` AS `transactions00` \
                     WHERE `transactions00`.causetx = 1000 \
                     LIMIT 1");
    assert_eq!(args, vec![]);

    // This is awkward since the transactions table is queried twice, once to list transaction IDs
    // and a second time to extract data.  https://github.com/whtcorpsinc/einsteindb/issues/644 tracks
    // improving this, perhaps by optimizing certain combinations of functions and bindings.
    let causetq = r#"[:find ?e ?a ?v ?causetx ?added :where [(causetx-ids $ 1000 2000) [[?causetx]]] [(causetx-data $ ?causetx) [[?e ?a ?v _ ?added]]]]"#;
    let SQLCausetQ { allegrosql, args } = translate(&schema, causetq);
    assert_eq!(allegrosql, "SELECT DISTINCT `transactions01`.e AS `?e`, \
                     `transactions01`.a AS `?a`, \
                     `transactions01`.v AS `?v`, \
                     `transactions01`.value_type_tag AS `?v_value_type_tag`, \
                     `transactions00`.causetx AS `?causetx`, \
                     `transactions01`.added AS `?added` \
                     FROM `transactions` AS `transactions00`, \
                     `transactions` AS `transactions01` \
                     WHERE 1000 <= `transactions00`.causetx \
                     AND `transactions00`.causetx < 2000 \
                     AND `transactions01`.causetx = `transactions00`.causetx");
    assert_eq!(args, vec![]);
}
