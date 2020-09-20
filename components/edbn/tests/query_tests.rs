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

use edbn::{
    Keyword,
    PlainSymbol,
};

use edbn::causetq::{
    Direction,
    Element,
    FindSpec,
    FnArg,
    Limit,
    NonIntegerConstant,
    Order,
    OrJoin,
    OrWhereClause,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    Predicate,
    UnifyVars,
    Variable,
    WhereClause,
};

use edbn::parse::{
    parse_causetq,
};

///! N.B., parsing a causetq can be done without reference to a EDB.
///! Processing the parsed causetq into something we can work with
///! for planning involves interrogating the schema and causetIds in
///! the store.
///! See <https://github.com/whtcorpsinc/einsteindb/wiki/CausetQing> for more.
#[test]
fn can_parse_predicates() {
    let s = "[:find [?x ...] :where [?x _ ?y] [(< ?y 10)]]";
    let p = parse_causetq(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindColl(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::Pattern(Pattern {
                       source: None,
                       instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                       attribute: PatternNonValuePlace::Placeholder,
                       value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                       causetx: PatternNonValuePlace::Placeholder,
                   }),
                   WhereClause::Pred(Predicate { operator: PlainSymbol::plain("<"), args: vec![
                       FnArg::Variable(Variable::from_valid_name("?y")), FnArg::SolitonIdOrInteger(10),
                   ]}),
               ]);
}

#[test]
fn can_parse_simple_or() {
    let s = "[:find ?x . :where (or [?x _ 10] [?x _ 15])]";
    let p = parse_causetq(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Implicit,
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::SolitonIdOrInteger(10),
                                   causetx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::SolitonIdOrInteger(15),
                                   causetx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_unit_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 15])]";
    let p = parse_causetq(s).expect("to be able to parse find");

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Explicit(std::iter::once(Variable::from_valid_name("?x")).collect()),
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::SolitonIdOrInteger(15),
                                   causetx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_simple_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 10] [?x _ -15])]";
    let p = parse_causetq(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Explicit(std::iter::once(Variable::from_valid_name("?x")).collect()),
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::SolitonIdOrInteger(10),
                                   causetx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::SolitonIdOrInteger(-15),
                                   causetx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[cfg(test)]
fn causetid(ns: &str, name: &str) -> PatternNonValuePlace {
    Keyword::namespaced(ns, name).into()
}

#[test]
fn can_parse_simple_or_and_join() {
    let s = "[:find ?x . :where (or [?x _ 10] (and (or [?x :foo/bar ?y] [?x :foo/baz ?y]) [(< ?y 1)]))]";
    let p = parse_causetq(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Implicit,
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::SolitonIdOrInteger(10),
                                   causetx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::And(
                               vec![
                                   WhereClause::OrJoin(OrJoin::new(
                                       UnifyVars::Implicit,
                                       vec![
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: causetid("foo", "bar"),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               causetx: PatternNonValuePlace::Placeholder,
                                           })),
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               instanton: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: causetid("foo", "baz"),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               causetx: PatternNonValuePlace::Placeholder,
                                           })),
                                       ],
                                   )),

                                   WhereClause::Pred(Predicate { operator: PlainSymbol::plain("<"), args: vec![
                                       FnArg::Variable(Variable::from_valid_name("?y")), FnArg::SolitonIdOrInteger(1),
                                   ]}),
                               ],
                           )
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_order_by() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :order]";
    assert!(parse_causetq(invalid).is_err());

    // Defaults to ascending.
    let default = "[:find ?x :where [?x :foo/baz ?y] :order ?y]";
    assert_eq!(parse_causetq(default).unwrap().order,
               Some(vec![Order(Direction::Ascending, Variable::from_valid_name("?y"))]));

    let ascending = "[:find ?x :where [?x :foo/baz ?y] :order (asc ?y)]";
    assert_eq!(parse_causetq(ascending).unwrap().order,
               Some(vec![Order(Direction::Ascending, Variable::from_valid_name("?y"))]));

    let descending = "[:find ?x :where [?x :foo/baz ?y] :order (desc ?y)]";
    assert_eq!(parse_causetq(descending).unwrap().order,
               Some(vec![Order(Direction::Descending, Variable::from_valid_name("?y"))]));

    let mixed = "[:find ?x :where [?x :foo/baz ?y] :order (desc ?y) (asc ?x)]";
    assert_eq!(parse_causetq(mixed).unwrap().order,
               Some(vec![Order(Direction::Descending, Variable::from_valid_name("?y")),
                         Order(Direction::Ascending, Variable::from_valid_name("?x"))]));
}

#[test]
fn can_parse_limit() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :limit]";
    assert!(parse_causetq(invalid).is_err());

    let zero_invalid = "[:find ?x :where [?x :foo/baz ?y] :limit 00]";
    assert!(parse_causetq(zero_invalid).is_err());

    let none = "[:find ?x :where [?x :foo/baz ?y]]";
    assert_eq!(parse_causetq(none).unwrap().limit,
               Limit::None);

    let one = "[:find ?x :where [?x :foo/baz ?y] :limit 1]";
    assert_eq!(parse_causetq(one).unwrap().limit,
               Limit::Fixed(1));

    let onethousand = "[:find ?x :where [?x :foo/baz ?y] :limit 1000]";
    assert_eq!(parse_causetq(onethousand).unwrap().limit,
               Limit::Fixed(1000));

    let variable_with_in = "[:find ?x :in ?limit :where [?x :foo/baz ?y] :limit ?limit]";
    assert_eq!(parse_causetq(variable_with_in).unwrap().limit,
               Limit::Variable(Variable::from_valid_name("?limit")));

    let variable_with_in_used = "[:find ?x :in ?limit :where [?x :foo/baz ?limit] :limit ?limit]";
    assert_eq!(parse_causetq(variable_with_in_used).unwrap().limit,
               Limit::Variable(Variable::from_valid_name("?limit")));
}

#[test]
fn can_parse_uuid() {
    let expected = edbn::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").expect("valid uuid");
    let s = "[:find ?x :where [?x :foo/baz #uuid \"4cb3f828-752d-497a-90c9-b1fd516d5644\"]]";
    assert_eq!(parse_causetq(s).expect("parsed").where_clauses.pop().expect("a where clause"),
               WhereClause::Pattern(
                   Pattern::new(None,
                                PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                Keyword::namespaced("foo", "baz").into(),
                                PatternValuePlace::Constant(NonIntegerConstant::Uuid(expected)),
                                PatternNonValuePlace::Placeholder)
                       .expect("valid pattern")));
}

#[test]
fn can_parse_exotic_whitespace() {
    let expected = edbn::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").expect("valid uuid");
    // The causetq string from `can_parse_uuid`, with newlines, commas, and line comments interspersed.
    let s = r#"[:find
?x ,, :where,   ;atest
[?x :foo/baz #uuid
   "4cb3f828-752d-497a-90c9-b1fd516d5644", ;testa
,],,  ,],;"#;
    assert_eq!(parse_causetq(s).expect("parsed").where_clauses.pop().expect("a where clause"),
               WhereClause::Pattern(
                   Pattern::new(None,
                                PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                Keyword::namespaced("foo", "baz").into(),
                                PatternValuePlace::Constant(NonIntegerConstant::Uuid(expected)),
                                PatternNonValuePlace::Placeholder)
                       .expect("valid pattern")));
}
