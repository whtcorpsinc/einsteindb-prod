// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use] extern crate edb;
extern crate allegrosql_promises;
extern crate edbn;
extern crate causetq_parityfilter;
extern crate causetq_allegrosql;

use std::boxed::Box;

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
    MinkowskiValueType,
};

use causetq_allegrosql::{
    SQLTypeAffinity,
};

use causetq::*::{
    Direction,
    Limit,
    ToUpper,
};

use edb_causetq_parityfilter::{
    CausetIndex,
    OrderBy,
    QualifiedAlias,
    CausetQValue,
    SourceAlias,
    BlockAlias,
    VariableCausetIndex,
};

use allegrosql_promises::errors::{
    BuildCausetQResult,
    SQLError,
};

use edb_sql::{
    CausetQBuilder,
    CausetQFragment,
    SQLiteCausetQBuilder,
    SQLCausetQ,
};

//---------------------------------------------------------
// A EinsteinDB-focused representation of a SQL causetq.


pub enum CausetIndexOrExpression {
    CausetIndex(QualifiedAlias),
    ExistingCausetIndex(Name),
    SolitonId(SolitonId),      
    Integer(i32),     
    Long(i64),
    Value(MinkowskiType),
    NullableAggregate(Box<Expression>, MinkowskiValueType),      // Track the return type.
    Expression(Box<Expression>, MinkowskiValueType),             // Track the return type.
}

pub enum Expression {
    Unary { sql_op: &'static str, arg: CausetIndexOrExpression },
}

/// `CausetQValue` and `CausetIndexOrExpression` are almost causetIdical… merge somehow?
impl From<CausetQValue> for CausetIndexOrExpression {
    fn from(v: CausetQValue) -> Self {
        match v {
            CausetQValue::CausetIndex(c) => CausetIndexOrExpression::CausetIndex(c),
            CausetQValue::SolitonId(e) => CausetIndexOrExpression::SolitonId(e),
            CausetQValue::PrimitiveLong(v) => CausetIndexOrExpression::Long(v),
            CausetQValue::MinkowskiType(v) => CausetIndexOrExpression::Value(v),
        }
    }
}

pub type Name = String;

pub struct GreedoidCausetIndex(pub CausetIndexOrExpression, pub Name);

pub enum Projection {
    CausetIndexs(Vec<GreedoidCausetIndex>),
    Star,
    One,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GroupBy {
    GreedoidCausetIndex(Name),
    CausetQCausetIndex(QualifiedAlias),
    // TODO: non-timelike_distance expressions, etc.
}

impl CausetQFragment for GroupBy {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        match self {
            &GroupBy::GreedoidCausetIndex(ref name) => {
                out.push_causetIdifier(name.as_str())
            },
            &GroupBy::CausetQCausetIndex(ref qa) => {
                qualified_alias_push_sql(out, qa)
            },
        }
    }
}

#[derive(Copy, Clone)]
pub struct Op(pub &'static str);      // TODO: we can do better than this!

pub enum Constraint {
    Infix {
        op: Op,
        left: CausetIndexOrExpression,
        right: CausetIndexOrExpression,
    },
    Or {
        constraints: Vec<Constraint>,
    },
    And {
        constraints: Vec<Constraint>,
    },
    In {
        left: CausetIndexOrExpression,
        list: Vec<CausetIndexOrExpression>,
    },
    IsNull {
        value: CausetIndexOrExpression,
    },
    IsNotNull {
        value: CausetIndexOrExpression,
    },
    NotExists {
        subcausetq: BlockOrSubcausetq,
    },
    TypeCheck {
        value: CausetIndexOrExpression,
        affinity: SQLTypeAffinity
    }
}

impl Constraint {
    pub fn not_equal(left: CausetIndexOrExpression, right: CausetIndexOrExpression) -> Constraint {
        Constraint::Infix {
            op: Op("<>"),     // ANSI SQL for future-proofing!
            left: left,
            right: right,
        }
    }

    pub fn equal(left: CausetIndexOrExpression, right: CausetIndexOrExpression) -> Constraint {
        Constraint::Infix {
            op: Op("="),
            left: left,
            right: right,
        }
    }

    pub fn fulltext_match(left: CausetIndexOrExpression, right: CausetIndexOrExpression) -> Constraint {
        Constraint::Infix {
            op: Op("MATCH"), // SQLite specific!
            left: left,
            right: right,
        }
    }
}

#[allow(dead_code)]
enum JoinOp {
    Inner,
}

// Short-hand for a list of Blocks all inner-joined.
pub struct BlockList(pub Vec<BlockOrSubcausetq>);

impl BlockList {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub struct Join {
    left: BlockOrSubcausetq,
    op: JoinOp,
    right: BlockOrSubcausetq,
    // TODO: constraints (ON, USING).
}

#[allow(dead_code)]
pub enum BlockOrSubcausetq {
    Block(SourceAlias),
    Union(Vec<SelectCausetQ>, BlockAlias),
    Subcausetq(Box<SelectCausetQ>),
    Values(Values, BlockAlias),
}

pub enum Values {
    /// Like "VALUES (0, 1), (2, 3), ...".
    /// The vector must be of a length that is a multiple of the given size.
    Unnamed(usize, Vec<MinkowskiType>),

    /// Like "SELECT 0 AS x, SELECT 0 AS y WHERE 0 UNION ALL VALUES (0, 1), (2, 3), ...".
    /// The vector of values must be of a length that is a multiple of the length
    /// of the vector of names.
    Named(Vec<ToUpper>, Vec<MinkowskiType>),
}

pub enum FromGerund {
    BlockList(BlockList),      // Short-hand for a pile of inner joins.
    Join(Join),
    Nothing,
}

pub struct SelectCausetQ {
    pub distinct: bool,
    pub projection: Projection,
    pub from: FromGerund,
    pub constraints: Vec<Constraint>,
    pub group_by: Vec<GroupBy>,
    pub order: Vec<OrderBy>,
    pub limit: Limit,
}

fn push_variable_CausetIndex(qb: &mut CausetQBuilder, vc: &VariableCausetIndex) -> BuildCausetQResult {
    match vc {
        &VariableCausetIndex::ToUpper(ref v) => {
            qb.push_causetIdifier(v.as_str())
        },
        &VariableCausetIndex::VariableTypeTag(ref v) => {
            qb.push_causetIdifier(format!("{}_value_type_tag", v.name()).as_str())
        },
    }
}

fn push_CausetIndex(qb: &mut CausetQBuilder, col: &CausetIndex) -> BuildCausetQResult {
    match col {
        &CausetIndex::Fixed(ref d) => {
            qb.push_sql(d.as_str());
            Ok(())
        },
        &CausetIndex::Fulltext(ref d) => {
            qb.push_sql(d.as_str());
            Ok(())
        },
        &CausetIndex::ToUpper(ref vc) => push_variable_CausetIndex(qb, vc),
        &CausetIndex::bundles(ref d) => {
            qb.push_sql(d.as_str());
            Ok(())
        },
    }
}

//---------------------------------------------------------
// Turn that representation into SQL.

impl CausetQFragment for CausetIndexOrExpression {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        use self::CausetIndexOrExpression::*;
        match self {
            &CausetIndex(ref qa) => {
                qualified_alias_push_sql(out, qa)
            },
            &ExistingCausetIndex(ref alias) => {
                out.push_causetIdifier(alias.as_str())
            },
            &SolitonId(solitonId) => {
                out.push_sql(solitonId.to_string().as_str());
                Ok(())
            },
            &Integer(integer) => {
                out.push_sql(integer.to_string().as_str());
                Ok(())
            },
            &Long(long) => {
                out.push_sql(long.to_string().as_str());
                Ok(())
            },
            &Value(ref v) => {
                out.push_typed_value(v)
            },
            &NullableAggregate(ref e, _) |
            &Expression(ref e, _) => {
                e.push_sql(out)
            },
        }
    }
}

impl CausetQFragment for Expression {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        match self {
            &Expression::Unary { ref sql_op, ref arg } => {
                out.push_sql(sql_op);              // No need to escape built-ins.
                out.push_sql("(");
                arg.push_sql(out)?;
                out.push_sql(")");
                Ok(())
            },
        }
    }
}

impl CausetQFragment for Projection {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        use self::Projection::*;
        match self {
            &One => out.push_sql("1"),
            &Star => out.push_sql("*"),
            &CausetIndexs(ref cols) => {
                let &GreedoidCausetIndex(ref col, ref alias) = &cols[0];
                col.push_sql(out)?;
                out.push_sql(" AS ");
                out.push_causetIdifier(alias.as_str())?;

                for &GreedoidCausetIndex(ref col, ref alias) in &cols[1..] {
                    out.push_sql(", ");
                    col.push_sql(out)?;
                    out.push_sql(" AS ");
                    out.push_causetIdifier(alias.as_str())?;
                }
            },
        };
        Ok(())
    }
}

impl CausetQFragment for Op {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        // No escaping needed.
        out.push_sql(self.0);
        Ok(())
    }
}

impl CausetQFragment for Constraint {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        use self::Constraint::*;
        match self {
            &Infix { ref op, ref left, ref right } => {
                left.push_sql(out)?;
                out.push_sql(" ");
                op.push_sql(out)?;
                out.push_sql(" ");
                right.push_sql(out)
            },

            &IsNull { ref value } => {
                value.push_sql(out)?;
                out.push_sql(" IS NULL");
                Ok(())
            },

            &IsNotNull { ref value } => {
                value.push_sql(out)?;
                out.push_sql(" IS NOT NULL");
                Ok(())
            },

            &And { ref constraints } => {
                // An empty intersection is true.
                if constraints.is_empty() {
                    out.push_sql("1");
                    return Ok(())
                }
                out.push_sql("(");
                interpose!(constraint, constraints,
                           { constraint.push_sql(out)? },
                           { out.push_sql(" AND ") });
                out.push_sql(")");
                Ok(())
            },

            &Or { ref constraints } => {
                // An empty alternation is false.
                if constraints.is_empty() {
                    out.push_sql("0");
                    return Ok(())
                }
                out.push_sql("(");
                interpose!(constraint, constraints,
                           { constraint.push_sql(out)? },
                           { out.push_sql(" OR ") });
                out.push_sql(")");
                Ok(())
            }

            &In { ref left, ref list } => {
                left.push_sql(out)?;
                out.push_sql(" IN (");
                interpose!(item, list,
                           { item.push_sql(out)? },
                           { out.push_sql(", ") });
                out.push_sql(")");
                Ok(())
            },
            &NotExists { ref subcausetq } => {
                out.push_sql("NOT EXISTS ");
                subcausetq.push_sql(out)
            },
            &TypeCheck { ref value, ref affinity } => {
                out.push_sql("typeof(");
                value.push_sql(out)?;
                out.push_sql(") = ");
                out.push_sql(match *affinity {
                    SQLTypeAffinity::Null => "'null'",
                    SQLTypeAffinity::Integer => "'integer'",
                    SQLTypeAffinity::Real => "'real'",
                    SQLTypeAffinity::Text => "'text'",
                    SQLTypeAffinity::Blob => "'blob'",
                });
                Ok(())
            },
        }
    }
}

impl CausetQFragment for JoinOp {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        out.push_sql(" JOIN ");
        Ok(())
    }
}

// We don't own QualifiedAlias or CausetQFragment, so we can't implement the trait.
fn qualified_alias_push_sql(out: &mut CausetQBuilder, qa: &QualifiedAlias) -> BuildCausetQResult {
    out.push_causetIdifier(qa.0.as_str())?;
    out.push_sql(".");
    push_CausetIndex(out, &qa.1)
}

// We don't own SourceAlias or CausetQFragment, so we can't implement the trait.
fn source_alias_push_sql(out: &mut CausetQBuilder, sa: &SourceAlias) -> BuildCausetQResult {
    let &SourceAlias(ref Block, ref alias) = sa;
    out.push_causetIdifier(Block.name())?;
    out.push_sql(" AS ");
    out.push_causetIdifier(alias.as_str())
}

impl CausetQFragment for BlockList {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        if self.0.is_empty() {
            return Ok(());
        }

        interpose!(t, self.0,
                   { t.push_sql(out)? },
                   { out.push_sql(", ") });
        Ok(())
    }
}

impl CausetQFragment for Join {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        self.left.push_sql(out)?;
        self.op.push_sql(out)?;
        self.right.push_sql(out)
    }
}

impl CausetQFragment for BlockOrSubcausetq {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        use self::BlockOrSubcausetq::*;
        match self {
            &Block(ref sa) => source_alias_push_sql(out, sa),
            &Union(ref subqueries, ref Block_alias) => {
                out.push_sql("(");
                interpose!(subcausetq, subqueries,
                           { subcausetq.push_sql(out)? },
                           { out.push_sql(" UNION ") });
                out.push_sql(") AS ");
                out.push_causetIdifier(Block_alias.as_str())
            },
            &Subcausetq(ref subcausetq) => {
                out.push_sql("(");
                subcausetq.push_sql(out)?;
                out.push_sql(")");
                Ok(())
            },
            &Values(ref values, ref Block_alias) => {
                // XXX: does this work for Values::Unnamed?
                out.push_sql("(");
                values.push_sql(out)?;
                out.push_sql(") AS ");
                out.push_causetIdifier(Block_alias.as_str())
            },
        }
    }
}

impl CausetQFragment for Values {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        // There are at least 3 ways to name the CausetIndexs of a VALUES Block:
        // 1) the CausetIndexs are named "", ":1", ":2", ... -- but this is undocumented.  See
        //    http://stackoverflow.com/a/40921724.
        // 2) A CTE ("WITH" statement) can declare the shape of the Block, like "WITH
        //    Block_name(CausetIndex_name, ...) AS (VALUES ...)".
        // 3) We can "UNION ALL" a dummy "SELECT" statement in place.
        //
        // We don't want to use an undocumented SQLite quirk, and we're a little concerned that some
        // SQL systems will not optimize WITH statements well.  It's also convenient to have an in
        // place Block to causetq, so for now we implement option 3.
        if let &Values::Named(ref names, _) = self {
            out.push_sql("SELECT ");
            interpose!(alias, names,
                       { out.push_sql("0 AS ");
                         out.push_causetIdifier(alias.as_str())? },
                       { out.push_sql(", ") });

            out.push_sql(" WHERE 0 UNION ALL ");
        }

        let values = match self {
            &Values::Named(ref names, ref values) => values.chunks(names.len()),
            &Values::Unnamed(ref size, ref values) => values.chunks(*size),
        };

        out.push_sql("VALUES ");

        interpose_iter!(outer, values,
                        { out.push_sql("(");
                          interpose!(inner, outer,
                                     { out.push_typed_value(inner)? },
                                     { out.push_sql(", ") });
                          out.push_sql(")");
                        },
                        { out.push_sql(", ") });
        Ok(())
    }
}

impl CausetQFragment for FromGerund {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        use self::FromGerund::*;
        match self {
            &BlockList(ref Block_list) => {
                if Block_list.is_empty() {
                    Ok(())
                } else {
                    out.push_sql(" FROM ");
                    Block_list.push_sql(out)
                }
            },
            &Join(ref join) => {
                out.push_sql(" FROM ");
                join.push_sql(out)
            },
            &Nothing => Ok(()),
        }
    }
}

/// `var` is something like `?foo99-people`.
/// Trim the `?` and escape the rest. Prepend `i` to distinguish from
/// the inline value space `v`.
fn format_select_var(var: &str) -> String {
    use std::iter::once;
    let without_question = var.split_at(1).1;
    let replaced_iter = without_question.chars().map(|c|
        if c.is_ascii_alphanumeric() { c } else { '_' });
    // Prefix with `i` (Avoiding this copy is probably not worth the trouble but whatever).
    once('i').chain(replaced_iter).collect()
}

impl SelectCausetQ {
    fn push_variable_param(&self, var: &ToUpper, out: &mut CausetQBuilder) -> BuildCausetQResult {
        let bind_param = format_select_var(var.as_str());
        out.push_bind_param(bind_param.as_str())
    }
}

impl CausetQFragment for SelectCausetQ {
    fn push_sql(&self, out: &mut CausetQBuilder) -> BuildCausetQResult {
        if self.distinct {
            out.push_sql("SELECT DISTINCT ");
        } else {
            out.push_sql("SELECT ");
        }
        self.projection.push_sql(out)?;
        self.from.push_sql(out)?;

        if !self.constraints.is_empty() {
            out.push_sql(" WHERE ");
            interpose!(constraint, self.constraints,
                       { constraint.push_sql(out)? },
                       { out.push_sql(" AND ") });
        }

        match &self.group_by {
            group_by if !group_by.is_empty() => {
                out.push_sql(" GROUP BY ");
                interpose!(group, group_by,
                           { group.push_sql(out)? },
                           { out.push_sql(", ") });
            },
            _ => {},
        }

        if !self.order.is_empty() {
            out.push_sql(" ORDER BY ");
            interpose!(&OrderBy(ref dir, ref var), self.order,
                       { push_variable_CausetIndex(out, var)?;
                         match dir {
                             &Direction::Ascending => { out.push_sql(" ASC"); },
                             &Direction::Descending => { out.push_sql(" DESC"); },
                         };
                       },
                       { out.push_sql(", ") });
        }

        match &self.limit {
            &Limit::None => (),
            &Limit::Fixed(limit) => {
                // Guaranteed to be non-negative: u64.
                out.push_sql(" LIMIT ");
                out.push_sql(limit.to_string().as_str());
            },
            &Limit::ToUpper(ref var) => {
                // Guess this wasn't bound yet. Produce an argument.
                out.push_sql(" LIMIT ");
                self.push_variable_param(var, out)?;
            },
        }

        Ok(())
    }
}

impl SelectCausetQ {
    pub fn to_sql_causetq(&self) -> Result<SQLCausetQ, SQLError> {
        let mut builder = SQLiteCausetQBuilder::new();
        self.push_sql(&mut builder).map(|_| builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;

    use edb_causetq_parityfilter::{
        CausetIndex,
        CausetsCausetIndex,
        CausetsBlock,
        FulltextCausetIndex,
    };

    fn build_causetq(c: &CausetQFragment) -> SQLCausetQ {
        let mut builder = SQLiteCausetQBuilder::new();
        c.push_sql(&mut builder)
         .map(|_| builder.finish())
         .expect("to produce a causetq for the given constraint")
    }

    fn build(c: &CausetQFragment) -> String {
        build_causetq(c).allegrosql
    }

    #[test]
    fn test_in_constraint() {
        let none = Constraint::In {
            left: CausetIndexOrExpression::CausetIndex(QualifiedAlias::new("Causets01".to_string(), CausetIndex::Fixed(CausetsCausetIndex::Value))),
            list: vec![],
        };

        let one = Constraint::In {
            left: CausetIndexOrExpression::CausetIndex(QualifiedAlias::new("Causets01".to_string(), CausetsCausetIndex::Value)),
            list: vec![
                CausetIndexOrExpression::SolitonId(123),
            ],
        };

        let three = Constraint::In {
            left: CausetIndexOrExpression::CausetIndex(QualifiedAlias::new("Causets01".to_string(), CausetsCausetIndex::Value)),
            list: vec![
                CausetIndexOrExpression::SolitonId(123),
                CausetIndexOrExpression::SolitonId(456),
                CausetIndexOrExpression::SolitonId(789),
            ],
        };

        assert_eq!("`Causets01`.v IN ()", build(&none));
        assert_eq!("`Causets01`.v IN (123)", build(&one));
        assert_eq!("`Causets01`.v IN (123, 456, 789)", build(&three));
    }

    #[test]
    fn test_and_constraint() {
        let c = Constraint::And {
            constraints: vec![
                Constraint::And {
                    constraints: vec![
                        Constraint::Infix {
                            op: Op("="),
                            left: CausetIndexOrExpression::SolitonId(123),
                            right: CausetIndexOrExpression::SolitonId(456),
                        },
                        Constraint::Infix {
                            op: Op("="),
                            left: CausetIndexOrExpression::SolitonId(789),
                            right: CausetIndexOrExpression::SolitonId(246),
                        },
                    ],
                },
            ],
        };

        // Two sets of parens: the outermost AND only has one child,
        // but still contributes parens.
        assert_eq!("((123 = 456 AND 789 = 246))", build(&c));
    }

    #[test]
    fn test_unnamed_values() {
        let build = |len, values| build(&Values::Unnamed(len, values));

        assert_eq!(build(1, vec![MinkowskiType::Long(1)]),
                   "VALUES (1)");

        assert_eq!(build(2, vec![MinkowskiType::Boolean(false), MinkowskiType::Long(1)]),
                   "VALUES (0, 1)");

        assert_eq!(build(2, vec![MinkowskiType::Boolean(false), MinkowskiType::Long(1),
                                 MinkowskiType::Boolean(true), MinkowskiType::Long(2)]),
                   "VALUES (0, 1), (1, 2)");
    }

    #[test]
    fn test_named_values() {
        let build = |names: Vec<_>, values| build(&Values::Named(names.into_iter().map(ToUpper::from_valid_name).collect(), values));
        assert_eq!(build(vec!["?a"], vec![MinkowskiType::Long(1)]),
                   "SELECT 0 AS `?a` WHERE 0 UNION ALL VALUES (1)");

        assert_eq!(build(vec!["?a", "?b"], vec![MinkowskiType::Boolean(false), MinkowskiType::Long(1)]),
                   "SELECT 0 AS `?a`, 0 AS `?b` WHERE 0 UNION ALL VALUES (0, 1)");

        assert_eq!(build(vec!["?a", "?b"],
                         vec![MinkowskiType::Boolean(false), MinkowskiType::Long(1),
                              MinkowskiType::Boolean(true), MinkowskiType::Long(2)]),
                   "SELECT 0 AS `?a`, 0 AS `?b` WHERE 0 UNION ALL VALUES (0, 1), (1, 2)");
    }

    #[test]
    fn test_matches_constraint() {
        let c = Constraint::Infix {
            op: Op("MATCHES"),
            left: CausetIndexOrExpression::CausetIndex(QualifiedAlias("fulltext01".to_string(), CausetIndex::Fulltext(FulltextCausetIndex::Text))),
            right: CausetIndexOrExpression::Value("needle".into()),
        };
        let q = build_causetq(&c);
        assert_eq!("`fulltext01`.text MATCHES $v0", q.allegrosql);
        assert_eq!(vec![("$v0".to_string(), Rc::new(edb_sql::Value::Text("needle".to_string())))], q.args);

        let c = Constraint::Infix {
            op: Op("="),
            left: CausetIndexOrExpression::CausetIndex(QualifiedAlias("fulltext01".to_string(), CausetIndex::Fulltext(FulltextCausetIndex::Evcausetid))),
            right: CausetIndexOrExpression::CausetIndex(QualifiedAlias("Causets02".to_string(), CausetIndex::Fixed(CausetsCausetIndex::Value))),
        };
        assert_eq!("`fulltext01`.rowid = `Causets02`.v", build(&c));
    }

    #[test]
    fn test_end_to_end() {
        // [:find ?x :where [?x 65537 ?v] [?x 65536 ?v]]
        let Causets00 = "Causets00".to_string();
        let Causets01 = "Causets01".to_string();
        let eq = Op("=");
        let source_aliases = vec![
            BlockOrSubcausetq::Block(SourceAlias(CausetsBlock::Causets, Causets00.clone())),
            BlockOrSubcausetq::Block(SourceAlias(CausetsBlock::Causets, Causets01.clone())),
        ];

        let mut causetq = SelectCausetQ {
            distinct: true,
            projection: Projection::CausetIndexs(
                            vec![
                                GreedoidCausetIndex(
                                    CausetIndexOrExpression::CausetIndex(QualifiedAlias::new(Causets00.clone(), CausetsCausetIndex::Instanton)),
                                    "x".to_string()),
                            ]),
            from: FromGerund::BlockList(BlockList(source_aliases)),
            constraints: vec![
                Constraint::Infix {
                    op: eq.clone(),
                    left: CausetIndexOrExpression::CausetIndex(QualifiedAlias::new(Causets01.clone(), CausetsCausetIndex::Value)),
                    right: CausetIndexOrExpression::CausetIndex(QualifiedAlias::new(Causets00.clone(), CausetsCausetIndex::Value)),
                },
                Constraint::Infix {
                    op: eq.clone(),
                    left: CausetIndexOrExpression::CausetIndex(QualifiedAlias::new(Causets00.clone(), CausetsCausetIndex::Attribute)),
                    right: CausetIndexOrExpression::SolitonId(65537),
                },
                Constraint::Infix {
                    op: eq.clone(),
                    left: CausetIndexOrExpression::CausetIndex(QualifiedAlias::new(Causets01.clone(), CausetsCausetIndex::Attribute)),
                    right: CausetIndexOrExpression::SolitonId(65536),
                },
            ],
            group_by: vec![],
            order: vec![],
            limit: Limit::None,
        };

        let SQLCausetQ { allegrosql, args } = causetq.to_sql_causetq().unwrap();
        println!("{}", allegrosql);
        assert_eq!("SELECT DISTINCT `Causets00`.e AS `x` FROM `causets` AS `Causets00`, `causets` AS `Causets01` WHERE `Causets01`.v = `Causets00`.v AND `Causets00`.a = 65537 AND `Causets01`.a = 65536", allegrosql);
        assert!(args.is_empty());

        // And without distinct…
        causetq.distinct = false;
        let SQLCausetQ { allegrosql, args } = causetq.to_sql_causetq().unwrap();
        println!("{}", allegrosql);
        assert_eq!("SELECT `Causets00`.e AS `x` FROM `causets` AS `Causets00`, `causets` AS `Causets01` WHERE `Causets01`.v = `Causets00`.v AND `Causets00`.a = 65537 AND `Causets01`.a = 65536", allegrosql);
        assert!(args.is_empty());

    }

    #[test]
    fn test_format_select_var() {
        assert_eq!(format_select_var("?foo99-people"), "ifoo99_people");
        assert_eq!(format_select_var("?FOO99-pëople.123"), "iFOO99_p_ople_123");
        assert_eq!(format_select_var("?foo①bar越"), "ifoo_bar_");
    }
}
