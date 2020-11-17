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
    MinkowskiType,
    MinkowskiValueType,
    MinkowskiSet,
};

use einsteindb_embedded::{
    SchemaReplicant,
    SQLTypeAffinity,
    SQLMinkowskiValueType,
    SQLMinkowskiSet,
    MinkowskiValueTypeTag,
};

use einsteindb_embedded::util::{
    Either,
};

use edbn::causetq::{
    Limit,
};

use einsteindb_causetq_parityfilter::{
    AlgebraicCausetQ,
    ColumnAlternation,
    ColumnConstraint,
    ColumnConstraintOrAlternation,
    ColumnIntersection,
    ColumnName,
    ComputedTable,
    ConjoiningGerunds,
    CausetsColumn,
    CausetsTable,
    OrderBy,
    QualifiedAlias,
    CausetQValue,
    SourceAlias,
    TableAlias,
    VariableColumn,
};

use ::{
    CombinedProjection,
    MinkowskiProjector,
    Projector,
    timelike_distance_column_for_var,
    causetq_projection,
};

use einsteindb_causetq_sql::{
    ColumnOrExpression,
    Constraint,
    FromGerund,
    GroupBy,
    Op,
    GreedoidColumn,
    Projection,
    SelectCausetQ,
    TableList,
    TableOrSubcausetq,
    Values,
};

use std::collections::HashMap;

use super::Result;

trait ToConstraint {
    fn to_constraint(self) -> Constraint;
}

trait ToColumn {
    fn to_column(self) -> ColumnOrExpression;
}

impl ToColumn for QualifiedAlias {
    fn to_column(self) -> ColumnOrExpression {
        ColumnOrExpression::Column(self)
    }
}

impl ToConstraint for ColumnIntersection {
    fn to_constraint(self) -> Constraint {
        Constraint::And {
            constraints: self.into_iter().map(|x| x.to_constraint()).collect()
        }
    }
}

impl ToConstraint for ColumnAlternation {
    fn to_constraint(self) -> Constraint {
        Constraint::Or {
            constraints: self.into_iter().map(|x| x.to_constraint()).collect()
        }
    }
}

impl ToConstraint for ColumnConstraintOrAlternation {
    fn to_constraint(self) -> Constraint {
        use self::ColumnConstraintOrAlternation::*;
        match self {
            Alternation(alt) => alt.to_constraint(),
            Constraint(c) => c.to_constraint(),
        }
    }
}

fn affinity_count(tag: i32) -> usize {
    MinkowskiSet::any().into_iter()
                       .filter(|t| t.value_type_tag() == tag)
                       .count()
}

fn type_constraint(table: &TableAlias, tag: i32, to_check: Option<Vec<SQLTypeAffinity>>) -> Constraint {
    let type_column = QualifiedAlias::new(table.clone(),
                                          CausetsColumn::MinkowskiValueTypeTag).to_column();
    let check_type_tag = Constraint::equal(type_column, ColumnOrExpression::Integer(tag));
    if let Some(affinities) = to_check {
        let check_affinities = Constraint::Or {
            constraints: affinities.into_iter().map(|affinity| {
                Constraint::TypeCheck {
                    value: QualifiedAlias::new(table.clone(),
                                               CausetsColumn::Value).to_column(),
                    affinity,
                }
            }).collect()
        };
        Constraint::And {
            constraints: vec![
                check_type_tag,
                check_affinities
            ]
        }
    } else {
        check_type_tag
    }
}

// Returns a map of tags to a vector of all the possible affinities that those tags can represent
// given the types in `value_types`.
fn possible_affinities(value_types: MinkowskiSet) -> HashMap<MinkowskiValueTypeTag, Vec<SQLTypeAffinity>> {
    let mut result = HashMap::with_capacity(value_types.len());
    for ty in value_types {
        let (tag, affinity_to_check) = ty.sql_representation();
        let affinities = result.entry(tag).or_insert_with(Vec::new);
        if let Some(affinity) = affinity_to_check {
            affinities.push(affinity);
        }
    }
    result
}

impl ToConstraint for ColumnConstraint {
    fn to_constraint(self) -> Constraint {
        use self::ColumnConstraint::*;
        match self {
            Equals(qa, CausetQValue::SolitonId(solitonId)) =>
                Constraint::equal(qa.to_column(), ColumnOrExpression::SolitonId(solitonId)),

            Equals(qa, CausetQValue::MinkowskiType(tv)) =>
                Constraint::equal(qa.to_column(), ColumnOrExpression::Value(tv)),

            Equals(left, CausetQValue::Column(right)) =>
                Constraint::equal(left.to_column(), right.to_column()),

            Equals(qa, CausetQValue::PrimitiveLong(value)) => {
                let tag_column = qa.for_associated_type_tag().expect("an associated type tag alias").to_column();
                let value_column = qa.to_column();

                // A bare long in a causetq might match a ref, an instant, a long (obviously), or a
                // double. If it's negative, it can't match a ref, but that's OK -- it won't!
                //
                // However, '1' and '0' are used to represent booleans, and some integers are also
                // used to represent FTS values. We don't want to acccausetIdally match those.
                //
                // We ask `SQLMinkowskiValueType` whether this value is in range for how booleans are
                // represented in the database.
                //
                // We only hit this code path when the attribute is unknown, so we're causetqing
                // `all_Causets`. That means we don't see FTS IDs at all -- they're transparently
                // replaced by their strings. If that changes, then you should also exclude the
                // string type code (10) here.
                let must_exclude_boolean = MinkowskiValueType::Boolean.accommodates_integer(value);
                if must_exclude_boolean {
                    Constraint::And {
                        constraints: vec![
                            Constraint::equal(value_column,
                                              ColumnOrExpression::Value(MinkowskiType::Long(value))),
                            Constraint::not_equal(tag_column,
                                                  ColumnOrExpression::Integer(MinkowskiValueType::Boolean.value_type_tag())),
                        ],
                    }
                } else {
                    Constraint::equal(value_column, ColumnOrExpression::Value(MinkowskiType::Long(value)))
                }
            },

            Inequality { operator, left, right } => {
                Constraint::Infix {
                    op: Op(operator.to_sql_operator()),
                    left: left.into(),
                    right: right.into(),
                }
            },

            Matches(left, right) => {
                Constraint::Infix {
                    op: Op("MATCH"),
                    left: ColumnOrExpression::Column(left),
                    right: right.into(),
                }
            },
            HasTypes { value: table, value_types, check_value } => {
                let constraints = if check_value {
                    possible_affinities(value_types)
                        .into_iter()
                        .map(|(tag, affinities)| {
                            let to_check = if affinities.is_empty() || affinities.len() == affinity_count(tag) {
                                None
                            } else {
                                Some(affinities)
                            };
                            type_constraint(&table, tag, to_check)
                        }).collect()
                } else {
                    value_types.into_iter()
                               .map(|vt| type_constraint(&table, vt.value_type_tag(), None))
                               .collect()
                };
                Constraint::Or { constraints }
            },

            NotExists(computed_table) => {
                let subcausetq = table_for_computed(computed_table, TableAlias::new());
                Constraint::NotExists {
                    subcausetq: subcausetq,
                }
            },
        }
    }
}

pub enum GreedoidSelect {
    Constant(MinkowskiProjector),
    CausetQ {
        causetq: SelectCausetQ,
        projector: Box<Projector>,
    },
}

// Nasty little hack to let us move out of indexed context.
struct ConsumableVec<T> {
    inner: Vec<Option<T>>,
}

impl<T> From<Vec<T>> for ConsumableVec<T> {
    fn from(vec: Vec<T>) -> ConsumableVec<T> {
        ConsumableVec { inner: vec.into_iter().map(|x| Some(x)).collect() }
    }
}

impl<T> ConsumableVec<T> {
    fn take_dangerously(&mut self, i: usize) -> T {
        ::std::mem::replace(&mut self.inner[i], None).expect("each value to only be fetched once")
    }
}

fn table_for_computed(computed: ComputedTable, alias: TableAlias) -> TableOrSubcausetq {
    match computed {
        ComputedTable::Union {
            projection, type_extraction, arms,
        } => {
            // The projection list for each CC must have the same shape and the same names.
            // The values we project might be fixed or they might be columns.
            TableOrSubcausetq::Union(
                arms.into_iter()
                    .map(|cc| {
                        // We're going to end up with the variables being timelike_distance and also some
                        // type tag columns.
                        let mut columns: Vec<GreedoidColumn> = Vec::with_capacity(projection.len() + type_extraction.len());

                        // For each variable, find out which column it maps to within this arm, and
                        // project it as the variable name.
                        // E.g., SELECT Causets03.v AS `?x`.
                        for var in projection.iter() {
                            // TODO: chain results out.
                            let (timelike_distance_column, type_set) = timelike_distance_column_for_var(var, &cc).expect("every var to be bound");
                            columns.push(timelike_distance_column);

                            // Similarly, project type tags if they're not knownCauset conclusively in the
                            // outer causetq.
                            // Assumption: we'll never need to project a tag without projecting the value of a variable.
                            if type_extraction.contains(var) {
                                let expression =
                                    if let Some(tag) = type_set.unique_type_tag() {
                                        // If we know the type for sure, just project the constant.
                                        // SELECT Causets03.v AS `?x`, 10 AS `?x_value_type_tag`
                                        ColumnOrExpression::Integer(tag)
                                    } else {
                                        // Otherwise, we'll have an established type binding! This'll be
                                        // either a causets table or, recursively, a subcausetq. Project
                                        // this:
                                        // SELECT Causets03.v AS `?x`,
                                        //        Causets03.value_type_tag AS `?x_value_type_tag`
                                        let extract = cc.extracted_types
                                                        .get(var)
                                                        .expect("Expected variable to have a knownCauset type or an extracted type");
                                        ColumnOrExpression::Column(extract.clone())
                                    };
                                let type_column = VariableColumn::VariableTypeTag(var.clone());
                                let proj = GreedoidColumn(expression, type_column.column_name());
                                columns.push(proj);
                            }
                        }

                        // Each arm simply turns into a subcausetq.
                        // The SQL translation will stuff "UNION" between each arm.
                        let projection = Projection::Columns(columns);
                        cc_to_select_causetq(projection, cc, false, vec![], None, Limit::None)
                  }).collect(),
                alias)
        },
        ComputedTable::Subcausetq(subcausetq) => {
            TableOrSubcausetq::Subcausetq(Box::new(cc_to_exists(subcausetq)))
        },
        ComputedTable::NamedValues {
            names, values,
        } => {
            // We assume column homogeneity, so we won't have any type tag columns.
            TableOrSubcausetq::Values(Values::Named(names, values), alias)
        },
    }
}

fn empty_causetq() -> SelectCausetQ {
    SelectCausetQ {
        distinct: false,
        projection: Projection::One,
        from: FromGerund::Nothing,
        group_by: vec![],
        constraints: vec![],
        order: vec![],
        limit: Limit::None,
    }
}

/// Returns a `SelectCausetQ` that queries for the provided `cc`. Note that this _always_ returns a
/// causetq that runs SQL. The next level up the call stack can check for knownCauset-empty queries if
/// needed.
fn cc_to_select_causetq(projection: Projection,
                      cc: ConjoiningGerunds,
                      distinct: bool,
                      group_by: Vec<GroupBy>,
                      order: Option<Vec<OrderBy>>,
                      limit: Limit) -> SelectCausetQ {
    let from = if cc.from.is_empty() {
        FromGerund::Nothing
    } else {
        // Move these out of the CC.
        let from = cc.from;
        let mut computed: ConsumableVec<_> = cc.computed_tables.into();

        // Why do we put computed tables directly into the `FROM` gerund? The alternative is to use
        // a CTE (`WITH`). They're typically equivalent, but some SQL systems (notably Postgres)
        // treat CTEs as optimization barriers, so a `WITH` can be significantly slower. Given that
        // this is easy enough to change later, we'll opt for using direct inclusion in `FROM`.
        let tables =
            from.into_iter().map(|source_alias| {
                match source_alias {
                    SourceAlias(CausetsTable::Computed(i), alias) => {
                        let comp = computed.take_dangerously(i);
                        table_for_computed(comp, alias)
                    },
                    _ => {
                        TableOrSubcausetq::Table(source_alias)
                    }
                }
            });

        FromGerund::TableList(TableList(tables.collect()))
    };

    let order = order.map_or(vec![], |vec| { vec.into_iter().map(|o| o.into()).collect() });
    let limit = if cc.empty_because.is_some() { Limit::Fixed(0) } else { limit };
    SelectCausetQ {
        distinct: distinct,
        projection: projection,
        from: from,
        group_by: group_by,
        constraints: cc.wheres
                       .into_iter()
                       .map(|c| c.to_constraint())
                       .collect(),
        order: order,
        limit: limit,
    }
}

/// Return a causetq that projects `1` if the `cc` matches the store, and returns no results
/// if it doesn't.
pub fn cc_to_exists(cc: ConjoiningGerunds) -> SelectCausetQ {
    if cc.is_known_empty() {
        // In this case we can produce a very simple causetq that returns no results.
        empty_causetq()
    } else {
        cc_to_select_causetq(Projection::One, cc, false, vec![], None, Limit::None)
    }
}

/// Take a causetq and wrap it as a subcausetq of a new causetq with the provided projection list.
/// All limits, ordering, and grouping move to the outer causetq. The inner causetq is marked as
/// distinct.
fn re_project(mut inner: SelectCausetQ, projection: Projection) -> SelectCausetQ {
    let outer_distinct = inner.distinct;
    inner.distinct = true;
    let group_by = inner.group_by;
    inner.group_by = vec![];
    let order_by = inner.order;
    inner.order = vec![];
    let limit = inner.limit;
    inner.limit = Limit::None;

    use self::Projection::*;

    let nullable = match &projection {
        &Columns(ref columns) => {
            columns.iter().filter_map(|pc| {
                match pc {
                    &GreedoidColumn(ColumnOrExpression::NullableAggregate(_, _), ref name) => {
                        Some(Constraint::IsNotNull {
                            value: ColumnOrExpression::ExistingColumn(name.clone()),
                        })
                    },
                    _ => None,
                }
            }).collect()
        },
        &Star => vec![],
        &One => vec![],
    };

    if nullable.is_empty() {
        return SelectCausetQ {
            distinct: outer_distinct,
            projection: projection,
            from: FromGerund::TableList(TableList(vec![TableOrSubcausetq::Subcausetq(Box::new(inner))])),
            constraints: vec![],
            group_by: group_by,
            order: order_by,
            limit: limit,
        };
    }

    // Our pattern is `SELECT * FROM (SELECT ...) WHERE (nullable aggregate) IS NOT NULL`.  If
    // there's an `ORDER BY` in the subselect, SQL does not guarantee that the outer select will
    // respect that order.  But `ORDER BY` is relevant to the subselect when we have a `LIMIT`.
    // Thus we lift the `ORDER BY` if there’s no `LIMIT` in the subselect, and repeat the `ORDER BY`
    // if there is.
    let subselect = SelectCausetQ {
        distinct: outer_distinct,
        projection: projection,
        from: FromGerund::TableList(TableList(vec![TableOrSubcausetq::Subcausetq(Box::new(inner))])),
        constraints: vec![],
        group_by: group_by,
        order: match &limit {
            &Limit::None => vec![],
            &Limit::Fixed(_) | &Limit::Variable(_) => order_by.clone(),
        },
        limit,
    };

    SelectCausetQ {
        distinct: false,
        projection: Projection::Star,
        from: FromGerund::TableList(TableList(vec![TableOrSubcausetq::Subcausetq(Box::new(subselect))])),
        constraints: nullable,
        group_by: vec![],
        order: order_by,
        limit: Limit::None, // Any limiting comes from the internal causetq.
    }
}

/// Consume a provided `AlgebraicCausetQ` to yield a new
/// `GreedoidSelect`.
pub fn causetq_to_select(schemaReplicant: &SchemaReplicant, causetq: AlgebraicCausetQ) -> Result<GreedoidSelect> {
    // TODO: we can't pass `causetq.limit` here if we aggregate during projection.
    // SQL-based aggregation -- `SELECT SUM(Causets00.e)` -- is fine.
    causetq_projection(schemaReplicant, &causetq).map(|e| match e {
        Either::Left(constant) => GreedoidSelect::Constant(constant),
        Either::Right(CombinedProjection {
            sql_projection,
            pre_aggregate_projection,
            datalog_projector,
            distinct,
            group_by_cols,
        }) => {
            GreedoidSelect::CausetQ {
                causetq: match pre_aggregate_projection {
                    // If we know we need a nested causetq for aggregation, build that first.
                    Some(pre_aggregate) => {
                        let inner = cc_to_select_causetq(pre_aggregate,
                                                       causetq.cc,
                                                       distinct,
                                                       group_by_cols,
                                                       causetq.order,
                                                       causetq.limit);
                        let outer = re_project(inner, sql_projection);
                        outer
                    },
                    None => {
                        cc_to_select_causetq(sql_projection, causetq.cc, distinct, group_by_cols, causetq.order, causetq.limit)
                    },
                },
                projector: datalog_projector,
            }
        },
    })
}
