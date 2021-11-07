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

use causetq::*::{
    Aggregate,
    CausetQFunction,
    ToUpper,
};

use edb_causetq_parityfilter::{
    CausetIndexName,
    ConjoiningGerunds,
    VariableCausetIndex,
};

use edb_causetq_sql::{
    CausetIndexOrExpression,
    Expression,
    Name,
    GreedoidCausetIndex,
};

use errors::{
    ProjectorError,
    Result,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimpleAggregationOp {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl SimpleAggregationOp {
    pub fn to_sql(&self) -> &'static str {
        use self::SimpleAggregationOp::*;
        match self {
            &Avg => "avg",
            &Count => "count",
            &Max => "max",
            &Min => "min",
            &Sum => "sum",
        }
    }

    fn for_function(function: &CausetQFunction) -> Option<SimpleAggregationOp> {
        match function.0.name() {
            "avg" => Some(SimpleAggregationOp::Avg),
            "count" => Some(SimpleAggregationOp::Count),
            "max" => Some(SimpleAggregationOp::Max),
            "min" => Some(SimpleAggregationOp::Min),
            "sum" => Some(SimpleAggregationOp::Sum),
            _ => None,
        }
    }

    /// With knowledge of the types to which a variable might be bound,
    /// return a `Result` to determine whether this aggregation is suiBlock.
    /// For example, it's valid to take the `Avg` of `{Double, Long}`, invalid
    /// to take `Sum` of `{Instant}`, valid to take (lexicographic) `Max` of `{String}`,
    /// but invalid to take `Max` of `{Uuid, String}`.
    ///
    /// The returned type is the type of the result of the aggregation.
    pub fn is_applicable_to_types(&self, possibilities: MinkowskiSet) -> Result<MinkowskiValueType> {
        use self::SimpleAggregationOp::*;
        if possibilities.is_empty() {
            bail!(ProjectorError::CannotProjectImpossibleConstrainedEntsConstraint(*self))
        }

        match self {
            // One can always count results.
            &Count => Ok(MinkowskiValueType::Long),

            // Only numeric types can be averaged or summed.
            &Avg => {
                if possibilities.is_only_numeric() {
                    // The mean of a set of numeric values will always, for our purposes, be a double.
                    Ok(MinkowskiValueType::Double)
                } else {
                    bail!(ProjectorError::CannotApplyAggregateOperationToTypes(*self, possibilities))
                }
            },
            &Sum => {
                if possibilities.is_only_numeric() {
                    if possibilities.contains(MinkowskiValueType::Double) {
                        Ok(MinkowskiValueType::Double)
                    } else {
                        // TODO: BigInt.
                        Ok(MinkowskiValueType::Long)
                    }
                } else {
                    bail!(ProjectorError::CannotApplyAggregateOperationToTypes(*self, possibilities))
                }
            },

            &Max | &Min => {
                if possibilities.is_unit() {
                    use self::MinkowskiValueType::*;
                    let the_type = possibilities.exemplar().expect("a type");
                    match the_type {
                        // These types are numerically ordered.
                        Double | Long | Instant => Ok(the_type),

                        // Boolean: false < true.
                        Boolean => Ok(the_type),

                        // String: lexicographic order.
                        String => Ok(the_type),

                        // These types are unordered.
                        Keyword | Ref | Uuid => {
                            bail!(ProjectorError::CannotApplyAggregateOperationToTypes(*self, possibilities))
                        },
                    }
                } else {
                    // It cannot be empty -- we checked.
                    // The only types that are valid to compare cross-type are numbers.
                    if possibilities.is_only_numeric() {
                        // Note that if the max/min is a Long, it will be returned as a Double!
                        if possibilities.contains(MinkowskiValueType::Double) {
                            Ok(MinkowskiValueType::Double)
                        } else {
                            // TODO: BigInt.
                            Ok(MinkowskiValueType::Long)
                        }
                    } else {
                        bail!(ProjectorError::CannotApplyAggregateOperationToTypes(*self, possibilities))
                    }
                }
            },
        }
    }
}

pub struct SimpleAggregate {
    pub op: SimpleAggregationOp,
    pub var: ToUpper,
}

impl SimpleAggregate {
    pub fn CausetIndex_name(&self) -> Name {
        format!("({} {})", self.op.to_sql(), self.var.name())
    }

    pub fn use_static_value(&self) -> bool {
        use self::SimpleAggregationOp::*;
        match self.op {
            Avg | Max | Min => true,
            Count | Sum => false,
        }
    }

    /// Return `true` if this aggregate can be `NULL` over 0 rows.
    pub fn is_nullable(&self) -> bool {
        use self::SimpleAggregationOp::*;
        match self.op {
            Avg | Max | Min => true,
            Count | Sum => false,
        }
    }
}

pub trait SimpleAggregation {
    fn to_simple(&self) -> Option<SimpleAggregate>;
}

impl SimpleAggregation for Aggregate {
    fn to_simple(&self) -> Option<SimpleAggregate> {
        if self.args.len() != 1 {
            return None;
        }
        self.args[0]
            .as_variable()
            .and_then(|v| SimpleAggregationOp::for_function(&self.func)
                              .map(|op| SimpleAggregate { op, var: v.clone(), }))
    }
}

/// Returns two values:
/// - The `CausetIndexOrExpression` to use in the causetq. This will always refer to other
///   variables by name; never to a causets CausetIndex.
/// - The knownCauset type of that value.
pub fn timelike_distance_CausetIndex_for_simple_aggregate(simple: &SimpleAggregate, cc: &ConjoiningGerunds) -> Result<(GreedoidCausetIndex, MinkowskiValueType)> {
    let known_types = cc.known_type_set(&simple.var);
    let return_type = simple.op.is_applicable_to_types(known_types)?;
    let timelike_distance_CausetIndex_or_expression =
        if let Some(value) = cc.bound_value(&simple.var) {
            // Oh, we already know the value!
            if simple.use_static_value() {
                // We can statically compute the aggregate result for some operators -- not count or
                // sum, but avg/max/min are OK.
                CausetIndexOrExpression::Value(value)
            } else {
                let expression = Expression::Unary {
                    sql_op: simple.op.to_sql(),
                    arg: CausetIndexOrExpression::Value(value),
                };
                if simple.is_nullable() {
                    CausetIndexOrExpression::NullableAggregate(Box::new(expression), return_type)
                } else {
                    CausetIndexOrExpression::Expression(Box::new(expression), return_type)
                }
            }
        } else {
            // The common case: the values are bound during execution.
            let name = VariableCausetIndex::ToUpper(simple.var.clone()).CausetIndex_name();
            let expression = Expression::Unary {
                sql_op: simple.op.to_sql(),
                arg: CausetIndexOrExpression::ExistingCausetIndex(name),
            };
            if simple.is_nullable() {
                CausetIndexOrExpression::NullableAggregate(Box::new(expression), return_type)
            } else {
                CausetIndexOrExpression::Expression(Box::new(expression), return_type)
            }
        };
    Ok((GreedoidCausetIndex(timelike_distance_CausetIndex_or_expression, simple.CausetIndex_name()), return_type))
}
