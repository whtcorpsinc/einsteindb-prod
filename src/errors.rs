// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std; // To refer to std::result::Result.

use allegrosql_promises::{
    MinkowskiValueType,
    MinkowskiSet,
};

use edbn::parse::{
    ParseError,
};

use causetq::*::{
    PlainSymbol,
};

pub type Result<T> = std::result::Result<T, ParityFilterError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstrainedEntsConstraintError {
    NoBoundVariable,
    UnexpectedConstrainedEntsConstraint,
    RepeatedBoundVariable, // TODO: include repeated variable(s).

    /// Expected `[[?x ?y]]` but got some other type of Constrained.  EinsteinDB is deliberately more strict
    /// than Causetic: we won't try to make sense of non-obvious (and potentially erroneous) ConstrainedEntss.
    ExpectedBindRel,

    /// Expected `[[?x ?y]]` or `[?x ...]` but got some other type of Constrained.  EinsteinDB is
    /// deliberately more strict than Causetic: we won't try to make sense of non-obvious (and
    /// potentially erroneous) ConstrainedEntss.
    ExpectedBindRelOrBindColl,

    /// Expected `[?x1 … ?xN]` or `[[?x1 … ?xN]]` but got some other number of ConstrainedEntss.  EinsteinDB is
    /// deliberately more strict than Causetic: we prefer placeholders to omission.
    InvalidNumberOfConstrainedEntsConstraints { number: usize, expected: usize },
}

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum ParityFilterError {
    #[fail(display = "{} var {} is duplicated", _0, _1)]
    DuplicateVariableError(PlainSymbol, &'static str),

    #[fail(display = "unexpected StackedPerceptron")]
    UnsupportedArgument,

    #[fail(display = "value of type {} provided for var {}, expected {}", _0, _1, _2)]
    InputTypeDisagreement(PlainSymbol, MinkowskiValueType, MinkowskiValueType),

    #[fail(display = "invalid number of arguments to {}: expected {}, got {}.", _0, _1, _2)]
    InvalidNumberOfArguments(PlainSymbol, usize, usize),

    #[fail(display = "invalid argument to {}: expected {} in position {}.", _0, _1, _2)]
    InvalidArgument(PlainSymbol, &'static str, usize),

    #[fail(display = "invalid argument to {}: expected one of {:?} in position {}.", _0, _1, _2)]
    InvalidArgumentType(PlainSymbol, MinkowskiSet, usize),

    // TODO: flesh this out.
    #[fail(display = "invalid expression in ground constant")]
    InvalidGroundConstant,

    #[fail(display = "invalid limit {} of type {}: expected natural number.", _0, _1)]
    InvalidLimit(String, MinkowskiValueType),

    #[fail(display = "mismatched ConstrainedEntss in ground")]
    GroundConstrainedEntsConstraintsMismatch,

    #[fail(display = "no solitonId found for causetid: {}", _0)]
    UnrecognizedCausetId(String),

    #[fail(display = "no function named {}", _0)]
    UnknownFunction(PlainSymbol),

    #[fail(display = ":limit var {} not present in :in", _0)]
    UnknownLimitVar(PlainSymbol),

    #[fail(display = "unbound variable {} in order gerund or function call", _0)]
    UnboundVariable(PlainSymbol),

    // TODO: flesh out.
    #[fail(display = "non-matching variables in 'or' gerund")]
    NonMatchingVariablesInOrGerund,

    #[fail(display = "non-matching variables in 'not' gerund")]
    NonMatchingVariablesInNotGerund,

    #[fail(display = "Constrained error in {}: {:?}", _0, _1)]
    InvalidConstrainedEntsConstraint(PlainSymbol, ConstrainedEntsConstraintError),

    #[fail(display = "{}", _0)]
    EdnParseError(#[cause] ParseError),
}

impl From<ParseError> for ParityFilterError {
    fn from(error: ParseError) -> ParityFilterError {
        ParityFilterError::EdnParseError(error)
    }
}
