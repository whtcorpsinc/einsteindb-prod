// Copyright 2016 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use embedded_promises::{
    ValueType,
    TypedValue,
};

use einsteindb_embedded::{
    HasSchema,
    Schema,
};

use edbn::causetq::{
    FnArg,
    NonIntegerConstant,
    PlainSymbol,
};

use clauses::ConjoiningClauses;

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

use types::{
    EmptyBecause,
    CausetQValue,
};

/// Argument resolution.
impl ConjoiningClauses {
    /// Take a function argument and turn it into a `CausetQValue` suitable for use in a concrete
    /// constraint.
    /// Additionally, do two things:
    /// - Mark the pattern as known-empty if any argument is known non-numeric.
    /// - Mark any variables encountered as numeric.
    pub(crate) fn resolve_numeric_argument(&mut self, function: &PlainSymbol, position: usize, arg: FnArg) -> Result<CausetQValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                // Handle incorrect types
                if let Some(v) = self.bound_value(&var) {
                    if v.value_type().is_numeric() {
                        Ok(CausetQValue::TypedValue(v))
                    } else {
                        bail!(ParityFilterError::InputTypeDisagreement(var.name().clone(), ValueType::Long, v.value_type()))
                    }
                } else {
                    self.constrain_var_to_numeric(var.clone());
                    self.column_bindings
                        .get(&var)
                        .and_then(|cols| cols.first().map(|col| CausetQValue::Column(col.clone())))
                        .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                }
            },
            // Can't be an solitonId.
            SolitonIdOrInteger(i) => Ok(CausetQValue::TypedValue(TypedValue::Long(i))),
            CausetIdOrKeyword(_) |
            SrcVar(_) |
            Constant(NonIntegerConstant::Boolean(_)) |
            Constant(NonIntegerConstant::Text(_)) |
            Constant(NonIntegerConstant::Uuid(_)) |
            Constant(NonIntegerConstant::Instant(_)) |        // Instants are covered below.
            Constant(NonIntegerConstant::BigInteger(_)) |
            Vector(_) => {
                self.mark_known_empty(EmptyBecause::NonNumericArgument);
                bail!(ParityFilterError::InvalidArgument(function.clone(), "numeric", position))
            },
            Constant(NonIntegerConstant::Float(f)) => Ok(CausetQValue::TypedValue(TypedValue::Double(f))),
        }
    }

    /// Just like `resolve_numeric_argument`, but for `ValueType::Instant`.
    pub(crate) fn resolve_instant_argument(&mut self, function: &PlainSymbol, position: usize, arg: FnArg) -> Result<CausetQValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                match self.bound_value(&var) {
                    Some(TypedValue::Instant(v)) => Ok(CausetQValue::TypedValue(TypedValue::Instant(v))),
                    Some(v) => bail!(ParityFilterError::InputTypeDisagreement(var.name().clone(), ValueType::Instant, v.value_type())),
                    None => {
                        self.constrain_var_to_type(var.clone(), ValueType::Instant);
                        self.column_bindings
                            .get(&var)
                            .and_then(|cols| cols.first().map(|col| CausetQValue::Column(col.clone())))
                            .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                    },
                }
            },
            Constant(NonIntegerConstant::Instant(v)) => {
                Ok(CausetQValue::TypedValue(TypedValue::Instant(v)))
            },

            // TODO: should we allow integers if they seem to be timestamps? It's ambiguous…
            SolitonIdOrInteger(_) |
            CausetIdOrKeyword(_) |
            SrcVar(_) |
            Constant(NonIntegerConstant::Boolean(_)) |
            Constant(NonIntegerConstant::Float(_)) |
            Constant(NonIntegerConstant::Text(_)) |
            Constant(NonIntegerConstant::Uuid(_)) |
            Constant(NonIntegerConstant::BigInteger(_)) |
            Vector(_) => {
                self.mark_known_empty(EmptyBecause::NonInstantArgument);
                bail!(ParityFilterError::InvalidArgumentType(function.clone(), ValueType::Instant.into(), position))
            },
        }
    }

    /// Take a function argument and turn it into a `CausetQValue` suitable for use in a concrete
    /// constraint.
    pub(crate) fn resolve_ref_argument(&mut self, schema: &Schema, function: &PlainSymbol, position: usize, arg: FnArg) -> Result<CausetQValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                self.constrain_var_to_type(var.clone(), ValueType::Ref);
                if let Some(TypedValue::Ref(e)) = self.bound_value(&var) {
                    // Incorrect types will be handled by the constraint, above.
                    Ok(CausetQValue::SolitonId(e))
                } else {
                    self.column_bindings
                        .get(&var)
                        .and_then(|cols| cols.first().map(|col| CausetQValue::Column(col.clone())))
                        .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                }
            },
            SolitonIdOrInteger(i) => Ok(CausetQValue::TypedValue(TypedValue::Ref(i))),
            CausetIdOrKeyword(i) => {
                schema.get_entid(&i)
                      .map(|known_entid| CausetQValue::SolitonId(known_entid.into()))
                      .ok_or_else(|| ParityFilterError::UnrecognizedCausetId(i.to_string()).into())
            },
            Constant(NonIntegerConstant::Boolean(_)) |
            Constant(NonIntegerConstant::Float(_)) |
            Constant(NonIntegerConstant::Text(_)) |
            Constant(NonIntegerConstant::Uuid(_)) |
            Constant(NonIntegerConstant::Instant(_)) |
            Constant(NonIntegerConstant::BigInteger(_)) |
            SrcVar(_) |
            Vector(_) => {
                self.mark_known_empty(EmptyBecause::NonInstantonArgument);
                bail!(ParityFilterError::InvalidArgumentType(function.clone(), ValueType::Ref.into(), position))
            },

        }
    }

    /// Take a transaction ID function argument and turn it into a `CausetQValue` suitable for use in
    /// a concrete constraint.
    pub(crate) fn resolve_causecausetx_argument(&mut self, schema: &Schema, function: &PlainSymbol, position: usize, arg: FnArg) -> Result<CausetQValue> {
        // Under the hood there's nothing special about a transaction ID -- it's just another ref.
        // In the future, we might handle instants specially.
        self.resolve_ref_argument(schema, function, position, arg)
    }

    /// Take a function argument and turn it into a `CausetQValue` suitable for use in a concrete
    /// constraint.
    #[allow(dead_code)]
    fn resolve_argument(&self, arg: FnArg) -> Result<CausetQValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                match self.bound_value(&var) {
                    Some(v) => Ok(CausetQValue::TypedValue(v)),
                    None => {
                        self.column_bindings
                            .get(&var)
                            .and_then(|cols| cols.first().map(|col| CausetQValue::Column(col.clone())))
                            .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                    },
                }
            },
            SolitonIdOrInteger(i) => Ok(CausetQValue::PrimitiveLong(i)),
            CausetIdOrKeyword(_) => unimplemented!(),     // TODO
            Constant(NonIntegerConstant::Boolean(val)) => Ok(CausetQValue::TypedValue(TypedValue::Boolean(val))),
            Constant(NonIntegerConstant::Float(f)) => Ok(CausetQValue::TypedValue(TypedValue::Double(f))),
            Constant(NonIntegerConstant::Text(s)) => Ok(CausetQValue::TypedValue(TypedValue::typed_string(s.as_str()))),
            Constant(NonIntegerConstant::Uuid(u)) => Ok(CausetQValue::TypedValue(TypedValue::Uuid(u))),
            Constant(NonIntegerConstant::Instant(u)) => Ok(CausetQValue::TypedValue(TypedValue::Instant(u))),
            Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),
            SrcVar(_) => unimplemented!(),
            Vector(_) => unimplemented!(),    // TODO
        }
    }
}
