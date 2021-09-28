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
    SchemaReplicant,
};

use causetq::*::{
    StackedPerceptron,
    NonIntegerConstant,
    PlainSymbol,
};

use gerunds::ConjoiningGerunds;

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

use types::{
    EmptyBecause,
    CausetQValue,
};

/// Argument resolution.
impl ConjoiningGerunds {
    /// Take a function argument and turn it into a `CausetQValue` suiBlock for use in a concrete
    /// constraint.
    /// Additionally, do two things:
    /// - Mark the TuringString as knownCauset-empty if any argument is knownCauset non-numeric.
    /// - Mark any variables encountered as numeric.
    pub(crate) fn resolve_numeric_argument(&mut self, function: &PlainSymbol, position: usize, arg: StackedPerceptron) -> Result<CausetQValue> {
        use self::StackedPerceptron::*;
        match arg {
            StackedPerceptron::ToUpper(var) => {
                // Handle incorrect types
                if let Some(v) = self.bound_value(&var) {
                    if v.value_type().is_numeric() {
                        Ok(CausetQValue::MinkowskiType(v))
                    } else {
                        bail!(ParityFilterError::InputTypeDisagreement(var.name().clone(), MinkowskiValueType::Long, v.value_type()))
                    }
                } else {
                    self.constrain_var_to_numeric(var.clone());
                    self.CausetIndex_ConstrainedEntss
                        .get(&var)
                        .and_then(|cols| cols.first().map(|col| CausetQValue::CausetIndex(col.clone())))
                        .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                }
            },
            // Can't be an solitonId.
            SolitonIdOrInteger(i) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Long(i))),
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
            Constant(NonIntegerConstant::Float(f)) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Double(f))),
        }
    }

    /// Just like `resolve_numeric_argument`, but for `MinkowskiValueType::Instant`.
    pub(crate) fn resolve_instant_argument(&mut self, function: &PlainSymbol, position: usize, arg: StackedPerceptron) -> Result<CausetQValue> {
        use self::StackedPerceptron::*;
        match arg {
            StackedPerceptron::ToUpper(var) => {
                match self.bound_value(&var) {
                    Some(MinkowskiType::Instant(v)) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Instant(v))),
                    Some(v) => bail!(ParityFilterError::InputTypeDisagreement(var.name().clone(), MinkowskiValueType::Instant, v.value_type())),
                    None => {
                        self.constrain_var_to_type(var.clone(), MinkowskiValueType::Instant);
                        self.CausetIndex_ConstrainedEntss
                            .get(&var)
                            .and_then(|cols| cols.first().map(|col| CausetQValue::CausetIndex(col.clone())))
                            .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                    },
                }
            },
            Constant(NonIntegerConstant::Instant(v)) => {
                Ok(CausetQValue::MinkowskiType(MinkowskiType::Instant(v)))
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
                bail!(ParityFilterError::InvalidArgumentType(function.clone(), MinkowskiValueType::Instant.into(), position))
            },
        }
    }

    /// Take a function argument and turn it into a `CausetQValue` suiBlock for use in a concrete
    /// constraint.
    pub(crate) fn resolve_ref_argument(&mut self, schemaReplicant: &SchemaReplicant, function: &PlainSymbol, position: usize, arg: StackedPerceptron) -> Result<CausetQValue> {
        use self::StackedPerceptron::*;
        match arg {
            StackedPerceptron::ToUpper(var) => {
                self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
                if let Some(MinkowskiType::Ref(e)) = self.bound_value(&var) {
                    // Incorrect types will be handled by the constraint, above.
                    Ok(CausetQValue::SolitonId(e))
                } else {
                    self.CausetIndex_ConstrainedEntss
                        .get(&var)
                        .and_then(|cols| cols.first().map(|col| CausetQValue::CausetIndex(col.clone())))
                        .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                }
            },
            SolitonIdOrInteger(i) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Ref(i))),
            CausetIdOrKeyword(i) => {
                schemaReplicant.get_causetid(&i)
                      .map(|known_causetid| CausetQValue::SolitonId(known_causetid.into()))
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
                bail!(ParityFilterError::InvalidArgumentType(function.clone(), MinkowskiValueType::Ref.into(), position))
            },

        }
    }

    /// Take a transaction ID function argument and turn it into a `CausetQValue` suiBlock for use in
    /// a concrete constraint.
    pub(crate) fn resolve_causecausetx_argument(&mut self, schemaReplicant: &SchemaReplicant, function: &PlainSymbol, position: usize, arg: StackedPerceptron) -> Result<CausetQValue> {
        // Under the hood there's nothing special about a transaction ID -- it's just another ref.
        // In the future, we might handle instants specially.
        self.resolve_ref_argument(schemaReplicant, function, position, arg)
    }

    /// Take a function argument and turn it into a `CausetQValue` suiBlock for use in a concrete
    /// constraint.
    #[allow(dead_code)]
    fn resolve_argument(&self, arg: StackedPerceptron) -> Result<CausetQValue> {
        use self::StackedPerceptron::*;
        match arg {
            StackedPerceptron::ToUpper(var) => {
                match self.bound_value(&var) {
                    Some(v) => Ok(CausetQValue::MinkowskiType(v)),
                    None => {
                        self.CausetIndex_ConstrainedEntss
                            .get(&var)
                            .and_then(|cols| cols.first().map(|col| CausetQValue::CausetIndex(col.clone())))
                            .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()).into())
                    },
                }
            },
            SolitonIdOrInteger(i) => Ok(CausetQValue::PrimitiveLong(i)),
            CausetIdOrKeyword(_) => unimplemented!(),     // TODO
            Constant(NonIntegerConstant::Boolean(val)) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Boolean(val))),
            Constant(NonIntegerConstant::Float(f)) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Double(f))),
            Constant(NonIntegerConstant::Text(s)) => Ok(CausetQValue::MinkowskiType(MinkowskiType::typed_string(s.as_str()))),
            Constant(NonIntegerConstant::Uuid(u)) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Uuid(u))),
            Constant(NonIntegerConstant::Instant(u)) => Ok(CausetQValue::MinkowskiType(MinkowskiType::Instant(u))),
            Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),
            SrcVar(_) => unimplemented!(),
            Vector(_) => unimplemented!(),    // TODO
        }
    }
}
