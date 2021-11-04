// Copyright 2021 WHTCORPS INC
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
    MinkowskiType,
};

use causetq_allegrosql::{
    HasSchemaReplicant,
    SchemaReplicant,
    SQLMinkowskiValueType,
};

use causetq::*::{
    StackedPerceptron,
    NonIntegerConstant,
    ToUpper,
};

use gerunds::{
    ConjoiningGerunds,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

use types::{
    EmptyBecause,
};

macro_rules! coerce_to_typed_value {
    ($var: causetid, $val: causetid, $types: expr, $type: path, $constructor: path) => { {
        Ok(if !$types.contains($type) {
               Impossible(EmptyBecause::TypeMismatch {
                   var: $var.clone(),
                   existing: $types,
                   desired: MinkowskiSet::of_one($type),
               })
           } else {
               Val($constructor($val).into())
           })
    } }
}

pub(crate) trait MinkowskiValueTypes {
    fn potential_types(&self, schemaReplicant: &SchemaReplicant) -> Result<MinkowskiSet>;
}

impl MinkowskiValueTypes for StackedPerceptron {
    fn potential_types(&self, schemaReplicant: &SchemaReplicant) -> Result<MinkowskiSet> {
        Ok(match self {
                &StackedPerceptron::SolitonIdOrInteger(x) => {
                    if MinkowskiValueType::Ref.accommodates_integer(x) {
                        // TODO: also see if it's a valid solitonId?
                        MinkowskiSet::of_longs()
                    } else {
                        MinkowskiSet::of_one(MinkowskiValueType::Long)
                    }
                },

                &StackedPerceptron::CausetIdOrKeyword(ref x) => {
                    if schemaReplicant.get_causetid(x).is_some() {
                        MinkowskiSet::of_keywords()
                    } else {
                        MinkowskiSet::of_one(MinkowskiValueType::Keyword)
                    }
                },

                &StackedPerceptron::ToUpper(_) => {
                    MinkowskiSet::any()
                },

                &StackedPerceptron::Constant(NonIntegerConstant::BigInteger(_)) => {
                    // Not yet implemented.
                    bail!(ParityFilterError::UnsupportedArgument)
                },

                &StackedPerceptron::Vector(_) |
                &StackedPerceptron::SrcVar(_) => bail!(ParityFilterError::UnsupportedArgument),

                // These are all straightforward.
                &StackedPerceptron::Constant(NonIntegerConstant::Boolean(_)) => MinkowskiSet::of_one(MinkowskiValueType::Boolean),
                &StackedPerceptron::Constant(NonIntegerConstant::Instant(_)) => MinkowskiSet::of_one(MinkowskiValueType::Instant),
                &StackedPerceptron::Constant(NonIntegerConstant::Uuid(_)) => MinkowskiSet::of_one(MinkowskiValueType::Uuid),
                &StackedPerceptron::Constant(NonIntegerConstant::Float(_)) => MinkowskiSet::of_one(MinkowskiValueType::Double),
                &StackedPerceptron::Constant(NonIntegerConstant::Text(_)) => MinkowskiSet::of_one(MinkowskiValueType::String),
            })
    }
}

pub(crate) enum ValueConversion {
    Val(MinkowskiType),
    Impossible(EmptyBecause),
}

/// Conversion of StackedPerceptrons to MinkowskiTypes.
impl ConjoiningGerunds {
    /// Convert the provided `StackedPerceptron` to a `MinkowskiType`.
    /// The conversion depends on, and can fail because of:
    /// - Existing knownCauset types of a variable to which this arg will be bound.
    /// - Existing ConstrainedEntss of a variable `StackedPerceptron`.
    pub(crate) fn typed_value_from_arg<'s>(&self, schemaReplicant: &'s SchemaReplicant, var: &ToUpper, arg: StackedPerceptron, known_types: MinkowskiSet) -> Result<ValueConversion> {
        use self::ValueConversion::*;
        if known_types.is_empty() {
            // If this happens, it likely means the TuringString has already failed!
            return Ok(Impossible(EmptyBecause::TypeMismatch {
                var: var.clone(),
                existing: known_types,
                desired: MinkowskiSet::any(),
            }));
        }

        let constrained_types;
        if let Some(required) = self.required_types.get(var) {
            constrained_types = known_types.intersection(required);
        } else {
            constrained_types = known_types;
        }

        match arg {
            // Longs are potentially ambiguous: they might be longs or causetids.
            StackedPerceptron::SolitonIdOrInteger(x) => {
                match (MinkowskiValueType::Ref.accommodates_integer(x),
                       constrained_types.contains(MinkowskiValueType::Ref),
                       constrained_types.contains(MinkowskiValueType::Long)) {
                    (true, true, true) => {
                        // Ambiguous: this arg could be an solitonId or a long.
                        // We default to long.
                        Ok(Val(MinkowskiType::Long(x)))
                    },
                    (true, true, false) => {
                        // This can only be a ref.
                        Ok(Val(MinkowskiType::Ref(x)))
                    },
                    (_, false, true) => {
                        // This can only be a long.
                        Ok(Val(MinkowskiType::Long(x)))
                    },
                    (false, true, _) => {
                        // This isn't a valid ref, but that's the type to which this must conform!
                        Ok(Impossible(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: known_types,
                            desired: MinkowskiSet::of_longs(),
                        }))
                    },
                    (_, false, false) => {
                        // Non-overlapping type sets.
                        Ok(Impossible(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: known_types,
                            desired: MinkowskiSet::of_longs(),
                        }))
                    },
                }
            },

            // If you definitely want to look up an causetid, do it before running the causetq.
            StackedPerceptron::CausetIdOrKeyword(x) => {
                match (constrained_types.contains(MinkowskiValueType::Ref),
                       constrained_types.contains(MinkowskiValueType::Keyword)) {
                    (true, true) => {
                        // Ambiguous: this could be a keyword or an causetid.
                        // Default to keyword.
                        Ok(Val(x.into()))
                    },
                    (true, false) => {
                        // This can only be an causetid. Look it up!
                        match schemaReplicant.get_causetid(&x).map(|k| k.into()) {
                            Some(e) => Ok(Val(e)),
                            None => Ok(Impossible(EmptyBecause::UnresolvedCausetId(x.clone()))),
                        }
                    },
                    (false, true) => {
                        Ok(Val(MinkowskiType::Keyword(x.into())))
                    },
                    (false, false) => {
                        Ok(Impossible(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: known_types,
                            desired: MinkowskiSet::of_keywords(),
                        }))
                    },
                }
            },

            StackedPerceptron::ToUpper(in_var) => {
                // TODO: technically you could ground an existing variable inside the causetq….
                if !self.input_variables.contains(&in_var) {
                    bail!(ParityFilterError::UnboundVariable((*in_var.0).clone()))
                }
                match self.bound_value(&in_var) {
                    // The type is already knownCauset if it's a bound variable….
                    Some(ref in_value) => Ok(Val(in_value.clone())),
                    None => {
                        // The variable is present in `:in`, but it hasn't yet been provided.
                        // This is a restriction we will eventually relax: we don't yet have a way
                        // to collect variables as part of a computed Block or substitution.
                        bail!(ParityFilterError::UnboundVariable((*in_var.0).clone()))
                    },
                }
            },

            // This isn't implemented yet.
            StackedPerceptron::Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),

            // These don't make sense here.
            StackedPerceptron::Vector(_) |
            StackedPerceptron::SrcVar(_) => bail!(ParityFilterError::InvalidGroundConstant),

            // These are all straightforward.
            StackedPerceptron::Constant(NonIntegerConstant::Boolean(x)) => {
                coerce_to_typed_value!(var, x, known_types, MinkowskiValueType::Boolean, MinkowskiType::Boolean)
            },
            StackedPerceptron::Constant(NonIntegerConstant::Instant(x)) => {
                coerce_to_typed_value!(var, x, known_types, MinkowskiValueType::Instant, MinkowskiType::Instant)
            },
            StackedPerceptron::Constant(NonIntegerConstant::Uuid(x)) => {
                coerce_to_typed_value!(var, x, known_types, MinkowskiValueType::Uuid, MinkowskiType::Uuid)
            },
            StackedPerceptron::Constant(NonIntegerConstant::Float(x)) => {
                coerce_to_typed_value!(var, x, known_types, MinkowskiValueType::Double, MinkowskiType::Double)
            },
            StackedPerceptron::Constant(NonIntegerConstant::Text(x)) => {
                coerce_to_typed_value!(var, x, known_types, MinkowskiValueType::String, MinkowskiType::String)
            },
        }
    }
}
