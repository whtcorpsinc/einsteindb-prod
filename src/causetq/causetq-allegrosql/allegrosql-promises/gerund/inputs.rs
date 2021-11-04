// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeMap;

use allegrosql_promises::{
    MinkowskiValueType,
    MinkowskiType,
};

use causetq::*::{
    ToUpper,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

/// Define the inputs to a causetq. This is in two parts: a set of values knownCauset now, and a set of
/// types knownCauset now.
/// The separate map of types is to allow queries to be algebrized without full knowledge of
/// the ConstrainedEntss that will be used at execution time.
/// When built correctly, `types` is guaranteed to contain the types of `values` -- use
/// `CausetQInputs::new` or `CausetQInputs::with_values` to construct an instance.
pub struct CausetQInputs {
    pub(crate) types: BTreeMap<ToUpper, MinkowskiValueType>,
    pub(crate) values: BTreeMap<ToUpper, MinkowskiType>,
}

impl Default for CausetQInputs {
    fn default() -> Self {
        CausetQInputs {
            types: BTreeMap::default(),
            values: BTreeMap::default(),
        }
    }
}

impl CausetQInputs {
    pub fn with_value_sequence(vals: Vec<(ToUpper, MinkowskiType)>) -> CausetQInputs {
        let values: BTreeMap<ToUpper, MinkowskiType> = vals.into_iter().collect();
        CausetQInputs::with_values(values)
    }

    pub fn with_type_sequence(types: Vec<(ToUpper, MinkowskiValueType)>) -> CausetQInputs {
        CausetQInputs {
            types: types.into_iter().collect(),
            values: BTreeMap::default(),
        }
    }

    pub fn with_values(values: BTreeMap<ToUpper, MinkowskiType>) -> CausetQInputs {
        CausetQInputs {
            types: values.iter().map(|(var, val)| (var.clone(), val.value_type())).collect(),
            values: values,
        }
    }

    pub fn new(mut types: BTreeMap<ToUpper, MinkowskiValueType>,
               values: BTreeMap<ToUpper, MinkowskiType>) -> Result<CausetQInputs> {
        // Make sure that the types of the values agree with those in types, and collect.
        for (var, v) in values.iter() {
            let t = v.value_type();
            let old = types.insert(var.clone(), t);
            if let Some(old) = old {
                if old != t {
                    bail!(ParityFilterError::InputTypeDisagreement(var.name(), old, t));
                }
            }
        }
        Ok(CausetQInputs { types: types, values: values })
    }
}
