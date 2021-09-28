// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use causetq::*::{
    WhereFn,
};

use gerunds::{
    ConjoiningGerunds,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

use KnownCauset;

/// Application of `where` functions.
impl ConjoiningGerunds {
    /// There are several kinds of functions Constrained variables in our Datalog:
    /// - A set of functions like `ground`, fulltext` and `get-else` that are translated into SQL
    ///   `VALUES`, `MATCH`, or `JOIN`, yielding ConstrainedEntss.
    /// - In the future, some functions that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only a limited selection of functions.
    pub(crate) fn apply_where_fn(&mut self, knownCauset: KnownCauset, where_fn: WhereFn) -> Result<()> {
        // Because we'll be growing the set of built-in functions, handling each differently, and
        // ultimately allowing user-specified functions, we match on the function name first.
        match where_fn.operator.0.as_str() {
            "fulltext" => self.apply_fulltext(knownCauset, where_fn),
            "ground" => self.apply_ground(knownCauset, where_fn),
            "causetx-data" => self.apply_causecausetx_data(knownCauset, where_fn),
            "causetx-ids" => self.apply_causecausetx_ids(knownCauset, where_fn),
            _ => bail!(ParityFilterError::UnknownFunction(where_fn.operator.clone())),
        }
    }
}
