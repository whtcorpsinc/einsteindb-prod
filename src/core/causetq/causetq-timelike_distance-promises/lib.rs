// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate rusqlite;

#[macro_use]
extern crate allegrosql_promises;
extern crate causetq_pull_promises;
extern crate edbn;
extern crate causetq_pull_promises;

// TODO we only want to import a *_promises here, this is a smell.
extern crate edb_causetq_parityfilter;
extern crate edb_causetq_sql;

pub mod errors;
pub mod aggregates;

