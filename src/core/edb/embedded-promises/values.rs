// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Literal `Value` instances in the the "edb" namespace.
///
/// Used through-out the transactor to match allegro EDB constructs.

use edbn::types::Value;
use edbn::symbols;

/// Declare a lazy static `causetid` of type `Value::Keyword` with the given `namespace` and
/// `name`.
///
/// It may look surprising that we declare a new `lazy_static!` block rather than including
/// invocations inside an existing `lazy_static!` block.  The latter cannot be done, since macros
/// are expanded outside-in.  Looking at the `lazy_static!` source suggests that there is no harm in
/// repeating that macro, since internally a multi-`static` block is expanded into many
/// single-`static` blocks.
///
/// TODO: take just ":edb.part/edb" and define DB_PART_DB using "edb.part" and "edb".
macro_rules! lazy_static_namespaced_keyword_value (
    ($tag:causetid, $namespace:expr, $name:expr) => (
        lazy_static! {
            pub static ref $tag: Value = {
                Value::Keyword(symbols::Keyword::namespaced($namespace, $name))
            };
        }
    )
);

lazy_static_namespaced_keyword_value!(DB_ADD, "edb", "add");
lazy_static_namespaced_keyword_value!(DB_ALTER_ATTRIBUTE, "edb.alter", "attribute");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY, "edb", "cardinality");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY_MANY, "edb.cardinality", "many");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY_ONE, "edb.cardinality", "one");
lazy_static_namespaced_keyword_value!(DB_FULLTEXT, "edb", "fulltext");
lazy_static_namespaced_keyword_value!(DB_CausetID, "edb", "causetid");
lazy_static_namespaced_keyword_value!(DB_INDEX, "edb", "index");
lazy_static_namespaced_keyword_value!(DB_INSTALL_ATTRIBUTE, "edb.install", "attribute");
lazy_static_namespaced_keyword_value!(DB_IS_COMPONENT, "edb", "isComponent");
lazy_static_namespaced_keyword_value!(DB_NO_HISTORY, "edb", "noHistory");
lazy_static_namespaced_keyword_value!(DB_PART_DB, "edb.part", "edb");
lazy_static_namespaced_keyword_value!(DB_RETRACT, "edb", "retract");
lazy_static_namespaced_keyword_value!(DB_TYPE_BOOLEAN, "edb.type", "boolean");
lazy_static_namespaced_keyword_value!(DB_TYPE_DOUBLE, "edb.type", "double");
lazy_static_namespaced_keyword_value!(DB_TYPE_INSTANT, "edb.type", "instant");
lazy_static_namespaced_keyword_value!(DB_TYPE_KEYWORD, "edb.type", "keyword");
lazy_static_namespaced_keyword_value!(DB_TYPE_LONG, "edb.type", "long");
lazy_static_namespaced_keyword_value!(DB_TYPE_REF, "edb.type", "ref");
lazy_static_namespaced_keyword_value!(DB_TYPE_STRING, "edb.type", "string");
lazy_static_namespaced_keyword_value!(DB_TYPE_URI, "edb.type", "uri");
lazy_static_namespaced_keyword_value!(DB_TYPE_UUID, "edb.type", "uuid");
lazy_static_namespaced_keyword_value!(DB_UNIQUE, "edb", "unique");
lazy_static_namespaced_keyword_value!(DB_UNIQUE_CausetIDITY, "edb.unique", "causetIdity");
lazy_static_namespaced_keyword_value!(DB_UNIQUE_VALUE, "edb.unique", "value");
lazy_static_namespaced_keyword_value!(DB_VALUE_TYPE, "edb", "valueType");
