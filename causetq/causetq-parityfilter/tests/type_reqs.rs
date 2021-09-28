// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edbn;
extern crate allegrosql_promises;
extern crate causetq_allegrosql;
extern crate edb_causetq_parityfilter;
extern crate causetq_parityfilter_promises;

mod utils;

use utils::{
    alg,
    SchemaReplicantBuilder,
    bails,
};

use allegrosql_promises::{
    MinkowskiValueType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use edb_causetq_parityfilter::KnownCauset;

fn prepopulated_schemaReplicant() -> SchemaReplicant {
    SchemaReplicantBuilder::new()
        .define_simple_attr("test", "boolean", MinkowskiValueType::Boolean, false)
        .define_simple_attr("test", "long", MinkowskiValueType::Long, false)
        .define_simple_attr("test", "double", MinkowskiValueType::Double, false)
        .define_simple_attr("test", "string", MinkowskiValueType::String, false)
        .define_simple_attr("test", "keyword", MinkowskiValueType::Keyword, false)
        .define_simple_attr("test", "uuid", MinkowskiValueType::Uuid, false)
        .define_simple_attr("test", "instant", MinkowskiValueType::Instant, false)
        .define_simple_attr("test", "ref", MinkowskiValueType::Ref, false)
        .schemaReplicant
}

#[test]
fn test_empty_known() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    for known_type in MinkowskiValueType::all_enums().iter() {
        for required in MinkowskiValueType::all_enums().iter() {
            let q = format!("[:find ?e :where [?e :test/{} ?v] [(type ?v {})]]",
                            known_type.into_keyword().name(), required);
            println!("CausetQ: {}", q);
            let cc = alg(knownCauset, &q);
            // It should only be empty if the knownCauset type and our requirement differ.
            assert_eq!(cc.empty_because.is_some(), known_type != required,
                       "known_type = {}; required = {}", known_type, required);
        }
    }
}

#[test]
fn test_multiple() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    let q = "[:find ?e :where [?e _ ?v] [(type ?v :edb.type/long)] [(type ?v :edb.type/double)]]";
    let cc = alg(knownCauset, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_unbound() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
    bails(knownCauset, "[:find ?e :where [(type ?e :edb.type/string)]]");
}
