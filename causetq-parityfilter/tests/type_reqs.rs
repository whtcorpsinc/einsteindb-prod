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
extern crate embedded_promises;
extern crate einsteindb_embedded;
extern crate einsteindb_causetq_parityfilter;
extern crate causetq_parityfilter_promises;

mod utils;

use utils::{
    alg,
    SchemaBuilder,
    bails,
};

use embedded_promises::{
    ValueType,
};

use einsteindb_embedded::{
    Schema,
};

use einsteindb_causetq_parityfilter::Known;

fn prepopulated_schema() -> Schema {
    SchemaBuilder::new()
        .define_simple_attr("test", "boolean", ValueType::Boolean, false)
        .define_simple_attr("test", "long", ValueType::Long, false)
        .define_simple_attr("test", "double", ValueType::Double, false)
        .define_simple_attr("test", "string", ValueType::String, false)
        .define_simple_attr("test", "keyword", ValueType::Keyword, false)
        .define_simple_attr("test", "uuid", ValueType::Uuid, false)
        .define_simple_attr("test", "instant", ValueType::Instant, false)
        .define_simple_attr("test", "ref", ValueType::Ref, false)
        .schema
}

#[test]
fn test_empty_known() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    for known_type in ValueType::all_enums().iter() {
        for required in ValueType::all_enums().iter() {
            let q = format!("[:find ?e :where [?e :test/{} ?v] [(type ?v {})]]",
                            known_type.into_keyword().name(), required);
            println!("CausetQ: {}", q);
            let cc = alg(known, &q);
            // It should only be empty if the known type and our requirement differ.
            assert_eq!(cc.empty_because.is_some(), known_type != required,
                       "known_type = {}; required = {}", known_type, required);
        }
    }
}

#[test]
fn test_multiple() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let q = "[:find ?e :where [?e _ ?v] [(type ?v :edb.type/long)] [(type ?v :edb.type/double)]]";
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_unbound() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    bails(known, "[:find ?e :where [(type ?e :edb.type/string)]]");
}
