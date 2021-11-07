// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate *;


mod utils;

use allegrosql_promises::{
    Attribute,
    MinkowskiValueType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use causetq::*::{
    Keyword,
};

use utils::{
    add_attribute,
    alg,
    associate_causetId,
};

use edb_causetq_parityfilter::KnownCauset;

fn prepopulated_schemaReplicant() -> SchemaReplicant {
    let mut schemaReplicant = SchemaReplicant::default();
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "name"), 65);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "description"), 66);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "parent"), 67);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "age"), 68);
    associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "height"), 69);
    add_attribute(&mut schemaReplicant, 65, Attribute {
        value_type: MinkowskiValueType::String,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 66, Attribute {
        value_type: MinkowskiValueType::String,
        index: true,
        fulltext: true,
        multival: true,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 67, Attribute {
        value_type: MinkowskiValueType::String,
        multival: true,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 68, Attribute {
        value_type: MinkowskiValueType::Long,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schemaReplicant, 69, Attribute {
        value_type: MinkowskiValueType::Long,
        multival: false,
        ..Default::default()
    });
    schemaReplicant
}

#[test]
fn test_apply_fulltext() {
    let schemaReplicant = prepopulated_schemaReplicant();
    let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

    // If you use a non-FTS attribute, we will short-circuit.
    let causetq = r#"[:find ?val
                    :where [(fulltext $ :foo/name "hello") [[?instanton ?val _ _]]]]"#;
    assert!(alg(knownCauset, causetq).is_known_empty());

    // If you get a type mismatch, we will short-circuit.
    let causetq = r#"[:find ?val
                    :where [(fulltext $ :foo/description "hello") [[?instanton ?val ?causetx ?sallegro]]]
                    [?sallegro :foo/bar _]]"#;
    assert!(alg(knownCauset, causetq).is_known_empty());
}
