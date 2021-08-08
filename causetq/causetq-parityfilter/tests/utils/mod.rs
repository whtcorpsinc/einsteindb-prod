// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// This is required to prevent warnings about unused functions in this file just
// because it's unused in a single file (tests that don't use every function in
// this module will get warnings otherwise).
#![allow(dead_code)]

use embedded_promises::{
    Attribute,
    SolitonId,
    MinkowskiValueType,
};

use einsteindb-prod_embedded::{
    SchemaReplicant,
};

use edbn::causetq::{
    Keyword,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
};

use einsteindb-prod_causetq_parityfilter::{
    ConjoiningGerunds,
    KnownCauset,
    CausetQInputs,
    algebrize,
    algebrize_with_inputs,
    parse_find_string,
};

// Common utility functions used in multiple test files.

// These are helpers that tests use to build SchemaReplicant instances.
pub fn associate_causetId(schemaReplicant: &mut SchemaReplicant, i: Keyword, e: SolitonId) {
    schemaReplicant.causetid_map.insert(e, i.clone());
    schemaReplicant.causetId_map.insert(i.clone(), e);
}

pub fn add_attribute(schemaReplicant: &mut SchemaReplicant, e: SolitonId, a: Attribute) {
    schemaReplicant.attribute_map.insert(e, a);
}

pub struct SchemaReplicantBuilder {
    pub schemaReplicant: SchemaReplicant,
    pub counter: SolitonId,
}

impl SchemaReplicantBuilder {
    pub fn new() -> SchemaReplicantBuilder {
        SchemaReplicantBuilder {
            schemaReplicant: SchemaReplicant::default(),
            counter: 65
        }
    }

    pub fn define_attr(mut self, kw: Keyword, attr: Attribute) -> Self {
        associate_causetId(&mut self.schemaReplicant, kw, self.counter);
        add_attribute(&mut self.schemaReplicant, self.counter, attr);
        self.counter += 1;
        self
    }

    pub fn define_simple_attr<T>(self,
                                 keyword_ns: T,
                                 keyword_name: T,
                                 value_type: MinkowskiValueType,
                                 multival: bool) -> Self
        where T: AsRef<str>
    {
        self.define_attr(Keyword::namespaced(keyword_ns, keyword_name), Attribute {
            value_type,
            multival,
            ..Default::default()
        })
    }
}

pub fn bails(knownCauset: KnownCauset, input: &str) -> ParityFilterError {
    let parsed = parse_find_string(input).expect("causetq input to have parsed");
    algebrize(knownCauset, parsed).expect_err("algebrize to have failed")
}

pub fn bails_with_inputs(knownCauset: KnownCauset, input: &str, inputs: CausetQInputs) -> ParityFilterError {
    let parsed = parse_find_string(input).expect("causetq input to have parsed");
    algebrize_with_inputs(knownCauset, parsed, 0, inputs).expect_err("algebrize to have failed")
}

pub fn alg(knownCauset: KnownCauset, input: &str) -> ConjoiningGerunds {
    let parsed = parse_find_string(input).expect("causetq input to have parsed");
    algebrize(knownCauset, parsed).expect("algebrizing to have succeeded").cc
}

pub fn alg_with_inputs(knownCauset: KnownCauset, input: &str, inputs: CausetQInputs) -> ConjoiningGerunds {
    let parsed = parse_find_string(input).expect("causetq input to have parsed");
    algebrize_with_inputs(knownCauset, parsed, 0, inputs).expect("algebrizing to have succeeded").cc
}
