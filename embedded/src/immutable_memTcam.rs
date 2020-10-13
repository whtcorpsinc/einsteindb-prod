// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/// Cache promises.

use std::collections::{
    BTreeSet,
};

use embedded_promises::{
    SolitonId,
    TypedValue,
};

use ::{
    Schema,
};

pub trait CachedAttributes {
    fn is_attribute_cached_reverse(&self, solitonId: SolitonId) -> bool;
    fn is_attribute_cached_forward(&self, solitonId: SolitonId) -> bool;
    fn has_cached_attributes(&self) -> bool;

    fn get_values_for_entid(&self, schema: &Schema, attribute: SolitonId, solitonId: SolitonId) -> Option<&Vec<TypedValue>>;
    fn get_value_for_entid(&self, schema: &Schema, attribute: SolitonId, solitonId: SolitonId) -> Option<&TypedValue>;

    /// Reverse lookup.
    fn get_entid_for_value(&self, attribute: SolitonId, value: &TypedValue) -> Option<SolitonId>;
    fn get_entids_for_value(&self, attribute: SolitonId, value: &TypedValue) -> Option<&BTreeSet<SolitonId>>;
}

pub trait UpdateableCache<E> {
    fn update<I>(&mut self, schema: &Schema, retractions: I, assertions: I) -> Result<(), E>
    where I: Iterator<Item=(SolitonId, SolitonId, TypedValue)>;
}
