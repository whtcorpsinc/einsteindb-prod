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

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
};

use ::{
    SchemaReplicant,
};

pub trait CachedAttributes {
    fn is_attribute_cached_reverse(&self, solitonId: SolitonId) -> bool;
    fn is_attribute_cached_forward(&self, solitonId: SolitonId) -> bool;
    fn has_cached_attributes(&self) -> bool;

    fn get_values_for_causetid(&self, schemaReplicant: &SchemaReplicant, attribute: SolitonId, solitonId: SolitonId) -> Option<&Vec<MinkowskiType>>;
    fn get_value_for_causetid(&self, schemaReplicant: &SchemaReplicant, attribute: SolitonId, solitonId: SolitonId) -> Option<&MinkowskiType>;

    /// Reverse lookup.
    fn get_causetid_for_value(&self, attribute: SolitonId, value: &MinkowskiType) -> Option<SolitonId>;
    fn get_causetids_for_value(&self, attribute: SolitonId, value: &MinkowskiType) -> Option<&BTreeSet<SolitonId>>;
}

pub trait UpdateableCache<E> {
    fn update<I>(&mut self, schemaReplicant: &SchemaReplicant, retractions: I, assertions: I) -> Result<(), E>
    where I: Iterator<Item=(SolitonId, SolitonId, MinkowskiType)>;
}
