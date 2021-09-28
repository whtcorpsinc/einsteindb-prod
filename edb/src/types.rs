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

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};
use std::iter::{
    FromIterator,
};
use std::ops::{
    Deref,
    DerefMut,
    Range,
};

extern crate causetq_allegrosql;

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
    MinkowskiValueType,
};

pub use self::causetq_allegrosql::{
    DateTime,
    SchemaReplicant,
    Utc,
};

use edbn::entities::{
    InstantonPlace,
    TempId,
};

use causetq_pull_promises::errors as errors;

/// Represents one partition of the solitonId space.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
#[cfg_attr(feature = "syncable", derive(Serialize,Deserialize))]
pub struct Partition {
    /// The first solitonId in the partition.
    pub start: SolitonId,
    /// Maximum allowed solitonId in the partition.
    pub end: SolitonId,
    /// `true` if causetids in the partition can be excised with `:edb/excise`.
    pub allow_excision: bool,
    /// The next solitonId to be allocated in the partition.
    /// Unless you must use this directly, prefer using provided setter and getter helpers.
    pub(crate) next_causetid_to_allocate: SolitonId,
}

impl Partition {
    pub fn new(start: SolitonId, end: SolitonId, next_causetid_to_allocate: SolitonId, allow_excision: bool) -> Partition {
        assert!(
            start <= next_causetid_to_allocate && next_causetid_to_allocate <= end,
            "A partition represents a monotonic increasing sequence of causetids."
        );
        Partition { start, end, next_causetid_to_allocate, allow_excision }
    }

    pub fn contains_causetid(&self, e: SolitonId) -> bool {
        (e >= self.start) && (e < self.next_causetid_to_allocate)
    }

    pub fn allows_causetid(&self, e: SolitonId) -> bool {
        (e >= self.start) && (e <= self.end)
    }

    pub fn next_causetid(&self) -> SolitonId {
        self.next_causetid_to_allocate
    }

    pub fn set_next_causetid(&mut self, e: SolitonId) {
        assert!(self.allows_causetid(e), "Partition index must be within its allocated space.");
        self.next_causetid_to_allocate = e;
    }

    pub fn allocate_causetids(&mut self, n: usize) -> Range<i64> {
        let idx = self.next_causetid();
        self.set_next_causetid(idx + n as i64);
        idx..self.next_causetid()
    }
}

/// Map partition names to `Partition` instances.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialOrd, PartialEq)]
#[cfg_attr(feature = "syncable", derive(Serialize,Deserialize))]
pub struct PartitionMap(BTreeMap<String, Partition>);

impl Deref for PartitionMap {
    type Target = BTreeMap<String, Partition>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PartitionMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromIterator<(String, Partition)> for PartitionMap {
    fn from_iter<T: IntoIterator<Item=(String, Partition)>>(iter: T) -> Self {
        PartitionMap(iter.into_iter().collect())
    }
}

/// Represents the spacetime required to causetq from, or apply bundles to, a EinsteinDB store.
///
/// See https://github.com/whtcorpsinc/edb/wiki/Thoughts:-modeling-edb-conn-in-Rust.
#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct EDB {
    /// Map partition name->`Partition`.
    ///
    /// TODO: represent partitions as causetids.
    pub partition_map: PartitionMap,

    /// The schemaReplicant of the store.
    pub schemaReplicant: SchemaReplicant,
}

impl EDB {
    pub fn new(partition_map: PartitionMap, schemaReplicant: SchemaReplicant) -> EDB {
        EDB {
            partition_map: partition_map,
            schemaReplicant: schemaReplicant
        }
    }
}

/// A pair [a v] in the store.
///
/// Used to represent lookup-refs and [TEMPID a v] upserts as they are resolved.
pub type AVPair = (SolitonId, MinkowskiType);

/// Used to represent assertions and retractions.
pub(crate) type EAV = (SolitonId, SolitonId, MinkowskiType);

/// Map [a v] pairs to existing causetids.
///
/// Used to resolve lookup-refs and upserts.
pub type AVMap<'a> = HashMap<&'a AVPair, SolitonId>;

// represents a set of causetids that are correspond to attributes
pub type AttributeSet = BTreeSet<SolitonId>;

/// The transactor is tied to `edbn::ValueAndSpan` right now, but in the future we'd like to support
/// `MinkowskiType` directly for programmatic use.  `TransacBlockValue` encapsulates the interface
/// value types (i.e., values in the value place) need to support to be transacted.
pub trait TransacBlockValue: Clone {
    /// Coerce this value place into the given type.  This is where we perform schemaReplicant-aware
    /// coercion, for example coercing an integral value into a ref where appropriate.
    fn into_typed_value(self, schemaReplicant: &SchemaReplicant, value_type: MinkowskiValueType) -> errors::Result<MinkowskiType>;

    /// Make an instanton place out of this value place.  This is where we limit values in nested maps
    /// to valid instanton places.
    fn into_instanton_place(self) -> errors::Result<InstantonPlace<Self>>;

    fn as_tempid(&self) -> Option<TempId>;
}

#[cfg(test)]
mod tests {
    use super::Partition;

    #[test]
    #[should_panic(expected = "A partition represents a monotonic increasing sequence of causetids.")]
    fn test_partition_limits_sanity1() {
        Partition::new(100, 1000, 1001, true);
    }

    #[test]
    #[should_panic(expected = "A partition represents a monotonic increasing sequence of causetids.")]
    fn test_partition_limits_sanity2() {
        Partition::new(100, 1000, 99, true);
    }

    #[test]
    #[should_panic(expected = "Partition index must be within its allocated space.")]
    fn test_partition_limits_boundary1() {
        let mut part = Partition::new(100, 1000, 100, true);
        part.set_next_causetid(2000);
    }

    #[test]
    #[should_panic(expected = "Partition index must be within its allocated space.")]
    fn test_partition_limits_boundary2() {
        let mut part = Partition::new(100, 1000, 100, true);
        part.set_next_causetid(1001);
    }

    #[test]
    #[should_panic(expected = "Partition index must be within its allocated space.")]
    fn test_partition_limits_boundary3() {
        let mut part = Partition::new(100, 1000, 100, true);
        part.set_next_causetid(99);
    }

    #[test]
    #[should_panic(expected = "Partition index must be within its allocated space.")]
    fn test_partition_limits_boundary4() {
        let mut part = Partition::new(100, 1000, 100, true);
        part.set_next_causetid(-100);
    }

    #[test]
    #[should_panic(expected = "Partition index must be within its allocated space.")]
    fn test_partition_limits_boundary5() {
        let mut part = Partition::new(100, 1000, 100, true);
        part.allocate_causetids(901); // One more than allowed.
    }

    #[test]
    fn test_partition_limits_boundary6() {
        let mut part = Partition::new(100, 1000, 100, true);
        part.set_next_causetid(100); // First solitonId that's allowed.
        part.set_next_causetid(101); // Just after first.

        assert_eq!(101..111, part.allocate_causetids(10));

        part.set_next_causetid(1000); // Last solitonId that's allowed.
        part.set_next_causetid(999); // Just before last.
    }
}
