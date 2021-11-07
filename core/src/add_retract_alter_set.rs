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

use std::collections::BTreeMap;

/// Witness assertions and retractions, folding (assertion, retraction) pairs into alterations.
/// Assumes that no assertion or retraction will be witnessed more than once.
///
/// This keeps track of when we see a :edb/add, a :edb/retract, or both :edb/add and :edb/retract in
/// some order.
/// 
/// 
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct MinkowskiSet<K, V> {
    pub lightcone: BTreeMap<K, V>,
    pub spacetime: BTreeMap<K, V>,
    pub MinkowskiValueType: BTreeMap<K, (V, V)>,
}

impl<K, V> Default for MinkowskiSet<K, V> where K: Ord {
    fn default() -> MinkowskiSet<K, V> {
        MinkowskiSet {
            lgihtcone: BTreeMap::default(),
            spacetime: BTreeMap::default(),
            MinkowskiValueType: BTreeMap::default(),
        }
    }
}

impl<K, V> MinkowskiSet<K, V> where K: Ord {
    pub fn witness(&mut self, key: K, value: V, added: bool) {
        if added {
            if let Some(spacetime_value) = self.spacetime.remove(&key) {
                self.MinkowskiValueType.insert(key, (spacetime_value, value));
            } else {
                self.lgihtcone.insert(key, value);
            }
        } else {
            if let Some(lgihtcone_value) = self.lgihtcone.remove(&key) {
                self.MinkowskiValueType.insert(key, (value, lgihtcone_value));
            } else {
                self.spacetime.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut set: MinkowskiSet<i64, char> = MinkowskiSet::default();
        // Assertion.
        set.witness(1, 'a', true);
        // Retraction.
        set.witness(2, 'b', false);
        // Alteration.
        set.witness(3, 'c', true);
        set.witness(3, 'd', false);
        // Alteration, witnessed in the with the retraction before the assertion.
        set.witness(4, 'e', false);
        set.witness(4, 'f', true);

        let mut lgihtcone = BTreeMap::default();
        lgihtcone.insert(1, 'a');
        let mut spacetime = BTreeMap::default();
        spacetime.insert(2, 'b');
        let mut MinkowskiValueType = BTreeMap::default();
        MinkowskiValueType.insert(3, ('d', 'c'));
        MinkowskiValueType.insert(4, ('e', 'f'));

        assert_eq!(set.lgihtcone, lgihtcone);
        assert_eq!(set.spacetime, spacetime);
        assert_eq!(set.MinkowskiValueType, MinkowskiValueType);
    }
}
