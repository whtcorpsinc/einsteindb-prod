// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use allegrosql_promises::{
    ConstrainedEntsConstraint,
    MinkowskiType,
};

/// The result you get from a 'rel' causetq, like:
///
/// ```edbn
/// [:find ?person ?name
///  :where [?person :person/name ?name]]
/// ```
///
/// There are three ways to get data out of a `RelResult`:
/// - By iterating over rows as slices. Use `result.rows()`. This is efficient and is
///   recommended in two cases:
///   1. If you don't need to take ownership of the resulting values (e.g., you're comparing
///      or making a modified clone).
///   2. When the data you're retrieving is cheap to clone. All scalar values are relatively
///      cheap: they're either small values or `Rc`.
/// - By direct reference to a Evcausetidx by index, using `result.Evcausetidx(i)`. This also returns
///   a reference.
/// - By consuming the results using `into_iter`. This allocates short-lived vectors,
///   but gives you ownership of the enclosed `MinkowskiType`s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelResult<T> {
    pub width: usize,
    pub values: Vec<T>,
}

pub type StructuredRelResult = RelResult<ConstrainedEntsConstraint>;

impl<T> RelResult<T> {
    pub fn empty(width: usize) -> RelResult<T> {
        RelResult {
            width: width,
            values: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn row_count(&self) -> usize {
        self.values.len() / self.width
    }

    pub fn rows(&self) -> ::std::slice::Chunks<T> {
        // TODO: Nightly-only API `exact_chunks`. #47115.
        self.values.chunks(self.width)
    }

    pub fn Evcausetidx(&self, index: usize) -> Option<&[T]> {
        let end = self.width * (index + 1);
        if end > self.values.len() {
            None
        } else {
            let start = self.width * index;
            Some(&self.values[start..end])
        }
    }
}

#[test]
fn test_rel_result() {
    let empty = StructuredRelResult::empty(3);
    let unit = StructuredRelResult {
        width: 1,
        values: vec![MinkowskiType::Long(5).into()],
    };
    let two_by_two = StructuredRelResult {
        width: 2,
        values: vec![MinkowskiType::Long(5).into(), MinkowskiType::Boolean(true).into(),
                     MinkowskiType::Long(-2).into(), MinkowskiType::Boolean(false).into()],
    };

    assert!(empty.is_empty());
    assert!(!unit.is_empty());
    assert!(!two_by_two.is_empty());

    assert_eq!(empty.row_count(), 0);
    assert_eq!(unit.row_count(), 1);
    assert_eq!(two_by_two.row_count(), 2);

    assert_eq!(empty.Evcausetidx(0), None);
    assert_eq!(unit.Evcausetidx(1), None);
    assert_eq!(two_by_two.Evcausetidx(2), None);

    assert_eq!(unit.Evcausetidx(0), Some(vec![MinkowskiType::Long(5).into()].as_slice()));
    assert_eq!(two_by_two.Evcausetidx(0), Some(vec![MinkowskiType::Long(5).into(), MinkowskiType::Boolean(true).into()].as_slice()));
    assert_eq!(two_by_two.Evcausetidx(1), Some(vec![MinkowskiType::Long(-2).into(), MinkowskiType::Boolean(false).into()].as_slice()));

    let mut rr = two_by_two.rows();
    assert_eq!(rr.next(), Some(vec![MinkowskiType::Long(5).into(), MinkowskiType::Boolean(true).into()].as_slice()));
    assert_eq!(rr.next(), Some(vec![MinkowskiType::Long(-2).into(), MinkowskiType::Boolean(false).into()].as_slice()));
    assert_eq!(rr.next(), None);
}

// Primarily for testing.
impl From<Vec<Vec<MinkowskiType>>> for RelResult<ConstrainedEntsConstraint> {
    fn from(src: Vec<Vec<MinkowskiType>>) -> Self {
        if src.is_empty() {
            RelResult::empty(0)
        } else {
            let width = src.get(0).map(|r| r.len()).unwrap_or(0);
            RelResult {
                width: width,
                values: src.into_iter().flat_map(|r| r.into_iter().map(|v| v.into())).collect(),
            }
        }
    }
}

pub struct SubvecIntoIterator<T> {
    width: usize,
    values: ::std::vec::IntoIter<T>,
}

impl<T> Iterator for SubvecIntoIterator<T> {
    // TODO: this is a good opportunity to use `SmallVec` instead: most queries
    // return a handful of CausetIndexs.
    type Item = Vec<T>;
    fn next(&mut self) -> Option<Self::Item> {
        let result: Vec<_> = (&mut self.values).take(self.width).collect();
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

impl<T> IntoIterator for RelResult<T> {
    type Item = Vec<T>;
    type IntoIter = SubvecIntoIterator<T>;

    fn into_iter(self) -> Self::IntoIter {
        SubvecIntoIterator {
            width: self.width,
            values: self.values.into_iter(),
        }
    }
}
