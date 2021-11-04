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
};

use causetq_projector_promises::errors::{
    ProjectorError,
    Result,
};

/// A `ConstrainedEntsConstraintTuple` is any type that can accommodate a EinsteinDB tuple causetq result of fixed length.
///
/// Currently Rust tuples of length 1 through 6 (i.e., `(A)` through `(A, B, C, D, E, F)`) are
/// supported as are vectors (i.e., `Vec<>`).
pub trait ConstrainedEntsConstraintTuple: Sized {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>>;
}

// This is a no-op, essentially: we can always produce a vector representation of a tuple result.
impl ConstrainedEntsConstraintTuple for Vec<ConstrainedEntsConstraint> {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>> {
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    Ok(Some(vec))
                }
            },
        }
    }
}

// TODO: generate these repetitive impleedbions with a little macro.
impl ConstrainedEntsConstraintTuple for (ConstrainedEntsConstraint,) {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>> {
        if expected != 1 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(1, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(),)))
                }
            }
        }
    }
}

impl ConstrainedEntsConstraintTuple for (ConstrainedEntsConstraint, ConstrainedEntsConstraint) {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>> {
        if expected != 2 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(2, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl ConstrainedEntsConstraintTuple for (ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint) {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>> {
        if expected != 3 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(3, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl ConstrainedEntsConstraintTuple for (ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint) {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>> {
        if expected != 4 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(4, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl ConstrainedEntsConstraintTuple for (ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint) {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>> {
        if expected != 5 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(5, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

// TODO: allow Constrained tuples of length more than 6.  Folks who are Constrained such large tuples are
// probably doing something wrong -- they should investigate a pull expression.
impl ConstrainedEntsConstraintTuple for (ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint, ConstrainedEntsConstraint) {
    fn from_ConstrainedEnts_vec(expected: usize, vec: Option<Vec<ConstrainedEntsConstraint>>) -> Result<Option<Self>> {
        if expected != 6 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(6, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}
