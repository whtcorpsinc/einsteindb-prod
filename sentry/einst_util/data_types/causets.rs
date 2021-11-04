// Copyright WHTCORPS INC 2020
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module defines allegro types that support the transaction processor.

use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

use std::collections::BTreeMap;
use std::fmt;

use value_rc::{
    ValueRc,
};

use symbols::{
    Keyword,
    PlainSymbol,
};

use types::{
    ValueAndSpan,
};

pub trait TransacBlockValueMarker {}

/// `ValueAndSpan` is the value type coming out of the instanton parser.
impl TransacBlockValueMarker for ValueAndSpan {}

/// A tempid, either an external tempid given in a transaction (usually as an `Value::Text`),
/// or an internal tempid allocated by EinsteinDB itself.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum TempId {
    External(String),
    Internal(i64),
}

impl TempId {
    pub fn into_external(self) -> Option<String> {
        match self {
            TempId::External(s) => Some(s),
            TempId::Internal(_) => None,
        }
    }
}

impl fmt::Display for TempId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &TempId::External(ref s) => write!(f, "{}", s),
            &TempId::Internal(x) => write!(f, "<tempid {}>", x),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum SolitonIdOrCausetId {
    SolitonId(i64),
    CausetId(Keyword),
}

impl From<i64> for SolitonIdOrCausetId {
    fn from(v: i64) -> Self {
        SolitonIdOrCausetId::SolitonId(v)
    }
}

impl From<Keyword> for SolitonIdOrCausetId {
    fn from(v: Keyword) -> Self {
        SolitonIdOrCausetId::CausetId(v)
    }
}

impl SolitonIdOrCausetId {
    pub fn unreversed(&self) -> Option<SolitonIdOrCausetId> {
        match self {
            &SolitonIdOrCausetId::SolitonId(_) => None,
            &SolitonIdOrCausetId::CausetId(ref a) => a.unreversed().map(SolitonIdOrCausetId::CausetId),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef<V> {
    pub a: AttributePlace,
    // In theory we could allow nested lookup-refs.  In practice this would require us to process
    // lookup-refs in multiple phases, like how we resolve tempids, which isn't worth the effort.
    pub v: V, // An atom.
}

/// A "transaction function" that exposes some value determined by the current transaction.  The
/// prototypical example is the current transaction ID, `(transaction-causetx)`.
///
/// A natural next step might be to expose the current transaction instant `(transaction-instant)`,
/// but that's more difficult: the transaction itself can set the transaction instant (with some
/// restrictions), so the transaction function must be late-Constrained.  Right now, that's difficult to
/// arrange in the transactor.
///
/// In the future, we might accept arguments; for example, perhaps we might expose `(ancestor
/// (transaction-causetx) n)` to find the n-th ancestor of the current transaction.  If we do accept
/// arguments, then the special case of `(lookup-ref a v)` should be handled as part of the
/// generalization.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TxFunction {
    pub op: PlainSymbol,
}

pub type MapNotation<V> = BTreeMap<SolitonIdOrCausetId, ValuePlace<V>>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum ValuePlace<V> {
    // We never know at parse-time whether an integer or causetid is really an solitonId, but we will often
    // know when building entities programmatically.
    SolitonId(SolitonIdOrCausetId),
    // We never know at parse-time whether a string is really a tempid, but we will often know when
    // building entities programmatically.
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
    Vector(Vec<ValuePlace<V>>),
    Atom(V),
    MapNotation(MapNotation<V>),
}

impl<V: TransacBlockValueMarker> From<SolitonIdOrCausetId> for ValuePlace<V> {
    fn from(v: SolitonIdOrCausetId) -> Self {
        ValuePlace::SolitonId(v)
    }
}

impl<V: TransacBlockValueMarker> From<TempId> for ValuePlace<V> {
    fn from(v: TempId) -> Self {
        ValuePlace::TempId(v.into())
    }
}

impl<V: TransacBlockValueMarker> From<ValueRc<TempId>> for ValuePlace<V> {
    fn from(v: ValueRc<TempId>) -> Self {
        ValuePlace::TempId(v)
    }
}

impl<V: TransacBlockValueMarker> From<LookupRef<V>> for ValuePlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        ValuePlace::LookupRef(v)
    }
}

impl<V: TransacBlockValueMarker> From<TxFunction> for ValuePlace<V> {
    fn from(v: TxFunction) -> Self {
        ValuePlace::TxFunction(v)
    }
}

impl<V: TransacBlockValueMarker> From<Vec<ValuePlace<V>>> for ValuePlace<V> {
    fn from(v: Vec<ValuePlace<V>>) -> Self {
        ValuePlace::Vector(v)
    }
}

impl<V: TransacBlockValueMarker> From<V> for ValuePlace<V> {
    fn from(v: V) -> Self {
        ValuePlace::Atom(v)
    }
}

impl<V: TransacBlockValueMarker> From<MapNotation<V>> for ValuePlace<V> {
    fn from(v: MapNotation<V>) -> Self {
        ValuePlace::MapNotation(v)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum InstantonPlace<V> {
    SolitonId(SolitonIdOrCausetId),
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
}

impl<V, E: Into<SolitonIdOrCausetId>> From<E> for InstantonPlace<V> {
    fn from(v: E) -> Self {
        InstantonPlace::SolitonId(v.into())
    }
}

impl<V: TransacBlockValueMarker> From<TempId> for InstantonPlace<V> {
    fn from(v: TempId) -> Self {
        InstantonPlace::TempId(v.into())
    }
}

impl<V: TransacBlockValueMarker> From<ValueRc<TempId>> for InstantonPlace<V> {
    fn from(v: ValueRc<TempId>) -> Self {
        InstantonPlace::TempId(v)
    }
}

impl<V: TransacBlockValueMarker> From<LookupRef<V>> for InstantonPlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        InstantonPlace::LookupRef(v)
    }
}

impl<V: TransacBlockValueMarker> From<TxFunction> for InstantonPlace<V> {
    fn from(v: TxFunction) -> Self {
        InstantonPlace::TxFunction(v)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributePlace {
    SolitonId(SolitonIdOrCausetId),
}

impl<A: Into<SolitonIdOrCausetId>> From<A> for AttributePlace {
    fn from(v: A) -> Self {
        AttributePlace::SolitonId(v.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum OpType {
    Add,
    Retract,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Instanton<V> {
    // Like [:edb/add|:edb/retract e a v].
    AddOrRetract {
        op: OpType,
        e: InstantonPlace<V>,
        a: AttributePlace,
        v: ValuePlace<V>,
    },
    // Like {:edb/id "tempid" a1 v1 a2 v2}.
    MapNotation(MapNotation<V>),
}
