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

//! Types used only within the transactor.  These should not be exposed outside of this crate.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};

use allegrosql_promises::{
    Attribute,
    SolitonId,
    KnownSolitonId,
    MinkowskiType,
    MinkowskiValueType,
};

use causetq_allegrosql::util::Either;

use edbn;
use edbn::{
    SpannedValue,
    ValueAndSpan,
    ValueRc,
};
use edbn::entities;
use edbn::entities::{
    InstantonPlace,
    OpType,
    TempId,
    TxFunction,
};

use causetq_pull_promises::errors as errors;
use causetq_pull_promises::errors::{
    DbErrorKind,
    Result,
};
use schemaReplicant::{
    SchemaReplicantTypeChecking,
};
use types::{
    AVMap,
    AVPair,
    SchemaReplicant,
    TransacBlockValue,
};

impl TransacBlockValue for ValueAndSpan {
    fn into_typed_value(self, schemaReplicant: &SchemaReplicant, value_type: MinkowskiValueType) -> Result<MinkowskiType> {
        schemaReplicant.to_typed_value(&self, value_type)
    }

    fn into_instanton_place(self) -> Result<InstantonPlace<Self>> {
        use self::SpannedValue::*;
        match self.inner {
            Integer(v) => Ok(InstantonPlace::SolitonId(entities::SolitonIdOrCausetId::SolitonId(v))),
            Keyword(v) => {
                if v.is_namespaced() {
                    Ok(InstantonPlace::SolitonId(entities::SolitonIdOrCausetId::CausetId(v)))
                } else {
                    // We only allow namespaced causetIds.
                    bail!(DbErrorKind::InputError(errors::InputError::BadInstantonPlace))
                }
            },
            Text(v) => Ok(InstantonPlace::TempId(TempId::External(v).into())),
            List(ls) => {
                let mut it = ls.iter();
                match (it.next().map(|x| &x.inner), it.next(), it.next(), it.next()) {
                    // Like "(transaction-id)".
                    (Some(&PlainSymbol(ref op)), None, None, None) => {
                        Ok(InstantonPlace::TxFunction(TxFunction { op: op.clone() }))
                    },
                    // Like "(lookup-ref)".
                    (Some(&PlainSymbol(edbn::PlainSymbol(ref s))), Some(a), Some(v), None) if s == "lookup-ref" => {
                        match a.clone().into_instanton_place()? {
                            InstantonPlace::SolitonId(a) => Ok(InstantonPlace::LookupRef(entities::LookupRef { a: entities::AttributePlace::SolitonId(a), v: v.clone() })),
                            InstantonPlace::TempId(_) |
                            InstantonPlace::TxFunction(_) |
                            InstantonPlace::LookupRef(_) => bail!(DbErrorKind::InputError(errors::InputError::BadInstantonPlace)),
                        }
                    },
                    _ => bail!(DbErrorKind::InputError(errors::InputError::BadInstantonPlace)),
                }
            },
            Nil |
            Boolean(_) |
            Instant(_) |
            BigInteger(_) |
            Float(_) |
            Uuid(_) |
            PlainSymbol(_) |
            NamespacedSymbol(_) |
            Vector(_) |
            Set(_) |
            Map(_) => bail!(DbErrorKind::InputError(errors::InputError::BadInstantonPlace)),
        }
    }

    fn as_tempid(&self) -> Option<TempId> {
        self.inner.as_text().cloned().map(TempId::External).map(|v| v.into())
    }
}

impl TransacBlockValue for MinkowskiType {
    fn into_typed_value(self, _schemaReplicant: &SchemaReplicant, value_type: MinkowskiValueType) -> Result<MinkowskiType> {
        if self.value_type() != value_type {
            bail!(DbErrorKind::BadValuePair(format!("{:?}", self), value_type));
        }
        Ok(self)
    }

    fn into_instanton_place(self) -> Result<InstantonPlace<Self>> {
        match self {
            MinkowskiType::Ref(x) => Ok(InstantonPlace::SolitonId(entities::SolitonIdOrCausetId::SolitonId(x))),
            MinkowskiType::Keyword(x) => Ok(InstantonPlace::SolitonId(entities::SolitonIdOrCausetId::CausetId((*x).clone()))),
            MinkowskiType::String(x) => Ok(InstantonPlace::TempId(TempId::External((*x).clone()).into())),
            MinkowskiType::Boolean(_) |
            MinkowskiType::Long(_) |
            MinkowskiType::Double(_) |
            MinkowskiType::Instant(_) |
            MinkowskiType::Uuid(_) => bail!(DbErrorKind::InputError(errors::InputError::BadInstantonPlace)),
        }
    }

    fn as_tempid(&self) -> Option<TempId> {
        match self {
            &MinkowskiType::String(ref s) => Some(TempId::External((**s).clone()).into()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Term<E, V> {
    AddOrRetract(OpType, E, SolitonId, V),
}

use self::Either::*;

pub type KnownSolitonIdOr<T> = Either<KnownSolitonId, T>;
pub type MinkowskiTypeOr<T> = Either<MinkowskiType, T>;

pub type TempIdHandle = ValueRc<TempId>;
pub type TempIdMap = HashMap<TempIdHandle, KnownSolitonId>;

pub type LookupRef = ValueRc<AVPair>;

/// Internal representation of an solitonId on its way to resolution.  We either have the simple case (a
/// numeric solitonId), a lookup-ref that still needs to be resolved (an atomized [a v] pair), or a temp
/// ID that needs to be upserted or allocated (an atomized tempid).
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum LookupRefOrTempId {
    LookupRef(LookupRef),
    TempId(TempIdHandle)
}

pub type TermWithTempIdsAndLookupRefs = Term<KnownSolitonIdOr<LookupRefOrTempId>, MinkowskiTypeOr<LookupRefOrTempId>>;
pub type TermWithTempIds = Term<KnownSolitonIdOr<TempIdHandle>, MinkowskiTypeOr<TempIdHandle>>;
pub type TermWithoutTempIds = Term<KnownSolitonId, MinkowskiType>;
pub type Population = Vec<TermWithTempIds>;

impl TermWithTempIds {
    // These have no tempids by definition, and just need to be unwrapped.  This operation might
    // also be called "lowering" or "level lowering", but the concept of "unwrapping" is common in
    // Rust and seems appropriate here.
    pub(crate) fn unwrap(self) -> TermWithoutTempIds {
        match self {
            Term::AddOrRetract(op, Left(n), a, Left(v)) => Term::AddOrRetract(op, n, a, v),
            _ => unreachable!(),
        }
    }
}

impl TermWithoutTempIds {
    pub(crate) fn rewrap<A, B>(self) -> Term<KnownSolitonIdOr<A>, MinkowskiTypeOr<B>> {
        match self {
            Term::AddOrRetract(op, n, a, v) => Term::AddOrRetract(op, Left(n), a, Left(v))
        }
    }
}

/// Given a `KnownSolitonIdOr` or a `MinkowskiTypeOr`, replace any internal `LookupRef` with the solitonId from
/// the given map.  Fail if any `LookupRef` cannot be replaced.
///
/// `lift` allows to specify how the solitonId found is mapped into the output type.  (This could
/// also be an `Into` or `From` requirement.)
///
/// The reason for this awkward expression is that we're parameterizing over the _type constructor_
/// (`SolitonIdOr` or `MinkowskiTypeOr`), which is not trivial to express in Rust.  This only works because
/// they're both the same `Result<...>` type with different parameterizations.
pub fn replace_lookup_ref<T, U>(lookup_map: &AVMap, desired_or: Either<T, LookupRefOrTempId>, lift: U) -> errors::Result<Either<T, TempIdHandle>> where U: FnOnce(SolitonId) -> T {
    match desired_or {
        Left(desired) => Ok(Left(desired)), // N.b., must unwrap here -- the ::Left types are different!
        Right(other) => {
            match other {
                LookupRefOrTempId::TempId(t) => Ok(Right(t)),
                LookupRefOrTempId::LookupRef(av) => lookup_map.get(&*av)
                    .map(|x| lift(*x)).map(Left)
                    // XXX TODO: fix this error kind!
                    .ok_or_else(|| DbErrorKind::UnrecognizedCausetId(format!("couldn't lookup [a v]: {:?}", (*av).clone())).into()),
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AddAndRetract {
    pub(crate) add: BTreeSet<MinkowskiType>,
    pub(crate) retract: BTreeSet<MinkowskiType>,
}

// A trie-like structure mapping a -> e -> v that prefix compresses and makes uniqueness constraint
// checking more efficient.  BTree* for deterministic errors.
pub(crate) type AEVTrie<'schemaReplicant> = BTreeMap<(SolitonId, &'schemaReplicant Attribute), BTreeMap<SolitonId, AddAndRetract>>;
