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

//! This module implements the transaction application algorithm described at
//! https://github.com/whtcorpsinc/edb/wiki/Transacting and its children pages.
//!
//! The impleedbion proceeds in four main stages, labeled "Pipeline stage 1" through "Pipeline
//! stage 4".  _Pipeline_ may be a misnomer, since the stages as written **cannot** be interleaved
//! in parallel.  That is, a single transacted instanton cannot flow through all the stages without its
//! sibling entities.
//!
//! This unintuitive architectural decision was made because the second and third stages (resolving
//! lookup refs and tempids, respectively) operate _in bulk_ to minimize the number of expensive
//! SQLite queries by processing many in one SQLite invocation.  Pipeline stage 2 doesn't need to
//! operate like this: it is easy to handle each transacted instanton independently of all the others
//! (and earlier, less efficient, impleedbions did this).  However, Pipeline stage 3 appears to
//! require processing multiple elements at the same time, since there can be arbitrarily complex
//! graph relationships between tempids.  Pipeline stage 4 (inserting elements into the SQL store)
//! could also be expressed as an independent operation per transacted instanton, but there are
//! non-trivial uniqueness relationships inside a single transaction that need to enforced.
//! Therefore, some multi-instanton processing is required, and a per-instanton pipeline becomes less
//! attractive.
//!
//! A note on the types in the impleedbion.  The pipeline stages are strongly typed: each stage
//! accepts and produces a subset of the previous.  We hope this will reduce errors as data moves
//! through the system.  In contrast the Clojure impleedbion rewrote the fundamental instanton type
//! in place and suffered bugs where particular code paths missed cases.
//!
//! The type hierarchy accepts `Instanton` instances from the transaction parser and flows `Term`
//! instances through the term-rewriting transaction applier.  `Term` is a general `[:edb/add e a v]`
//! with restrictions on the `e` and `v` components.  The hierarchy is expressed using `Result` to
//! model either/or, and layers of `Result` are stripped -- we might say the `Term` instances are
//! _lowered_ as they flow through the pipeline.  This type hierarchy could have been expressed by
//! combinatorially increasing `enum` cases, but this makes it difficult to handle the `e` and `v`
//! components symmetrically.  Hence, layers of `Result` type aliases.  Hopefully the explanatory
//! names -- `TermWithTempIdsAndLookupRefs`, anyone? -- and strongly typed stage functions will help
//! keep everything straight.

use std::borrow::{
    Cow,
};
use std::collections::{
    BTreeMap,
    BTreeSet,
    VecDeque,
};
use std::iter::{
    once,
};

use edb;
use edb::{
    EinsteinDBStoring,
};
use edbn::{
    InternSet,
    Keyword,
};
use causetids;
use causetq_pull_promises::errors as errors;
use causetq_pull_promises::errors::{
    DbErrorKind,
    Result,
};
use internal_types::{
    AddAndRetract,
    AEVTrie,
    KnownSolitonIdOr,
    LookupRef,
    LookupRefOrTempId,
    TempIdHandle,
    TempIdMap,
    Term,
    TermWithTempIds,
    TermWithTempIdsAndLookupRefs,
    TermWithoutTempIds,
    MinkowskiTypeOr,
    replace_lookup_ref,
};

use causetq_allegrosql::util::Either;

use allegrosql_promises::{
    attribute,
    Attribute,
    SolitonId,
    KnownSolitonId,
    MinkowskiType,
    MinkowskiValueType,
    now,
};

use causetq_allegrosql::{
    DateTime,
    SchemaReplicant,
    TxReport,
    Utc,
};

use edbn::entities as entmod;
use edbn::entities::{
    AttributePlace,
    Instanton,
    OpType,
    TempId,
};
use spacetime;
use rusqlite;
use schemaReplicant::{
    SchemaReplicantBuilding,
};
use causetx_redshift;
use types::{
    AVMap,
    AVPair,
    PartitionMap,
    TransacBlockValue,
};
use upsert_resolution::{
    FinalPopulations,
    Generation,
};
use watcher::{
    TransactWatcher,
};

/// Defines transactor's high level behaviour.
pub(crate) enum TransactorAction {
    /// Materialize transaction into 'causets' and spacetime
    /// views, but do not commit it into 'bundles' Block.
    /// Use this if you need transaction's "side-effects", but
    /// don't want its by-products to end-up in the transaction log,
    /// e.g. when rewinding.
    Materialize,

    /// Materialize transaction into 'causets' and spacetime
    /// views, and also commit it into the 'bundles' Block.
    /// Use this for regular bundles.
    MaterializeAndCommit,
}

/// A transaction on its way to being applied.
#[derive(Debug)]
pub struct Tx<'conn, 'a, W> where W: TransactWatcher {
    /// The storage to apply against.  In the future, this will be a EinsteinDB connection.
    store: &'conn rusqlite::Connection, // TODO: edb::EinsteinDBStoring,

    /// The partition map to allocate causetids from.
    ///
    /// The partition map is volatile in the sense that every succesful transaction updates
    /// allocates at least one causetx ID, so we own and modify our own partition map.
    partition_map: PartitionMap,

    /// The schemaReplicant to update from the transaction entities.
    ///
    /// bundles only update the schemaReplicant infrequently, so we borrow this schemaReplicant until we need to
    /// modify it.
    schemaReplicant_for_mutation: Cow<'a, SchemaReplicant>,

    /// The schemaReplicant to use when interpreting the transaction entities.
    ///
    /// This schemaReplicant is not updated, so we just borrow it.
    schemaReplicant: &'a SchemaReplicant,

    watcher: W,

    /// The transaction ID of the transaction.
    causecausetx_id: SolitonId,
}

/// Remove any :edb/id value from the given map notation, converting the returned value into
/// something suiBlock for the instanton position rather than something suiBlock for a value position.
pub fn remove_edb_id<V: TransacBlockValue>(map: &mut entmod::MapNotation<V>) -> Result<Option<entmod::InstantonPlace<V>>> {
    // TODO: extract lazy defined constant.
    let edb_id_key = entmod::SolitonIdOrCausetId::CausetId(Keyword::namespaced("edb", "id"));

    let edb_id: Option<entmod::InstantonPlace<V>> = if let Some(id) = map.remove(&edb_id_key) {
        match id {
            entmod::ValuePlace::SolitonId(e) => Some(entmod::InstantonPlace::SolitonId(e)),
            entmod::ValuePlace::LookupRef(e) => Some(entmod::InstantonPlace::LookupRef(e)),
            entmod::ValuePlace::TempId(e) => Some(entmod::InstantonPlace::TempId(e)),
            entmod::ValuePlace::TxFunction(e) => Some(entmod::InstantonPlace::TxFunction(e)),
            entmod::ValuePlace::Atom(v) => Some(v.into_instanton_place()?),
            entmod::ValuePlace::Vector(_) |
            entmod::ValuePlace::MapNotation(_) => {
                bail!(DbErrorKind::InputError(errors::InputError::BadDbId))
            },
        }
    } else {
        None
    };

    Ok(edb_id)
}

impl<'conn, 'a, W> Tx<'conn, 'a, W> where W: TransactWatcher {
    pub fn new(
        store: &'conn rusqlite::Connection,
        partition_map: PartitionMap,
        schemaReplicant_for_mutation: &'a SchemaReplicant,
        schemaReplicant: &'a SchemaReplicant,
        watcher: W,
        causecausetx_id: SolitonId) -> Tx<'conn, 'a, W> {
        Tx {
            store: store,
            partition_map: partition_map,
            schemaReplicant_for_mutation: Cow::Borrowed(schemaReplicant_for_mutation),
            schemaReplicant: schemaReplicant,
            watcher: watcher,
            causecausetx_id: causecausetx_id,
        }
    }

    /// Given a collection of tempids and the [a v] pairs that they might upsert to, resolve exactly
    /// which [a v] pairs do upsert to causetids, and map each tempid that upserts to the upserted
    /// solitonId.  The keys of the resulting map are exactly those tempids that upserted.
    pub(crate) fn resolve_temp_id_avs<'b>(&self, temp_id_avs: &'b [(TempIdHandle, AVPair)]) -> Result<TempIdMap> {
        if temp_id_avs.is_empty() {
            return Ok(TempIdMap::default());
        }

        // Map [a v]->solitonId.
        let mut av_pairs: Vec<&AVPair> = vec![];
        for i in 0..temp_id_avs.len() {
            av_pairs.push(&temp_id_avs[i].1);
        }

        // Lookup in the store.
        let av_map: AVMap = self.store.resolve_avs(&av_pairs[..])?;

        debug!("looked up avs {:?}", av_map);

        // Map id->solitonId.
        let mut tempids: TempIdMap = TempIdMap::default();

        // Errors.  BTree* since we want deterministic results.
        let mut conflicting_upserts: BTreeMap<TempId, BTreeSet<KnownSolitonId>> = BTreeMap::default();

        for &(ref tempid, ref av_pair) in temp_id_avs {
            trace!("tempid {:?} av_pair {:?} -> {:?}", tempid, av_pair, av_map.get(&av_pair));
            if let Some(solitonId) = av_map.get(&av_pair).cloned().map(KnownSolitonId) {
                tempids.insert(tempid.clone(), solitonId).map(|previous| {
                    if solitonId != previous {
                        conflicting_upserts.entry((**tempid).clone()).or_insert_with(|| once(previous).collect::<BTreeSet<_>>()).insert(solitonId);
                    }
                });
            }
        }

        if !conflicting_upserts.is_empty() {
            bail!(DbErrorKind::SchemaReplicantConstraintViolation(errors::SchemaReplicantConstraintViolation::ConflictingUpserts { conflicting_upserts }));
        }

        Ok(tempids)
    }

    /// Pipeline stage 1: convert `Instanton` instances into `Term` instances, ready for term
    /// rewriting.
    ///
    /// The `Term` instances produce share interned TempId and LookupRef handles, and we return the
    /// interned handle sets so that consumers can ensure all handles are used appropriately.
    fn entities_into_terms_with_temp_ids_and_lookup_refs<I, V: TransacBlockValue>(&self, entities: I) -> Result<(Vec<TermWithTempIdsAndLookupRefs>, InternSet<TempId>, InternSet<AVPair>)> where I: IntoIterator<Item=Instanton<V>> {
        struct InProcess<'a> {
            partition_map: &'a PartitionMap,
            schemaReplicant: &'a SchemaReplicant,
            edb_id_count: i64,
            causecausetx_id: KnownSolitonId,
            temp_ids: InternSet<TempId>,
            lookup_refs: InternSet<AVPair>,
        }

        impl<'a> InProcess<'a> {
            fn with_schemaReplicant_and_partition_map(schemaReplicant: &'a SchemaReplicant, partition_map: &'a PartitionMap, causecausetx_id: KnownSolitonId) -> InProcess<'a> {
                InProcess {
                    partition_map,
                    schemaReplicant,
                    edb_id_count: 0,
                    causecausetx_id,
                    temp_ids: InternSet::new(),
                    lookup_refs: InternSet::new(),
                }
            }

            fn ensure_causetid_exists(&self, e: SolitonId) -> Result<KnownSolitonId> {
                if self.partition_map.contains_causetid(e) {
                    Ok(KnownSolitonId(e))
                } else {
                    bail!(DbErrorKind::UnallocatedSolitonId(e))
                }
            }

            fn ensure_causetId_exists(&self, e: &Keyword) -> Result<KnownSolitonId> {
                self.schemaReplicant.require_causetid(e)
            }

            fn intern_lookup_ref<W: TransacBlockValue>(&mut self, lookup_ref: &entmod::LookupRef<W>) -> Result<LookupRef> {
                let lr_a: i64 = match lookup_ref.a {
                    AttributePlace::SolitonId(entmod::SolitonIdOrCausetId::SolitonId(ref a)) => *a,
                    AttributePlace::SolitonId(entmod::SolitonIdOrCausetId::CausetId(ref a)) => self.schemaReplicant.require_causetid(&a)?.into(),
                };
                let lr_attribute: &Attribute = self.schemaReplicant.require_attribute_for_causetid(lr_a)?;

                let lr_typed_value: MinkowskiType = lookup_ref.v.clone().into_typed_value(&self.schemaReplicant, lr_attribute.value_type)?;
                if lr_attribute.unique.is_none() {
                    bail!(DbErrorKind::NotYetImplemented(format!("Cannot resolve (lookup-ref {} {:?}) with attribute that is not :edb/unique", lr_a, lr_typed_value)))
                }

                Ok(self.lookup_refs.intern((lr_a, lr_typed_value)))
            }

            /// Allocate private internal tempids reserved for EinsteinDB.  Internal tempids just need to be
            /// unique within one transaction; they should never escape a transaction.
            fn allocate_edb_id<W: TransacBlockValue>(&mut self) -> entmod::InstantonPlace<W> {
                self.edb_id_count += 1;
                entmod::InstantonPlace::TempId(TempId::Internal(self.edb_id_count).into())
            }

            fn instanton_e_into_term_e<W: TransacBlockValue>(&mut self, x: entmod::InstantonPlace<W>) -> Result<KnownSolitonIdOr<LookupRefOrTempId>> {
                match x {
                    entmod::InstantonPlace::SolitonId(e) => {
                        let e = match e {
                            entmod::SolitonIdOrCausetId::SolitonId(ref e) => self.ensure_causetid_exists(*e)?,
                            entmod::SolitonIdOrCausetId::CausetId(ref e) => self.ensure_causetId_exists(&e)?,
                        };
                        Ok(Either::Left(e))
                    },

                    entmod::InstantonPlace::TempId(e) => {
                        Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(e))))
                    },

                    entmod::InstantonPlace::LookupRef(ref lookup_ref) => {
                        Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?)))
                    },

                    entmod::InstantonPlace::TxFunction(ref causecausetx_function) => {
                        match causecausetx_function.op.0.as_str() {
                            "transaction-causetx" => Ok(Either::Left(self.causecausetx_id)),
                            unknown @ _ => bail!(DbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                        }
                    },
                }
            }

            fn instanton_a_into_term_a(&mut self, x: entmod::SolitonIdOrCausetId) -> Result<SolitonId> {
                let a = match x {
                    entmod::SolitonIdOrCausetId::SolitonId(ref a) => *a,
                    entmod::SolitonIdOrCausetId::CausetId(ref a) => self.schemaReplicant.require_causetid(&a)?.into(),
                };
                Ok(a)
            }

            fn instanton_e_into_term_v<W: TransacBlockValue>(&mut self, x: entmod::InstantonPlace<W>) -> Result<MinkowskiTypeOr<LookupRefOrTempId>> {
                self.instanton_e_into_term_e(x).map(|r| r.map_left(|ke| MinkowskiType::Ref(ke.0)))
            }

            fn instanton_v_into_term_e<W: TransacBlockValue>(&mut self, x: entmod::ValuePlace<W>, backward_a: &entmod::SolitonIdOrCausetId) -> Result<KnownSolitonIdOr<LookupRefOrTempId>> {
                match backward_a.unreversed() {
                    None => {
                        bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for forward attribute")));
                    },
                    Some(forward_a) => {
                        let forward_a = self.instanton_a_into_term_a(forward_a)?;
                        let forward_attribute = self.schemaReplicant.require_attribute_for_causetid(forward_a)?;
                        if forward_attribute.value_type != MinkowskiValueType::Ref {
                            bail!(DbErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for attribute {} that is not :edb/valueType :edb.type/ref", forward_a)))
                        }

                        match x {
                            entmod::ValuePlace::Atom(v) => {
                                // Here is where we do schemaReplicant-aware typechecking: we either assert
                                // that the given value is in the attribute's value set, or (in
                                // limited cases) coerce the value into the attribute's value set.
                                match v.as_tempid() {
                                    Some(tempid) => Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(tempid)))),
                                    None => {
                                        if let MinkowskiType::Ref(solitonId) = v.into_typed_value(&self.schemaReplicant, MinkowskiValueType::Ref)? {
                                            Ok(Either::Left(KnownSolitonId(solitonId)))
                                        } else {
                                            // The given value is expected to be :edb.type/ref, so this shouldn't happen.
                                            bail!(DbErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for attribute {} with value that is not :edb.valueType :edb.type/ref", forward_a)))
                                        }
                                    }
                                }
                            },

                            entmod::ValuePlace::SolitonId(solitonId) =>
                                Ok(Either::Left(KnownSolitonId(self.instanton_a_into_term_a(solitonId)?))),

                            entmod::ValuePlace::TempId(tempid) =>
                                Ok(Either::Right(LookupRefOrTempId::TempId(self.temp_ids.intern(tempid)))),

                            entmod::ValuePlace::LookupRef(ref lookup_ref) =>
                                Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?))),

                            entmod::ValuePlace::TxFunction(ref causecausetx_function) => {
                                match causecausetx_function.op.0.as_str() {
                                    "transaction-causetx" => Ok(Either::Left(KnownSolitonId(self.causecausetx_id.0))),
                                    unknown @ _ => bail!(DbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                                }
                            },

                            entmod::ValuePlace::Vector(_) =>
                                bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode vector value in :attr/_reversed notation for attribute {}", forward_a))),

                            entmod::ValuePlace::MapNotation(_) =>
                                bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for attribute {}", forward_a))),
                        }
                    },
                }
            }
        }

        let mut in_process = InProcess::with_schemaReplicant_and_partition_map(&self.schemaReplicant, &self.partition_map, KnownSolitonId(self.causecausetx_id));

        // We want to handle entities in the order they're given to us, while also "exploding" some
        // entities into many.  We therefore push the initial entities onto the back of the deque,
        // take from the front of the deque, and explode onto the front as well.
        let mut deque: VecDeque<Instanton<V>> = VecDeque::default();
        deque.extend(entities);

        let mut terms: Vec<TermWithTempIdsAndLookupRefs> = Vec::with_capacity(deque.len());

        while let Some(instanton) = deque.pop_front() {
            match instanton {
                Instanton::MapNotation(mut map_notation) => {
                    // :edb/id is optional; if it's not given, we generate a special internal tempid
                    // to use for upserting.  This tempid will not be reported in the TxReport.
                    let edb_id: entmod::InstantonPlace<V> = remove_edb_id(&mut map_notation)?.unwrap_or_else(|| in_process.allocate_edb_id());

                    // We're not nested, so :edb/isComponent is not relevant.  We just explode the
                    // map notation.
                    for (a, v) in map_notation {
                        deque.push_front(Instanton::AddOrRetract {
                            op: OpType::Add,
                            e: edb_id.clone(),
                            a: AttributePlace::SolitonId(a),
                            v: v,
                        });
                    }
                },

                Instanton::AddOrRetract { op, e, a, v } => {
                    let AttributePlace::SolitonId(a) = a;

                    if let Some(reversed_a) = a.unreversed() {
                        let reversed_e = in_process.instanton_v_into_term_e(v, &a)?;
                        let reversed_a = in_process.instanton_a_into_term_a(reversed_a)?;
                        let reversed_v = in_process.instanton_e_into_term_v(e)?;
                        terms.push(Term::AddOrRetract(OpType::Add, reversed_e, reversed_a, reversed_v));
                    } else {
                        let a = in_process.instanton_a_into_term_a(a)?;
                        let attribute = self.schemaReplicant.require_attribute_for_causetid(a)?;

                        let v = match v {
                            entmod::ValuePlace::Atom(v) => {
                                // Here is where we do schemaReplicant-aware typechecking: we either assert
                                // that the given value is in the attribute's value set, or (in
                                // limited cases) coerce the value into the attribute's value set.
                                if attribute.value_type == MinkowskiValueType::Ref {
                                    match v.as_tempid() {
                                        Some(tempid) => Either::Right(LookupRefOrTempId::TempId(in_process.temp_ids.intern(tempid))),
                                        None => v.into_typed_value(&self.schemaReplicant, attribute.value_type).map(Either::Left)?,
                                    }
                                } else {
                                    v.into_typed_value(&self.schemaReplicant, attribute.value_type).map(Either::Left)?
                                }
                            },

                            entmod::ValuePlace::SolitonId(solitonId) =>
                                Either::Left(MinkowskiType::Ref(in_process.instanton_a_into_term_a(solitonId)?)),

                            entmod::ValuePlace::TempId(tempid) =>
                                Either::Right(LookupRefOrTempId::TempId(in_process.temp_ids.intern(tempid))),

                            entmod::ValuePlace::LookupRef(ref lookup_ref) => {
                                if attribute.value_type != MinkowskiValueType::Ref {
                                    bail!(DbErrorKind::NotYetImplemented(format!("Cannot resolve value lookup ref for attribute {} that is not :edb/valueType :edb.type/ref", a)))
                                }

                                Either::Right(LookupRefOrTempId::LookupRef(in_process.intern_lookup_ref(lookup_ref)?))
                            },

                            entmod::ValuePlace::TxFunction(ref causecausetx_function) => {
                                let typed_value = match causecausetx_function.op.0.as_str() {
                                    "transaction-causetx" => MinkowskiType::Ref(self.causecausetx_id),
                                    unknown @ _ => bail!(DbErrorKind::NotYetImplemented(format!("Unknown transaction function {}", unknown))),
                                };

                                // Here we do schemaReplicant-aware typechecking: we assert that the computed
                                // value is in the attribute's value set.  If and when we have
                                // transaction functions that produce numeric values, we'll have to
                                // be more careful here, because a function that produces an integer
                                // value can be used where a double is expected.  See also
                                // `SchemaReplicantTypeChecking.to_typed_value(...)`.
                                if attribute.value_type != typed_value.value_type() {
                                    bail!(DbErrorKind::NotYetImplemented(format!("Transaction function {} produced value of type {} but expected type {}",
                                                                               causecausetx_function.op.0.as_str(), typed_value.value_type(), attribute.value_type)));
                                }

                                Either::Left(typed_value)
                            },

                            entmod::ValuePlace::Vector(vs) => {
                                if !attribute.multival {
                                    bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode vector value for attribute {} that is not :edb.cardinality :edb.cardinality/many", a)));
                                }

                                for vv in vs {
                                    deque.push_front(Instanton::AddOrRetract {
                                        op: op.clone(),
                                        e: e.clone(),
                                        a: AttributePlace::SolitonId(entmod::SolitonIdOrCausetId::SolitonId(a)),
                                        v: vv,
                                    });
                                }
                                continue
                            },

                            entmod::ValuePlace::MapNotation(mut map_notation) => {
                                // TODO: consider handling this at the causetx-parser level.  That would be
                                // more strict and expressive, but it would lead to splitting
                                // AddOrRetract, which proliferates types and code, or only handling
                                // nested maps rather than map values, like Causetic does.
                                if op != OpType::Add {
                                    bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode nested map value in :edb/retract for attribute {}", a)));
                                }

                                if attribute.value_type != MinkowskiValueType::Ref {
                                    bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode nested map value for attribute {} that is not :edb/valueType :edb.type/ref", a)))
                                }

                                // :edb/id is optional; if it's not given, we generate a special internal tempid
                                // to use for upserting.  This tempid will not be reported in the TxReport.
                                let edb_id: Option<entmod::InstantonPlace<V>> = remove_edb_id(&mut map_notation)?;
                                let mut dangling = edb_id.is_none();
                                let edb_id: entmod::InstantonPlace<V> = edb_id.unwrap_or_else(|| in_process.allocate_edb_id());

                                // We're nested, so we want to ensure we're not creating "dangling"
                                // entities that can't be reached.  If we're :edb/isComponent, then this
                                // is not dangling.  Otherwise, the resulting map needs to have a
                                // :edb/unique :edb.unique/causetIdity [a v] pair, so that it's reachable.
                                // Per http://docs.Causetic.com/bundles.html: "Either the reference
                                // to the nested map must be a component attribute, or the nested map
                                // must include a unique attribute. This constraint prevents the
                                // acccausetIdal creation of easily-orphaned entities that have no causetIdity
                                // or relation to other entities."
                                if attribute.component {
                                    dangling = false;
                                }

                                for (causet_set_a, causet_set_v) in map_notation {
                                    if let Some(reversed_a) = causet_set_a.unreversed() {
                                        // We definitely have a reference.  The reference might be
                                        // dangling (a bare solitonId, for example), but we don't yet
                                        // support nested maps and reverse notation simultaneously
                                        // (i.e., we don't accept {:reverse/_attribute {:nested map}})
                                        // so we don't need to check that the nested map reference isn't
                                        // dangling.
                                        dangling = false;

                                        let reversed_e = in_process.instanton_v_into_term_e(causet_set_v, &causet_set_a)?;
                                        let reversed_a = in_process.instanton_a_into_term_a(reversed_a)?;
                                        let reversed_v = in_process.instanton_e_into_term_v(edb_id.clone())?;
                                        terms.push(Term::AddOrRetract(OpType::Add, reversed_e, reversed_a, reversed_v));
                                    } else {
                                        let causet_set_a = in_process.instanton_a_into_term_a(causet_set_a)?;
                                        let causet_set_attribute = self.schemaReplicant.require_attribute_for_causetid(causet_set_a)?;
                                        if causet_set_attribute.unique == Some(attribute::Unique::CausetIdity) {
                                            dangling = false;
                                        }

                                        deque.push_front(Instanton::AddOrRetract {
                                            op: OpType::Add,
                                            e: edb_id.clone(),
                                            a: AttributePlace::SolitonId(entmod::SolitonIdOrCausetId::SolitonId(causet_set_a)),
                                            v: causet_set_v,
                                        });
                                    }
                                }

                                if dangling {
                                    bail!(DbErrorKind::NotYetImplemented(format!("Cannot explode nested map value that would lead to dangling instanton for attribute {}", a)));
                                }

                                in_process.instanton_e_into_term_v(edb_id)?
                            },
                        };

                        let e = in_process.instanton_e_into_term_e(e)?;
                        terms.push(Term::AddOrRetract(op, e, a, v));
                    }
                },
            }
        };
        Ok((terms, in_process.temp_ids, in_process.lookup_refs))
    }

    /// Pipeline stage 2: rewrite `Term` instances with lookup refs into `Term` instances without
    /// lookup refs.
    ///
    /// The `Term` instances produced share interned TempId handles and have no LookupRef references.
    fn resolve_lookup_refs<I>(&self, lookup_ref_map: &AVMap, terms: I) -> Result<Vec<TermWithTempIds>> where I: IntoIterator<Item=TermWithTempIdsAndLookupRefs> {
        terms.into_iter().map(|term: TermWithTempIdsAndLookupRefs| -> Result<TermWithTempIds> {
            match term {
                Term::AddOrRetract(op, e, a, v) => {
                    let e = replace_lookup_ref(&lookup_ref_map, e, |x| KnownSolitonId(x))?;
                    let v = replace_lookup_ref(&lookup_ref_map, v, |x| MinkowskiType::Ref(x))?;
                    Ok(Term::AddOrRetract(op, e, a, v))
                },
            }
        }).collect::<Result<Vec<_>>>()
    }

    /// Transact the given `entities` against the store.
    ///
    /// This approach is explained in https://github.com/whtcorpsinc/edb/wiki/Transacting.
    // TODO: move this to the transactor layer.
    pub fn transact_entities<I, V: TransacBlockValue>(&mut self, entities: I) -> Result<TxReport>
    where I: IntoIterator<Item=Instanton<V>> {
        // Pipeline stage 1: entities -> terms with tempids and lookup refs.
        let (terms_with_temp_ids_and_lookup_refs, tempid_set, lookup_ref_set) = self.entities_into_terms_with_temp_ids_and_lookup_refs(entities)?;

        // Pipeline stage 2: resolve lookup refs -> terms with tempids.
        let lookup_ref_avs: Vec<&(i64, MinkowskiType)> = lookup_ref_set.iter().map(|rc| &**rc).collect();
        let lookup_ref_map: AVMap = self.store.resolve_avs(&lookup_ref_avs[..])?;

        let terms_with_temp_ids = self.resolve_lookup_refs(&lookup_ref_map, terms_with_temp_ids_and_lookup_refs)?;

        self.transact_simple_terms_with_action(terms_with_temp_ids, tempid_set, TransactorAction::MaterializeAndCommit)
    }

    pub fn transact_simple_terms<I>(&mut self, terms: I, tempid_set: InternSet<TempId>) -> Result<TxReport>
    where I: IntoIterator<Item=TermWithTempIds> {
        self.transact_simple_terms_with_action(terms, tempid_set, TransactorAction::MaterializeAndCommit)
    }

    fn transact_simple_terms_with_action<I>(&mut self, terms: I, tempid_set: InternSet<TempId>, action: TransactorAction) -> Result<TxReport>
    where I: IntoIterator<Item=TermWithTempIds> {
        // TODO: push these into an internal transaction report?
        let mut tempids: BTreeMap<TempId, KnownSolitonId> = BTreeMap::default();

        // Pipeline stage 3: upsert tempids -> terms without tempids or lookup refs.
        // Now we can collect upsert populations.
        let (mut generation, inert_terms) = Generation::from(terms, &self.schemaReplicant)?;

        // And evolve them forward.
        while generation.can_evolve() {
            debug!("generation {:?}", generation);

            let tempid_avs = generation.temp_id_avs();
            debug!("trying to resolve avs {:?}", tempid_avs);

            // Evolve further.
            let temp_id_map: TempIdMap = self.resolve_temp_id_avs(&tempid_avs[..])?;

            debug!("resolved avs for tempids {:?}", temp_id_map);

            generation = generation.evolve_one_step(&temp_id_map);

            // Errors.  BTree* since we want deterministic results.
            let mut conflicting_upserts: BTreeMap<TempId, BTreeSet<KnownSolitonId>> = BTreeMap::default();

            // Report each tempid that resolves via upsert.
            for (tempid, solitonId) in temp_id_map {
                // Since `UpsertEV` instances always transition to `UpsertE` instances, it might be
                // that a tempid resolves in two generations, and those resolutions might conflict.
                tempids.insert((*tempid).clone(), solitonId).map(|previous| {
                    if solitonId != previous {
                        conflicting_upserts.entry((*tempid).clone()).or_insert_with(|| once(previous).collect::<BTreeSet<_>>()).insert(solitonId);
                    }
                });
            }

            if !conflicting_upserts.is_empty() {
                bail!(DbErrorKind::SchemaReplicantConstraintViolation(errors::SchemaReplicantConstraintViolation::ConflictingUpserts { conflicting_upserts }));
            }

            debug!("tempids {:?}", tempids);
        }

        generation.allocate_unresolved_upserts()?;

        debug!("final generation {:?}", generation);

        // Allocate causetids for tempids that didn't upsert.  BTreeMap so this is deterministic.
        let unresolved_temp_ids: BTreeMap<TempIdHandle, usize> = generation.temp_ids_in_allocations(&self.schemaReplicant)?;

        debug!("unresolved tempids {:?}", unresolved_temp_ids);

        // TODO: track partitions for temporary IDs.
        let causetids = self.partition_map.allocate_causetids(":edb.part/user", unresolved_temp_ids.len());

        let temp_id_allocations = unresolved_temp_ids
            .into_iter()
            .map(|(tempid, index)| (tempid, KnownSolitonId(causetids.start + (index as i64))))
            .collect();

        debug!("tempid allocations {:?}", temp_id_allocations);

        let final_populations = generation.into_final_populations(&temp_id_allocations)?;

        // Report each tempid that is allocated.
        for (tempid, &solitonId) in &temp_id_allocations {
            // Every tempid should be allocated at most once.
            assert!(!tempids.contains_key(&**tempid));
            tempids.insert((**tempid).clone(), solitonId);
        }

        // Verify that every tempid we interned either resolved or has been allocated.
        assert_eq!(tempids.len(), tempid_set.len());
        for tempid in tempid_set.iter() {
            assert!(tempids.contains_key(&**tempid));
        }

        // Any internal tempid has been allocated by the system and is a private impleedbion
        // detail; it shouldn't be exposed in the final transaction report.
        let tempids = tempids.into_iter().filter_map(|(tempid, e)| tempid.into_external().map(|s| (s, e.0))).collect();

        // A transaction might try to add or retract :edb/causetid assertions or other spacetime mutating
        // assertions , but those assertions might not make it to the store.  If we see a possible
        // spacetime mutation, we will figure out if any assertions made it through later.  This is
        // strictly an optimization: it would be correct to _always_ check what made it to the
        // store.
        let mut causecausetx_might_update_spacetime = false;

        // MuBlock so that we can add the transaction :edb/causecausetxInstant.
        let mut aev_trie = into_aev_trie(&self.schemaReplicant, final_populations, inert_terms)?;

        let causecausetx_instant;
        { // TODO: Don't use this block to scope borrowing the schemaReplicant; instead, extract a helper function.

        // Assertions that are :edb.cardinality/one and not :edb.fulltext.
        let mut non_fts_one: Vec<edb::ReducedInstanton> = vec![];

        // Assertions that are :edb.cardinality/many and not :edb.fulltext.
        let mut non_fts_many: Vec<edb::ReducedInstanton> = vec![];

        // Assertions that are :edb.cardinality/one and :edb.fulltext.
        let mut fts_one: Vec<edb::ReducedInstanton> = vec![];

        // Assertions that are :edb.cardinality/many and :edb.fulltext.
        let mut fts_many: Vec<edb::ReducedInstanton> = vec![];

        // We need to ensure that callers can't blindly transact entities that haven't been
        // allocated by this store.

        let errors = causetx_redshift::type_disagreements(&aev_trie);
        if !errors.is_empty() {
            bail!(DbErrorKind::SchemaReplicantConstraintViolation(errors::SchemaReplicantConstraintViolation::TypeDisagreements { conflicting_Causets: errors }));
        }

        let errors = causetx_redshift::cardinality_conflicts(&aev_trie);
        if !errors.is_empty() {
            bail!(DbErrorKind::SchemaReplicantConstraintViolation(errors::SchemaReplicantConstraintViolation::CardinalityConflicts { conflicts: errors }));
        }

        // Pipeline stage 4: final terms (after rewriting) -> EDB insertions.
        // Collect into non_fts_*.

        causecausetx_instant = get_or_insert_causecausetx_instant(&mut aev_trie, &self.schemaReplicant, self.causecausetx_id)?;

        for ((a, attribute), evs) in aev_trie {
            if causetids::might_update_spacetime(a) {
                causecausetx_might_update_spacetime = true;
            }

            let mut queue = match (attribute.fulltext, attribute.multival) {
                (false, true) => &mut non_fts_many,
                (false, false) => &mut non_fts_one,
                (true, false) => &mut fts_one,
                (true, true) => &mut fts_many,
            };

            for (e, ars) in evs {
                for (added, v) in ars.add.into_iter().map(|v| (true, v)).chain(ars.retract.into_iter().map(|v| (false, v))) {
                    let op = match added {
                        true => OpType::Add,
                        false => OpType::Retract,
                    };
                    self.watcher.Causet(op, e, a, &v);
                    queue.push((e, a, attribute, v, added));
                }
            }
        }

        if !non_fts_one.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_one[..], edb::SearchType::Inexact)?;
        }

        if !non_fts_many.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_many[..], edb::SearchType::Exact)?;
        }

        if !fts_one.is_empty() {
            self.store.insert_fts_searches(&fts_one[..], edb::SearchType::Inexact)?;
        }

        if !fts_many.is_empty() {
            self.store.insert_fts_searches(&fts_many[..], edb::SearchType::Exact)?;
        }

        match action {
            TransactorAction::Materialize => {
                self.store.materialize_edb_transaction(self.causecausetx_id)?;
            },
            TransactorAction::MaterializeAndCommit => {
                self.store.materialize_edb_transaction(self.causecausetx_id)?;
                self.store.commit_edb_transaction(self.causecausetx_id)?;
            }
        }

        }

        self.watcher.done(&self.causecausetx_id, self.schemaReplicant)?;

        if causecausetx_might_update_spacetime {
            // Extract changes to spacetime from the store.
            let spacetime_assertions = match action {
                TransactorAction::Materialize => self.store.resolved_spacetime_assertions()?,
                TransactorAction::MaterializeAndCommit => edb::committed_spacetime_assertions(self.store, self.causecausetx_id)?
            };
            let mut new_schemaReplicant = (*self.schemaReplicant_for_mutation).clone(); // Clone the underlying SchemaReplicant for modification.
            let spacetime_report = spacetime::update_schemaReplicant_from_causetid_quadruples(&mut new_schemaReplicant, spacetime_assertions)?;
            // We might not have made any changes to the schemaReplicant, even though it looked like we
            // would.  This should not happen, even during bootstrapping: we mutate an empty
            // `SchemaReplicant` in this case specifically to run the bootstrapped assertions through the
            // regular transactor code paths, updating the schemaReplicant and materialized views uniformly.
            // But, belt-and-braces: handle it gracefully.
            if new_schemaReplicant != *self.schemaReplicant_for_mutation {
                let old_schemaReplicant = (*self.schemaReplicant_for_mutation).clone(); // Clone the original SchemaReplicant for comparison.
                *self.schemaReplicant_for_mutation.to_mut() = new_schemaReplicant; // Store the new SchemaReplicant.
                edb::update_spacetime(self.store, &old_schemaReplicant, &*self.schemaReplicant_for_mutation, &spacetime_report)?;
            }
        }

        Ok(TxReport {
            causecausetx_id: self.causecausetx_id,
            causecausetx_instant,
            tempids: tempids,
        })
    }
}

/// Initialize a new Tx object with a new causetx id and a causetx instant. Kick off the SQLite conn, too.
fn start_causecausetx<'conn, 'a, W>(conn: &'conn rusqlite::Connection,
                       mut partition_map: PartitionMap,
                       schemaReplicant_for_mutation: &'a SchemaReplicant,
                       schemaReplicant: &'a SchemaReplicant,
                       watcher: W) -> Result<Tx<'conn, 'a, W>>
    where W: TransactWatcher {
    let causecausetx_id = partition_map.allocate_causetid(":edb.part/causetx");
    conn.begin_causecausetx_application()?;

    Ok(Tx::new(conn, partition_map, schemaReplicant_for_mutation, schemaReplicant, watcher, causecausetx_id))
}

fn conclude_causecausetx<W>(causetx: Tx<W>, report: TxReport) -> Result<(TxReport, PartitionMap, Option<SchemaReplicant>, W)>
where W: TransactWatcher {
    // If the schemaReplicant has moved on, return it.
    let next_schemaReplicant = match causetx.schemaReplicant_for_mutation {
        Cow::Borrowed(_) => None,
        Cow::Owned(next_schemaReplicant) => Some(next_schemaReplicant),
    };
    Ok((report, causetx.partition_map, next_schemaReplicant, causetx.watcher))
}

/// Transact the given `entities` against the given SQLite `conn`, using the given spacetime.
/// If you want this work to occur inside a SQLite transaction, establish one on the connection
/// prior to calling this function.
///
/// This approach is explained in https://github.com/whtcorpsinc/edb/wiki/Transacting.
// TODO: move this to the transactor layer.
pub fn transact<'conn, 'a, I, V, W>(conn: &'conn rusqlite::Connection,
                                 partition_map: PartitionMap,
                                 schemaReplicant_for_mutation: &'a SchemaReplicant,
                                 schemaReplicant: &'a SchemaReplicant,
                                 watcher: W,
                                 entities: I) -> Result<(TxReport, PartitionMap, Option<SchemaReplicant>, W)>
    where I: IntoIterator<Item=Instanton<V>>,
          V: TransacBlockValue,
          W: TransactWatcher {

    let mut causetx = start_causecausetx(conn, partition_map, schemaReplicant_for_mutation, schemaReplicant, watcher)?;
    let report = causetx.transact_entities(entities)?;
    conclude_causecausetx(causetx, report)
}

/// Just like `transact`, but accepts lower-level inputs to allow bypassing the parser interface.
pub fn transact_terms<'conn, 'a, I, W>(conn: &'conn rusqlite::Connection,
                                       partition_map: PartitionMap,
                                       schemaReplicant_for_mutation: &'a SchemaReplicant,
                                       schemaReplicant: &'a SchemaReplicant,
                                       watcher: W,
                                       terms: I,
                                       tempid_set: InternSet<TempId>) -> Result<(TxReport, PartitionMap, Option<SchemaReplicant>, W)>
    where I: IntoIterator<Item=TermWithTempIds>,
          W: TransactWatcher {

    transact_terms_with_action(
        conn, partition_map, schemaReplicant_for_mutation, schemaReplicant, watcher, terms, tempid_set,
        TransactorAction::MaterializeAndCommit
    )
}

pub(crate) fn transact_terms_with_action<'conn, 'a, I, W>(conn: &'conn rusqlite::Connection,
                                       partition_map: PartitionMap,
                                       schemaReplicant_for_mutation: &'a SchemaReplicant,
                                       schemaReplicant: &'a SchemaReplicant,
                                       watcher: W,
                                       terms: I,
                                       tempid_set: InternSet<TempId>,
                                       action: TransactorAction) -> Result<(TxReport, PartitionMap, Option<SchemaReplicant>, W)>
    where I: IntoIterator<Item=TermWithTempIds>,
          W: TransactWatcher {

    let mut causetx = start_causecausetx(conn, partition_map, schemaReplicant_for_mutation, schemaReplicant, watcher)?;
    let report = causetx.transact_simple_terms_with_action(terms, tempid_set, action)?;
    conclude_causecausetx(causetx, report)
}

fn extend_aev_trie<'schemaReplicant, I>(schemaReplicant: &'schemaReplicant SchemaReplicant, terms: I, trie: &mut AEVTrie<'schemaReplicant>) -> Result<()>
where I: IntoIterator<Item=TermWithoutTempIds>
{
    for Term::AddOrRetract(op, KnownSolitonId(e), a, v) in terms.into_iter() {
        let attribute: &Attribute = schemaReplicant.require_attribute_for_causetid(a)?;

        let a_and_r = trie
            .entry((a, attribute)).or_insert(BTreeMap::default())
            .entry(e).or_insert(AddAndRetract::default());

        match op {
            OpType::Add => a_and_r.add.insert(v),
            OpType::Retract => a_and_r.retract.insert(v),
        };
    }

    Ok(())
}

pub(crate) fn into_aev_trie<'schemaReplicant>(schemaReplicant: &'schemaReplicant SchemaReplicant, final_populations: FinalPopulations, inert_terms: Vec<TermWithTempIds>) -> Result<AEVTrie<'schemaReplicant>> {
    let mut trie = AEVTrie::default();
    extend_aev_trie(schemaReplicant, final_populations.resolved, &mut trie)?;
    extend_aev_trie(schemaReplicant, final_populations.allocated, &mut trie)?;
    // Inert terms need to be unwrapped.  It is a coding error if a term can't be unwrapped.
    extend_aev_trie(schemaReplicant, inert_terms.into_iter().map(|term| term.unwrap()), &mut trie)?;

    Ok(trie)
}

/// Transact [:edb/add :edb/causecausetxInstant causecausetx_instant (transaction-causetx)] if the trie doesn't contain it
/// already.  Return the instant from the input or the instant inserted.
fn get_or_insert_causecausetx_instant<'schemaReplicant>(aev_trie: &mut AEVTrie<'schemaReplicant>, schemaReplicant: &'schemaReplicant SchemaReplicant, causecausetx_id: SolitonId) -> Result<DateTime<Utc>> {
    let ars = aev_trie
        .entry((causetids::DB_TX_INSTANT, schemaReplicant.require_attribute_for_causetid(causetids::DB_TX_INSTANT)?))
        .or_insert(BTreeMap::default())
        .entry(causecausetx_id)
        .or_insert(AddAndRetract::default());
    if !ars.retract.is_empty() {
        // Cannot retract :edb/causecausetxInstant!
    }

    // Otherwise we have a coding error -- we should have cardinality checked this already.
    assert!(ars.add.len() <= 1);

    let first = ars.add.iter().next().cloned();
    match first {
        Some(MinkowskiType::Instant(instant)) => Ok(instant),
        Some(_) => unreachable!(), // This is a coding error -- we should have typechecked this already.
        None => {
            let instant = now();
            ars.add.insert(instant.into());
            Ok(instant)
        },
    }
}
