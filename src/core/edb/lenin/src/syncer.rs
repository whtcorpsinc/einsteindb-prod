// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt;

use std::collections::HashSet;

use rusqlite;
use uuid::Uuid;

use allegrosql_promises::{
    SolitonId,
    KnownSolitonId,
    MinkowskiType,
};

use edbn::{
    PlainSymbol,
};
use edbn::entities::{
    TxFunction,
    InstantonPlace,
    LookupRef,
};
use einstein_db::{
    CORE_SCHEMA_VERSION,
    lightcones,
    debug,
    causetids,
    PartitionMap,
};
use edb_transaction::{
    InProgress,
    TermBuilder,
    CausetQable,
};

use edb_transaction::instanton_builder::{
    BuildTerms,
};

use edb_transaction::causetq::{
    CausetQInputs,
    ToUpper,
};

use bootstrap::{
    BootstrapHelper,
};

use public_promises::errors::{
    Result,
};

use lenin_promises::errors::{
    LeninError,
};
use spacetime::{
    PartitionsBlock,
    SyncSpacetime,
};
use schemaReplicant::{
    ensure_current_version,
};
use causecausetx_uploader::TxUploader;
use causecausetx_processor::{
    Processor,
    TxReceiver,
};
use causecausetx_mapper::{
    TxMapper,
};
use types::{
    LocalTx,
    Tx,
    TxPart,
    GlobalTransactionLog,
};

use logger::d;

pub struct Syncer {}

#[derive(Debug,PartialEq,Clone)]
pub enum SyncFollowup {
    None,
    FullSync,
}

impl fmt::Display for SyncFollowup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SyncFollowup::None => write!(f, "None"),
            SyncFollowup::FullSync => write!(f, "Full sync"),
        }
    }
}

#[derive(Debug,PartialEq,Clone)]
pub enum SyncReport {
    IncompatibleRemoteBootstrap(i64, i64),
    BadRemoteState(String),
    NoChanges,
    RemoteFastForward,
    LocalFastForward,
    Merge(SyncFollowup),
}

pub enum SyncResult {
    Atomic(SyncReport),
    NonAtomic(Vec<SyncReport>),
}

impl fmt::Display for SyncReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SyncReport::IncompatibleRemoteBootstrap(local, remote) => {
                write!(f, "Incompatible remote bootstrap transaction version. Local: {}, remote: {}.", local, remote)
            },
            SyncReport::BadRemoteState(err) => {
                write!(f, "Bad remote state: {}", err)
            },
            SyncReport::NoChanges => {
                write!(f, "Neither local nor remote have any new changes")
            },
            SyncReport::RemoteFastForward => {
                write!(f, "Fast-forwarded remote")
            },
            SyncReport::LocalFastForward => {
                write!(f, "Fast-forwarded local")
            },
            SyncReport::Merge(follow_up) => {
                write!(f, "Merged local and remote, requesting a follow-up: {}", follow_up)
            }
        }
    }
}

impl fmt::Display for SyncResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SyncResult::Atomic(report) => write!(f, "Single atomic sync: {}", report),
            SyncResult::NonAtomic(reports) => {
                writeln!(f, "Series of atomic syncs ({})", reports.len())?;
                for report in reports {
                    writeln!(f, "{}", report)?;
                }
                writeln!(f, "\\o/")
            }
        }
    }
}

#[derive(Debug,PartialEq)]
enum SyncAction {
    NoOp,
    // TODO this is the same as remote fast-forward from local root.
    // It's currently distinguished from remote fast-forward for a more
    // path through the "first-sync against non-empty remote" flow.
    PopulateRemote,
    RemoteFastForward,
    LocalFastForward,
    // Generic name since we might merge, or rebase, or do something else.
    CombineChanges,
}

/// Represents remote state relative to previous sync.
/// On first sync, it's always "Changed" unless remote is "Empty".
pub enum RemoteDataState {
    Empty,
    Changed,
    Unchanged,
}

/// Remote state is expressed in terms of what "remote head" actually is,
/// and what we think it is.
impl<'a> From<(&'a Uuid, &'a Uuid)> for RemoteDataState {
    fn from((known_remote_head, actual_remote_head): (&Uuid, &Uuid)) -> RemoteDataState {
        if *actual_remote_head == Uuid::nil() {
            RemoteDataState::Empty
        } else if actual_remote_head != known_remote_head {
            RemoteDataState::Changed
        } else {
            RemoteDataState::Unchanged
        }
    }
}

/// Represents local state relative to previous sync.
/// On first sync it's always "Changed".
/// Local client can't be empty: there's always at least a bootstrap transaction.
pub enum LocalDataState {
    Changed,
    Unchanged,
}

/// Local state is expressed in terms of presence of a "mapping" for the local head.
/// Presence of a mapping means that we've uploaded our local head,
/// indicating that there's no local changes.
/// Absence of a mapping indicates that local head hasn't been uploaded
/// and that we have local changes.
impl From<Option<Uuid>> for LocalDataState {
    fn from(mapped_local_head: Option<Uuid>) -> LocalDataState {
        match mapped_local_head {
            Some(_) => LocalDataState::Unchanged,
            None => LocalDataState::Changed
        }
    }
}

// TODO rename this thing.
pub struct LocalTxSet {
    causecausetxs: Vec<LocalTx>,
}

impl LocalTxSet {
    pub fn new() -> LocalTxSet {
        LocalTxSet {
            causecausetxs: vec![]
        }
    }
}

impl TxReceiver<Vec<LocalTx>> for LocalTxSet {
    fn causetx<T>(&mut self, causecausetx_id: SolitonId, causets: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        self.causecausetxs.push(LocalTx {
            causetx: causecausetx_id,
            parts: causets.collect()
        });
        Ok(())
    }

    fn done(self) -> Vec<LocalTx> {
        self.causecausetxs
    }
}

impl Syncer {
    /// Produces a SyncAction based on local and remote states.
    fn what_do(remote_state: RemoteDataState, local_state: LocalDataState) -> SyncAction {
        match remote_state {
            RemoteDataState::Empty => {
                SyncAction::PopulateRemote
            },

            RemoteDataState::Changed => {
                match local_state {
                    LocalDataState::Changed => {
                        SyncAction::CombineChanges
                    },

                    LocalDataState::Unchanged => {
                        SyncAction::LocalFastForward
                    },
                }
            },

            RemoteDataState::Unchanged => {
                match local_state {
                    LocalDataState::Changed => {
                        SyncAction::RemoteFastForward
                    },

                    LocalDataState::Unchanged => {
                        SyncAction::NoOp
                    },
                }
            },
        }
    }

    /// Upload local causecausetxs: (from_causecausetx, HEAD]. Remote head is necessary here because we need to specify
    /// "parent" for each transaction we'll upload; remote head will be first transaction's parent.
    fn fast_forward_remote<R>(edb_causecausetx: &mut rusqlite::Transaction, from_causecausetx: Option<SolitonId>, remote_client: &mut R, remote_head: &Uuid) -> Result<()>
        where R: GlobalTransactionLog {

        // TODO consider moving head manipulations into uploader?

        let report;

        // Scope to avoid double-borrowing muBlock remote_client.
        {
            // Prepare an uploader.
            let uploader = TxUploader::new(
                remote_client,
                remote_head,
                SyncSpacetime::get_partitions(edb_causecausetx, PartitionsBlock::Lenin)?
            );
            // Walk the local bundles in the database and upload them.
            report = Processor::process(edb_causecausetx, from_causecausetx, uploader)?;
        }

        if let Some(last_causecausetx_uploaded) = report.head {
            // Upload remote head.
            remote_client.set_head(&last_causecausetx_uploaded)?;

            // On success:
            // - persist local mappings from the receiver
            // - update our local "remote head".
            TxMapper::set_lg_mappings(
                edb_causecausetx,
                report.temp_uuids.iter().map(|v| (*v.0, v.1).into()).collect()
            )?;

            SyncSpacetime::set_remote_head(edb_causecausetx, &last_causecausetx_uploaded)?;
        }

        Ok(())
    }

    fn local_causecausetx_for_uuid(edb_causecausetx: &rusqlite::Transaction, uuid: &Uuid) -> Result<SolitonId> {
        match TxMapper::get_causecausetx_for_uuid(edb_causecausetx, uuid)? {
            Some(t) => Ok(t),
            None => bail!(LeninError::TxIncorrectlyMapped(0))
        }
    }

    fn remote_parts_to_builder(builder: &mut TermBuilder, parts: Vec<TxPart>) -> Result<()> {
        for part in parts {
            let e: InstantonPlace<MinkowskiType>;
            let a = KnownSolitonId(part.a);
            let v = part.v;

            // Instead of providing a 'causecausetxInstant' Causet directly, we map it
            // into a (transaction-causetx) style assertion.
            // Transactor knows how to pick out a causecausetxInstant value out of these
            // assertions and use that value for the generated transaction's causecausetxInstant.
            if part.a == causetids::DB_TX_INSTANT {
                e = InstantonPlace::TxFunction(TxFunction { op: PlainSymbol("transaction-causetx".to_string()) } ).into();
            } else {
                e = KnownSolitonId(part.e).into();
            }

            if part.added {
                builder.add(e, a, v)?;
            } else {
                builder.retract(e, a, v)?;
            }
        }

        Ok(())
    }

    /// In context of a "transaction to be applied", a PartitionMap supplied here
    /// represents what a PartitionMap will be once this transaction is applied.
    /// This works well for regular assertions: causetids are supplied, and we need
    /// them to be allocated in the user partition space.
    /// However, we need to decrement 'causetx' partition's index, so that the transactor's
    /// allocated causetx will match what came off the wire.
    /// N.B.: this depends on absence of holes in the 'causetx' partition!
    fn rewind_causecausetx_partition_by_one(partition_map: &mut PartitionMap) -> Result<()> {
        if let Some(causecausetx_part) = partition_map.get_mut(":edb.part/causetx") {
            assert_eq!(false, causecausetx_part.allow_excision); // Sanity check.

            let next_causetid = causecausetx_part.next_causetid() - 1;
            causecausetx_part.set_next_causetid(next_causetid);
            Ok(())
        } else {
            bail!(LeninError::BadRemoteState("Missing causetx partition in an incoming transaction".to_string()));
        }
    }

    fn fast_forward_local<'a, 'c>(in_progress: &mut InProgress<'a, 'c>, causecausetxs: Vec<Tx>) -> Result<SyncReport> {
        let mut last_causecausetx = None;

        for causetx in causecausetxs {
            let mut builder = TermBuilder::new();

            // TODO both here and in the merge scenario we're doing the same thing with the partition maps
            // and with the causecausetxInstant Causet rewriting.
            // Figure out how to combine these operations into a resuable primitive(s).
            // See notes in 'merge' for why we're doing this stuff.
            let mut partition_map = match causetx.parts[0].partitions.clone() {
                Some(parts) => parts,
                None => return Ok(SyncReport::BadRemoteState("Missing partition map in incoming transaction".to_string()))
            };

            // Make space in the provided causetx partition for the transaction we're about to create.
            // See function's notes for details.
            Syncer::rewind_causecausetx_partition_by_one(&mut partition_map)?;
            Syncer::remote_parts_to_builder(&mut builder, causetx.parts)?;

            // Allocate space for the incoming causetids.
            in_progress.partition_map = partition_map;
            let report = in_progress.transact_builder(builder)?;
            last_causecausetx = Some((report.causecausetx_id, causetx.causetx.clone()));
        }

        // We've just transacted a new causetx, and generated a new causetx solitonId.  Map it to the corresponding
        // incoming causetx uuid, advance our "locally knownCauset remote head".
        if let Some((solitonId, uuid)) = last_causecausetx {
            SyncSpacetime::set_remote_head_and_map(&mut in_progress.transaction, (solitonId, &uuid).into())?;
        }

        Ok(SyncReport::LocalFastForward)
    }

    fn merge(ip: &mut InProgress, incoming_causecausetxs: Vec<Tx>, mut local_causecausetxs_to_merge: Vec<LocalTx>) -> Result<SyncReport> {
        d(&format!("Rewinding local bundles."));

        // 1) Rewind local to shared root.
        local_causecausetxs_to_merge.sort(); // TODO sort at the interface level?

        let (new_schemaReplicant, new_partition_map) = lightcones::move_from_main_lightcone(
            &ip.transaction,
            &ip.schemaReplicant,
            ip.partition_map.clone(),
            local_causecausetxs_to_merge[0].causetx..,
            // A poor man's parent reference. This might be brittle, although
            // excisions are prohibited in the 'causetx' partition, so this should hold...
            local_causecausetxs_to_merge[0].causetx - 1
        )?;
        match new_schemaReplicant {
            Some(schemaReplicant) => ip.schemaReplicant = schemaReplicant,
            None => ()
        };
        ip.partition_map = new_partition_map;

        // 2) Transact incoming.
        // 2.1) Prepare remote causetx tuples (TermBuilder, PartitionMap, Uuid), which represent
        // a remote transaction, its global causetIdifier and partitions after it's applied.
        d(&format!("Transacting incoming..."));
        let mut builders = vec![];
        for remote_causecausetx in incoming_causecausetxs {
            let mut builder = TermBuilder::new();

            let partition_map = match remote_causecausetx.parts[0].partitions.clone() {
                Some(parts) => parts,
                None => return Ok(SyncReport::BadRemoteState("Missing partition map in incoming transaction".to_string()))
            };

            Syncer::remote_parts_to_builder(&mut builder, remote_causecausetx.parts)?;

            builders.push((builder, partition_map, remote_causecausetx.causetx));
        }

        let mut remote_report = None;
        for (builder, mut partition_map, remote_causecausetx) in builders {
            // Make space in the provided causetx partition for the transaction we're about to create.
            // See function's notes for details.
            Syncer::rewind_causecausetx_partition_by_one(&mut partition_map)?;

            // This allocates our incoming causetids in each builder,
            // letting us just use KnownSolitonId in the builders.
            ip.partition_map = partition_map;
            remote_report = Some((ip.transact_builder(builder)?.causecausetx_id, remote_causecausetx));
        }

        d(&format!("Transacting local on top of incoming..."));
        // 3) Rebase local bundles on top of remote.
        let mut clean_rebase = true;
        for local_causecausetx in local_causecausetxs_to_merge {
            let mut builder = TermBuilder::new();

            // This is the beginnings of instanton merging.

            // An solitonId might be already knownCauset to the SchemaReplicant, or it
            // might be allocated in this transaction.
            // In the former case, refer to it verbatim.
            // In the latter case, rewrite it as a tempid, and let the transactor allocate it.
            let mut causetids_that_will_allocate = HashSet::new();

            // We currently support "strict schemaReplicant merging": we'll smush attribute definitions,
            // but only if they're the same.
            // e.g. prohibited would be defining different cardinality for the same attribute.
            // Defining new attributes is allowed if:
            // - attribute is defined either on local or remote,
            // - attribute is defined on both local and remote in the same way.
            // Modifying an attribute is currently not supported (requires higher order schemaReplicant migrations).
            // Note that "same" local and remote attributes might have different causetids in the
            // two sets of bundles.

            // Set of entities that may alter "installed" attribute.
            // Since this is a rebase of local on top of remote, an "installed"
            // attribute might be one that was present in the root, or one that was
            // defined by remote.
            let mut might_alter_installed_attributes = HashSet::new();

            // Set of entities that describe a new attribute, not present in the root
            // or on the remote.
            let mut will_not_alter_installed_attribute = HashSet::new();

            // Note that at this point, remote and local have flipped - we're transacting
            // local on top of incoming (which are already in the schemaReplicant).

            // Go through local causets, and classify any schemaReplicant-altering causetids into
            // one of the two sets above.
            for part in &local_causecausetx.parts {
                // If we have an causetid definition locally, check if remote
                // already defined this causetid. If it did, we'll need to ensure
                // both local and remote are defining it in the same way.
                if part.a == causetids::DB_CausetID {
                    match part.v {
                        MinkowskiType::Keyword(ref local_kw) => {
                            // Remote did not define this causetid. Make a note of it,
                            // so that we'll know to ignore its attribute causets.
                            if !ip.schemaReplicant.causetId_map.contains_key(local_kw) {
                                will_not_alter_installed_attribute.insert(part.e);

                            // Otherwise, we'll need to ensure we have the same attribute definition
                            // for it.
                            } else {
                                might_alter_installed_attributes.insert(part.e);
                            }
                        },
                        _ => panic!("programming error: wrong value type for a local causetid")
                    }
                } else if causetids::is_a_schemaReplicant_attribute(part.a) && !will_not_alter_installed_attribute.contains(&part.e) {
                    might_alter_installed_attributes.insert(part.e);
                }
            }

            for part in &local_causecausetx.parts {
                match part.a {
                    // We'll be ignoring this Causet later on (to be generated by the transactor).
                    // During a merge we're concerned with entities in the "user" partition,
                    // while this falls into the "causetx" partition.
                    // We have preserved the original causecausetxInstant value on the alternate lightcone.
                    causetids::DB_TX_INSTANT => continue,

                    // 'e's will be replaced with tempids, letting transactor handle everything.
                    // Non-unique entities are "duplicated". Unique entities are upserted.
                    _ => {
                        // Retractions never allocated tempids in the transactor.
                        if part.added {
                            causetids_that_will_allocate.insert(part.e);
                        }
                    },
                }
            }

            // :edb/causetid is a edb.unique/causetIdity attribute, which means transactor will upsert
            // attribute assertions. E.g. if a new attribute was defined on local and not on remote,
            // it will be inserted. If both local and remote defined the same attribute
            // with different causetids, we'll converge and use remote's solitonId.

            // Same follows for other types of edb.unique/causetIdity attributes.
            // If user-defined attribute is edb.unique/causetIdity, we'll "smush" local and remote
            // assertions against it.
            // For example, {:p/name "Grisha"} assertion on local and
            // {:p/name "Grisha"} assertion on remote will result in a single instanton.

            // If user-defined attribute is not unique, however, no smushing will be performed.
            // The above example will result in two entities.

            for part in local_causecausetx.parts {
                // Skip the "causetx instant" Causet: it will be generated by our transactor.
                // We don't care about preserving exact state of these causets: they're already
                // stashed away on the lightcone we've created above.
                if part.a == causetids::DB_TX_INSTANT {
                    continue;
                }

                let e: InstantonPlace<MinkowskiType>;
                let a = KnownSolitonId(part.a);
                let v = part.v;

                // Rewrite causetids if they will allocate (see instanton merging notes above).
                if causetids_that_will_allocate.contains(&part.e) {
                    e = builder.named_tempid(format!("{}", part.e)).into();
                // Otherwise, refer to existing entities.
                } else {
                    e = KnownSolitonId(part.e).into();
                }

                // TODO we need to do the same rewriting for part.v if it's a Ref.

                // N.b.: attribute can't refer to an unallocated instanton, so it's always a KnownSolitonId.
                // To illustrate, this is not a valid transaction, and will fail ("no solitonId found for causetid: :person/name"):
                // [
                //  {:edb/causetid :person/name :edb/valueType :edb.type/string :edb/cardinality :edb.cardinality/one}
                //  {:person/name "Grisha"}
                // ]
                // One would need to split that transaction into two,
                // at which point :person/name will refer to an allocated instanton.

                match part.added {
                    true => builder.add(e, a, v)?,
                    false => {
                        if causetids_that_will_allocate.contains(&part.e) {
                            builder.retract(e, a, v)?;
                            continue;
                        }

                        // TODO handle tempids in ValuePlace, as well.

                        // Retractions with non-upserting tempids are not currently supported.
                        // We work around this by using a lookup-ref instead of the instanton tempid.
                        // However:
                        // - lookup-ref can only be used for attributes which are :edb/unique,
                        // - a lookup-ref must resolve. If it doesn't, our transaction will fail.
                        // And so:
                        // - we skip retractions of non-unique attributes,
                        // - we "pre-run" a lookup-ref to ensure it will resolve,
                        //   and skip the retraction otherwise.
                        match ip.schemaReplicant.attribute_map.get(&part.a) {
                            Some(attributes) => {
                                // A lookup-ref using a non-unique attribute will fail.
                                // Skip this retraction, since we can't make sense of it.
                                if attributes.unique.is_none() {
                                    continue;
                                }
                            },
                            None => panic!("programming error: missing attribute map for a knownCauset attribute")
                        }

                        // TODO prepare a causetq and re-use it for all retractions of this type
                        let pre_lookup = ip.q_once(
                            "[:find ?e . :in ?a ?v :where [?e ?a ?v]]",
                            CausetQInputs::with_value_sequence(
                                vec![
                                    (ToUpper::from_valid_name("?a"), a.into()),
                                    (ToUpper::from_valid_name("?v"), v.clone()),
                                ]
                            )
                        )?;

                        if pre_lookup.is_empty() {
                            continue;
                        }

                        // TODO just use the value from the causetq instead of doing _another_ lookup-ref!

                        builder.retract(
                            InstantonPlace::LookupRef(LookupRef {a: a.into(), v: v.clone()}), a, v
                        )?;
                    }
                }
            }

            // After all these checks, our builder might be empty: short-circuit.
            if builder.is_empty() {
                continue;
            }

            d(&format!("Savepoint before transacting a local causetx..."));
            ip.savepoint("speculative_local")?;

            d(&format!("Transacting builder filled with local causecausetxs... {:?}", builder));

            let report = ip.transact_builder(builder)?;

            // Let's check that we didn't modify any schemaReplicant attributes.
            // Our current attribute map in the schemaReplicant isn't rich enough to allow
            // for this check: it's missing a notion of "attribute absence" - we can't
            // distinguish between a missing attribute and a default value.
            // Instead, we simply causetq the database, checking if transaction produced
            // any schemaReplicant-altering causets.
            for e in might_alter_installed_attributes.iter() {
                match report.tempids.get(&format!("{}", e)) {
                    Some(resolved_e) => {
                        if SyncSpacetime::has_instanton_assertions_in_causecausetx(&ip.transaction, *resolved_e, report.causecausetx_id)? {
                            bail!(LeninError::NotYetImplemented("Can't sync with schemaReplicant alterations yet.".to_string()));
                        }
                    },
                    None => ()
                }
            }

            if !SyncSpacetime::is_causecausetx_empty(&ip.transaction, report.causecausetx_id)? {
                d(&format!("causetx {} is not a no-op", report.causecausetx_id));
                clean_rebase = false;
                ip.release_savepoint("speculative_local")?;
            } else {
                d(&format!("Applied causetx {} as a no-op. Rolling back the savepoint (empty causetx clean-up).", report.causecausetx_id));
                ip.rollback_savepoint("speculative_local")?;
            }
        }

        // TODO
        // At this point, we've rebased local bundles on top of remote.
        // This would be a good point to create a "merge commit" and upload our loosing lightcone.

        // Since we don't upload during a merge (instead, we request a follow-up sync),
        // set the locally knownCauset remote HEAD to what we received from the 'remote'.
        if let Some((solitonId, uuid)) = remote_report {
            SyncSpacetime::set_remote_head_and_map(&mut ip.transaction, (solitonId, &uuid).into())?;
        }

        // If necessary, request a full sync as a follow-up to fast-forward remote.
        if clean_rebase {
            Ok(SyncReport::Merge(SyncFollowup::None))
        } else {
            Ok(SyncReport::Merge(SyncFollowup::FullSync))
        }
    }

    fn first_sync_against_non_empty<R>(ip: &mut InProgress, remote_client: &R, local_spacetime: &SyncSpacetime) -> Result<SyncReport>
        where R: GlobalTransactionLog {

        d(&format!("remote non-empty on first sync, adopting remote state."));

        // 1) Download remote bundles.
        let incoming_causecausetxs = remote_client.bundles_after(&Uuid::nil())?;
        if incoming_causecausetxs.len() == 0 {
            return Ok(SyncReport::BadRemoteState("Remote specified non-root HEAD but gave no bundles".to_string()));
        }

        // 2) Process remote bootstrap.
        let remote_bootstrap = &incoming_causecausetxs[0];
        let local_bootstrap = local_spacetime.root;
        let bootstrap_helper = BootstrapHelper::new(remote_bootstrap);

        if !bootstrap_helper.is_compatible()? {
            return Ok(SyncReport::IncompatibleRemoteBootstrap(CORE_SCHEMA_VERSION as i64, bootstrap_helper.allegro_schemaReplicant_version()?));
        }

        d(&format!("mapping incoming bootstrap causetx uuid to local bootstrap solitonId: {} -> {}", remote_bootstrap.causetx, local_bootstrap));

        // Map incoming bootstrap causetx uuid to local bootstrap solitonId.
        // If there's more work to do, we'll move the head again.
        SyncSpacetime::set_remote_head_and_map(&mut ip.transaction, (local_bootstrap, &remote_bootstrap.causetx).into())?;

        // 3) Determine new local and remote data states, now that bootstrap has been dealt with.
        let remote_state = if incoming_causecausetxs.len() > 1 {
            RemoteDataState::Changed
        } else {
            RemoteDataState::Unchanged
        };

        let local_state = if local_spacetime.root != local_spacetime.head {
            LocalDataState::Changed
        } else {
            LocalDataState::Unchanged
        };

        // 4) The rest of this flow isn't that special anymore.
        // Since we've "merged" with the remote bootstrap, the "no-op" and
        // "local fast-forward" cases are reported as merges.
        match Syncer::what_do(remote_state, local_state) {
            SyncAction::NoOp => {
                Ok(SyncReport::Merge(SyncFollowup::None))
            },

            SyncAction::PopulateRemote => {
                // This is a programming error.
                bail!(LeninError::UnexpectedState(format!("Remote state can't be empty on first sync against non-empty remote")))
            },

            SyncAction::RemoteFastForward => {
                bail!(LeninError::NotYetImplemented(format!("TODO fast-forward remote on first sync when remote is just bootstrap and local has more")))
            },

            SyncAction::LocalFastForward => {
                Syncer::fast_forward_local(ip, incoming_causecausetxs[1 ..].to_vec())?;
                Ok(SyncReport::Merge(SyncFollowup::None))
            },

            SyncAction::CombineChanges => {
                let local_causecausetxs = Processor::process(
                    &mut ip.transaction, Some(local_spacetime.root), LocalTxSet::new())?;
                Syncer::merge(
                    ip,
                    incoming_causecausetxs[1 ..].to_vec(),
                    local_causecausetxs
                )
            }
        }
    }

    pub fn sync<R>(ip: &mut InProgress, remote_client: &mut R) -> Result<SyncReport>
        where R: GlobalTransactionLog {

        d(&format!("sync flowing"));

        ensure_current_version(&mut ip.transaction)?;

        let remote_head = remote_client.head()?;
        d(&format!("remote head {:?}", remote_head));

        let locally_known_remote_head = SyncSpacetime::remote_head(&mut ip.transaction)?;
        d(&format!("local head {:?}", locally_known_remote_head));

        let (root, head) = SyncSpacetime::root_and_head_causecausetx(&mut ip.transaction)?;
        let local_spacetime = SyncSpacetime::new(root, head);

        // impl From ... vs ::new() calls to constuct "state" objects?
        let local_state = TxMapper::get(&mut ip.transaction, local_spacetime.head)?.into();
        let remote_state = (&locally_known_remote_head, &remote_head).into();

        // Currently, first sync against a non-empty remote is special.
        if locally_known_remote_head == Uuid::nil() && remote_head != Uuid::nil() {
            return Syncer::first_sync_against_non_empty(ip, remote_client, &local_spacetime);
        }

        match Syncer::what_do(remote_state, local_state) {
            SyncAction::NoOp => {
                d(&format!("local HEAD did not move. Nothing to do!"));
                Ok(SyncReport::NoChanges)
            },

            SyncAction::PopulateRemote => {
                d(&format!("empty remote!"));
                Syncer::fast_forward_remote(&mut ip.transaction, None, remote_client, &remote_head)?;
                Ok(SyncReport::RemoteFastForward)
            },

            SyncAction::RemoteFastForward => {
                d(&format!("local HEAD moved."));
                let upload_from_causecausetx = Syncer::local_causecausetx_for_uuid(
                    &mut ip.transaction, &locally_known_remote_head
                )?;

                d(&format!("Fast-forwarding the remote."));

                // TODO it's possible that we've successfully advanced remote head previously,
                // but failed to advance our own local head. If that's the case, and we can recognize it,
                // our sync becomes just bumping our local head. AFAICT below would currently fail.
                Syncer::fast_forward_remote(
                    &mut ip.transaction, Some(upload_from_causecausetx), remote_client, &remote_head
                )?;
                Ok(SyncReport::RemoteFastForward)
            },

            SyncAction::LocalFastForward => {
                d(&format!("fast-forwarding local store."));
                Syncer::fast_forward_local(
                    ip,
                    remote_client.bundles_after(&locally_known_remote_head)?
                )?;
                Ok(SyncReport::LocalFastForward)
            },

            SyncAction::CombineChanges => {
                d(&format!("combining changes from local and remote stores."));
                // Get the starting point for out local set of causecausetxs to merge.
                let combine_local_from_causecausetx = Syncer::local_causecausetx_for_uuid(
                    &mut ip.transaction, &locally_known_remote_head
                )?;
                let local_causecausetxs = Processor::process(
                    &mut ip.transaction,
                    Some(combine_local_from_causecausetx),
                    LocalTxSet::new()
                )?;
                // Merge!
                Syncer::merge(
                    ip,
                    // Remote causecausetxs to merge...
                    remote_client.bundles_after(&locally_known_remote_head)?,
                    // ... with the local causecausetxs.
                    local_causecausetxs
                )
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_what_do() {
        assert_eq!(SyncAction::PopulateRemote, Syncer::what_do(RemoteDataState::Empty, LocalDataState::Unchanged));
        assert_eq!(SyncAction::PopulateRemote, Syncer::what_do(RemoteDataState::Empty, LocalDataState::Changed));

        assert_eq!(SyncAction::NoOp,              Syncer::what_do(RemoteDataState::Unchanged, LocalDataState::Unchanged));
        assert_eq!(SyncAction::RemoteFastForward, Syncer::what_do(RemoteDataState::Unchanged, LocalDataState::Changed));

        assert_eq!(SyncAction::LocalFastForward, Syncer::what_do(RemoteDataState::Changed, LocalDataState::Unchanged));
        assert_eq!(SyncAction::CombineChanges,   Syncer::what_do(RemoteDataState::Changed, LocalDataState::Changed));
    }
}
