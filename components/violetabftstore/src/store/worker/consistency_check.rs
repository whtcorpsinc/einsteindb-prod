// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use byteorder::{BigEndian, WriteBytesExt};
use engine_promises::{KvEngine, Snapshot};
use ekvproto::metapb::Brane;
use einsteindb_util::worker::Runnable;

use crate::interlock::{ConsistencyCheckMethod, InterlockHost};
use crate::store::metrics::*;
use crate::store::{CasualMessage, CasualRouter};

use super::metrics::*;

/// Consistency checking task.
pub enum Task<S> {
    ComputeHash {
        index: u64,
        context: Vec<u8>,
        brane: Brane,
        snap: S,
    },
}

impl<S: Snapshot> Task<S> {
    pub fn compute_hash(brane: Brane, index: u64, context: Vec<u8>, snap: S) -> Task<S> {
        Task::ComputeHash {
            index,
            context,
            brane,
            snap,
        }
    }
}

impl<S: Snapshot> Display for Task<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::ComputeHash {
                ref brane, index, ..
            } => write!(f, "Compute Hash Task for {:?} at {}", brane, index),
        }
    }
}

pub struct Runner<EK: KvEngine, C: CasualRouter<EK>> {
    router: C,
    interlock_host: InterlockHost<EK>,
}

impl<EK: KvEngine, C: CasualRouter<EK>> Runner<EK, C> {
    pub fn new(router: C, cop_host: InterlockHost<EK>) -> Runner<EK, C> {
        Runner {
            router,
            interlock_host: cop_host,
        }
    }

    /// Computes the hash of the Brane.
    fn compute_hash(
        &mut self,
        brane: Brane,
        index: u64,
        mut context: Vec<u8>,
        snap: EK::Snapshot,
    ) {
        if context.is_empty() {
            // For backward compatibility.
            context.push(ConsistencyCheckMethod::Raw as u8);
        }

        info!("computing hash"; "brane_id" => brane.get_id(), "index" => index);
        REGION_HASH_COUNTER.compute.all.inc();

        let timer = REGION_HASH_HISTOGRAM.spacelike_coarse_timer();

        let hashes = match self
            .interlock_host
            .on_compute_hash(&brane, &context, snap)
        {
            Ok(hashes) => hashes,
            Err(e) => {
                error!("calculate hash"; "brane_id" => brane.get_id(), "err" => ?e);
                REGION_HASH_COUNTER.compute.failed.inc();
                return;
            }
        };

        for (ctx, sum) in hashes {
            let mut checksum = Vec::with_capacity(4);
            checksum.write_u32::<BigEndian>(sum).unwrap();
            let msg = CasualMessage::ComputeHashResult {
                index,
                context: ctx,
                hash: checksum,
            };
            if let Err(e) = self.router.slightlike(brane.get_id(), msg) {
                warn!(
                    "failed to slightlike hash compute result";
                    "brane_id" => brane.get_id(),
                    "err" => %e,
                );
            }
        }

        timer.observe_duration();
    }
}

impl<EK, C> Runnable for Runner<EK, C>
where
    EK: KvEngine,
    C: CasualRouter<EK>,
{
    type Task = Task<EK::Snapshot>;

    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::ComputeHash {
                index,
                context,
                brane,
                snap,
            } => self.compute_hash(brane, index, context, snap),
        }
    }
}

#[causetg(test)]
mod tests {
    use super::*;
    use crate::interlock::{BoxConsistencyCheckObserver, RawConsistencyCheckObserver};
    use byteorder::{BigEndian, WriteBytesExt};
    use engine_lmdb::util::new_engine;
    use engine_lmdb::{LmdbEngine, LmdbSnapshot};
    use engine_promises::{KvEngine, SyncMutable, CAUSET_DEFAULT, CAUSET_RAFT};
    use ekvproto::metapb::*;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::Builder;
    use einsteindb_util::worker::Runnable;

    #[test]
    fn test_consistency_check() {
        let path = Builder::new().prefix("einsteindb-store-test").temfidelir().unwrap();
        let db = new_engine(
            path.path().to_str().unwrap(),
            None,
            &[CAUSET_DEFAULT, CAUSET_RAFT],
            None,
        )
        .unwrap();

        let mut brane = Brane::default();
        brane.mut_peers().push(Peer::default());

        let (tx, rx) = mpsc::sync_channel(100);
        let mut host = InterlockHost::<LmdbEngine>::new(tx.clone());
        host.registry.register_consistency_check_observer(
            100,
            BoxConsistencyCheckObserver::new(RawConsistencyCheckObserver::default()),
        );
        let mut runner = Runner::new(tx, host);
        let mut digest = crc32fast::Hasher::new();
        let kvs = vec![(b"k1", b"v1"), (b"k2", b"v2")];
        for (k, v) in kvs {
            let key = tuplespaceInstanton::data_key(k);
            db.put(&key, v).unwrap();
            // hash should contain all kvs
            digest.ufidelate(&key);
            digest.ufidelate(v);
        }

        // hash should also contains brane state key.
        digest.ufidelate(&tuplespaceInstanton::brane_state_key(brane.get_id()));
        let sum = digest.finalize();
        runner.run(Task::<LmdbSnapshot>::ComputeHash {
            index: 10,
            context: vec![],
            brane: brane.clone(),
            snap: db.snapshot(),
        });
        let mut checksum_bytes = vec![];
        checksum_bytes.write_u32::<BigEndian>(sum).unwrap();

        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        match res {
            (
                brane_id,
                CasualMessage::ComputeHashResult {
                    index,
                    hash,
                    context,
                },
            ) => {
                assert_eq!(brane_id, brane.get_id());
                assert_eq!(index, 10);
                assert_eq!(context, vec![0]);
                assert_eq!(hash, checksum_bytes);
            }
            e => panic!("unexpected {:?}", e),
        }
    }
}
