//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use crate::store::{CasualMessage, CasualRouter};
use edb::{CausetEngine, Cone};
use error_code::ErrorCodeExt;
use ekvproto::{meta_timeshare::Brane, fidel_timeshare::CheckPolicy};
use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Mutex};

use super::super::error::Result;
use super::super::metrics::*;
use super::super::{Interlock, KeyEntry, SemaphoreContext, SplitCheckSemaphore, SplitChecker};
use super::Host;

pub struct Checker {
    max_tuplespaceInstanton_count: u64,
    split_memory_barrier: u64,
    current_count: u64,
    split_tuplespaceInstanton: Vec<Vec<u8>>,
    batch_split_limit: u64,
    policy: CheckPolicy,
}

impl Checker {
    pub fn new(
        max_tuplespaceInstanton_count: u64,
        split_memory_barrier: u64,
        batch_split_limit: u64,
        policy: CheckPolicy,
    ) -> Checker {
        Checker {
            max_tuplespaceInstanton_count,
            split_memory_barrier,
            current_count: 0,
            split_tuplespaceInstanton: Vec::with_capacity(1),
            batch_split_limit,
            policy,
        }
    }
}

impl<E> SplitChecker<E> for Checker
where
    E: CausetEngine,
{
    fn on_kv(&mut self, _: &mut SemaphoreContext<'_>, key: &KeyEntry) -> bool {
        if !key.is_commit_version() {
            return false;
        }
        self.current_count += 1;

        let mut over_limit = self.split_tuplespaceInstanton.len() as u64 >= self.batch_split_limit;
        if self.current_count > self.split_memory_barrier && !over_limit {
            self.split_tuplespaceInstanton.push(tuplespaceInstanton::origin_key(key.key()).to_vec());
            // if for previous on_kv() self.current_count == self.split_memory_barrier,
            // the split key would be pushed this time, but the entry for this time should not be ignored.
            self.current_count = 1;
            over_limit = self.split_tuplespaceInstanton.len() as u64 >= self.batch_split_limit;
        }

        // For a large brane, scan over the cone maybe cost too much time,
        // so limit the number of produced split_key for one batch.
        // Also need to scan over self.max_tuplespaceInstanton_count for last part.
        over_limit && self.current_count + self.split_memory_barrier >= self.max_tuplespaceInstanton_count
    }

    fn split_tuplespaceInstanton(&mut self) -> Vec<Vec<u8>> {
        // make sure not to split when less than max_tuplespaceInstanton_count for last part
        if self.current_count + self.split_memory_barrier < self.max_tuplespaceInstanton_count {
            self.split_tuplespaceInstanton.pop();
        }
        if !self.split_tuplespaceInstanton.is_empty() {
            mem::replace(&mut self.split_tuplespaceInstanton, vec![])
        } else {
            vec![]
        }
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

#[derive(Clone)]
pub struct TuplespaceInstantonCheckSemaphore<C, E> {
    router: Arc<Mutex<C>>,
    _phantom: PhantomData<E>,
}

impl<C: CasualRouter<E>, E> TuplespaceInstantonCheckSemaphore<C, E>
where
    E: CausetEngine,
{
    pub fn new(router: C) -> TuplespaceInstantonCheckSemaphore<C, E> {
        TuplespaceInstantonCheckSemaphore {
            router: Arc::new(Mutex::new(router)),
            _phantom: PhantomData,
        }
    }
}

impl<C: lightlike, E: lightlike> Interlock for TuplespaceInstantonCheckSemaphore<C, E> {}

impl<C: CasualRouter<E> + lightlike, E> SplitCheckSemaphore<E> for TuplespaceInstantonCheckSemaphore<C, E>
where
    E: CausetEngine,
{
    fn add_checker(
        &self,
        ctx: &mut SemaphoreContext<'_>,
        host: &mut Host<'_, E>,
        engine: &E,
        policy: CheckPolicy,
    ) {
        let brane = ctx.brane();
        let brane_id = brane.get_id();
        let brane_tuplespaceInstanton = match get_brane_approximate_tuplespaceInstanton(
            engine,
            brane,
            host.causet.brane_max_tuplespaceInstanton * host.causet.batch_split_limit,
        ) {
            Ok(tuplespaceInstanton) => tuplespaceInstanton,
            Err(e) => {
                warn!(
                    "failed to get approximate tuplespaceInstanton";
                    "brane_id" => brane_id,
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                // Need to check tuplespaceInstanton.
                host.add_checker(Box::new(Checker::new(
                    host.causet.brane_max_tuplespaceInstanton,
                    host.causet.brane_split_tuplespaceInstanton,
                    host.causet.batch_split_limit,
                    policy,
                )));
                return;
            }
        };

        let res = CasualMessage::BraneApproximateTuplespaceInstanton { tuplespaceInstanton: brane_tuplespaceInstanton };
        if let Err(e) = self.router.dagger().unwrap().lightlike(brane_id, res) {
            warn!(
                "failed to lightlike approximate brane tuplespaceInstanton";
                "brane_id" => brane_id,
                "err" => %e,
                "error_code" => %e.error_code(),
            );
        }

        REGION_KEYS_HISTOGRAM.observe(brane_tuplespaceInstanton as f64);
        if brane_tuplespaceInstanton >= host.causet.brane_max_tuplespaceInstanton {
            info!(
                "approximate tuplespaceInstanton over memory_barrier, need to do split check";
                "brane_id" => brane.get_id(),
                "tuplespaceInstanton" => brane_tuplespaceInstanton,
                "memory_barrier" => host.causet.brane_max_tuplespaceInstanton,
            );
            // Need to check tuplespaceInstanton.
            host.add_checker(Box::new(Checker::new(
                host.causet.brane_max_tuplespaceInstanton,
                host.causet.brane_split_tuplespaceInstanton,
                host.causet.batch_split_limit,
                policy,
            )));
        } else {
            // Does not need to check tuplespaceInstanton.
            debug!(
                "approximate tuplespaceInstanton less than memory_barrier, does not need to do split check";
                "brane_id" => brane.get_id(),
                "tuplespaceInstanton" => brane_tuplespaceInstanton,
                "memory_barrier" => host.causet.brane_max_tuplespaceInstanton,
            );
        }
    }
}

/// Get the approximate number of tuplespaceInstanton in the cone.
pub fn get_brane_approximate_tuplespaceInstanton(
    db: &impl CausetEngine,
    brane: &Brane,
    large_memory_barrier: u64,
) -> Result<u64> {
    let spacelike = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike = tuplespaceInstanton::enc_lightlike_key(brane);
    let cone = Cone::new(&spacelike, &lightlike);
    Ok(box_try!(db.get_cone_approximate_tuplespaceInstanton(
        cone,
        brane.get_id(),
        large_memory_barrier
    )))
}

pub fn get_brane_approximate_tuplespaceInstanton_causet(
    db: &impl CausetEngine,
    causetname: &str,
    brane: &Brane,
    large_memory_barrier: u64,
) -> Result<u64> {
    let spacelike = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike = tuplespaceInstanton::enc_lightlike_key(brane);
    let cone = Cone::new(&spacelike, &lightlike);
    Ok(box_try!(db.get_cone_approximate_tuplespaceInstanton_causet(
        causetname,
        cone,
        brane.get_id(),
        large_memory_barrier
    )))
}

#[causet(test)]
mod tests {
    use super::super::size::tests::must_split_at;
    use crate::interlock::{Config, InterlockHost};
    use crate::store::{CasualMessage, SplitCheckRunner, SplitCheckTask};
    use engine_lmdb::properties::{
        MvccPropertiesCollectorFactory, ConePropertiesCollectorFactory,
    };
    use engine_lmdb::raw::DB;
    use engine_lmdb::raw::{PrimaryCausetNetworkOptions, DBOptions, WriBlock};
    use engine_lmdb::raw_util::{new_engine_opt, CausetOptions};
    use engine_lmdb::Compat;
    use edb::{ALL_CausetS, Causet_DEFAULT, Causet_WRITE, LARGE_CausetS};
    use ekvproto::meta_timeshare::{Peer, Brane};
    use ekvproto::fidel_timeshare::CheckPolicy;
    use std::cmp;
    use std::sync::{mpsc, Arc};
    use std::u64;
    use tempfile::Builder;
    use violetabftstore::interlock::::worker::Runnable;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;

    fn put_data(engine: &DB, mut spacelike_idx: u64, lightlike_idx: u64, fill_short_value: bool) {
        let write_causet = engine.causet_handle(Causet_WRITE).unwrap();
        let default_causet = engine.causet_handle(Causet_DEFAULT).unwrap();
        let write_value = if fill_short_value {
            Write::new(
                WriteType::Put,
                TimeStamp::zero(),
                Some(b"shortvalue".to_vec()),
            )
        } else {
            Write::new(WriteType::Put, TimeStamp::zero(), None)
        }
        .as_ref()
        .to_bytes();

        while spacelike_idx < lightlike_idx {
            let batch_idx = cmp::min(spacelike_idx + 20, lightlike_idx);
            for i in spacelike_idx..batch_idx {
                let key = tuplespaceInstanton::data_key(
                    Key::from_raw(format!("{:04}", i).as_bytes())
                        .applightlike_ts(2.into())
                        .as_encoded(),
                );
                engine.put_causet(write_causet, &key, &write_value).unwrap();
                engine.put_causet(default_causet, &key, &[0; 1024]).unwrap();
            }
            // Flush to generate SST files, so that properties can be utilized.
            engine.flush_causet(write_causet, true).unwrap();
            engine.flush_causet(default_causet, true).unwrap();
            spacelike_idx = batch_idx;
        }
    }

    #[test]
    fn test_split_check() {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.cone-properties-collector", f);

        let causets_opts = ALL_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let mut brane = Brane::default();
        brane.set_id(1);
        brane.set_spacelike_key(vec![]);
        brane.set_lightlike_key(vec![]);
        brane.mut_peers().push(Peer::default());
        brane.mut_brane_epoch().set_version(2);
        brane.mut_brane_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let mut causet = Config::default();
        causet.brane_max_tuplespaceInstanton = 100;
        causet.brane_split_tuplespaceInstanton = 80;
        causet.batch_split_limit = 5;

        let mut runnable = SplitCheckRunner::new(
            engine.c().clone(),
            tx.clone(),
            InterlockHost::new(tx),
            causet,
        );

        // so split key will be z0080
        put_data(&engine, 0, 90, false);
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        // tuplespaceInstanton has not reached the max_tuplespaceInstanton 100 yet.
        match rx.try_recv() {
            Ok((brane_id, CasualMessage::BraneApproximateSize { .. }))
            | Ok((brane_id, CasualMessage::BraneApproximateTuplespaceInstanton { .. })) => {
                assert_eq!(brane_id, brane.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        put_data(&engine, 90, 160, true);
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(
            &rx,
            &brane,
            vec![Key::from_raw(b"0080").applightlike_ts(2.into()).into_encoded()],
        );

        put_data(&engine, 160, 300, false);
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(
            &rx,
            &brane,
            vec![
                Key::from_raw(b"0080").applightlike_ts(2.into()).into_encoded(),
                Key::from_raw(b"0160").applightlike_ts(2.into()).into_encoded(),
                Key::from_raw(b"0240").applightlike_ts(2.into()).into_encoded(),
            ],
        );

        put_data(&engine, 300, 500, false);
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(
            &rx,
            &brane,
            vec![
                Key::from_raw(b"0080").applightlike_ts(2.into()).into_encoded(),
                Key::from_raw(b"0160").applightlike_ts(2.into()).into_encoded(),
                Key::from_raw(b"0240").applightlike_ts(2.into()).into_encoded(),
                Key::from_raw(b"0320").applightlike_ts(2.into()).into_encoded(),
                Key::from_raw(b"0400").applightlike_ts(2.into()).into_encoded(),
            ],
        );

        drop(rx);
        // It should be safe even the result can't be sent back.
        runnable.run(SplitCheckTask::split_check(brane, true, CheckPolicy::Scan));
    }

    #[test]
    fn test_brane_approximate_tuplespaceInstanton() {
        let path = Builder::new()
            .prefix("_test_brane_approximate_tuplespaceInstanton")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.cone-properties-collector", f);
        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let db =
            Arc::new(engine_lmdb::raw_util::new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        for &(key, vlen) in &cases {
            let key = tuplespaceInstanton::data_key(
                Key::from_raw(key.as_bytes())
                    .applightlike_ts(2.into())
                    .as_encoded(),
            );
            let write_v = Write::new(WriteType::Put, TimeStamp::zero(), None)
                .as_ref()
                .to_bytes();
            let write_causet = db.causet_handle(Causet_WRITE).unwrap();
            db.put_causet(write_causet, &key, &write_v).unwrap();
            db.flush_causet(write_causet, true).unwrap();

            let default_v = vec![0; vlen as usize];
            let default_causet = db.causet_handle(Causet_DEFAULT).unwrap();
            db.put_causet(default_causet, &key, &default_v).unwrap();
            db.flush_causet(default_causet, true).unwrap();
        }

        let mut brane = Brane::default();
        brane.mut_peers().push(Peer::default());
        let cone_tuplespaceInstanton = get_brane_approximate_tuplespaceInstanton(db.c(), &brane, 0).unwrap();
        assert_eq!(cone_tuplespaceInstanton, cases.len() as u64);
    }

    #[test]
    fn test_brane_approximate_tuplespaceInstanton_sub_brane() {
        let path = Builder::new()
            .prefix("_test_brane_approximate_tuplespaceInstanton_sub_brane")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.tail_pointer-properties-collector", f);
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.cone-properties-collector", f);
        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let db =
            Arc::new(engine_lmdb::raw_util::new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let write_causet = db.causet_handle(Causet_WRITE).unwrap();
        let default_causet = db.causet_handle(Causet_DEFAULT).unwrap();
        // size >= 4194304 will insert a new point in cone properties
        // 3 points will be inserted into cone properties
        let cases = [("a", 4194304), ("b", 4194304), ("c", 4194304)];
        for &(key, vlen) in &cases {
            let key = tuplespaceInstanton::data_key(
                Key::from_raw(key.as_bytes())
                    .applightlike_ts(2.into())
                    .as_encoded(),
            );
            let write_v = Write::new(WriteType::Put, TimeStamp::zero(), None)
                .as_ref()
                .to_bytes();
            db.put_causet(write_causet, &key, &write_v).unwrap();

            let default_v = vec![0; vlen as usize];
            db.put_causet(default_causet, &key, &default_v).unwrap();
        }
        // only flush once, so that tail_pointer properties will insert one point only
        db.flush_causet(write_causet, true).unwrap();
        db.flush_causet(default_causet, true).unwrap();

        // cone properties get 0, tail_pointer properties get 3
        let mut brane = Brane::default();
        brane.set_id(1);
        brane.set_spacelike_key(b"b1".to_vec());
        brane.set_lightlike_key(b"b2".to_vec());
        brane.mut_peers().push(Peer::default());
        let cone_tuplespaceInstanton = get_brane_approximate_tuplespaceInstanton(db.c(), &brane, 0).unwrap();
        assert_eq!(cone_tuplespaceInstanton, 0);

        // cone properties get 1, tail_pointer properties get 3
        brane.set_spacelike_key(b"a".to_vec());
        brane.set_lightlike_key(b"c".to_vec());
        let cone_tuplespaceInstanton = get_brane_approximate_tuplespaceInstanton(db.c(), &brane, 0).unwrap();
        assert_eq!(cone_tuplespaceInstanton, 1);
    }
}
