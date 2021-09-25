// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Mutex};

use edb::{CausetEngine, Cone};
use error_code::ErrorCodeExt;
use ekvproto::meta_timeshare::Brane;
use ekvproto::fidel_timeshare::CheckPolicy;

use crate::store::{CasualMessage, CasualRouter};

use super::super::error::Result;
use super::super::metrics::*;
use super::super::{Interlock, KeyEntry, SemaphoreContext, SplitCheckSemaphore, SplitChecker};
use super::Host;

pub struct Checker {
    max_size: u64,
    split_size: u64,
    current_size: u64,
    split_tuplespaceInstanton: Vec<Vec<u8>>,
    batch_split_limit: u64,
    policy: CheckPolicy,
}

impl Checker {
    pub fn new(
        max_size: u64,
        split_size: u64,
        batch_split_limit: u64,
        policy: CheckPolicy,
    ) -> Checker {
        Checker {
            max_size,
            split_size,
            current_size: 0,
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
    fn on_kv(&mut self, _: &mut SemaphoreContext<'_>, entry: &KeyEntry) -> bool {
        let size = entry.entry_size() as u64;
        self.current_size += size;

        let mut over_limit = self.split_tuplespaceInstanton.len() as u64 >= self.batch_split_limit;
        if self.current_size > self.split_size && !over_limit {
            self.split_tuplespaceInstanton.push(tuplespaceInstanton::origin_key(entry.key()).to_vec());
            // if for previous on_kv() self.current_size == self.split_size,
            // the split key would be pushed this time, but the entry size for this time should not be ignored.
            self.current_size = if self.current_size - size == self.split_size {
                size
            } else {
                0
            };
            over_limit = self.split_tuplespaceInstanton.len() as u64 >= self.batch_split_limit;
        }

        // For a large brane, scan over the cone maybe cost too much time,
        // so limit the number of produced split_key for one batch.
        // Also need to scan over self.max_size for last part.
        over_limit && self.current_size + self.split_size >= self.max_size
    }

    fn split_tuplespaceInstanton(&mut self) -> Vec<Vec<u8>> {
        // make sure not to split when less than max_size for last part
        if self.current_size + self.split_size < self.max_size {
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

    fn approximate_split_tuplespaceInstanton(&mut self, brane: &Brane, engine: &E) -> Result<Vec<Vec<u8>>> {
        Ok(box_try!(get_approximate_split_tuplespaceInstanton(
            engine,
            brane,
            self.split_size,
            self.max_size,
            self.batch_split_limit,
        )))
    }
}

#[derive(Clone)]
pub struct SizeCheckSemaphore<C, E> {
    router: Arc<Mutex<C>>,
    _phantom: PhantomData<E>,
}

impl<C: CasualRouter<E>, E> SizeCheckSemaphore<C, E>
where
    E: CausetEngine,
{
    pub fn new(router: C) -> SizeCheckSemaphore<C, E> {
        SizeCheckSemaphore {
            router: Arc::new(Mutex::new(router)),
            _phantom: PhantomData,
        }
    }
}

impl<C: lightlike, E: lightlike> Interlock for SizeCheckSemaphore<C, E> {}

impl<C: CasualRouter<E> + lightlike, E> SplitCheckSemaphore<E> for SizeCheckSemaphore<C, E>
where
    E: CausetEngine,
{
    fn add_checker(
        &self,
        ctx: &mut SemaphoreContext<'_>,
        host: &mut Host<'_, E>,
        engine: &E,
        mut policy: CheckPolicy,
    ) {
        let brane = ctx.brane();
        let brane_id = brane.get_id();
        let brane_size = match get_brane_approximate_size(
            engine,
            &brane,
            host.causet.brane_max_size.0 * host.causet.batch_split_limit,
        ) {
            Ok(size) => size,
            Err(e) => {
                warn!(
                    "failed to get approximate stat";
                    "brane_id" => brane_id,
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                // Need to check size.
                host.add_checker(Box::new(Checker::new(
                    host.causet.brane_max_size.0,
                    host.causet.brane_split_size.0,
                    host.causet.batch_split_limit,
                    policy,
                )));
                return;
            }
        };

        // lightlike it to violetabftstore to fidelio brane approximate size
        let res = CasualMessage::BraneApproximateSize { size: brane_size };
        if let Err(e) = self.router.dagger().unwrap().lightlike(brane_id, res) {
            warn!(
                "failed to lightlike approximate brane size";
                "brane_id" => brane_id,
                "err" => %e,
                "error_code" => %e.error_code(),
            );
        }

        REGION_SIZE_HISTOGRAM.observe(brane_size as f64);
        if brane_size >= host.causet.brane_max_size.0 {
            info!(
                "approximate size over memory_barrier, need to do split check";
                "brane_id" => brane.get_id(),
                "size" => brane_size,
                "memory_barrier" => host.causet.brane_max_size.0,
            );
            // when meet large brane use approximate way to produce split tuplespaceInstanton
            if brane_size >= host.causet.brane_max_size.0 * host.causet.batch_split_limit * 2 {
                policy = CheckPolicy::Approximate
            }
            // Need to check size.
            host.add_checker(Box::new(Checker::new(
                host.causet.brane_max_size.0,
                host.causet.brane_split_size.0,
                host.causet.batch_split_limit,
                policy,
            )));
        } else {
            // Does not need to check size.
            debug!(
                "approximate size less than memory_barrier, does not need to do split check";
                "brane_id" => brane.get_id(),
                "size" => brane_size,
                "memory_barrier" => host.causet.brane_max_size.0,
            );
        }
    }
}

/// Get the approximate size of the cone.
pub fn get_brane_approximate_size(
    db: &impl CausetEngine,
    brane: &Brane,
    large_memory_barrier: u64,
) -> Result<u64> {
    let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    let cone = Cone::new(&spacelike_key, &lightlike_key);
    Ok(box_try!(db.get_cone_approximate_size(
        cone,
        brane.get_id(),
        large_memory_barrier
    )))
}

pub fn get_brane_approximate_size_causet(
    db: &impl CausetEngine,
    causetname: &str,
    brane: &Brane,
    large_memory_barrier: u64,
) -> Result<u64> {
    let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    let cone = Cone::new(&spacelike_key, &lightlike_key);
    Ok(box_try!(db.get_cone_approximate_size_causet(
        causetname,
        cone,
        brane.get_id(),
        large_memory_barrier
    )))
}

/// Get brane approximate split tuplespaceInstanton based on default, write and dagger causet.
fn get_approximate_split_tuplespaceInstanton(
    db: &impl CausetEngine,
    brane: &Brane,
    split_size: u64,
    max_size: u64,
    batch_split_limit: u64,
) -> Result<Vec<Vec<u8>>> {
    let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    let cone = Cone::new(&spacelike_key, &lightlike_key);
    Ok(box_try!(db.get_cone_approximate_split_tuplespaceInstanton(
        cone,
        brane.get_id(),
        split_size,
        max_size,
        batch_split_limit
    )))
}

#[causet(test)]
pub mod tests {
    use super::Checker;
    use crate::interlock::{Config, InterlockHost, SemaphoreContext, SplitChecker};
    use crate::store::{CasualMessage, KeyEntry, SplitCheckRunner, SplitCheckTask};
    use engine_lmdb::properties::ConePropertiesCollectorFactory;
    use engine_lmdb::raw::{PrimaryCausetNetworkOptions, DBOptions, WriBlock};
    use engine_lmdb::raw_util::{new_engine_opt, CausetOptions};
    use engine_lmdb::{Compat, LmdbEngine};
    use edb::Causet_DAGGER;
    use edb::{CfName, ALL_CausetS, Causet_DEFAULT, Causet_WRITE, LARGE_CausetS};
    use ekvproto::meta_timeshare::Peer;
    use ekvproto::meta_timeshare::Brane;
    use ekvproto::fidel_timeshare::CheckPolicy;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::{
        iter::{self, FromIterator},
        u64,
    };
    use tempfile::Builder;
    use violetabftstore::interlock::::collections::HashSet;
    use violetabftstore::interlock::::config::ReadableSize;
    use violetabftstore::interlock::::worker::Runnable;
    use txn_types::Key;

    use super::*;

    fn must_split_at_impl(
        rx: &mpsc::Receiver<(u64, CasualMessage<LmdbEngine>)>,
        exp_brane: &Brane,
        exp_split_tuplespaceInstanton: Vec<Vec<u8>>,
        ignore_split_tuplespaceInstanton: bool,
    ) {
        loop {
            match rx.try_recv() {
                Ok((brane_id, CasualMessage::BraneApproximateSize { .. }))
                | Ok((brane_id, CasualMessage::BraneApproximateTuplespaceInstanton { .. })) => {
                    assert_eq!(brane_id, exp_brane.get_id());
                }
                Ok((
                    brane_id,
                    CasualMessage::SplitBrane {
                        brane_epoch,
                        split_tuplespaceInstanton,
                        ..
                    },
                )) => {
                    assert_eq!(brane_id, exp_brane.get_id());
                    assert_eq!(&brane_epoch, exp_brane.get_brane_epoch());
                    if !ignore_split_tuplespaceInstanton {
                        assert_eq!(split_tuplespaceInstanton, exp_split_tuplespaceInstanton);
                    }
                    break;
                }
                others => panic!("expect split check result, but got {:?}", others),
            }
        }
    }

    pub fn must_split_at(
        rx: &mpsc::Receiver<(u64, CasualMessage<LmdbEngine>)>,
        exp_brane: &Brane,
        exp_split_tuplespaceInstanton: Vec<Vec<u8>>,
    ) {
        must_split_at_impl(rx, exp_brane, exp_split_tuplespaceInstanton, false)
    }

    fn test_split_check_impl(causets_with_cone_prop: &[CfName], data_causet: CfName) {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let causets_with_cone_prop = HashSet::from_iter(causets_with_cone_prop.iter().cloned());
        let mut causet_opt = PrimaryCausetNetworkOptions::new();
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opt.add_Block_properties_collector_factory("edb.cone-collector", f);

        let causets_opts = ALL_CausetS
            .iter()
            .map(|causet| {
                if causets_with_cone_prop.contains(causet) {
                    CausetOptions::new(causet, causet_opt.clone())
                } else {
                    CausetOptions::new(causet, PrimaryCausetNetworkOptions::new())
                }
            })
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
        causet.brane_max_size = ReadableSize(100);
        causet.brane_split_size = ReadableSize(60);
        causet.batch_split_limit = 5;

        let mut runnable = SplitCheckRunner::new(
            engine.c().clone(),
            tx.clone(),
            InterlockHost::new(tx),
            causet,
        );

        let causet_handle = engine.causet_handle(data_causet).unwrap();
        // so split key will be [z0006]
        for i in 0..7 {
            let s = tuplespaceInstanton::data_key(format!("{:04}", i).as_bytes());
            engine.put_causet(&causet_handle, &s, &s).unwrap();
        }

        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        // size has not reached the max_size 100 yet.
        match rx.try_recv() {
            Ok((brane_id, CasualMessage::BraneApproximateSize { .. })) => {
                assert_eq!(brane_id, brane.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        for i in 7..11 {
            let s = tuplespaceInstanton::data_key(format!("{:04}", i).as_bytes());
            engine.put_causet(&causet_handle, &s, &s).unwrap();
        }

        // Approximate size of memBlock is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush_causet(&causet_handle, true).unwrap();

        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(&rx, &brane, vec![b"0006".to_vec()]);

        // so split tuplespaceInstanton will be [z0006, z0012]
        for i in 11..19 {
            let s = tuplespaceInstanton::data_key(format!("{:04}", i).as_bytes());
            engine.put_causet(&causet_handle, &s, &s).unwrap();
        }
        engine.flush_causet(&causet_handle, true).unwrap();
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(&rx, &brane, vec![b"0006".to_vec(), b"0012".to_vec()]);

        // for test batch_split_limit
        // so split kets will be [z0006, z0012, z0018, z0024, z0030]
        for i in 19..51 {
            let s = tuplespaceInstanton::data_key(format!("{:04}", i).as_bytes());
            engine.put_causet(&causet_handle, &s, &s).unwrap();
        }
        engine.flush_causet(&causet_handle, true).unwrap();
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(
            &rx,
            &brane,
            vec![
                b"0006".to_vec(),
                b"0012".to_vec(),
                b"0018".to_vec(),
                b"0024".to_vec(),
                b"0030".to_vec(),
            ],
        );

        drop(rx);
        // It should be safe even the result can't be sent back.
        runnable.run(SplitCheckTask::split_check(brane, true, CheckPolicy::Scan));
    }

    #[test]
    fn test_split_check() {
        test_split_check_impl(&[Causet_DEFAULT, Causet_WRITE], Causet_DEFAULT);
        test_split_check_impl(&[Causet_DEFAULT, Causet_WRITE], Causet_WRITE);
        for causet in LARGE_CausetS {
            test_split_check_impl(LARGE_CausetS, causet);
        }
    }

    #[test]
    fn test_causet_lock_without_cone_prop() {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opt = PrimaryCausetNetworkOptions::new();
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opt.add_Block_properties_collector_factory("edb.cone-collector", f);

        let causets_opts = ALL_CausetS
            .iter()
            .map(|causet| {
                if causet != &Causet_DAGGER {
                    CausetOptions::new(causet, causet_opt.clone())
                } else {
                    CausetOptions::new(causet, PrimaryCausetNetworkOptions::new())
                }
            })
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
        causet.brane_max_size = ReadableSize(100);
        causet.brane_split_size = ReadableSize(60);
        causet.batch_split_limit = 5;

        let mut runnable = SplitCheckRunner::new(
            engine.c().clone(),
            tx.clone(),
            InterlockHost::new(tx.clone()),
            causet.clone(),
        );

        for causet in LARGE_CausetS {
            let causet_handle = engine.causet_handle(causet).unwrap();
            for i in 0..7 {
                let s = tuplespaceInstanton::data_key(format!("{:04}", i).as_bytes());
                engine.put_causet(&causet_handle, &s, &s).unwrap();
            }
            engine.flush_causet(&causet_handle, true).unwrap();
        }

        for policy in &[CheckPolicy::Scan, CheckPolicy::Approximate] {
            runnable.run(SplitCheckTask::split_check(brane.clone(), true, *policy));
            // Ignore the split tuplespaceInstanton. Only check whether it can split or not.
            must_split_at_impl(&rx, &brane, vec![], true);
        }

        drop(engine);
        drop(runnable);

        // Reopen the engine and all causets have cone properties.
        let causets_opts = ALL_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, PrimaryCausetNetworkOptions::new()))
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, DBOptions::new(), causets_opts).unwrap());

        let mut runnable = SplitCheckRunner::new(
            engine.c().clone(),
            tx.clone(),
            InterlockHost::new(tx),
            causet,
        );

        // Flush a sst of Causet_DAGGER with cone properties.
        let causet_handle = engine.causet_handle(Causet_DAGGER).unwrap();
        for i in 7..15 {
            let s = tuplespaceInstanton::data_key(format!("{:04}", i).as_bytes());
            engine.put_causet(&causet_handle, &s, &s).unwrap();
        }
        engine.flush_causet(&causet_handle, true).unwrap();
        for policy in &[CheckPolicy::Scan, CheckPolicy::Approximate] {
            runnable.run(SplitCheckTask::split_check(brane.clone(), true, *policy));
            // Ignore the split tuplespaceInstanton. Only check whether it can split or not.
            must_split_at_impl(&rx, &brane, vec![], true);
        }
    }

    #[test]
    fn test_checker_with_same_max_and_split_size() {
        let mut checker = Checker::new(24, 24, 1, CheckPolicy::Scan);
        let brane = Brane::default();
        let mut ctx = SemaphoreContext::new(&brane);
        loop {
            let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 4, Causet_WRITE);
            if SplitChecker::<LmdbEngine>::on_kv(&mut checker, &mut ctx, &data) {
                break;
            }
        }

        assert!(!SplitChecker::<LmdbEngine>::split_tuplespaceInstanton(&mut checker).is_empty());
    }

    #[test]
    fn test_checker_with_max_twice_bigger_than_split_size() {
        let mut checker = Checker::new(20, 10, 1, CheckPolicy::Scan);
        let brane = Brane::default();
        let mut ctx = SemaphoreContext::new(&brane);
        for _ in 0..2 {
            let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 5, Causet_WRITE);
            if SplitChecker::<LmdbEngine>::on_kv(&mut checker, &mut ctx, &data) {
                break;
            }
        }

        assert!(!SplitChecker::<LmdbEngine>::split_tuplespaceInstanton(&mut checker).is_empty());
    }

    fn make_brane(id: u64, spacelike_key: Vec<u8>, lightlike_key: Vec<u8>) -> Brane {
        let mut peer = Peer::default();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut brane = Brane::default();
        brane.set_id(id);
        brane.set_spacelike_key(spacelike_key);
        brane.set_lightlike_key(lightlike_key);
        brane.mut_peers().push(peer);
        brane
    }

    #[test]
    fn test_get_approximate_split_tuplespaceInstanton_error() {
        let tmp = Builder::new()
            .prefix("test_violetabftstore_util")
            .temfidelir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_file_num_compaction_trigger(10);

        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let engine =
            Arc::new(engine_lmdb::raw_util::new_engine_opt(path, db_opts, causets_opts).unwrap());

        let brane = make_brane(1, vec![], vec![]);
        assert_eq!(
            get_approximate_split_tuplespaceInstanton(engine.c(), &brane, 3, 5, 1).is_err(),
            true
        );

        let causet_handle = engine.causet_handle(Causet_DEFAULT).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extlightlike(iter::repeat(b'v').take(256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = tuplespaceInstanton::data_key(Key::from_raw(&k).as_encoded());
            engine.put_causet(causet_handle, &k, &big_value).unwrap();
            engine.flush_causet(causet_handle, true).unwrap();
        }
        assert_eq!(
            get_approximate_split_tuplespaceInstanton(engine.c(), &brane, 3, 5, 1).is_err(),
            true
        );
    }

    fn test_get_approximate_split_tuplespaceInstanton_impl(data_causet: CfName) {
        let tmp = Builder::new()
            .prefix("test_violetabftstore_util")
            .temfidelir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.size-collector", f);
        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let engine =
            Arc::new(engine_lmdb::raw_util::new_engine_opt(path, db_opts, causets_opts).unwrap());

        let causet_handle = engine.causet_handle(data_causet).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extlightlike(iter::repeat(b'v').take(256));

        // total size for one key and value
        const ENTRY_SIZE: u64 = 256 + 9;

        for i in 0..4 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = tuplespaceInstanton::data_key(Key::from_raw(&k).as_encoded());
            engine.put_causet(causet_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_causet(causet_handle, true).unwrap();
        }
        let brane = make_brane(1, vec![], vec![]);
        let split_tuplespaceInstanton =
            get_approximate_split_tuplespaceInstanton(engine.c(), &brane, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 1)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(tuplespaceInstanton::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_tuplespaceInstanton.is_empty(), true);

        for i in 4..5 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = tuplespaceInstanton::data_key(Key::from_raw(&k).as_encoded());
            engine.put_causet(causet_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_causet(causet_handle, true).unwrap();
        }
        let split_tuplespaceInstanton =
            get_approximate_split_tuplespaceInstanton(engine.c(), &brane, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(tuplespaceInstanton::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_tuplespaceInstanton, vec![b"key_002".to_vec()]);

        for i in 5..10 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = tuplespaceInstanton::data_key(Key::from_raw(&k).as_encoded());
            engine.put_causet(causet_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_causet(causet_handle, true).unwrap();
        }
        let split_tuplespaceInstanton =
            get_approximate_split_tuplespaceInstanton(engine.c(), &brane, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(tuplespaceInstanton::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_tuplespaceInstanton, vec![b"key_002".to_vec(), b"key_005".to_vec()]);

        for i in 10..20 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = tuplespaceInstanton::data_key(Key::from_raw(&k).as_encoded());
            engine.put_causet(causet_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_causet(causet_handle, true).unwrap();
        }
        let split_tuplespaceInstanton =
            get_approximate_split_tuplespaceInstanton(engine.c(), &brane, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(tuplespaceInstanton::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(
            split_tuplespaceInstanton,
            vec![
                b"key_002".to_vec(),
                b"key_005".to_vec(),
                b"key_008".to_vec(),
                b"key_011".to_vec(),
                b"key_014".to_vec(),
            ]
        );
    }

    #[test]
    fn test_get_approximate_split_tuplespaceInstanton() {
        for causet in LARGE_CausetS {
            test_get_approximate_split_tuplespaceInstanton_impl(*causet);
        }
    }

    #[test]
    fn test_brane_approximate_size() {
        let path = Builder::new()
            .prefix("_test_violetabftstore_brane_approximate_size")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.cone-collector", f);
        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let db =
            Arc::new(engine_lmdb::raw_util::new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        let causet_size = 2 + 1024 + 2 + 2048 + 2 + 4096;
        for &(key, vlen) in &cases {
            for causetname in LARGE_CausetS {
                let k1 = tuplespaceInstanton::data_key(key.as_bytes());
                let v1 = vec![0; vlen as usize];
                assert_eq!(k1.len(), 2);
                let causet = db.causet_handle(causetname).unwrap();
                db.put_causet(causet, &k1, &v1).unwrap();
                db.flush_causet(causet, true).unwrap();
            }
        }

        let brane = make_brane(1, vec![], vec![]);
        let size = get_brane_approximate_size(db.c(), &brane, 0).unwrap();
        assert_eq!(size, causet_size * LARGE_CausetS.len() as u64);
        for causetname in LARGE_CausetS {
            let size = get_brane_approximate_size_causet(db.c(), causetname, &brane, 0).unwrap();
            assert_eq!(size, causet_size);
        }
    }

    #[test]
    fn test_brane_maybe_inaccurate_approximate_size() {
        let path = Builder::new()
            .prefix("_test_violetabftstore_brane_maybe_inaccurate_approximate_size")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_disable_auto_compactions(true);
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.cone-collector", f);
        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let db =
            Arc::new(engine_lmdb::raw_util::new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let mut causet_size = 0;
        for i in 0..100 {
            let k1 = tuplespaceInstanton::data_key(format!("k1{}", i).as_bytes());
            let k2 = tuplespaceInstanton::data_key(format!("k9{}", i).as_bytes());
            let v = vec![0; 4096];
            causet_size += k1.len() + k2.len() + v.len() * 2;
            let causet = db.causet_handle("default").unwrap();
            db.put_causet(causet, &k1, &v).unwrap();
            db.put_causet(causet, &k2, &v).unwrap();
            db.flush_causet(causet, true).unwrap();
        }

        let brane = make_brane(1, vec![], vec![]);
        let size = get_brane_approximate_size(db.c(), &brane, 0).unwrap();
        assert_eq!(size, causet_size as u64);

        let brane = make_brane(1, b"k2".to_vec(), b"k8".to_vec());
        let size = get_brane_approximate_size(db.c(), &brane, 0).unwrap();
        assert_eq!(size, 0);
    }

    use test::Bencher;

    #[bench]
    fn bench_get_brane_approximate_size(b: &mut Bencher) {
        let path = Builder::new()
            .prefix("_bench_get_brane_approximate_size")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_disable_auto_compactions(true);
        let f = Box::new(ConePropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.cone-collector", f);
        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let db =
            Arc::new(engine_lmdb::raw_util::new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let mut causet_size = 0;
        let causet = db.causet_handle("default").unwrap();
        for i in 0..10 {
            let v = vec![0; 4096];
            for j in 10000 * i..10000 * (i + 1) {
                let k1 = tuplespaceInstanton::data_key(format!("k1{:0100}", j).as_bytes());
                let k2 = tuplespaceInstanton::data_key(format!("k9{:0100}", j).as_bytes());
                causet_size += k1.len() + k2.len() + v.len() * 2;
                db.put_causet(causet, &k1, &v).unwrap();
                db.put_causet(causet, &k2, &v).unwrap();
            }
            db.flush_causet(causet, true).unwrap();
        }

        let brane = make_brane(1, vec![], vec![]);
        b.iter(|| {
            let size = get_brane_approximate_size(db.c(), &brane, 0).unwrap();
            assert_eq!(size, causet_size as u64);
        })
    }
}
