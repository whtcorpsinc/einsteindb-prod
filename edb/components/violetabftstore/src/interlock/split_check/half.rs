//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use edb::{CausetEngine, Cone};
use ekvproto::meta_timeshare::Brane;
use ekvproto::fidel_timeshare::CheckPolicy;

use violetabftstore::interlock::::config::ReadableSize;

use super::super::error::Result;
use super::super::{Interlock, KeyEntry, SemaphoreContext, SplitCheckSemaphore, SplitChecker};
use super::Host;

const BUCKET_NUMBER_LIMIT: usize = 1024;
const BUCKET_SIZE_LIMIT_MB: u64 = 512;

pub struct Checker {
    buckets: Vec<Vec<u8>>,
    cur_bucket_size: u64,
    each_bucket_size: u64,
    policy: CheckPolicy,
}

impl Checker {
    fn new(each_bucket_size: u64, policy: CheckPolicy) -> Checker {
        Checker {
            each_bucket_size,
            cur_bucket_size: 0,
            buckets: vec![],
            policy,
        }
    }
}

impl<E> SplitChecker<E> for Checker
where
    E: CausetEngine,
{
    fn on_kv(&mut self, _: &mut SemaphoreContext<'_>, entry: &KeyEntry) -> bool {
        if self.buckets.is_empty() || self.cur_bucket_size >= self.each_bucket_size {
            self.buckets.push(entry.key().to_vec());
            self.cur_bucket_size = 0;
        }
        self.cur_bucket_size += entry.entry_size() as u64;
        false
    }

    fn split_tuplespaceInstanton(&mut self) -> Vec<Vec<u8>> {
        let mid = self.buckets.len() / 2;
        if mid == 0 {
            vec![]
        } else {
            let data_key = self.buckets.swap_remove(mid);
            let key = tuplespaceInstanton::origin_key(&data_key).to_vec();
            vec![key]
        }
    }

    fn approximate_split_tuplespaceInstanton(&mut self, brane: &Brane, engine: &E) -> Result<Vec<Vec<u8>>> {
        let ks = box_try!(get_brane_approximate_middle(engine, brane)
            .map(|tuplespaceInstanton| tuplespaceInstanton.map_or(vec![], |key| vec![key])));

        Ok(ks)
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

#[derive(Clone)]
pub struct HalfCheckSemaphore;

impl Interlock for HalfCheckSemaphore {}

impl<E> SplitCheckSemaphore<E> for HalfCheckSemaphore
where
    E: CausetEngine,
{
    fn add_checker(
        &self,
        _: &mut SemaphoreContext<'_>,
        host: &mut Host<'_, E>,
        _: &E,
        policy: CheckPolicy,
    ) {
        if host.auto_split() {
            return;
        }
        host.add_checker(Box::new(Checker::new(
            half_split_bucket_size(host.causet.brane_max_size.0),
            policy,
        )))
    }
}

fn half_split_bucket_size(brane_max_size: u64) -> u64 {
    let mut half_split_bucket_size = brane_max_size / BUCKET_NUMBER_LIMIT as u64;
    let bucket_size_limit = ReadableSize::mb(BUCKET_SIZE_LIMIT_MB).0;
    if half_split_bucket_size == 0 {
        half_split_bucket_size = 1;
    } else if half_split_bucket_size > bucket_size_limit {
        half_split_bucket_size = bucket_size_limit;
    }
    half_split_bucket_size
}

/// Get brane approximate middle key based on default and write causet size.
pub fn get_brane_approximate_middle(
    db: &impl CausetEngine,
    brane: &Brane,
) -> Result<Option<Vec<u8>>> {
    let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    let cone = Cone::new(&spacelike_key, &lightlike_key);
    Ok(box_try!(
        db.get_cone_approximate_middle(cone, brane.get_id())
    ))
}

/// Get the approximate middle key of the brane. If we suppose the brane
/// is stored on disk as a plain file, "middle key" means the key whose
/// position is in the middle of the file.
///
/// The returned key maybe is timestamped if transaction KV is used,
/// and must spacelike with "z".
///
/// FIXME the causet(test) here probably indicates that the test doesn't belong
/// here. It should be a test of the edb or engine_lmdb crates.
#[causet(test)]
fn get_brane_approximate_middle_causet(
    db: &impl CausetEngine,
    causetname: &str,
    brane: &Brane,
) -> Result<Option<Vec<u8>>> {
    let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    let cone = Cone::new(&spacelike_key, &lightlike_key);
    Ok(box_try!(db.get_cone_approximate_middle_causet(
        causetname,
        cone,
        brane.get_id()
    )))
}

#[causet(test)]
mod tests {
    use std::iter;
    use std::sync::mpsc;
    use std::sync::Arc;

    use engine_lmdb::raw::WriBlock;
    use engine_lmdb::raw::{PrimaryCausetNetworkOptions, DBOptions};
    use engine_lmdb::raw_util::{new_engine_opt, CausetOptions};
    use engine_lmdb::Compat;
    use edb::{ALL_CausetS, Causet_DEFAULT, LARGE_CausetS};
    use ekvproto::meta_timeshare::Peer;
    use ekvproto::meta_timeshare::Brane;
    use ekvproto::fidel_timeshare::CheckPolicy;
    use tempfile::Builder;

    use crate::store::{SplitCheckRunner, SplitCheckTask};
    use engine_lmdb::properties::ConePropertiesCollectorFactory;
    use violetabftstore::interlock::::config::ReadableSize;
    use violetabftstore::interlock::::escape;
    use violetabftstore::interlock::::worker::Runnable;
    use txn_types::Key;

    use super::super::size::tests::must_split_at;
    use super::*;
    use crate::interlock::{Config, InterlockHost};

    #[test]
    fn test_split_check() {
        let path = Builder::new().prefix("test-violetabftstore").temfidelir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let causets_opts = ALL_CausetS
            .iter()
            .map(|causet| {
                let mut causet_opts = PrimaryCausetNetworkOptions::new();
                let f = Box::new(ConePropertiesCollectorFactory::default());
                causet_opts.add_Block_properties_collector_factory("edb.size-collector", f);
                CausetOptions::new(causet, causet_opts)
            })
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let mut brane = Brane::default();
        brane.set_id(1);
        brane.mut_peers().push(Peer::default());
        brane.mut_brane_epoch().set_version(2);
        brane.mut_brane_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let mut causet = Config::default();
        causet.brane_max_size = ReadableSize(BUCKET_NUMBER_LIMIT as u64);
        let mut runnable = SplitCheckRunner::new(
            engine.c().clone(),
            tx.clone(),
            InterlockHost::new(tx),
            causet,
        );

        // so split key will be z0005
        let causet_handle = engine.causet_handle(Causet_DEFAULT).unwrap();
        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            let k = tuplespaceInstanton::data_key(Key::from_raw(&k).as_encoded());
            engine.put_causet(causet_handle, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_causet(causet_handle, true).unwrap();
        }
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            false,
            CheckPolicy::Scan,
        ));
        let split_key = Key::from_raw(b"0005");
        must_split_at(&rx, &brane, vec![split_key.clone().into_encoded()]);
        runnable.run(SplitCheckTask::split_check(
            brane.clone(),
            false,
            CheckPolicy::Approximate,
        ));
        must_split_at(&rx, &brane, vec![split_key.into_encoded()]);
    }

    #[test]
    fn test_get_brane_approximate_middle_causet() {
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

        let causet_handle = engine.causet_handle(Causet_DEFAULT).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extlightlike(iter::repeat(b'v').take(256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = tuplespaceInstanton::data_key(Key::from_raw(&k).as_encoded());
            engine.put_causet(causet_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_causet(causet_handle, true).unwrap();
        }

        let mut brane = Brane::default();
        brane.mut_peers().push(Peer::default());
        let middle_key = get_brane_approximate_middle_causet(engine.c(), Causet_DEFAULT, &brane)
            .unwrap()
            .unwrap();

        let middle_key = Key::from_encoded_slice(tuplespaceInstanton::origin_key(&middle_key))
            .into_raw()
            .unwrap();
        assert_eq!(escape(&middle_key), "key_049");
    }
}
