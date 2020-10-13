// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::cmp::Ordering;

use engine_promises::{IterOptions, Iteron, KvEngine, SeekKey, CAUSET_WRITE};
use error_code::ErrorCodeExt;
use ekvproto::metapb::Brane;
use ekvproto::fidelpb::CheckPolicy;
use milevadb_query_datatype::codec::table as table_codec;
use einsteindb_util::keybuilder::KeyBuilder;
use txn_types::Key;

use super::super::{
    Interlock, KeyEntry, ObserverContext, Result, SplitCheckObserver, SplitChecker,
};
use super::Host;

#[derive(Default)]
pub struct Checker {
    first_encoded_table_prefix: Option<Vec<u8>>,
    split_key: Option<Vec<u8>>,
    policy: CheckPolicy,
}

impl<E> SplitChecker<E> for Checker
where
    E: KvEngine,
{
    /// Feed tuplespaceInstanton in order to find the split key.
    /// If `current_data_key` does not belong to `status.first_encoded_table_prefix`.
    /// it returns the encoded table prefix of `current_data_key`.
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, entry: &KeyEntry) -> bool {
        if self.split_key.is_some() {
            return true;
        }

        let current_encoded_key = tuplespaceInstanton::origin_key(entry.key());

        let split_key = if self.first_encoded_table_prefix.is_some() {
            if !is_same_table(
                self.first_encoded_table_prefix.as_ref().unwrap(),
                current_encoded_key,
            ) {
                // Different tables.
                Some(current_encoded_key)
            } else {
                None
            }
        } else if is_table_key(current_encoded_key) {
            // Now we meet the very first table key of this brane.
            Some(current_encoded_key)
        } else {
            None
        };
        self.split_key = split_key.and_then(to_encoded_table_prefix);
        self.split_key.is_some()
    }

    fn split_tuplespaceInstanton(&mut self) -> Vec<Vec<u8>> {
        match self.split_key.take() {
            None => vec![],
            Some(key) => vec![key],
        }
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

#[derive(Default, Clone)]
pub struct TableCheckObserver;

impl Interlock for TableCheckObserver {}

impl<E> SplitCheckObserver<E> for TableCheckObserver
where
    E: KvEngine,
{
    fn add_checker(
        &self,
        ctx: &mut ObserverContext<'_>,
        host: &mut Host<'_, E>,
        engine: &E,
        policy: CheckPolicy,
    ) {
        if !host.causetg.split_brane_on_table {
            return;
        }
        let brane = ctx.brane();
        if is_same_table(brane.get_spacelike_key(), brane.get_lightlike_key()) {
            // Brane is inside a table, skip for saving IO.
            return;
        }

        let lightlike_key = match last_key_of_brane(engine, brane) {
            Ok(Some(lightlike_key)) => lightlike_key,
            Ok(None) => return,
            Err(err) => {
                warn!(
                    "failed to get brane last key";
                    "brane_id" => brane.get_id(),
                    "err" => %err,
                    "error_code" => %err.error_code(),
                );
                return;
            }
        };

        let encoded_spacelike_key = brane.get_spacelike_key();
        let encoded_lightlike_key = tuplespaceInstanton::origin_key(&lightlike_key);

        if encoded_spacelike_key.len() < table_codec::TABLE_PREFIX_KEY_LEN
            || encoded_lightlike_key.len() < table_codec::TABLE_PREFIX_KEY_LEN
        {
            // For now, let us scan brane if encoded_spacelike_key or encoded_lightlike_key
            // is less than TABLE_PREFIX_KEY_LEN.
            host.add_checker(Box::new(Checker {
                policy,
                ..Default::default()
            }));
            return;
        }

        let mut first_encoded_table_prefix = None;
        let mut split_key = None;
        // Table data spacelikes with `TABLE_PREFIX`.
        // Find out the actual cone of this brane by comparing with `TABLE_PREFIX`.
        match (
            encoded_spacelike_key[..table_codec::TABLE_PREFIX_LEN].cmp(table_codec::TABLE_PREFIX),
            encoded_lightlike_key[..table_codec::TABLE_PREFIX_LEN].cmp(table_codec::TABLE_PREFIX),
        ) {
            // The cone does not cover table data.
            (Ordering::Less, Ordering::Less) | (Ordering::Greater, Ordering::Greater) => return,

            // Following arms matches when the brane contains table data.
            // Covers all table data.
            (Ordering::Less, Ordering::Greater) => {}
            // The later part contains table data.
            (Ordering::Less, Ordering::Equal) => {
                // It spacelikes from non-table area to table area,
                // try to extract a split key from `encoded_lightlike_key`, and save it in status.
                split_key = to_encoded_table_prefix(encoded_lightlike_key);
            }
            // Brane is in table area.
            (Ordering::Equal, Ordering::Equal) => {
                if is_same_table(encoded_spacelike_key, encoded_lightlike_key) {
                    // Same table.
                    return;
                } else {
                    // Different tables.
                    // Note that table id does not grow by 1, so have to use
                    // `encoded_lightlike_key` to extract a table prefix.
                    // See more: https://github.com/whtcorpsinc/milevadb/issues/4727
                    split_key = to_encoded_table_prefix(encoded_lightlike_key);
                }
            }
            // The brane spacelikes from tabel area to non-table area.
            (Ordering::Equal, Ordering::Greater) => {
                // As the comment above, outside needs scan for finding a split key.
                first_encoded_table_prefix = to_encoded_table_prefix(encoded_spacelike_key);
            }
            _ => panic!(
                "spacelike_key {} and lightlike_key {} out of order",
                hex::encode_upper(encoded_spacelike_key),
                hex::encode_upper(encoded_lightlike_key)
            ),
        }
        host.add_checker(Box::new(Checker {
            first_encoded_table_prefix,
            split_key,
            policy,
        }));
    }
}

fn last_key_of_brane(db: &impl KvEngine, brane: &Brane) -> Result<Option<Vec<u8>>> {
    let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
    let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
    let mut last_key = None;

    let iter_opt = IterOptions::new(
        Some(KeyBuilder::from_vec(spacelike_key, 0, 0)),
        Some(KeyBuilder::from_vec(lightlike_key, 0, 0)),
        false,
    );
    let mut iter = box_try!(db.Iteron_causet_opt(CAUSET_WRITE, iter_opt));

    // the last key
    let found: Result<bool> = iter.seek(SeekKey::End).map_err(|e| box_err!(e));
    if found? {
        let key = iter.key().to_vec();
        last_key = Some(key);
    } // else { No data in this CAUSET }

    match last_key {
        Some(lk) => Ok(Some(lk)),
        None => Ok(None),
    }
}

fn to_encoded_table_prefix(encoded_key: &[u8]) -> Option<Vec<u8>> {
    if let Ok(raw_key) = Key::from_encoded_slice(encoded_key).to_raw() {
        table_codec::extract_table_prefix(&raw_key)
            .map(|k| Key::from_raw(k).into_encoded())
            .ok()
    } else {
        None
    }
}

// Encode a key like `t{i64}` will applightlike some unnecessary bytes to the output,
// The first 10 bytes are enough to find out which table this key belongs to.
const ENCODED_TABLE_TABLE_PREFIX: usize = table_codec::TABLE_PREFIX_KEY_LEN + 1;

fn is_table_key(encoded_key: &[u8]) -> bool {
    encoded_key.spacelikes_with(table_codec::TABLE_PREFIX)
        && encoded_key.len() >= ENCODED_TABLE_TABLE_PREFIX
}

fn is_same_table(left_key: &[u8], right_key: &[u8]) -> bool {
    is_table_key(left_key)
        && is_table_key(right_key)
        && left_key[..ENCODED_TABLE_TABLE_PREFIX] == right_key[..ENCODED_TABLE_TABLE_PREFIX]
}

#[causetg(test)]
mod tests {
    use std::io::Write;
    use std::sync::mpsc;

    use ekvproto::metapb::Peer;
    use ekvproto::fidelpb::CheckPolicy;
    use tempfile::Builder;

    use crate::store::{CasualMessage, SplitCheckRunner, SplitCheckTask};
    use engine_lmdb::util::new_engine;
    use engine_promises::{SyncMutable, ALL_CAUSETS};
    use milevadb_query_datatype::codec::table::{TABLE_PREFIX, TABLE_PREFIX_KEY_LEN};
    use einsteindb_util::codec::number::NumberEncoder;
    use einsteindb_util::config::ReadableSize;
    use einsteindb_util::worker::Runnable;
    use txn_types::Key;

    use super::*;
    use crate::interlock::{Config, InterlockHost};

    /// Composes table record and index prefix: `t[table_id]`.
    // Port from MilevaDB
    fn gen_table_prefix(table_id: i64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(TABLE_PREFIX_KEY_LEN);
        buf.write_all(TABLE_PREFIX).unwrap();
        buf.encode_i64(table_id).unwrap();
        buf
    }

    #[test]
    fn test_last_key_of_brane() {
        let path = Builder::new()
            .prefix("test_last_key_of_brane")
            .temfidelir()
            .unwrap();
        let engine = new_engine(path.path().to_str().unwrap(), None, ALL_CAUSETS, None).unwrap();

        let mut brane = Brane::default();
        brane.set_id(1);
        brane.mut_peers().push(Peer::default());

        // arbitrary padding.
        let padding = b"_r00000005";
        // Put tuplespaceInstanton, t1_xxx, t2_xxx
        let mut data_tuplespaceInstanton = vec![];
        for i in 1..3 {
            let mut key = gen_table_prefix(i);
            key.extlightlike_from_slice(padding);
            let k = tuplespaceInstanton::data_key(Key::from_raw(&key).as_encoded());
            engine.put_causet(CAUSET_WRITE, &k, &k).unwrap();
            data_tuplespaceInstanton.push(k)
        }

        type Case = (Option<i64>, Option<i64>, Option<Vec<u8>>);
        let mut check_cases = |cases: Vec<Case>| {
            for (spacelike_id, lightlike_id, want) in cases {
                brane.set_spacelike_key(
                    spacelike_id
                        .map(|id| Key::from_raw(&gen_table_prefix(id)).into_encoded())
                        .unwrap_or_else(Vec::new),
                );
                brane.set_lightlike_key(
                    lightlike_id
                        .map(|id| Key::from_raw(&gen_table_prefix(id)).into_encoded())
                        .unwrap_or_else(Vec::new),
                );
                assert_eq!(last_key_of_brane(&engine, &brane).unwrap(), want);
            }
        };

        check_cases(vec![
            // ["", "") => t2_xx
            (None, None, data_tuplespaceInstanton.get(1).cloned()),
            // ["", "t1") => None
            (None, Some(1), None),
            // ["t1", "") => t2_xx
            (Some(1), None, data_tuplespaceInstanton.get(1).cloned()),
            // ["t1", "t2") => t1_xx
            (Some(1), Some(2), data_tuplespaceInstanton.get(0).cloned()),
        ]);
    }

    #[test]
    fn test_table_check_observer() {
        let path = Builder::new()
            .prefix("test_table_check_observer")
            .temfidelir()
            .unwrap();
        let engine = new_engine(path.path().to_str().unwrap(), None, ALL_CAUSETS, None).unwrap();

        let mut brane = Brane::default();
        brane.set_id(1);
        brane.mut_peers().push(Peer::default());
        brane.mut_brane_epoch().set_version(2);
        brane.mut_brane_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let (stx, _rx) = mpsc::sync_channel(100);

        let mut causetg = Config::default();
        // Enable table split.
        causetg.split_brane_on_table = true;

        // Try to "disable" size split.
        causetg.brane_max_size = ReadableSize::gb(2);
        causetg.brane_split_size = ReadableSize::gb(1);
        // Try to "disable" tuplespaceInstanton split
        causetg.brane_max_tuplespaceInstanton = 2000000000;
        causetg.brane_split_tuplespaceInstanton = 1000000000;
        // Try to ignore the ApproximateBraneSize
        let interlock = InterlockHost::new(stx);
        let mut runnable = SplitCheckRunner::new(engine.clone(), tx, interlock, causetg);

        type Case = (Option<Vec<u8>>, Option<Vec<u8>>, Option<i64>);
        let mut check_cases = |cases: Vec<Case>| {
            for (encoded_spacelike_key, encoded_lightlike_key, table_id) in cases {
                brane.set_spacelike_key(encoded_spacelike_key.unwrap_or_else(Vec::new));
                brane.set_lightlike_key(encoded_lightlike_key.unwrap_or_else(Vec::new));
                runnable.run(SplitCheckTask::split_check(
                    brane.clone(),
                    true,
                    CheckPolicy::Scan,
                ));

                if let Some(id) = table_id {
                    let key = Key::from_raw(&gen_table_prefix(id));
                    loop {
                        match rx.try_recv() {
                            Ok((_, CasualMessage::BraneApproximateSize { .. }))
                            | Ok((_, CasualMessage::BraneApproximateTuplespaceInstanton { .. })) => (),
                            Ok((_, CasualMessage::SplitBrane { split_tuplespaceInstanton, .. })) => {
                                assert_eq!(split_tuplespaceInstanton, vec![key.into_encoded()]);
                                break;
                            }
                            others => panic!("expect {:?}, but got {:?}", key, others),
                        }
                    }
                } else {
                    loop {
                        match rx.try_recv() {
                            Ok((_, CasualMessage::BraneApproximateSize { .. }))
                            | Ok((_, CasualMessage::BraneApproximateTuplespaceInstanton { .. })) => (),
                            Err(mpsc::TryRecvError::Empty) => {
                                break;
                            }
                            others => panic!("expect empty, but got {:?}", others),
                        }
                    }
                }
            }
        };

        let gen_encoded_table_prefix = |table_id| {
            let key = Key::from_raw(&gen_table_prefix(table_id));
            key.into_encoded()
        };

        // arbitrary padding.
        let padding = b"_r00000005";

        // Put some tables
        // t1_xx, t3_xx
        for i in 1..4 {
            if i % 2 == 0 {
                // leave some space.
                continue;
            }

            let mut key = gen_table_prefix(i);
            key.extlightlike_from_slice(padding);
            let s = tuplespaceInstanton::data_key(Key::from_raw(&key).as_encoded());
            engine.put_causet(CAUSET_WRITE, &s, &s).unwrap();
        }

        check_cases(vec![
            // ["", "") => t1
            (None, None, Some(1)),
            // ["t1", "") => t3
            (Some(gen_encoded_table_prefix(1)), None, Some(3)),
            // ["t1", "t5") => t3
            (
                Some(gen_encoded_table_prefix(1)),
                Some(gen_encoded_table_prefix(5)),
                Some(3),
            ),
            // ["t2", "t4") => t3
            (
                Some(gen_encoded_table_prefix(2)),
                Some(gen_encoded_table_prefix(4)),
                Some(3),
            ),
        ]);

        // Put some data to t3
        for i in 1..4 {
            let mut key = gen_table_prefix(3);
            key.extlightlike_from_slice(format!("{:?}{}", padding, i).as_bytes());
            let s = tuplespaceInstanton::data_key(Key::from_raw(&key).as_encoded());
            engine.put_causet(CAUSET_WRITE, &s, &s).unwrap();
        }

        check_cases(vec![
            // ["t1", "") => t3
            (Some(gen_encoded_table_prefix(1)), None, Some(3)),
            // ["t3", "") => skip
            (Some(gen_encoded_table_prefix(3)), None, None),
            // ["t3", "t5") => skip
            (
                Some(gen_encoded_table_prefix(3)),
                Some(gen_encoded_table_prefix(5)),
                None,
            ),
        ]);

        // Put some data before t and after t.
        for i in 0..3 {
            // m is less than t and is the prefix of meta tuplespaceInstanton.
            let key = format!("m{:?}{}", padding, i);
            let s = tuplespaceInstanton::data_key(Key::from_raw(key.as_bytes()).as_encoded());
            engine.put_causet(CAUSET_WRITE, &s, &s).unwrap();
            let key = format!("u{:?}{}", padding, i);
            let s = tuplespaceInstanton::data_key(Key::from_raw(key.as_bytes()).as_encoded());
            engine.put_causet(CAUSET_WRITE, &s, &s).unwrap();
        }

        check_cases(vec![
            // ["", "") => t1
            (None, None, Some(1)),
            // ["", "t1"] => skip
            (None, Some(gen_encoded_table_prefix(1)), None),
            // ["", "t3"] => t1
            (None, Some(gen_encoded_table_prefix(3)), Some(1)),
            // ["", "s"] => skip
            (None, Some(b"s".to_vec()), None),
            // ["u", ""] => skip
            (Some(b"u".to_vec()), None, None),
            // ["t3", ""] => None
            (Some(gen_encoded_table_prefix(3)), None, None),
            // ["t1", ""] => t3
            (Some(gen_encoded_table_prefix(1)), None, Some(3)),
        ]);
    }
}
