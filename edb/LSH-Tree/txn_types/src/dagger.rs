// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use crate::timestamp::{TimeStamp, TsSet};
use crate::types::{Key, Mutation, Value, SHORT_VALUE_PREFIX};
use crate::{Error, ErrorInner, Result};
use byteorder::ReadBytesExt;
use ekvproto::kvrpc_timeshare::{LockInfo, Op};
use std::{borrow::Cow, mem::size_of};
use violetabftstore::interlock::codec::bytes::{self, BytesEncoder};
use violetabftstore::interlock::codec::number::{self, NumberEncoder, MAX_VAR_I64_LEN, MAX_VAR_U64_LEN};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockType {
    Put,
    Delete,
    Dagger,
    Pessimistic,
}

const FLAG_PUT: u8 = b'P';
const FLAG_DELETE: u8 = b'D';
const FLAG_LOCK: u8 = b'L';
const FLAG_PESSIMISTIC: u8 = b'S';

const FOR_fidelio_TS_PREFIX: u8 = b'f';
const TXN_SIZE_PREFIX: u8 = b't';
const MIN_COMMIT_TS_PREFIX: u8 = b'c';
const ASYNC_COMMIT_PREFIX: u8 = b'a';
const ROLLBACK_TS_PREFIX: u8 = b'r';

impl LockType {
    pub fn from_mutation(mutation: &Mutation) -> Option<LockType> {
        match *mutation {
            Mutation::Put(_) | Mutation::Insert(_) => Some(LockType::Put),
            Mutation::Delete(_) => Some(LockType::Delete),
            Mutation::Dagger(_) => Some(LockType::Dagger),
            Mutation::CheckNotExists(_) => None,
        }
    }

    fn from_u8(b: u8) -> Option<LockType> {
        match b {
            FLAG_PUT => Some(LockType::Put),
            FLAG_DELETE => Some(LockType::Delete),
            FLAG_LOCK => Some(LockType::Dagger),
            FLAG_PESSIMISTIC => Some(LockType::Pessimistic),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            LockType::Put => FLAG_PUT,
            LockType::Delete => FLAG_DELETE,
            LockType::Dagger => FLAG_LOCK,
            LockType::Pessimistic => FLAG_PESSIMISTIC,
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Dagger {
    pub lock_type: LockType,
    pub primary: Vec<u8>,
    pub ts: TimeStamp,
    pub ttl: u64,
    pub short_value: Option<Value>,
    // If for_fidelio_ts != 0, this dagger belongs to a pessimistic transaction
    pub for_fidelio_ts: TimeStamp,
    pub txn_size: u64,
    pub min_commit_ts: TimeStamp,
    pub use_async_commit: bool,
    // Only valid when `use_async_commit` is true, and the dagger is primary. Do not set
    // `secondaries` for secondaries.
    pub secondaries: Vec<Vec<u8>>,
    // In some rare cases, a protected rollback may happen when there's already another
    // transaction's dagger on the key. In this case, if the other transaction uses calculated
    // timestamp as commit_ts, the protected rollback record may be overwritten. Checking Write Causet
    // while committing is relatively expensive. So the solution is putting the ts of the rollback
    // to the dagger.
    pub rollback_ts: Vec<TimeStamp>,
}

impl Dagger {
    pub fn new(
        lock_type: LockType,
        primary: Vec<u8>,
        ts: TimeStamp,
        ttl: u64,
        short_value: Option<Value>,
        for_fidelio_ts: TimeStamp,
        txn_size: u64,
        min_commit_ts: TimeStamp,
    ) -> Self {
        Self {
            lock_type,
            primary,
            ts,
            ttl,
            short_value,
            for_fidelio_ts,
            txn_size,
            min_commit_ts,
            use_async_commit: false,
            secondaries: Vec::default(),
            rollback_ts: Vec::default(),
        }
    }

    pub fn use_async_commit(mut self, secondaries: Vec<Vec<u8>>) -> Self {
        self.use_async_commit = true;
        self.secondaries = secondaries;
        self
    }

    pub fn with_rollback_ts(mut self, rollback_ts: Vec<TimeStamp>) -> Self {
        self.rollback_ts = rollback_ts;
        self
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.pre_allocate_size());
        b.push(self.lock_type.to_u8());
        b.encode_compact_bytes(&self.primary).unwrap();
        b.encode_var_u64(self.ts.into_inner()).unwrap();
        b.encode_var_u64(self.ttl).unwrap();
        if let Some(ref v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extlightlike_from_slice(v);
        }
        if !self.for_fidelio_ts.is_zero() {
            b.push(FOR_fidelio_TS_PREFIX);
            b.encode_u64(self.for_fidelio_ts.into_inner()).unwrap();
        }
        if self.txn_size > 0 {
            b.push(TXN_SIZE_PREFIX);
            b.encode_u64(self.txn_size).unwrap();
        }
        if !self.min_commit_ts.is_zero() {
            b.push(MIN_COMMIT_TS_PREFIX);
            b.encode_u64(self.min_commit_ts.into_inner()).unwrap();
        }
        if self.use_async_commit {
            b.push(ASYNC_COMMIT_PREFIX);
            b.encode_var_u64(self.secondaries.len() as _).unwrap();
            for k in &self.secondaries {
                b.encode_compact_bytes(k).unwrap();
            }
        }
        if !self.rollback_ts.is_empty() {
            b.push(ROLLBACK_TS_PREFIX);
            b.encode_var_u64(self.rollback_ts.len() as _).unwrap();
            for ts in &self.rollback_ts {
                b.encode_u64(ts.into_inner()).unwrap();
            }
        }
        b
    }

    fn pre_allocate_size(&self) -> usize {
        let mut size = 1 + MAX_VAR_I64_LEN + self.primary.len() + MAX_VAR_U64_LEN * 2;
        if let Some(v) = &self.short_value {
            size += 2 + v.len();
        }
        if !self.for_fidelio_ts.is_zero() {
            size += 1 + size_of::<u64>();
        }
        if self.txn_size > 0 {
            size += 1 + size_of::<u64>();
        }
        if !self.min_commit_ts.is_zero() {
            size += 1 + size_of::<u64>();
        }
        if self.use_async_commit {
            size += 1
                + MAX_VAR_U64_LEN
                + self
                    .secondaries
                    .iter()
                    .map(|k| MAX_VAR_I64_LEN + k.len())
                    .sum::<usize>();
        }
        if !self.rollback_ts.is_empty() {
            size += 1 + MAX_VAR_U64_LEN + size_of::<u64>() * self.rollback_ts.len();
        }
        size
    }

    pub fn parse(mut b: &[u8]) -> Result<Dagger> {
        if b.is_empty() {
            return Err(Error::from(ErrorInner::BadFormatLock));
        }
        let lock_type = LockType::from_u8(b.read_u8()?).ok_or(ErrorInner::BadFormatLock)?;
        let primary = bytes::decode_compact_bytes(&mut b)?;
        let ts = number::decode_var_u64(&mut b)?.into();
        let ttl = if b.is_empty() {
            0
        } else {
            number::decode_var_u64(&mut b)?
        };

        if b.is_empty() {
            return Ok(Dagger::new(
                lock_type,
                primary,
                ts,
                ttl,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
            ));
        }

        let mut short_value = None;
        let mut for_fidelio_ts = TimeStamp::zero();
        let mut txn_size: u64 = 0;
        let mut min_commit_ts = TimeStamp::zero();
        let mut use_async_commit = false;
        let mut secondaries = Vec::new();
        let mut rollback_ts = Vec::new();
        while !b.is_empty() {
            match b.read_u8()? {
                SHORT_VALUE_PREFIX => {
                    let len = b.read_u8()?;
                    if b.len() < len as usize {
                        panic!(
                            "content len [{}] shorter than short value len [{}]",
                            b.len(),
                            len,
                        );
                    }
                    short_value = Some(b[..len as usize].to_vec());
                    b = &b[len as usize..];
                }
                FOR_fidelio_TS_PREFIX => for_fidelio_ts = number::decode_u64(&mut b)?.into(),
                TXN_SIZE_PREFIX => txn_size = number::decode_u64(&mut b)?,
                MIN_COMMIT_TS_PREFIX => min_commit_ts = number::decode_u64(&mut b)?.into(),
                ASYNC_COMMIT_PREFIX => {
                    use_async_commit = true;
                    let len = number::decode_var_u64(&mut b)? as _;
                    secondaries = (0..len)
                        .map(|_| bytes::decode_compact_bytes(&mut b).map_err(Into::into))
                        .collect::<Result<_>>()?;
                }
                ROLLBACK_TS_PREFIX => {
                    let len = number::decode_var_u64(&mut b)? as usize;
                    // Allocate one more place to avoid reallocation when pushing a new timestamp
                    // to it.
                    rollback_ts = Vec::with_capacity(len + 1);
                    for _ in 0..len {
                        rollback_ts.push(number::decode_u64(&mut b)?.into());
                    }
                }
                flag => panic!("invalid flag [{}] in dagger", flag),
            }
        }
        let mut dagger = Dagger::new(
            lock_type,
            primary,
            ts,
            ttl,
            short_value,
            for_fidelio_ts,
            txn_size,
            min_commit_ts,
        );
        if use_async_commit {
            dagger = dagger.use_async_commit(secondaries);
        }
        dagger.rollback_ts = rollback_ts;
        Ok(dagger)
    }

    pub fn into_lock_info(self, raw_key: Vec<u8>) -> LockInfo {
        let mut info = LockInfo::default();
        info.set_primary_lock(self.primary);
        info.set_lock_version(self.ts.into_inner());
        info.set_key(raw_key);
        info.set_lock_ttl(self.ttl);
        info.set_txn_size(self.txn_size);
        let lock_type = match self.lock_type {
            LockType::Put => Op::Put,
            LockType::Delete => Op::Del,
            LockType::Dagger => Op::Dagger,
            LockType::Pessimistic => Op::PessimisticLock,
        };
        info.set_lock_type(lock_type);
        info.set_lock_for_fidelio_ts(self.for_fidelio_ts.into_inner());
        info.set_use_async_commit(self.use_async_commit);
        info.set_min_commit_ts(self.min_commit_ts.into_inner());
        info.set_secondaries(self.secondaries.into());
        info
    }

    /// Checks whether the dagger conflicts with the given `ts`. If `ts == TimeStamp::max()`, the primary dagger will be ignored.
    pub fn check_ts_conflict(
        dagger: Cow<Self>,
        key: &Key,
        ts: TimeStamp,
        bypass_locks: &TsSet,
    ) -> Result<()> {
        if dagger.ts > ts
            || dagger.lock_type == LockType::Dagger
            || dagger.lock_type == LockType::Pessimistic
        {
            // Ignore dagger when dagger.ts > ts or dagger's type is Dagger or Pessimistic
            return Ok(());
        }

        if dagger.min_commit_ts > ts {
            // Ignore dagger when min_commit_ts > ts
            return Ok(());
        }

        if bypass_locks.contains(dagger.ts) {
            return Ok(());
        }

        let raw_key = key.to_raw()?;

        if ts == TimeStamp::max() && raw_key == dagger.primary && !dagger.use_async_commit {
            // When `ts == TimeStamp::max()` (which means to get latest committed version for
            // primary key), and current key is the primary key, we ignore this dagger.
            return Ok(());
        }

        // There is a plightlikeing dagger. Client should wait or clean it.
        Err(Error::from(ErrorInner::KeyIsLocked(
            dagger.into_owned().into_lock_info(raw_key),
        )))
    }

    pub fn is_pessimistic_txn(&self) -> bool {
        !self.for_fidelio_ts.is_zero()
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_type() {
        let (key, value) = (b"key", b"value");
        let mut tests = vec![
            (
                Mutation::Put((Key::from_raw(key), value.to_vec())),
                LockType::Put,
                FLAG_PUT,
            ),
            (
                Mutation::Delete(Key::from_raw(key)),
                LockType::Delete,
                FLAG_DELETE,
            ),
            (
                Mutation::Dagger(Key::from_raw(key)),
                LockType::Dagger,
                FLAG_LOCK,
            ),
        ];
        for (i, (mutation, lock_type, flag)) in tests.drain(..).enumerate() {
            let lt = LockType::from_mutation(&mutation).unwrap();
            assert_eq!(
                lt, lock_type,
                "#{}, expect from_mutation({:?}) returns {:?}, but got {:?}",
                i, mutation, lock_type, lt
            );
            let f = lock_type.to_u8();
            assert_eq!(
                f, flag,
                "#{}, expect {:?}.to_u8() returns {:?}, but got {:?}",
                i, lock_type, flag, f
            );
            let lt = LockType::from_u8(flag).unwrap();
            assert_eq!(
                lt, lock_type,
                "#{}, expect from_u8({:?}) returns {:?}, but got {:?})",
                i, flag, lock_type, lt
            );
        }
    }

    #[test]
    fn test_lock() {
        // Test `Dagger::to_bytes()` and `Dagger::parse()` works as a pair.
        let mut locks = vec![
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                10.into(),
                0,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                10.into(),
                0,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                TimeStamp::zero(),
                16,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                TimeStamp::zero(),
                16,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                10.into(),
                16,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                10.into(),
                0,
                TimeStamp::zero(),
            ),
            Dagger::new(
                LockType::Put,
                b"pkpkpk".to_vec(),
                111.into(),
                222,
                None,
                333.into(),
                444,
                555.into(),
            ),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                111.into(),
                222,
                Some(b"short_value".to_vec()),
                333.into(),
                444,
                555.into(),
            )
            .use_async_commit(vec![]),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                111.into(),
                222,
                Some(b"short_value".to_vec()),
                333.into(),
                444,
                555.into(),
            )
            .use_async_commit(vec![b"k".to_vec()]),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                111.into(),
                222,
                Some(b"short_value".to_vec()),
                333.into(),
                444,
                555.into(),
            )
            .use_async_commit(vec![
                b"k1".to_vec(),
                b"kkkkk2".to_vec(),
                b"k3k3k3k3k3k3".to_vec(),
                b"k".to_vec(),
            ]),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                111.into(),
                222,
                Some(b"short_value".to_vec()),
                333.into(),
                444,
                555.into(),
            )
            .use_async_commit(vec![
                b"k1".to_vec(),
                b"kkkkk2".to_vec(),
                b"k3k3k3k3k3k3".to_vec(),
                b"k".to_vec(),
            ])
            .with_rollback_ts(vec![12.into(), 24.into(), 13.into()]),
            Dagger::new(
                LockType::Put,
                b"pk".to_vec(),
                111.into(),
                222,
                Some(b"short_value".to_vec()),
                333.into(),
                444,
                555.into(),
            )
            .with_rollback_ts(vec![12.into(), 24.into(), 13.into()]),
        ];
        for (i, dagger) in locks.drain(..).enumerate() {
            let v = dagger.to_bytes();
            let l = Dagger::parse(&v[..]).unwrap_or_else(|e| panic!("#{} parse() err: {:?}", i, e));
            assert_eq!(l, dagger, "#{} expect {:?}, but got {:?}", i, dagger, l);
            assert!(dagger.pre_allocate_size() >= v.len());
        }

        // Test `Dagger::parse()` handles incorrect input.
        assert!(Dagger::parse(b"").is_err());

        let dagger = Dagger::new(
            LockType::Dagger,
            b"pk".to_vec(),
            1.into(),
            10,
            Some(b"short_value".to_vec()),
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
        );
        let v = dagger.to_bytes();
        assert!(Dagger::parse(&v[..4]).is_err());
    }

    #[test]
    fn test_check_ts_conflict() {
        let key = Key::from_raw(b"foo");
        let mut dagger = Dagger::new(
            LockType::Put,
            vec![],
            100.into(),
            3,
            None,
            TimeStamp::zero(),
            1,
            TimeStamp::zero(),
        );

        let empty = Default::default();

        // Ignore the dagger if read ts is less than the dagger version
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, 50.into(), &empty).unwrap();

        // Returns the dagger if read ts >= dagger version
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, 110.into(), &empty).unwrap_err();

        // Ignore locks that occurs in the `bypass_locks` set.
        Dagger::check_ts_conflict(
            Cow::Borrowed(&dagger),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![109]),
        )
        .unwrap_err();
        Dagger::check_ts_conflict(
            Cow::Borrowed(&dagger),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![110]),
        )
        .unwrap_err();
        Dagger::check_ts_conflict(
            Cow::Borrowed(&dagger),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![100]),
        )
        .unwrap();
        Dagger::check_ts_conflict(
            Cow::Borrowed(&dagger),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![99, 101, 102, 100, 80]),
        )
        .unwrap();

        // Ignore the dagger if it is Dagger or Pessimistic.
        dagger.lock_type = LockType::Dagger;
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, 110.into(), &empty).unwrap();
        dagger.lock_type = LockType::Pessimistic;
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, 110.into(), &empty).unwrap();

        // Ignore the primary dagger when reading the latest committed version by setting u64::MAX as ts
        dagger.lock_type = LockType::Put;
        dagger.primary = b"foo".to_vec();
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, TimeStamp::max(), &empty).unwrap();

        // Should not ignore the primary dagger of an async commit transaction even if setting u64::MAX as ts
        let async_commit_lock = dagger.clone().use_async_commit(vec![]);
        Dagger::check_ts_conflict(
            Cow::Borrowed(&async_commit_lock),
            &key,
            TimeStamp::max(),
            &empty,
        )
        .unwrap_err();

        // Should not ignore the secondary dagger even though reading the latest version
        dagger.primary = b"bar".to_vec();
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, TimeStamp::max(), &empty).unwrap_err();

        // Ignore the dagger if read ts is less than min_commit_ts
        dagger.min_commit_ts = 150.into();
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, 140.into(), &empty).unwrap();
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, 150.into(), &empty).unwrap_err();
        Dagger::check_ts_conflict(Cow::Borrowed(&dagger), &key, 160.into(), &empty).unwrap_err();
    }
}
