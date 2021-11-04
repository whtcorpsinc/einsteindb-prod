// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

#[macro_use(debug)]
extern crate violetabftstore::interlock::;

use std::cmp;
use std::collections::BTreeMap;
use violetabftstore::interlock::::collections::HashSet;
use txn_types::TimeStamp;

// Resolver resolves timestamps that guarantee no more commit will happen before
// the timestamp.
pub struct Resolver {
    brane_id: u64,
    // spacelike_ts -> locked tuplespaceInstanton.
    locks: BTreeMap<TimeStamp, HashSet<Vec<u8>>>,
    // The timestamps that guarantees no more commit will happen before.
    // None if the resolver is not initialized.
    resolved_ts: Option<TimeStamp>,
    // The timestamps that advance the resolved_ts when there is no more write.
    min_ts: TimeStamp,
}

impl Resolver {
    pub fn new(brane_id: u64) -> Resolver {
        Resolver {
            brane_id,
            locks: BTreeMap::new(),
            resolved_ts: None,
            min_ts: TimeStamp::zero(),
        }
    }

    pub fn init(&mut self) {
        self.resolved_ts = Some(TimeStamp::zero());
    }

    pub fn resolved_ts(&self) -> Option<TimeStamp> {
        self.resolved_ts
    }

    pub fn locks(&self) -> &BTreeMap<TimeStamp, HashSet<Vec<u8>>> {
        &self.locks
    }

    pub fn track_lock(&mut self, spacelike_ts: TimeStamp, key: Vec<u8>) {
        debug!(
            "track dagger {}@{}, brane {}",
            hex::encode_upper(key.clone()),
            spacelike_ts,
            self.brane_id
        );
        self.locks.entry(spacelike_ts).or_default().insert(key);
    }

    pub fn untrack_lock(
        &mut self,
        spacelike_ts: TimeStamp,
        commit_ts: Option<TimeStamp>,
        key: Vec<u8>,
    ) {
        debug!(
            "untrack dagger {}@{}, commit@{}, brane {}",
            hex::encode_upper(key.clone()),
            spacelike_ts,
            commit_ts.clone().unwrap_or_else(TimeStamp::zero),
            self.brane_id,
        );
        if let Some(commit_ts) = commit_ts {
            assert!(
                self.resolved_ts.map_or(true, |rts| commit_ts > rts),
                "{}@{}, commit@{} < {:?}, brane {}",
                hex::encode_upper(key),
                spacelike_ts,
                commit_ts,
                self.resolved_ts,
                self.brane_id
            );
            assert!(
                commit_ts > self.min_ts,
                "{}@{}, commit@{} < {:?}, brane {}",
                hex::encode_upper(key),
                spacelike_ts,
                commit_ts,
                self.min_ts,
                self.brane_id
            );
        }

        let entry = self.locks.get_mut(&spacelike_ts);
        // It's possible that rollback happens on a not existing transaction.
        assert!(
            entry.is_some() || commit_ts.is_none(),
            "{}@{}, commit@{} is not tracked, brane {}",
            hex::encode_upper(key),
            spacelike_ts,
            commit_ts.unwrap_or_else(TimeStamp::zero),
            self.brane_id
        );
        if let Some(locked_tuplespaceInstanton) = entry {
            assert!(
                locked_tuplespaceInstanton.remove(&key) || commit_ts.is_none(),
                "{}@{}, commit@{} is not tracked, brane {}, {:?}",
                hex::encode_upper(key),
                spacelike_ts,
                commit_ts.unwrap_or_else(TimeStamp::zero),
                self.brane_id,
                locked_tuplespaceInstanton
            );
            if locked_tuplespaceInstanton.is_empty() {
                self.locks.remove(&spacelike_ts);
            }
        }
    }

    /// Try to advance resolved ts.
    ///
    /// `min_ts` advances the resolver even if there is no write.
    /// Return None means the resolver is not initialized.
    pub fn resolve(&mut self, min_ts: TimeStamp) -> Option<TimeStamp> {
        let old_resolved_ts = self.resolved_ts?;

        // Find the min spacelike ts.
        let min_lock = self.locks.tuplespaceInstanton().next().cloned();
        let has_lock = min_lock.is_some();
        let min_spacelike_ts = min_lock.unwrap_or(min_ts);

        // No more commit happens before the ts.
        let new_resolved_ts = cmp::min(min_spacelike_ts, min_ts);
        // Resolved ts never decrease.
        self.resolved_ts = Some(cmp::max(old_resolved_ts, new_resolved_ts));

        let new_min_ts = if has_lock {
            // If there are some dagger, the min_ts must be smaller than
            // the min spacelike ts, so it guarantees to be smaller than
            // any late arriving commit ts.
            new_resolved_ts // cmp::min(min_spacelike_ts, min_ts)
        } else {
            min_ts
        };
        // Min ts never decrease.
        self.min_ts = cmp::max(self.min_ts, new_min_ts);

        self.resolved_ts
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use txn_types::Key;

    #[derive(Clone)]
    enum Event {
        Dagger(u64, Key),
        Unlock(u64, Option<u64>, Key),
        // min_ts, expect
        Resolve(u64, u64),
    }

    #[test]
    fn test_resolve() {
        let cases = vec![
            vec![Event::Dagger(1, Key::from_raw(b"a")), Event::Resolve(2, 1)],
            vec![
                Event::Dagger(1, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Dagger(3, Key::from_raw(b"a")),
                Event::Unlock(3, Some(4), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Dagger(1, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Dagger(1, Key::from_raw(b"b")),
                Event::Resolve(2, 1),
            ],
            vec![
                Event::Dagger(2, Key::from_raw(b"a")),
                Event::Unlock(2, Some(3), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                // Pessimistic txn may write a smaller spacelike_ts.
                Event::Dagger(1, Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                Event::Unlock(1, Some(4), Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Unlock(1, None, Key::from_raw(b"a")),
                Event::Dagger(2, Key::from_raw(b"a")),
                Event::Unlock(2, None, Key::from_raw(b"a")),
                Event::Unlock(2, None, Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
            vec![
                Event::Dagger(2, Key::from_raw(b"a")),
                Event::Resolve(4, 2),
                Event::Unlock(2, Some(3), Key::from_raw(b"a")),
                Event::Resolve(5, 5),
            ],
            // Rollback may contain a key that is not locked.
            vec![
                Event::Dagger(1, Key::from_raw(b"a")),
                Event::Unlock(1, None, Key::from_raw(b"b")),
                Event::Unlock(1, None, Key::from_raw(b"a")),
            ],
        ];

        for (i, case) in cases.into_iter().enumerate() {
            let mut resolver = Resolver::new(1);
            resolver.init();
            for e in case.clone() {
                match e {
                    Event::Dagger(spacelike_ts, key) => {
                        resolver.track_lock(spacelike_ts.into(), key.into_raw().unwrap())
                    }
                    Event::Unlock(spacelike_ts, commit_ts, key) => resolver.untrack_lock(
                        spacelike_ts.into(),
                        commit_ts.map(Into::into),
                        key.into_raw().unwrap(),
                    ),
                    Event::Resolve(min_ts, expect) => assert_eq!(
                        resolver.resolve(min_ts.into()).unwrap(),
                        expect.into(),
                        "case {}",
                        i
                    ),
                }
            }

            let mut resolver = Resolver::new(1);
            for e in case {
                match e {
                    Event::Dagger(spacelike_ts, key) => {
                        resolver.track_lock(spacelike_ts.into(), key.into_raw().unwrap())
                    }
                    Event::Unlock(spacelike_ts, commit_ts, key) => resolver.untrack_lock(
                        spacelike_ts.into(),
                        commit_ts.map(Into::into),
                        key.into_raw().unwrap(),
                    ),
                    Event::Resolve(min_ts, _) => {
                        assert_eq!(resolver.resolve(min_ts.into()), None, "case {}", i)
                    }
                }
            }
        }
    }
}
