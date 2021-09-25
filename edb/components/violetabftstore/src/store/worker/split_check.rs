// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::{self, Display, Formatter};
use std::mem;

use edb::{CfName, IterOptions, Iterable, Iteron, CausetEngine, Causet_WRITE, LARGE_CausetS};
use ekvproto::meta_timeshare::Brane;
use ekvproto::meta_timeshare::BraneEpoch;
use ekvproto::fidel_timeshare::CheckPolicy;

use crate::interlock::Config;
use crate::interlock::InterlockHost;
use crate::interlock::SplitCheckerHost;
use crate::store::{Callback, CasualMessage, CasualRouter};
use crate::Result;
use configuration::{ConfigChange, Configuration};
use violetabftstore::interlock::::CausetLearnedKey::CausetLearnedKey;
use violetabftstore::interlock::::worker::Runnable;

use super::metrics::*;

#[derive(PartialEq, Eq)]
pub struct KeyEntry {
    key: Vec<u8>,
    pos: usize,
    value_size: usize,
    causet: CfName,
}

impl KeyEntry {
    pub fn new(key: Vec<u8>, pos: usize, value_size: usize, causet: CfName) -> KeyEntry {
        KeyEntry {
            key,
            pos,
            value_size,
            causet,
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn is_commit_version(&self) -> bool {
        self.causet == Causet_WRITE
    }

    pub fn entry_size(&self) -> usize {
        self.value_size + self.key.len()
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(self.key.cmp(&rhs.key).reverse())
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

struct MergedIterator<I> {
    iters: Vec<(CfName, I)>,
    heap: BinaryHeap<KeyEntry>,
}

impl<I> MergedIterator<I>
where
    I: Iteron,
{
    fn new<E: CausetEngine>(
        db: &E,
        causets: &[CfName],
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        fill_cache: bool,
    ) -> Result<MergedIterator<E::Iteron>> {
        let mut iters = Vec::with_capacity(causets.len());
        let mut heap = BinaryHeap::with_capacity(causets.len());
        for (pos, causet) in causets.iter().enumerate() {
            let iter_opt = IterOptions::new(
                Some(CausetLearnedKey::from_slice(spacelike_key, 0, 0)),
                Some(CausetLearnedKey::from_slice(lightlike_key, 0, 0)),
                fill_cache,
            );
            let mut iter = db.Iteron_causet_opt(causet, iter_opt)?;
            let found: Result<bool> = iter.seek(spacelike_key.into()).map_err(|e| box_err!(e));
            if found? {
                heap.push(KeyEntry::new(
                    iter.key().to_vec(),
                    pos,
                    iter.value().len(),
                    *causet,
                ));
            }
            iters.push((*causet, iter));
        }
        Ok(MergedIterator { iters, heap })
    }

    fn next(&mut self) -> Option<KeyEntry> {
        let pos = match self.heap.peek() {
            None => return None,
            Some(e) => e.pos,
        };
        let (causet, iter) = &mut self.iters[pos];
        if iter.next().unwrap() {
            // TODO: avoid copy key.
            let mut e = KeyEntry::new(iter.key().to_vec(), pos, iter.value().len(), causet);
            let mut front = self.heap.peek_mut().unwrap();
            mem::swap(&mut e, &mut front);
            Some(e)
        } else {
            self.heap.pop()
        }
    }
}

pub enum Task {
    SplitCheckTask {
        brane: Brane,
        auto_split: bool,
        policy: CheckPolicy,
    },
    ChangeConfig(ConfigChange),
    #[causet(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&Config) + lightlike>),
}

impl Task {
    pub fn split_check(brane: Brane, auto_split: bool, policy: CheckPolicy) -> Task {
        Task::SplitCheckTask {
            brane,
            auto_split,
            policy,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::SplitCheckTask {
                brane, auto_split, ..
            } => write!(
                f,
                "[split check worker] Split Check Task for {}, auto_split: {:?}",
                brane.get_id(),
                auto_split
            ),
            Task::ChangeConfig(_) => write!(f, "[split check worker] Change Config Task"),
            #[causet(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "[split check worker] Validate config"),
        }
    }
}

pub struct Runner<E, S>
where
    E: CausetEngine,
{
    engine: E,
    router: S,
    interlock: InterlockHost<E>,
    causet: Config,
}

impl<E, S> Runner<E, S>
where
    E: CausetEngine,
    S: CasualRouter<E>,
{
    pub fn new(engine: E, router: S, interlock: InterlockHost<E>, causet: Config) -> Runner<E, S> {
        Runner {
            engine,
            router,
            interlock,
            causet,
        }
    }

    /// Checks a Brane with split checkers to produce split tuplespaceInstanton and generates split admin command.
    fn check_split(&mut self, brane: &Brane, auto_split: bool, policy: CheckPolicy) {
        let brane_id = brane.get_id();
        let spacelike_key = tuplespaceInstanton::enc_spacelike_key(brane);
        let lightlike_key = tuplespaceInstanton::enc_lightlike_key(brane);
        debug!(
            "executing task";
            "brane_id" => brane_id,
            "spacelike_key" => log_wrappers::Key(&spacelike_key),
            "lightlike_key" => log_wrappers::Key(&lightlike_key),
        );
        CHECK_SPILT_COUNTER.all.inc();

        let mut host = self.interlock.new_split_checker_host(
            &self.causet,
            brane,
            &self.engine,
            auto_split,
            policy,
        );
        if host.skip() {
            debug!("skip split check"; "brane_id" => brane.get_id());
            return;
        }

        let split_tuplespaceInstanton = match host.policy() {
            CheckPolicy::Scan => {
                match self.scan_split_tuplespaceInstanton(&mut host, brane, &spacelike_key, &lightlike_key) {
                    Ok(tuplespaceInstanton) => tuplespaceInstanton,
                    Err(e) => {
                        error!(%e; "failed to scan split key"; "brane_id" => brane_id,);
                        return;
                    }
                }
            }
            CheckPolicy::Approximate => match host.approximate_split_tuplespaceInstanton(brane, &self.engine) {
                Ok(tuplespaceInstanton) => tuplespaceInstanton
                    .into_iter()
                    .map(|k| tuplespaceInstanton::origin_key(&k).to_vec())
                    .collect(),
                Err(e) => {
                    error!(%e;
                        "failed to get approximate split key, try scan way";
                        "brane_id" => brane_id,
                    );
                    match self.scan_split_tuplespaceInstanton(&mut host, brane, &spacelike_key, &lightlike_key) {
                        Ok(tuplespaceInstanton) => tuplespaceInstanton,
                        Err(e) => {
                            error!(%e; "failed to scan split key"; "brane_id" => brane_id,);
                            return;
                        }
                    }
                }
            },
            CheckPolicy::Usekey => vec![], // Handled by fidel worker directly.
        };

        if !split_tuplespaceInstanton.is_empty() {
            let brane_epoch = brane.get_brane_epoch().clone();
            let msg = new_split_brane(brane_epoch, split_tuplespaceInstanton);
            let res = self.router.lightlike(brane_id, msg);
            if let Err(e) = res {
                warn!("failed to lightlike check result"; "brane_id" => brane_id, "err" => %e);
            }

            CHECK_SPILT_COUNTER.success.inc();
        } else {
            debug!(
                "no need to lightlike, split key not found";
                "brane_id" => brane_id,
            );

            CHECK_SPILT_COUNTER.ignore.inc();
        }
    }

    /// Gets the split tuplespaceInstanton by scanning the cone.
    fn scan_split_tuplespaceInstanton(
        &self,
        host: &mut SplitCheckerHost<'_, E>,
        brane: &Brane,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let timer = CHECK_SPILT_HISTOGRAM.spacelike_coarse_timer();
        MergedIterator::<<E as Iterable>::Iteron>::new(
            &self.engine,
            LARGE_CausetS,
            spacelike_key,
            lightlike_key,
            false,
        )
        .map(|mut iter| {
            let mut size = 0;
            let mut tuplespaceInstanton = 0;
            while let Some(e) = iter.next() {
                if host.on_kv(brane, &e) {
                    return;
                }
                size += e.entry_size() as u64;
                tuplespaceInstanton += 1;
            }

            // if we scan the whole cone, we can fidelio approximate size and tuplespaceInstanton with accurate value.
            info!(
                "fidelio approximate size and tuplespaceInstanton with accurate value";
                "brane_id" => brane.get_id(),
                "size" => size,
                "tuplespaceInstanton" => tuplespaceInstanton,
            );
            let _ = self.router.lightlike(
                brane.get_id(),
                CasualMessage::BraneApproximateSize { size },
            );
            let _ = self.router.lightlike(
                brane.get_id(),
                CasualMessage::BraneApproximateTuplespaceInstanton { tuplespaceInstanton },
            );
        })?;
        timer.observe_duration();

        Ok(host.split_tuplespaceInstanton())
    }

    fn change_causet(&mut self, change: ConfigChange) {
        info!(
            "split check config fideliod";
            "change" => ?change
        );
        self.causet.fidelio(change);
    }
}

impl<E, S> Runnable for Runner<E, S>
where
    E: CausetEngine,
    S: CasualRouter<E>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::SplitCheckTask {
                brane,
                auto_split,
                policy,
            } => self.check_split(&brane, auto_split, policy),
            Task::ChangeConfig(c) => self.change_causet(c),
            #[causet(any(test, feature = "testexport"))]
            Task::Validate(f) => f(&self.causet),
        }
    }
}

fn new_split_brane<E>(brane_epoch: BraneEpoch, split_tuplespaceInstanton: Vec<Vec<u8>>) -> CasualMessage<E>
where
    E: CausetEngine,
{
    CasualMessage::SplitBrane {
        brane_epoch,
        split_tuplespaceInstanton,
        callback: Callback::None,
    }
}
