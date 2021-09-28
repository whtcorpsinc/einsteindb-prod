// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::error;
use std::fmt::{self, Display, Formatter};
use std::sync::mpsc::lightlikeer;

use crate::store::{CasualMessage, CasualRouter};

use edb::{Engines, CausetEngine, VioletaBftEngine};
use violetabftstore::interlock::::time::Duration;
use violetabftstore::interlock::::timer::Timer;
use violetabftstore::interlock::::worker::{Runnable, RunnableWithTimer};

const MAX_GC_REGION_BATCH: usize = 128;
const COMPACT_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub enum Task {
    Gc {
        brane_id: u64,
        spacelike_idx: u64,
        lightlike_idx: u64,
    },
    Purge,
}

impl Task {
    pub fn gc(brane_id: u64, spacelike: u64, lightlike: u64) -> Self {
        Task::Gc {
            brane_id,
            spacelike_idx: spacelike,
            lightlike_idx: lightlike,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Gc {
                brane_id,
                spacelike_idx,
                lightlike_idx,
            } => write!(
                f,
                "GC VioletaBft Logs [brane: {}, from: {}, to: {}]",
                brane_id, spacelike_idx, lightlike_idx
            ),
            Task::Purge => write!(f, "Purge Expired Files",),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<dyn error::Error + Sync + lightlike>) {
            from()
            cause(err.as_ref())
            display("violetabftlog gc failed {:?}", err)
        }
    }
}

pub struct Runner<EK: CausetEngine, ER: VioletaBftEngine, R: CasualRouter<EK>> {
    ch: R,
    tasks: Vec<Task>,
    engines: Engines<EK, ER>,
    gc_entries: Option<lightlikeer<usize>>,
}

impl<EK: CausetEngine, ER: VioletaBftEngine, R: CasualRouter<EK>> Runner<EK, ER, R> {
    pub fn new(ch: R, engines: Engines<EK, ER>) -> Runner<EK, ER, R> {
        Runner {
            ch,
            engines,
            tasks: vec![],
            gc_entries: None,
        }
    }

    /// Does the GC job and returns the count of logs collected.
    fn gc_violetabft_log(
        &mut self,
        brane_id: u64,
        spacelike_idx: u64,
        lightlike_idx: u64,
    ) -> Result<usize, Error> {
        let deleted = box_try!(self.engines.violetabft.gc(brane_id, spacelike_idx, lightlike_idx));
        Ok(deleted)
    }

    fn report_collected(&self, collected: usize) {
        if let Some(ref ch) = self.gc_entries {
            ch.lightlike(collected).unwrap();
        }
    }

    fn flush(&mut self) {
        // Sync wal of kv_db to make sure the data before apply_index has been persisted to disk.
        self.engines.kv.sync().unwrap_or_else(|e| {
            panic!("failed to sync kv_engine in violetabft_log_gc: {:?}", e);
        });
        let tasks = std::mem::replace(&mut self.tasks, vec![]);
        for t in tasks {
            match t {
                Task::Gc {
                    brane_id,
                    spacelike_idx,
                    lightlike_idx,
                } => {
                    debug!("gc violetabft log"; "brane_id" => brane_id, "lightlike_index" => lightlike_idx);
                    match self.gc_violetabft_log(brane_id, spacelike_idx, lightlike_idx) {
                        Err(e) => {
                            error!("failed to gc"; "brane_id" => brane_id, "err" => %e);
                            self.report_collected(0);
                        }
                        Ok(n) => {
                            debug!("gc log entries"; "brane_id" => brane_id, "entry_count" => n);
                            self.report_collected(n);
                        }
                    }
                }
                Task::Purge => {
                    let branes = match self.engines.violetabft.purge_expired_files() {
                        Ok(branes) => branes,
                        Err(e) => {
                            warn!("purge expired files"; "err" => %e);
                            return;
                        }
                    };
                    for brane_id in branes {
                        let _ = self.ch.lightlike(brane_id, CasualMessage::ForceCompactVioletaBftLogs);
                    }
                }
            }
        }
    }

    pub fn new_timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(COMPACT_LOG_INTERVAL, ());
        timer
    }
}

impl<EK, ER, R> Runnable for Runner<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    R: CasualRouter<EK>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        self.tasks.push(task);
        if self.tasks.len() < MAX_GC_REGION_BATCH {
            return;
        }
        self.flush();
    }

    fn shutdown(&mut self) {
        self.flush();
    }
}

impl<EK, ER, R> RunnableWithTimer for Runner<EK, ER, R>
where
    EK: CausetEngine,
    ER: VioletaBftEngine,
    R: CasualRouter<EK>,
{
    type TimeoutTask = ();
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        self.flush();
        timer.add_task(COMPACT_LOG_INTERVAL, ());
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use engine_lmdb::util::new_engine;
    use edb::{Engines, CausetEngine, MuBlock, WriteBatchExt, ALL_CausetS, Causet_DEFAULT};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::Builder;

    #[test]
    fn test_gc_violetabft_log() {
        let dir = Builder::new().prefix("gc-violetabft-log-test").temfidelir().unwrap();
        let path_violetabft = dir.path().join("violetabft");
        let path_kv = dir.path().join("kv");
        let violetabft_db = new_engine(path_kv.to_str().unwrap(), None, &[Causet_DEFAULT], None).unwrap();
        let kv_db = new_engine(path_violetabft.to_str().unwrap(), None, ALL_CausetS, None).unwrap();
        let engines = Engines::new(kv_db, violetabft_db.clone());

        let (tx, rx) = mpsc::channel();
        let (r, _) = mpsc::sync_channel(1);
        let mut runner = Runner {
            gc_entries: Some(tx),
            engines,
            ch: r,
            tasks: vec![],
        };

        // generate violetabft logs
        let brane_id = 1;
        let mut violetabft_wb = violetabft_db.write_batch();
        for i in 0..100 {
            let k = tuplespaceInstanton::violetabft_log_key(brane_id, i);
            violetabft_wb.put(&k, b"entry").unwrap();
        }
        violetabft_db.write(&violetabft_wb).unwrap();

        let tbls = vec![
            (Task::gc(brane_id, 0, 10), 10, (0, 10), (10, 100)),
            (Task::gc(brane_id, 0, 50), 40, (0, 50), (50, 100)),
            (Task::gc(brane_id, 50, 50), 0, (0, 50), (50, 100)),
            (Task::gc(brane_id, 50, 60), 10, (0, 60), (60, 100)),
        ];

        for (task, expected_collectd, not_exist_cone, exist_cone) in tbls {
            runner.run(task);
            runner.flush();
            let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(res, expected_collectd);
            violetabft_log_must_not_exist(&violetabft_db, 1, not_exist_cone.0, not_exist_cone.1);
            violetabft_log_must_exist(&violetabft_db, 1, exist_cone.0, exist_cone.1);
        }
    }

    fn violetabft_log_must_not_exist(
        violetabft_engine: &impl CausetEngine,
        brane_id: u64,
        spacelike_idx: u64,
        lightlike_idx: u64,
    ) {
        for i in spacelike_idx..lightlike_idx {
            let k = tuplespaceInstanton::violetabft_log_key(brane_id, i);
            assert!(violetabft_engine.get_value(&k).unwrap().is_none());
        }
    }

    fn violetabft_log_must_exist(
        violetabft_engine: &impl CausetEngine,
        brane_id: u64,
        spacelike_idx: u64,
        lightlike_idx: u64,
    ) {
        for i in spacelike_idx..lightlike_idx {
            let k = tuplespaceInstanton::violetabft_log_key(brane_id, i);
            assert!(violetabft_engine.get_value(&k).unwrap().is_some());
        }
    }
}
