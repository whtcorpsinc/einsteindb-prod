// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use super::cleanup_sst::{Runner as CleanupSSTRunner, Task as CleanupSSTTask};
use super::compact::{Runner as CompactRunner, Task as CompactTask};

use crate::store::StoreRouter;
use edb::CausetEngine;
use fidel_client::FidelClient;
use violetabftstore::interlock::::worker::Runnable;

pub enum Task {
    Compact(CompactTask),
    CleanupSST(CleanupSSTTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Compact(ref t) => t.fmt(f),
            Task::CleanupSST(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<E, C, S>
where
    E: CausetEngine,
    S: StoreRouter<E>,
{
    compact: CompactRunner<E>,
    cleanup_sst: CleanupSSTRunner<E, C, S>,
}

impl<E, C, S> Runner<E, C, S>
where
    E: CausetEngine,
    C: FidelClient,
    S: StoreRouter<E>,
{
    pub fn new(
        compact: CompactRunner<E>,
        cleanup_sst: CleanupSSTRunner<E, C, S>,
    ) -> Runner<E, C, S> {
        Runner {
            compact,
            cleanup_sst,
        }
    }
}

impl<E, C, S> Runnable for Runner<E, C, S>
where
    E: CausetEngine,
    C: FidelClient,
    S: StoreRouter<E>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact(t) => self.compact.run(t),
            Task::CleanupSST(t) => self.cleanup_sst.run(t),
        }
    }
}
