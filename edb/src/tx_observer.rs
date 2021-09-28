// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::sync::{
    Arc,
    Weak,
};

use std::sync::mpsc::{
    channel,
    Receiver,
    RecvError,
    Sender,
};

use std::thread;

use indexmap::{
    IndexMap,
};

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use edbn::entities::{
    OpType,
};

use causetq_pull_promises::errors::{
    Result,
};

use types::{
    AttributeSet,
};

use watcher::TransactWatcher;

pub struct TxSemaphore {
    notify_fn: Arc<Box<Fn(&str, IndexMap<&SolitonId, &AttributeSet>) + Send + Sync>>,
    attributes: AttributeSet,
}

impl TxSemaphore {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxSemaphore where F: Fn(&str, IndexMap<&SolitonId, &AttributeSet>) + 'static + Send + Sync {
        TxSemaphore {
            notify_fn: Arc::new(Box::new(notify_fn)),
            attributes,
        }
    }

    pub fn applicable_reports<'r>(&self, reports: &'r IndexMap<SolitonId, AttributeSet>) -> IndexMap<&'r SolitonId, &'r AttributeSet> {
        reports.into_iter()
               .filter(|&(_causecausetxid, attrs)| !self.attributes.is_disjoint(attrs))
               .collect()
    }

    fn notify(&self, key: &str, reports: IndexMap<&SolitonId, &AttributeSet>) {
        (*self.notify_fn)(key, reports);
    }
}

pub trait Command {
    fn execute(&mut self);
}

pub struct TxCommand {
    reports: IndexMap<SolitonId, AttributeSet>,
    semaphores: Weak<IndexMap<String, Arc<TxSemaphore>>>,
}

impl TxCommand {
    fn new(semaphores: &Arc<IndexMap<String, Arc<TxSemaphore>>>, reports: IndexMap<SolitonId, AttributeSet>) -> Self {
        TxCommand {
            reports,
            semaphores: Arc::downgrade(semaphores),
        }
    }
}

impl Command for TxCommand {
    fn execute(&mut self) {
        self.semaphores.upgrade().map(|semaphores| {
            for (key, semaphore) in semaphores.iter() {
                let applicable_reports = semaphore.applicable_reports(&self.reports);
                if !applicable_reports.is_empty() {
                    semaphore.notify(&key, applicable_reports);
                }
            }
        });
    }
}

pub struct TxObservationService {
    semaphores: Arc<IndexMap<String, Arc<TxSemaphore>>>,
    executor: Option<Sender<Box<Command + Send>>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            semaphores: Arc::new(IndexMap::new()),
            executor: None,
        }
    }

    // For testing purposes
    pub fn is_registered(&self, key: &String) -> bool {
        self.semaphores.contains_key(key)
    }

    pub fn register(&mut self, key: String, semaphore: Arc<TxSemaphore>) {
        Arc::make_mut(&mut self.semaphores).insert(key, semaphore);
    }

    pub fn deregister(&mut self, key: &String) {
        Arc::make_mut(&mut self.semaphores).remove(key);
    }

    pub fn has_semaphores(&self) -> bool {
        !self.semaphores.is_empty()
    }

    pub fn in_progress_did_commit(&mut self, causecausetxes: IndexMap<SolitonId, AttributeSet>) {
        // Don't spawn a thread only to say nothing.
        if !self.has_semaphores() {
            return;
        }

        let executor = self.executor.get_or_insert_with(|| {
            let (causetx, rx): (Sender<Box<Command + Send>>, Receiver<Box<Command + Send>>) = channel();
            let mut worker = CommandExecutor::new(rx);

            thread::spawn(move || {
                worker.main();
            });

            causetx
        });

        let cmd = Box::new(TxCommand::new(&self.semaphores, causecausetxes));
        executor.send(cmd).unwrap();
    }
}

impl Drop for TxObservationService {
    fn drop(&mut self) {
        self.executor = None;
    }
}

pub struct InProgressSemaphoreTransactWatcher {
    collected_attributes: AttributeSet,
    pub causecausetxes: IndexMap<SolitonId, AttributeSet>,
}

impl InProgressSemaphoreTransactWatcher {
    pub fn new() -> InProgressSemaphoreTransactWatcher {
        InProgressSemaphoreTransactWatcher {
            collected_attributes: Default::default(),
            causecausetxes: Default::default(),
        }
    }
}

impl TransactWatcher for InProgressSemaphoreTransactWatcher {
    fn Causet(&mut self, _op: OpType, _e: SolitonId, a: SolitonId, _v: &MinkowskiType) {
        self.collected_attributes.insert(a);
    }

    fn done(&mut self, t: &SolitonId, _schemaReplicant: &SchemaReplicant) -> Result<()> {
        let collected_attributes = ::std::mem::replace(&mut self.collected_attributes, Default::default());
        self.causecausetxes.insert(*t, collected_attributes);
        Ok(())
    }
}

struct CommandExecutor {
    receiver: Receiver<Box<Command + Send>>,
}

impl CommandExecutor {
    fn new(rx: Receiver<Box<Command + Send>>) -> Self {
        CommandExecutor {
            receiver: rx,
        }
    }

    fn main(&mut self) {
        loop {
            match self.receiver.recv() {
                Err(RecvError) => {
                    // "The recv operation can only fail if the sending half of a channel (or
                    // sync_channel) is disconnected, implying that no further messages will ever be
                    // received."
                    // No need to log here.
                    return
                },

                Ok(mut cmd) => {
                    cmd.execute()
                },
            }
        }
    }
}
