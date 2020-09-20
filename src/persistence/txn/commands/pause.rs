// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::persistence::kv::WriteData;
use crate::persistence::lock_manager::LockManager;
use crate::persistence::txn::commands::{
    Command, CommandExt, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::persistence::txn::Result;
use crate::persistence::{ProcessResult, Snapshot};
use std::thread;
use std::time::Duration;
use txn_types::Key;

command! {
    /// **Testing functionality:** Latch the given tuplespaceInstanton for given duration.
    ///
    /// This means other write operations that involve these tuplespaceInstanton will be blocked.
    Pause:
        cmd_ty => (),
        display => "kv::command::pause tuplespaceInstanton:({}) {} ms | {:?}", (tuplespaceInstanton.len, duration, ctx),
        content => {
            /// The tuplespaceInstanton to hold latches on.
            tuplespaceInstanton: Vec<Key>,
            /// The amount of time in milliseconds to latch for.
            duration: u64,
        }
}

impl CommandExt for Pause {
    ctx!();
    tag!(pause);
    write_bytes!(tuplespaceInstanton: multiple);
    gen_lock!(tuplespaceInstanton: multiple);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for Pause {
    fn process_write(self, _snapshot: S, _context: WriteContext<'_, L>) -> Result<WriteResult> {
        thread::sleep(Duration::from_millis(self.duration));
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: WriteData::default(),
            events: 0,
            pr: ProcessResult::Res,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
