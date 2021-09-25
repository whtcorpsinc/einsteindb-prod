//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;

use ekvproto::import_sst_timeshare::SstMeta;

use crate::store::util::is_epoch_stale;
use crate::store::{StoreMsg, StoreRouter};
use edb::CausetEngine;
use fidel_client::FidelClient;
use sst_importer::SSTImporter;
use std::marker::PhantomData;
use violetabftstore::interlock::::worker::Runnable;

pub enum Task {
    DeleteSST { ssts: Vec<SstMeta> },
    ValidateSST { ssts: Vec<SstMeta> },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Task::DeleteSST { ref ssts } => write!(f, "Delete {} ssts", ssts.len()),
            Task::ValidateSST { ref ssts } => write!(f, "Validate {} ssts", ssts.len()),
        }
    }
}

pub struct Runner<EK, C, S>
where
    EK: CausetEngine,
    S: StoreRouter<EK>,
{
    store_id: u64,
    store_router: S,
    importer: Arc<SSTImporter>,
    fidel_client: Arc<C>,
    _engine: PhantomData<EK>,
}

impl<EK, C, S> Runner<EK, C, S>
where
    EK: CausetEngine,
    C: FidelClient,
    S: StoreRouter<EK>,
{
    pub fn new(
        store_id: u64,
        store_router: S,
        importer: Arc<SSTImporter>,
        fidel_client: Arc<C>,
    ) -> Runner<EK, C, S> {
        Runner {
            store_id,
            store_router,
            importer,
            fidel_client,
            _engine: PhantomData,
        }
    }

    /// Deletes SST files from the importer.
    fn handle_delete_sst(&self, ssts: Vec<SstMeta>) {
        for sst in &ssts {
            let _ = self.importer.delete(sst);
        }
    }

    /// Validates whether the SST is stale or not.
    fn handle_validate_sst(&self, ssts: Vec<SstMeta>) {
        let store_id = self.store_id;
        let mut invalid_ssts = Vec::new();
        for sst in ssts {
            match self.fidel_client.get_brane(sst.get_cone().get_spacelike()) {
                Ok(r) => {
                    // The brane id may or may not be the same as the
                    // SST file, but it doesn't matter, because the
                    // epoch of a cone will not decrease anyway.
                    if is_epoch_stale(r.get_brane_epoch(), sst.get_brane_epoch()) {
                        // Brane has not been fideliod.
                        continue;
                    }
                    if r.get_id() == sst.get_brane_id()
                        && r.get_peers().iter().any(|p| p.get_store_id() == store_id)
                    {
                        // The SST still belongs to this store.
                        continue;
                    }
                    invalid_ssts.push(sst);
                }
                Err(e) => {
                    error!(%e; "get brane failed");
                }
            }
        }

        // We need to lightlike back the result to check for the stale
        // peer, which may ingest the stale SST before it is
        // destroyed.
        let msg = StoreMsg::ValidateSSTResult { invalid_ssts };
        if let Err(e) = self.store_router.lightlike(msg) {
            error!(%e; "lightlike validate sst result failed");
        }
    }
}

impl<EK, C, S> Runnable for Runner<EK, C, S>
where
    EK: CausetEngine,
    C: FidelClient,
    S: StoreRouter<EK>,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::DeleteSST { ssts } => {
                self.handle_delete_sst(ssts);
            }
            Task::ValidateSST { ssts } => {
                self.handle_validate_sst(ssts);
            }
        }
    }
}
