//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::Arc;

use crossbeam::channel::TrylightlikeError;
use engine_lmdb::raw::DB;
use engine_lmdb::{LmdbEngine, LmdbSnapshot};
use edb::{ALL_CausetS, Causet_DEFAULT};
use ekvproto::kvrpc_timeshare::{Context, ExtraOp as TxnExtraOp};
use ekvproto::meta_timeshare::Brane;
use ekvproto::Violetabft_cmd_timeshare::{VioletaBftCmdRequest, VioletaBftCmdResponse, Response};
use ekvproto::Violetabft_server_timeshare::VioletaBftMessage;
use violetabftstore::router::{LocalReadRouter, VioletaBftStoreRouter};
use violetabftstore::store::{
    cmd_resp, util, Callback, CasualMessage, CasualRouter, PeerMsg, ProposalRouter, VioletaBftCommand,
    ReadResponse, BraneSnapshot, SignificantMsg, StoreMsg, StoreRouter, WriteResponse,
};
use violetabftstore::Result;
use tempfile::{Builder, TempDir};
use edb::server::violetabftkv::{CmdRes, VioletaBftKv};
use edb::causet_storage::kv::{
    Callback as EngineCallback, CbContext, Modify, Result as EngineResult, WriteData,
};
use edb::causet_storage::Engine;
use violetabftstore::interlock::::time::ThreadReadId;
use txn_types::Key;

use crate::test;

#[derive(Clone)]
struct SyncBenchRouter {
    db: Arc<DB>,
    brane: Brane,
}

impl SyncBenchRouter {
    fn new(brane: Brane, db: Arc<DB>) -> SyncBenchRouter {
        SyncBenchRouter { db, brane }
    }
}

impl SyncBenchRouter {
    fn invoke(&self, cmd: VioletaBftCommand<LmdbSnapshot>) {
        let mut response = VioletaBftCmdResponse::default();
        cmd_resp::bind_term(&mut response, 1);
        match cmd.callback {
            Callback::Read(cb) => {
                let snapshot = LmdbSnapshot::new(Arc::clone(&self.db));
                let brane = Arc::new(self.brane.to_owned());
                cb(ReadResponse {
                    response,
                    snapshot: Some(BraneSnapshot::from_snapshot(Arc::new(snapshot), brane)),
                    txn_extra_op: TxnExtraOp::Noop,
                })
            }
            Callback::Write(cb) => {
                let mut resp = Response::default();
                let cmd_type = cmd.request.get_requests()[0].get_cmd_type();
                resp.set_cmd_type(cmd_type);
                response.mut_responses().push(resp);
                cb(WriteResponse { response })
            }
            _ => unreachable!(),
        }
    }
}

impl CasualRouter<LmdbEngine> for SyncBenchRouter {
    fn lightlike(&self, _: u64, _: CasualMessage<LmdbEngine>) -> Result<()> {
        Ok(())
    }
}

impl ProposalRouter<LmdbSnapshot> for SyncBenchRouter {
    fn lightlike(
        &self,
        _: VioletaBftCommand<LmdbSnapshot>,
    ) -> std::result::Result<(), TrylightlikeError<VioletaBftCommand<LmdbSnapshot>>> {
        Ok(())
    }
}
impl StoreRouter<LmdbEngine> for SyncBenchRouter {
    fn lightlike(&self, _: StoreMsg<LmdbEngine>) -> Result<()> {
        Ok(())
    }
}

impl VioletaBftStoreRouter<LmdbEngine> for SyncBenchRouter {
    /// lightlikes VioletaBftMessage to local store.
    fn lightlike_Violetabft_msg(&self, _: VioletaBftMessage) -> Result<()> {
        Ok(())
    }

    /// lightlikes a significant message. We should guarantee that the message can't be dropped.
    fn significant_lightlike(&self, _: u64, _: SignificantMsg<LmdbSnapshot>) -> Result<()> {
        Ok(())
    }

    fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<LmdbEngine>) {}

    fn lightlike_command(&self, req: VioletaBftCmdRequest, cb: Callback<LmdbSnapshot>) -> Result<()> {
        self.invoke(VioletaBftCommand::new(req, cb));
        Ok(())
    }
}

impl LocalReadRouter<LmdbEngine> for SyncBenchRouter {
    fn read(
        &self,
        _: Option<ThreadReadId>,
        req: VioletaBftCmdRequest,
        cb: Callback<LmdbSnapshot>,
    ) -> Result<()> {
        self.lightlike_command(req, cb)
    }

    fn release_snapshot_cache(&self) {}
}

fn new_engine() -> (TempDir, Arc<DB>) {
    let dir = Builder::new().prefix("bench_rafkv").temfidelir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = engine_lmdb::raw_util::new_engine(&path, None, ALL_CausetS, None).unwrap();
    (dir, Arc::new(db))
}

// The lower limit of time a async_snapshot may take.
#[bench]
fn bench_async_snapshots_noop(b: &mut test::Bencher) {
    let (_dir, db) = new_engine();
    let snapshot = LmdbSnapshot::new(Arc::clone(&db));
    let resp = ReadResponse {
        response: VioletaBftCmdResponse::default(),
        snapshot: Some(BraneSnapshot::from_snapshot(
            Arc::new(snapshot),
            Arc::new(Brane::default()),
        )),
        txn_extra_op: TxnExtraOp::Noop,
    };

    b.iter(|| {
        let cb1: EngineCallback<BraneSnapshot<LmdbSnapshot>> = Box::new(
            move |(_, res): (CbContext, EngineResult<BraneSnapshot<LmdbSnapshot>>)| {
                assert!(res.is_ok());
            },
        );
        let cb2: EngineCallback<CmdRes> =
            Box::new(move |(ctx, res): (CbContext, EngineResult<CmdRes>)| {
                if let Ok(CmdRes::Snap(snap)) = res {
                    cb1((ctx, Ok(snap)));
                }
            });
        let cb: Callback<LmdbSnapshot> =
            Callback::Read(Box::new(move |resp: ReadResponse<LmdbSnapshot>| {
                let res = CmdRes::Snap(resp.snapshot.unwrap());
                cb2((CbContext::new(), Ok(res)));
            }));
        cb.invoke_read(resp.clone());
    });
}

#[bench]
fn bench_async_snapshot(b: &mut test::Bencher) {
    let leader = util::new_peer(2, 3);
    let mut brane = Brane::default();
    brane.set_id(1);
    brane.set_spacelike_key(vec![]);
    brane.set_lightlike_key(vec![]);
    brane.mut_peers().push(leader.clone());
    brane.mut_brane_epoch().set_version(2);
    brane.mut_brane_epoch().set_conf_ver(5);
    let (_tmp, db) = new_engine();
    let kv = VioletaBftKv::new(
        SyncBenchRouter::new(brane.clone(), db.clone()),
        LmdbEngine::from_db(db),
    );

    let mut ctx = Context::default();
    ctx.set_brane_id(brane.get_id());
    ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    ctx.set_peer(leader);
    b.iter(|| {
        let on_finished: EngineCallback<BraneSnapshot<LmdbSnapshot>> = Box::new(move |results| {
            let _ = test::black_box(results);
        });
        kv.async_snapshot(&ctx, None, on_finished).unwrap();
    });
}

#[bench]
fn bench_async_write(b: &mut test::Bencher) {
    let leader = util::new_peer(2, 3);
    let mut brane = Brane::default();
    brane.set_id(1);
    brane.set_spacelike_key(vec![]);
    brane.set_lightlike_key(vec![]);
    brane.mut_peers().push(leader.clone());
    brane.mut_brane_epoch().set_version(2);
    brane.mut_brane_epoch().set_conf_ver(5);
    let (_tmp, db) = new_engine();
    let kv = VioletaBftKv::new(
        SyncBenchRouter::new(brane.clone(), db.clone()),
        LmdbEngine::from_db(db),
    );

    let mut ctx = Context::default();
    ctx.set_brane_id(brane.get_id());
    ctx.set_brane_epoch(brane.get_brane_epoch().clone());
    ctx.set_peer(leader);
    b.iter(|| {
        let on_finished: EngineCallback<()> = Box::new(|_| {
            test::black_box(());
        });
        kv.async_write(
            &ctx,
            WriteData::from_modifies(vec![Modify::Delete(
                Causet_DEFAULT,
                Key::from_encoded(b"fooo".to_vec()),
            )]),
            on_finished,
        )
        .unwrap();
    });
}
