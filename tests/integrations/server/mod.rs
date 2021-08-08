//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

mod gc_worker;
mod kv_service;
mod lock_manager;
mod violetabft_client;
mod security;
mod status_server;

use std::sync::Arc;

use ::security::{SecurityConfig, SecurityManager};
use futures::future::FutureExt;
use grpcio::RpcStatusCode;
use grpcio::*;
use ekvproto::interlock::*;
use ekvproto::kvrpcpb::*;
use ekvproto::violetabft_serverpb::{Done, VioletaBftMessage, SnapshotSoliton};
use ekvproto::einsteindb-prodpb::{
    create_einsteindb-prod, BatchCommandsRequest, BatchCommandsResponse, BatchVioletaBftMessage, EINSTEINDB,
};

macro_rules! unary_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(&mut self, ctx: RpcContext<'_>, _: $req_name, sink: UnarySink<$resp_name>) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! sstream_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            _: $req_name,
            sink: ServerStreamingSink<$resp_name>,
        ) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! cstream_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            _: RequestStream<$req_name>,
            sink: ClientStreamingSink<$resp_name>,
        ) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! bstream_call {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            _: RequestStream<$req_name>,
            sink: DuplexSink<$resp_name>,
        ) {
            let status = RpcStatus::new(RpcStatusCode::UNIMPLEMENTED, None);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    };
}

macro_rules! unary_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(&mut self, ctx: RpcContext<'_>, req: $req_name, sink: UnarySink<$resp_name>) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

macro_rules! sstream_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            req: $req_name,
            sink: ServerStreamingSink<$resp_name>,
        ) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

macro_rules! cstream_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            req: RequestStream<$req_name>,
            sink: ClientStreamingSink<$resp_name>,
        ) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

macro_rules! bstream_call_dispatch {
    ($name:tt, $req_name:tt, $resp_name:tt) => {
        fn $name(
            &mut self,
            ctx: RpcContext<'_>,
            req: RequestStream<$req_name>,
            sink: DuplexSink<$resp_name>,
        ) {
            (self.0).$name(ctx, req, sink)
        }
    };
}

#[derive(Clone)]
struct MockKv<T>(pub T);

trait MockKvService {
    unary_call!(kv_get, GetRequest, GetResponse);
    unary_call!(kv_scan, ScanRequest, ScanResponse);
    unary_call!(kv_prewrite, PrewriteRequest, PrewriteResponse);
    unary_call!(
        kv_pessimistic_lock,
        PessimisticLockRequest,
        PessimisticLockResponse
    );
    unary_call!(
        kv_pessimistic_rollback,
        PessimisticRollbackRequest,
        PessimisticRollbackResponse
    );
    unary_call!(kv_commit, CommitRequest, CommitResponse);
    unary_call!(kv_import, ImportRequest, ImportResponse);
    unary_call!(kv_cleanup, CleanupRequest, CleanupResponse);
    unary_call!(kv_batch_get, BatchGetRequest, BatchGetResponse);
    unary_call!(
        kv_batch_rollback,
        BatchRollbackRequest,
        BatchRollbackResponse
    );
    unary_call!(kv_txn_heart_beat, TxnHeartBeatRequest, TxnHeartBeatResponse);
    unary_call!(
        kv_check_txn_status,
        CheckTxnStatusRequest,
        CheckTxnStatusResponse
    );
    unary_call!(
        kv_check_secondary_locks,
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse
    );
    unary_call!(kv_scan_lock, ScanLockRequest, ScanLockResponse);
    unary_call!(kv_resolve_lock, ResolveLockRequest, ResolveLockResponse);
    unary_call!(kv_gc, GcRequest, GcResponse);
    unary_call!(kv_delete_cone, DeleteConeRequest, DeleteConeResponse);
    unary_call!(raw_get, RawGetRequest, RawGetResponse);
    unary_call!(raw_batch_get, RawBatchGetRequest, RawBatchGetResponse);
    unary_call!(raw_scan, RawScanRequest, RawScanResponse);
    unary_call!(raw_batch_scan, RawBatchScanRequest, RawBatchScanResponse);
    unary_call!(raw_put, RawPutRequest, RawPutResponse);
    unary_call!(raw_batch_put, RawBatchPutRequest, RawBatchPutResponse);
    unary_call!(raw_delete, RawDeleteRequest, RawDeleteResponse);
    unary_call!(
        raw_batch_delete,
        RawBatchDeleteRequest,
        RawBatchDeleteResponse
    );
    unary_call!(
        raw_delete_cone,
        RawDeleteConeRequest,
        RawDeleteConeResponse
    );
    unary_call!(ver_get, VerGetRequest, VerGetResponse);
    unary_call!(ver_batch_get, VerBatchGetRequest, VerBatchGetResponse);
    unary_call!(ver_mut, VerMutRequest, VerMutResponse);
    unary_call!(ver_batch_mut, VerBatchMutRequest, VerBatchMutResponse);
    unary_call!(ver_scan, VerScanRequest, VerScanResponse);
    unary_call!(
        ver_delete_cone,
        VerDeleteConeRequest,
        VerDeleteConeResponse
    );
    unary_call!(
        unsafe_destroy_cone,
        UnsafeDestroyConeRequest,
        UnsafeDestroyConeResponse
    );
    unary_call!(
        register_lock_semaphore,
        RegisterLockSemaphoreRequest,
        RegisterLockSemaphoreResponse
    );
    unary_call!(
        check_lock_semaphore,
        CheckLockSemaphoreRequest,
        CheckLockSemaphoreResponse
    );
    unary_call!(
        remove_lock_semaphore,
        RemoveLockSemaphoreRequest,
        RemoveLockSemaphoreResponse
    );
    unary_call!(
        physical_scan_lock,
        PhysicalScanLockRequest,
        PhysicalScanLockResponse
    );
    unary_call!(interlock, Request, Response);
    sstream_call!(batch_interlock, BatchRequest, BatchResponse);
    sstream_call!(interlock_stream, Request, Response);
    cstream_call!(violetabft, VioletaBftMessage, Done);
    cstream_call!(batch_violetabft, BatchVioletaBftMessage, Done);
    cstream_call!(snapshot, SnapshotSoliton, Done);
    unary_call!(
        tail_pointer_get_by_spacelike_ts,
        MvccGetByStartTsRequest,
        MvccGetByStartTsResponse
    );
    unary_call!(tail_pointer_get_by_key, MvccGetByKeyRequest, MvccGetByKeyResponse);
    unary_call!(split_brane, SplitBraneRequest, SplitBraneResponse);
    unary_call!(read_index, ReadIndexRequest, ReadIndexResponse);
    bstream_call!(batch_commands, BatchCommandsRequest, BatchCommandsResponse);
}

impl<T: MockKvService + Clone + lightlike + 'static> EINSTEINDB for MockKv<T> {
    unary_call_dispatch!(kv_get, GetRequest, GetResponse);
    unary_call_dispatch!(kv_scan, ScanRequest, ScanResponse);
    unary_call_dispatch!(kv_prewrite, PrewriteRequest, PrewriteResponse);
    unary_call_dispatch!(
        kv_pessimistic_lock,
        PessimisticLockRequest,
        PessimisticLockResponse
    );
    unary_call_dispatch!(
        kv_pessimistic_rollback,
        PessimisticRollbackRequest,
        PessimisticRollbackResponse
    );
    unary_call_dispatch!(kv_commit, CommitRequest, CommitResponse);
    unary_call_dispatch!(kv_import, ImportRequest, ImportResponse);
    unary_call_dispatch!(kv_cleanup, CleanupRequest, CleanupResponse);
    unary_call_dispatch!(kv_batch_get, BatchGetRequest, BatchGetResponse);
    unary_call_dispatch!(
        kv_batch_rollback,
        BatchRollbackRequest,
        BatchRollbackResponse
    );
    unary_call_dispatch!(kv_txn_heart_beat, TxnHeartBeatRequest, TxnHeartBeatResponse);
    unary_call_dispatch!(
        kv_check_txn_status,
        CheckTxnStatusRequest,
        CheckTxnStatusResponse
    );
    unary_call_dispatch!(
        kv_check_secondary_locks,
        CheckSecondaryLocksRequest,
        CheckSecondaryLocksResponse
    );
    unary_call_dispatch!(kv_scan_lock, ScanLockRequest, ScanLockResponse);
    unary_call_dispatch!(kv_resolve_lock, ResolveLockRequest, ResolveLockResponse);
    unary_call_dispatch!(kv_gc, GcRequest, GcResponse);
    unary_call_dispatch!(kv_delete_cone, DeleteConeRequest, DeleteConeResponse);
    unary_call_dispatch!(raw_get, RawGetRequest, RawGetResponse);
    unary_call_dispatch!(raw_batch_get, RawBatchGetRequest, RawBatchGetResponse);
    unary_call_dispatch!(raw_scan, RawScanRequest, RawScanResponse);
    unary_call_dispatch!(raw_batch_scan, RawBatchScanRequest, RawBatchScanResponse);
    unary_call_dispatch!(raw_put, RawPutRequest, RawPutResponse);
    unary_call_dispatch!(raw_batch_put, RawBatchPutRequest, RawBatchPutResponse);
    unary_call_dispatch!(raw_delete, RawDeleteRequest, RawDeleteResponse);
    unary_call_dispatch!(
        raw_batch_delete,
        RawBatchDeleteRequest,
        RawBatchDeleteResponse
    );
    unary_call_dispatch!(
        raw_delete_cone,
        RawDeleteConeRequest,
        RawDeleteConeResponse
    );
    unary_call_dispatch!(ver_get, VerGetRequest, VerGetResponse);
    unary_call_dispatch!(ver_batch_get, VerBatchGetRequest, VerBatchGetResponse);
    unary_call_dispatch!(ver_mut, VerMutRequest, VerMutResponse);
    unary_call_dispatch!(ver_batch_mut, VerBatchMutRequest, VerBatchMutResponse);
    unary_call_dispatch!(ver_scan, VerScanRequest, VerScanResponse);
    unary_call_dispatch!(
        ver_delete_cone,
        VerDeleteConeRequest,
        VerDeleteConeResponse
    );
    unary_call_dispatch!(
        unsafe_destroy_cone,
        UnsafeDestroyConeRequest,
        UnsafeDestroyConeResponse
    );
    unary_call_dispatch!(
        register_lock_semaphore,
        RegisterLockSemaphoreRequest,
        RegisterLockSemaphoreResponse
    );
    unary_call_dispatch!(
        check_lock_semaphore,
        CheckLockSemaphoreRequest,
        CheckLockSemaphoreResponse
    );
    unary_call_dispatch!(
        remove_lock_semaphore,
        RemoveLockSemaphoreRequest,
        RemoveLockSemaphoreResponse
    );
    unary_call_dispatch!(
        physical_scan_lock,
        PhysicalScanLockRequest,
        PhysicalScanLockResponse
    );
    unary_call_dispatch!(interlock, Request, Response);
    sstream_call_dispatch!(batch_interlock, BatchRequest, BatchResponse);
    sstream_call_dispatch!(interlock_stream, Request, Response);
    cstream_call_dispatch!(violetabft, VioletaBftMessage, Done);
    cstream_call_dispatch!(batch_violetabft, BatchVioletaBftMessage, Done);
    cstream_call_dispatch!(snapshot, SnapshotSoliton, Done);
    unary_call_dispatch!(
        tail_pointer_get_by_spacelike_ts,
        MvccGetByStartTsRequest,
        MvccGetByStartTsResponse
    );
    unary_call!(tail_pointer_get_by_key, MvccGetByKeyRequest, MvccGetByKeyResponse);
    unary_call_dispatch!(split_brane, SplitBraneRequest, SplitBraneResponse);
    unary_call_dispatch!(read_index, ReadIndexRequest, ReadIndexResponse);
    bstream_call_dispatch!(batch_commands, BatchCommandsRequest, BatchCommandsResponse);
}

fn mock_kv_service<T>(kv: MockKv<T>, ip: &str, port: u16) -> Result<Server>
where
    T: MockKvService + Clone + lightlike + 'static,
{
    let env = Arc::new(Environment::new(2));
    let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());

    let channel_args = ChannelBuilder::new(Arc::clone(&env))
        .max_concurrent_stream(2)
        .max_receive_message_len(-1)
        .max_lightlike_message_len(-1)
        .build_args();

    let mut sb = ServerBuilder::new(Arc::clone(&env))
        .channel_args(channel_args)
        .register_service(create_einsteindb-prod(kv));
    sb = security_mgr.bind(sb, ip, port);
    sb.build()
}
