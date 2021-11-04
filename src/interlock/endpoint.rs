//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{borrow::Cow, time::Duration};

use async_stream::try_stream;
use futures::channel::mpsc;
use futures::prelude::*;
use tokio::sync::Semaphore;

use ekvproto::kvrpc_timeshare::IsolationLevel;
use ekvproto::{interlock as cop_timeshare, error_timeshare, kvrpc_timeshare};
#[causet(feature = "protobuf-codec")]
use protobuf::CodedInputStream;
use protobuf::Message;
use fidel_timeshare::{AnalyzeReq, AnalyzeType, ChecksumRequest, ChecksumScanOn, PosetDagRequest, ExecType};

use crate::read_pool::ReadPoolHandle;
use crate::server::Config;
use crate::causet_storage::kv::{self, with_tls_engine};
use crate::causet_storage::tail_pointer::Error as MvccError;
use crate::causet_storage::{self, Engine, Snapshot, SnapshotStore};

use crate::interlock::cache::CachedRequestHandler;
use crate::interlock::sentinels::limit_concurrency;
use crate::interlock::sentinels::track;
use crate::interlock::metrics::*;
use crate::interlock::tracker::Tracker;
use crate::interlock::*;
use interlocking_directorate::ConcurrencyManager;
use minitrace::prelude::*;
use txn_types::Dagger;

/// Requests that need time of less than `LIGHT_TASK_THRESHOLD` is considered as light ones,
/// which means they don't need a permit from the semaphore before execution.
const LIGHT_TASK_THRESHOLD: Duration = Duration::from_millis(5);

/// A pool to build and run Interlock request handlers.
pub struct node<E: Engine> {
    /// The thread pool to run Interlock requests.
    read_pool: ReadPoolHandle,

    /// The concurrency limiter of the interlock.
    semaphore: Option<Arc<Semaphore>>,

    interlocking_directorate: ConcurrencyManager,

    check_memory_locks: bool,

    /// The recursion limit when parsing Interlock Protobuf requests.
    ///
    /// Note that this limit is ignored if we are using Prost.
    recursion_limit: u32,

    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    stream_channel_size: usize,

    /// The soft time limit of handling Interlock requests.
    max_handle_duration: Duration,

    _phantom: PhantomData<E>,
}

impl<E: Engine> Clone for node<E> {
    fn clone(&self) -> Self {
        Self {
            read_pool: self.read_pool.clone(),
            semaphore: self.semaphore.clone(),
            interlocking_directorate: self.interlocking_directorate.clone(),
            ..*self
        }
    }
}

impl<E: Engine> violetabftstore::interlock::::Assertlightlike for node<E> {}

impl<E: Engine> node<E> {
    pub fn new(
        causet: &Config,
        read_pool: ReadPoolHandle,
        interlocking_directorate: ConcurrencyManager,
    ) -> Self {
        // FIXME: When yatp is used, we need to limit interlock requests in progress to avoid
        // using too much memory. However, if there are a number of large requests, small requests
        // will still be blocked. This needs to be improved.
        let semaphore = match &read_pool {
            ReadPoolHandle::Yatp { .. } => {
                Some(Arc::new(Semaphore::new(causet.lightlike_point_max_concurrency)))
            }
            _ => None,
        };
        Self {
            read_pool,
            semaphore,
            interlocking_directorate,
            check_memory_locks: causet.lightlike_point_check_memory_locks,
            recursion_limit: causet.lightlike_point_recursion_limit,
            batch_row_limit: causet.lightlike_point_batch_row_limit,
            stream_batch_row_limit: causet.lightlike_point_stream_batch_row_limit,
            stream_channel_size: causet.lightlike_point_stream_channel_size,
            max_handle_duration: causet.lightlike_point_request_max_handle_duration.0,
            _phantom: Default::default(),
        }
    }

    fn check_memory_locks(
        &self,
        req_ctx: &ReqContext,
        key_cones: &[cop_timeshare::KeyCone],
    ) -> Result<()> {
        if !self.check_memory_locks {
            return Ok(());
        }

        let spacelike_ts = req_ctx.txn_spacelike_ts;
        self.interlocking_directorate.fidelio_max_ts(spacelike_ts);
        if req_ctx.context.get_isolation_level() == IsolationLevel::Si {
            for cone in key_cones {
                let spacelike_key = txn_types::Key::from_raw(cone.get_spacelike());
                let lightlike_key = txn_types::Key::from_raw(cone.get_lightlike());
                self.interlocking_directorate
                    .read_cone_check(Some(&spacelike_key), Some(&lightlike_key), |key, dagger| {
                        Dagger::check_ts_conflict(
                            Cow::Borrowed(dagger),
                            key,
                            spacelike_ts,
                            &req_ctx.bypass_locks,
                        )
                    })
                    .map_err(MvccError::from)?;
            }
        }
        Ok(())
    }

    /// Parse the raw `Request` to create `RequestHandlerBuilder` and `ReqContext`.
    /// Returns `Err` if fails.
    ///
    /// It also checks if there are locks in memory blocking this read request.
    fn parse_request_and_check_memory_locks(
        &self,
        mut req: cop_timeshare::Request,
        peer: Option<String>,
        is_streaming: bool,
    ) -> Result<(RequestHandlerBuilder<E::Snap>, ReqContext)> {
        // This `Parser` is here because rust-proto supports customising its
        // recursion limit and Prost does not. Therefore we lightlike up doing things
        // a bit differently for the two codecs.
        #[causet(feature = "protobuf-codec")]
        struct Parser<'a> {
            input: CodedInputStream<'a>,
        }

        #[causet(feature = "protobuf-codec")]
        impl<'a> Parser<'a> {
            fn new(data: &'a [u8], recursion_limit: u32) -> Parser<'a> {
                let mut input = CodedInputStream::from_bytes(data);
                input.set_recursion_limit(recursion_limit);
                Parser { input }
            }

            fn merge_to(&mut self, target: &mut impl Message) -> Result<()> {
                box_try!(target.merge_from(&mut self.input));
                Ok(())
            }
        }

        #[causet(feature = "prost-codec")]
        struct Parser<'a> {
            input: &'a [u8],
        }

        #[causet(feature = "prost-codec")]
        impl<'a> Parser<'a> {
            fn new(input: &'a [u8], _: u32) -> Parser<'a> {
                Parser { input }
            }

            fn merge_to(&self, target: &mut impl Message) -> Result<()> {
                box_try!(target.merge_from_bytes(&self.input));
                Ok(())
            }
        }

        fail_point!("interlock_parse_request", |_| Err(box_err!(
            "unsupported tp (failpoint)"
        )));

        let (context, data, cones, mut spacelike_ts) = (
            req.take_context(),
            req.take_data(),
            req.take_cones().to_vec(),
            req.get_spacelike_ts(),
        );
        let cache_match_version = if req.get_is_cache_enabled() {
            Some(req.get_cache_if_match_version())
        } else {
            None
        };

        // Prost and rust-proto require different mutability.
        #[allow(unused_mut)]
        let mut parser = Parser::new(&data, self.recursion_limit);
        let req_ctx: ReqContext;
        let builder: RequestHandlerBuilder<E::Snap>;

        match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut posetdag = PosetDagRequest::default();
                parser.merge_to(&mut posetdag)?;
                let mut Block_scan = false;
                let mut is_desc_scan = false;
                if let Some(scan) = posetdag.get_executors().iter().next() {
                    Block_scan = scan.get_tp() == ExecType::TypeBlockScan;
                    if Block_scan {
                        is_desc_scan = scan.get_tbl_scan().get_desc();
                    } else {
                        is_desc_scan = scan.get_idx_scan().get_desc();
                    }
                }
                if spacelike_ts == 0 {
                    spacelike_ts = posetdag.get_spacelike_ts_fallback();
                }
                let tag = if Block_scan {
                    ReqTag::select
                } else {
                    ReqTag::index
                };

                req_ctx = ReqContext::new(
                    tag,
                    context,
                    cones.as_slice(),
                    self.max_handle_duration,
                    peer,
                    Some(is_desc_scan),
                    spacelike_ts.into(),
                    cache_match_version,
                );

                self.check_memory_locks(&req_ctx, &cones)?;

                let batch_row_limit = self.get_batch_row_limit(is_streaming);
                builder = Box::new(move |snap, req_ctx: &ReqContext| {
                    // TODO: Remove explicit type once rust-lang#41078 is resolved
                    let data_version = snap.get_data_version();
                    let store = SnapshotStore::new(
                        snap,
                        spacelike_ts.into(),
                        req_ctx.context.get_isolation_level(),
                        !req_ctx.context.get_not_fill_cache(),
                        req_ctx.bypass_locks.clone(),
                        req.get_is_cache_enabled(),
                    );
                    posetdag::PosetDagHandlerBuilder::new(
                        posetdag,
                        cones,
                        store,
                        req_ctx.deadline,
                        batch_row_limit,
                        is_streaming,
                        req.get_is_cache_enabled(),
                    )
                    .data_version(data_version)
                    .build()
                });
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::default();
                parser.merge_to(&mut analyze)?;
                let Block_scan = analyze.get_tp() == AnalyzeType::TypePrimaryCauset;
                if spacelike_ts == 0 {
                    spacelike_ts = analyze.get_spacelike_ts_fallback();
                }

                let tag = if Block_scan {
                    ReqTag::analyze_Block
                } else {
                    ReqTag::analyze_index
                };
                req_ctx = ReqContext::new(
                    tag,
                    context,
                    cones.as_slice(),
                    self.max_handle_duration,
                    peer,
                    None,
                    spacelike_ts.into(),
                    cache_match_version,
                );

                self.check_memory_locks(&req_ctx, &cones)?;

                builder = Box::new(move |snap, req_ctx: &_| {
                    // TODO: Remove explicit type once rust-lang#41078 is resolved
                    statistics::analyze::AnalyzeContext::new(
                        analyze, cones, spacelike_ts, snap, req_ctx,
                    )
                    .map(|h| h.into_boxed())
                });
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::default();
                parser.merge_to(&mut checksum)?;
                let Block_scan = checksum.get_scan_on() == ChecksumScanOn::Block;
                if spacelike_ts == 0 {
                    spacelike_ts = checksum.get_spacelike_ts_fallback();
                }

                let tag = if Block_scan {
                    ReqTag::checksum_Block
                } else {
                    ReqTag::checksum_index
                };
                req_ctx = ReqContext::new(
                    tag,
                    context,
                    cones.as_slice(),
                    self.max_handle_duration,
                    peer,
                    None,
                    spacelike_ts.into(),
                    cache_match_version,
                );

                self.check_memory_locks(&req_ctx, &cones)?;

                builder = Box::new(move |snap, req_ctx: &_| {
                    // TODO: Remove explicit type once rust-lang#41078 is resolved
                    checksum::ChecksumContext::new(checksum, cones, spacelike_ts, snap, req_ctx)
                        .map(|h| h.into_boxed())
                });
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };
        Ok((builder, req_ctx))
    }

    /// Get the batch Evcausetidx limit configuration.
    #[inline]
    fn get_batch_row_limit(&self, is_streaming: bool) -> usize {
        if is_streaming {
            self.stream_batch_row_limit
        } else {
            self.batch_row_limit
        }
    }
    #[inline]
    fn async_snapshot(
        engine: &E,
        ctx: &kvrpc_timeshare::Context,
    ) -> impl std::future::Future<Output = Result<E::Snap>> {
        kv::snapshot(engine, None, ctx).map_err(Error::from)
    }
    /// The real implementation of handling a unary request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the unary request interface of the
    /// `RequestHandler` to process the request and produce a result.
    async fn handle_unary_request_impl(
        semaphore: Option<Arc<Semaphore>>,
        mut tracker: Box<Tracker>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> Result<cop_timeshare::Response> {
        // When this function is being executed, it may be queued for a long time, so that
        // deadline may exceed.
        tracker.on_scheduled();
        tracker.req_ctx.deadline.check()?;

        // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
        // exists.
        let snapshot = unsafe {
            with_tls_engine(|engine| Self::async_snapshot(engine, &tracker.req_ctx.context))
        }
        .trace_async(fidel_timeshare::Event::EINSTEINDBCoprGetSnapshot as u32)
        .await?;
        // When snapshot is retrieved, deadline may exceed.
        tracker.on_snapshot_finished();
        tracker.req_ctx.deadline.check()?;

        let mut handler = if tracker.req_ctx.cache_match_version.is_some()
            && tracker.req_ctx.cache_match_version == snapshot.get_data_version()
        {
            // Build a cached request handler instead if cache version is matching.
            CachedRequestHandler::builder()(snapshot, &tracker.req_ctx)?
        } else {
            handler_builder(snapshot, &tracker.req_ctx)?
        };

        tracker.on_begin_all_items();

        let handle_request_future = track(
            handler
                .handle_request()
                .trace_async(fidel_timeshare::Event::EINSTEINDBCoprHandleRequest as u32),
            &mut tracker,
        );
        let result = if let Some(semaphore) = &semaphore {
            limit_concurrency(handle_request_future, semaphore, LIGHT_TASK_THRESHOLD).await
        } else {
            handle_request_future.await
        };

        // There might be errors when handling requests. In this case, we still need its
        // execution metrics.
        let mut causet_storage_stats = Statistics::default();
        handler.collect_scan_statistics(&mut causet_storage_stats);
        tracker.collect_causet_storage_statistics(causet_storage_stats);
        let exec_details = tracker.get_exec_details();
        tracker.on_finish_all_items();

        let mut resp = match result {
            Ok(resp) => {
                COPR_RESP_SIZE.inc_by(resp.data.len() as i64);
                resp
            }
            Err(e) => make_error_response(e),
        };
        resp.set_exec_details(exec_details);
        Ok(resp)
    }

    /// Handle a unary request and run on the read pool.
    ///
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(future)` in other cases.
    /// The future inside may be an error however.
    fn handle_unary_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl Future<Output = Result<cop_timeshare::Response>> {
        let priority = req_ctx.context.get_priority();
        let task_id = req_ctx.build_task_id();
        // box the tracker so that moving it is cheap.
        let tracker = Box::new(Tracker::new(req_ctx));

        let res = self
            .read_pool
            .spawn_handle(
                Self::handle_unary_request_impl(self.semaphore.clone(), tracker, handler_builder)
                    .trace_task(fidel_timeshare::Event::EINSTEINDBCoprScheduleTask as u32),
                priority,
                task_id,
            )
            .map_err(|_| Error::MaxPlightlikeingTasksExceeded);
        async move { res.await? }
    }

    /// Parses and handles a unary request. Returns a future that will never fail. If there are
    /// errors during parsing or handling, they will be converted into a `Response` as the success
    /// result of the future.
    #[inline]
    pub fn parse_and_handle_unary_request(
        &self,
        req: cop_timeshare::Request,
        peer: Option<String>,
    ) -> impl Future<Output = cop_timeshare::Response> {
        let (_guard, collector) = minitrace::trace_may_enable(
            req.is_trace_enabled,
            fidel_timeshare::Event::EINSTEINDBCoprGetRequest as u32,
        );

        let result_of_future = self
            .parse_request_and_check_memory_locks(req, peer, false)
            .map(|(handler_builder, req_ctx)| self.handle_unary_request(req_ctx, handler_builder));

        async move {
            let mut resp = match result_of_future {
                Err(e) => make_error_response(e),
                Ok(handle_fut) => handle_fut.await.unwrap_or_else(|e| make_error_response(e)),
            };
            if let Some(collector) = collector {
                let span_sets = collector.collect();
                resp.set_spans(violetabftstore::interlock::::trace::encode_spans(span_sets).collect())
            }
            resp
        }
    }

    /// The real implementation of handling a stream request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the stream request interface of the
    /// `RequestHandler` multiple times to process the request and produce multiple results.
    fn handle_stream_request_impl(
        semaphore: Option<Arc<Semaphore>>,
        mut tracker: Box<Tracker>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl futures::stream::Stream<Item = Result<cop_timeshare::Response>> {
        try_stream! {
            let _permit = if let Some(semaphore) = semaphore.as_ref() {
                Some(semaphore.acquire().await)
            } else {
                None
            };

            // When this function is being executed, it may be queued for a long time, so that
            // deadline may exceed.
            tracker.on_scheduled();
            tracker.req_ctx.deadline.check()?;

            // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
            // exists.
            let snapshot = unsafe {
                with_tls_engine(|engine| Self::async_snapshot(engine, &tracker.req_ctx.context))
            }
            .await?;
            // When snapshot is retrieved, deadline may exceed.
            tracker.on_snapshot_finished();
            tracker.req_ctx.deadline.check()?;

            let mut handler = handler_builder(snapshot, &tracker.req_ctx)?;

            tracker.on_begin_all_items();

            loop {
                tracker.on_begin_item();

                let result = handler.handle_streaming_request();
                let mut causet_storage_stats = Statistics::default();
                handler.collect_scan_statistics(&mut causet_storage_stats);

                tracker.on_finish_item(Some(causet_storage_stats));
                let exec_details = tracker.get_item_exec_details();

                match result {
                    Err(e) => {
                        let mut resp = make_error_response(e);
                        resp.set_exec_details(exec_details);
                        yield resp;
                        break;
                    },
                    Ok((None, _)) => break,
                    Ok((Some(mut resp), finished)) => {
                        COPR_RESP_SIZE.inc_by(resp.data.len() as i64);
                        resp.set_exec_details(exec_details);
                        yield resp;
                        if finished {
                            break;
                        }
                    }
                }
            }
            tracker.on_finish_all_items();
        }
    }

    /// Handle a stream request and run on the read pool.
    ///
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(stream)` in other cases.
    /// The stream inside may produce errors however.
    fn handle_stream_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> Result<impl futures::stream::Stream<Item = Result<cop_timeshare::Response>>> {
        let (tx, rx) = mpsc::channel::<Result<cop_timeshare::Response>>(self.stream_channel_size);
        let priority = req_ctx.context.get_priority();
        let task_id = req_ctx.build_task_id();
        let tracker = Box::new(Tracker::new(req_ctx));

        self.read_pool
            .spawn(
                Self::handle_stream_request_impl(self.semaphore.clone(), tracker, handler_builder)
                    .then(futures::future::ok::<_, mpsc::lightlikeError>)
                    .forward(tx)
                    .unwrap_or_else(|e| {
                        warn!("interlock stream lightlike error"; "error" => %e);
                    }),
                priority,
                task_id,
            )
            .map_err(|_| Error::MaxPlightlikeingTasksExceeded)?;
        Ok(rx)
    }

    /// Parses and handles a stream request. Returns a stream that produce each result in a
    /// `Response` and will never fail. If there are errors during parsing or handling, they will
    /// be converted into a `Response` as the only stream item.
    #[inline]
    pub fn parse_and_handle_stream_request(
        &self,
        req: cop_timeshare::Request,
        peer: Option<String>,
    ) -> impl futures::stream::Stream<Item = cop_timeshare::Response> {
        let result_of_stream = self
            .parse_request_and_check_memory_locks(req, peer, true)
            .and_then(|(handler_builder, req_ctx)| {
                self.handle_stream_request(req_ctx, handler_builder)
            }); // Result<Stream<Resp, Error>, Error>

        futures::stream::once(futures::future::ready(result_of_stream)) // Stream<Stream<Resp, Error>, Error>
            .try_flatten() // Stream<Resp, Error>
            .or_else(|e| futures::future::ok(make_error_response(e))) // Stream<Resp, ()>
            .map(|item: std::result::Result<_, ()>| item.unwrap())
    }
}

fn make_error_response(e: Error) -> cop_timeshare::Response {
    warn!(
        "error-response";
        "err" => %e
    );
    let mut resp = cop_timeshare::Response::default();
    let tag;
    match e {
        Error::Brane(e) => {
            tag = causet_storage::get_tag_from_header(&e);
            resp.set_brane_error(e);
        }
        Error::Locked(info) => {
            tag = "meet_lock";
            resp.set_locked(info);
        }
        Error::DeadlineExceeded => {
            tag = "deadline_exceeded";
            resp.set_other_error(e.to_string());
        }
        Error::MaxPlightlikeingTasksExceeded => {
            tag = "max_plightlikeing_tasks_exceeded";
            let mut server_is_busy_err = error_timeshare::ServerIsBusy::default();
            server_is_busy_err.set_reason(e.to_string());
            let mut error_timeshare = error_timeshare::Error::default();
            error_timeshare.set_message(e.to_string());
            error_timeshare.set_server_is_busy(server_is_busy_err);
            resp.set_brane_error(error_timeshare);
        }
        Error::Other(_) => {
            tag = "other";
            resp.set_other_error(e.to_string());
        }
    };
    COPR_REQ_ERROR.with_label_values(&[tag]).inc();
    resp
}

#[causet(test)]
mod tests {
    use super::*;

    use std::sync::{atomic, mpsc};
    use std::thread;
    use std::vec;

    use futures::executor::{block_on, block_on_stream};

    use fidel_timeshare::FreeDaemon;
    use fidel_timeshare::Expr;

    use crate::config::CoprReadPoolConfig;
    use crate::interlock::readpool_impl::build_read_pool_for_test;
    use crate::read_pool::ReadPool;
    use crate::causet_storage::kv::LmdbEngine;
    use crate::causet_storage::TestEngineBuilder;
    use protobuf::Message;
    use txn_types::{Key, LockType};

    /// A unary `RequestHandler` that always produces a fixture.
    struct UnaryFixture {
        handle_duration_millis: u64,
        yieldable: bool,
        result: Option<Result<cop_timeshare::Response>>,
    }

    impl UnaryFixture {
        pub fn new(result: Result<cop_timeshare::Response>) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis: 0,
                yieldable: false,
                result: Some(result),
            }
        }

        pub fn new_with_duration(
            result: Result<cop_timeshare::Response>,
            handle_duration_millis: u64,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis,
                yieldable: false,
                result: Some(result),
            }
        }

        pub fn new_with_duration_yieldable(
            result: Result<cop_timeshare::Response>,
            handle_duration_millis: u64,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis,
                yieldable: true,
                result: Some(result),
            }
        }
    }

    #[async_trait]
    impl RequestHandler for UnaryFixture {
        async fn handle_request(&mut self) -> Result<cop_timeshare::Response> {
            if self.yieldable {
                // We split the task into small executions of 1 second.
                for _ in 0..self.handle_duration_millis / 1_000 {
                    thread::sleep(Duration::from_millis(1_000));
                    yatp::task::future::reschedule().await;
                }
                thread::sleep(Duration::from_millis(self.handle_duration_millis % 1_000));
            } else {
                thread::sleep(Duration::from_millis(self.handle_duration_millis));
            }

            self.result.take().unwrap()
        }
    }

    /// A streaming `RequestHandler` that always produces a fixture.
    struct StreamFixture {
        result_len: usize,
        result_iter: vec::IntoIter<Result<cop_timeshare::Response>>,
        handle_durations_millis: vec::IntoIter<u64>,
        nth: usize,
    }

    impl StreamFixture {
        pub fn new(result: Vec<Result<cop_timeshare::Response>>) -> StreamFixture {
            let len = result.len();
            StreamFixture {
                result_len: len,
                result_iter: result.into_iter(),
                handle_durations_millis: vec![0; len].into_iter(),
                nth: 0,
            }
        }

        pub fn new_with_duration(
            result: Vec<Result<cop_timeshare::Response>>,
            handle_durations_millis: Vec<u64>,
        ) -> StreamFixture {
            assert_eq!(result.len(), handle_durations_millis.len());
            StreamFixture {
                result_len: result.len(),
                result_iter: result.into_iter(),
                handle_durations_millis: handle_durations_millis.into_iter(),
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFixture {
        fn handle_streaming_request(&mut self) -> Result<(Option<cop_timeshare::Response>, bool)> {
            let is_finished = if self.result_len == 0 {
                true
            } else {
                self.nth >= (self.result_len - 1)
            };
            let ret = match self.result_iter.next() {
                None => {
                    assert!(is_finished);
                    Ok((None, is_finished))
                }
                Some(val) => {
                    let handle_duration_ms = self.handle_durations_millis.next().unwrap();
                    thread::sleep(Duration::from_millis(handle_duration_ms));
                    match val {
                        Ok(resp) => Ok((Some(resp), is_finished)),
                        Err(e) => Err(e),
                    }
                }
            };
            self.nth += 1;

            ret
        }
    }

    /// A streaming `RequestHandler` that produces values according a closure.
    struct StreamFromClosure {
        result_generator: Box<dyn Fn(usize) -> HandlerStreamStepResult + lightlike>,
        nth: usize,
    }

    impl StreamFromClosure {
        pub fn new<F>(result_generator: F) -> StreamFromClosure
        where
            F: Fn(usize) -> HandlerStreamStepResult + lightlike + 'static,
        {
            StreamFromClosure {
                result_generator: Box::new(result_generator),
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFromClosure {
        fn handle_streaming_request(&mut self) -> Result<(Option<cop_timeshare::Response>, bool)> {
            let result = (self.result_generator)(self.nth);
            self.nth += 1;
            result
        }
    }

    #[test]
    fn test_outdated_request() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        // a normal request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(cop_timeshare::Response::default())).into_boxed()));
        let resp =
            block_on(causet.handle_unary_request(ReqContext::default_for_test(), handler_builder))
                .unwrap();
        assert!(resp.get_other_error().is_empty());

        // an outdated request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(cop_timeshare::Response::default())).into_boxed()));
        let outdated_req_ctx = ReqContext::new(
            ReqTag::test,
            kvrpc_timeshare::Context::default(),
            &[],
            Duration::from_secs(0),
            None,
            None,
            TimeStamp::max(),
            None,
        );
        assert!(block_on(causet.handle_unary_request(outdated_req_ctx, handler_builder)).is_err());
    }

    #[test]
    fn test_stack_guard() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let mut causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);
        causet.recursion_limit = 100;

        let req = {
            let mut expr = Expr::default();
            // The recursion limit in Prost and rust-protobuf (by default) is 100 (for rust-protobuf,
            // that limit is set to 1000 as a configuration default).
            for _ in 0..101 {
                let mut e = Expr::default();
                e.mut_children().push(expr);
                expr = e;
            }
            let mut e = FreeDaemon::default();
            e.mut_selection().mut_conditions().push(expr);
            let mut posetdag = PosetDagRequest::default();
            posetdag.mut_executors().push(e);
            let mut req = cop_timeshare::Request::default();
            req.set_tp(REQ_TYPE_DAG);
            req.set_data(posetdag.write_to_bytes().unwrap());
            req
        };

        let resp: cop_timeshare::Response = block_on(causet.parse_and_handle_unary_request(req, None));
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_type() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        let mut req = cop_timeshare::Request::default();
        req.set_tp(9999);

        let resp: cop_timeshare::Response = block_on(causet.parse_and_handle_unary_request(req, None));
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_body() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        let mut req = cop_timeshare::Request::default();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(vec![1, 2, 3]);

        let resp = block_on(causet.parse_and_handle_unary_request(req, None));
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_full() {
        use crate::causet_storage::kv::{destroy_tls_engine, set_tls_engine};
        use std::sync::Mutex;
        use violetabftstore::interlock::::yatp_pool::{DefaultTicker, YatpPoolBuilder};

        let engine = TestEngineBuilder::new().build().unwrap();

        let read_pool = ReadPool::from(
            CoprReadPoolConfig {
                normal_concurrency: 1,
                max_tasks_per_worker_normal: 2,
                ..CoprReadPoolConfig::default_for_test()
            }
            .to_yatp_pool_configs()
            .into_iter()
            .map(|config| {
                let engine = Arc::new(Mutex::new(engine.clone()));
                YatpPoolBuilder::new(DefaultTicker::default())
                    .config(config)
                    .name_prefix("interlock_lightlikepoint_test_full")
                    .after_spacelike(move || set_tls_engine(engine.dagger().unwrap().clone()))
                    // Safety: we call `set_` and `destroy_` with the same engine type.
                    .before_stop(|| unsafe { destroy_tls_engine::<LmdbEngine>() })
                    .build_future_pool()
            })
            .collect::<Vec<_>>(),
        );

        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        let (tx, rx) = mpsc::channel();

        // first 2 requests are processed as normal and laters are returned as errors
        for i in 0..5 {
            let mut response = cop_timeshare::Response::default();
            response.set_data(vec![1, 2, i]);

            let mut context = kvrpc_timeshare::Context::default();
            context.set_priority(kvrpc_timeshare::CommandPri::Normal);

            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(Ok(response), 1000).into_boxed())
            });
            let future = causet.handle_unary_request(ReqContext::default_for_test(), handler_builder);
            let tx = tx.clone();
            thread::spawn(move || {
                tx.lightlike(block_on(future)).unwrap();
            });
            thread::sleep(Duration::from_millis(100));
        }

        // verify
        for _ in 2..5 {
            assert!(rx.recv().unwrap().is_err());
        }
        for i in 0..2 {
            let resp = rx.recv().unwrap().unwrap();
            assert_eq!(resp.get_data(), [1, 2, i]);
            assert!(!resp.has_brane_error());
        }
    }

    #[test]
    fn test_error_unary_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Err(box_err!("foo"))).into_boxed()));
        let resp =
            block_on(causet.handle_unary_request(ReqContext::default_for_test(), handler_builder))
                .unwrap();
        assert_eq!(resp.get_data().len(), 0);
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_error_streaming_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        // Fail immediately
        let handler_builder =
            Box::new(|_, _: &_| Ok(StreamFixture::new(vec![Err(box_err!("foo"))]).into_boxed()));
        let resp_vec = block_on_stream(
            causet.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data().len(), 0);
        assert!(!resp_vec[0].get_other_error().is_empty());

        // Fail after some success responses
        let mut responses = Vec::new();
        for i in 0..5 {
            let mut resp = cop_timeshare::Response::default();
            resp.set_data(vec![1, 2, i]);
            responses.push(Ok(resp));
        }
        responses.push(Err(box_err!("foo")));

        let handler_builder = Box::new(|_, _: &_| Ok(StreamFixture::new(responses).into_boxed()));
        let resp_vec = block_on_stream(
            causet.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 6);
        for i in 0..5 {
            assert_eq!(resp_vec[i].get_data(), [1, 2, i as u8]);
        }
        assert_eq!(resp_vec[5].get_data().len(), 0);
        assert!(!resp_vec[5].get_other_error().is_empty());
    }

    #[test]
    fn test_empty_streaming_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        let handler_builder = Box::new(|_, _: &_| Ok(StreamFixture::new(vec![]).into_boxed()));
        let resp_vec = block_on_stream(
            causet.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 0);
    }

    // TODO: Test panic?

    #[test]
    fn test_special_streaming_handlers() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&Config::default(), read_pool.handle(), cm);

        // handler returns `finished == true` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = cop_timeshare::Response::default();
                resp.set_data(vec![1, 2, 7]);
                Ok((Some(resp), true))
            }
            _ => {
                // we cannot use `unreachable!()` here because CpuPool catches panic.
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            causet.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 7]);
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);

        // handler returns `None` but `finished == false` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = cop_timeshare::Response::default();
                resp.set_data(vec![1, 2, 13]);
                Ok((Some(resp), false))
            }
            1 => Ok((None, false)),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            causet.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 13]);
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);

        // handler returns `Err(..)` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = cop_timeshare::Response::default();
                resp.set_data(vec![1, 2, 23]);
                Ok((Some(resp), false))
            }
            1 => Err(box_err!("foo")),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            causet.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 2);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 23]);
        assert!(!resp_vec[1].get_other_error().is_empty());
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn test_channel_size() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(
            &Config {
                lightlike_point_stream_channel_size: 3,
                ..Config::default()
            },
            read_pool.handle(),
            cm,
        );

        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| {
            // produce an infinite stream
            let mut resp = cop_timeshare::Response::default();
            resp.set_data(vec![1, 2, nth as u8]);
            counter_clone.fetch_add(1, atomic::Ordering::SeqCst);
            Ok((Some(resp), false))
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = block_on_stream(
            causet.handle_stream_request(ReqContext::default_for_test(), handler_builder)
                .unwrap(),
        )
        .take(7)
        .collect::<Result<Vec<_>>>()
        .unwrap();
        assert_eq!(resp_vec.len(), 7);
        assert!(counter.load(atomic::Ordering::SeqCst) < 14);
    }

    #[test]
    fn test_handle_time() {
        use violetabftstore::interlock::::config::ReadableDuration;

        /// Asserted that the snapshot can be retrieved in 500ms.
        const SNAPSHOT_DURATION_MS: i64 = 500;

        /// Asserted that the delay caused by OS scheduling other tasks is smaller than 200ms.
        /// This is mostly for CI.
        const HANDLE_ERROR_MS: i64 = 200;

        /// The accepBlock error cone for a coarse timer. Note that we use CLOCK_MONOTONIC_COARSE
        /// which can be slewed by time adjustment code (e.g., NTP, PTP).
        const COARSE_ERROR_MS: i64 = 50;

        /// The duration that payload executes.
        const PAYLOAD_SMALL: i64 = 3000;
        const PAYLOAD_LARGE: i64 = 6000;

        let engine = TestEngineBuilder::new().build().unwrap();

        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig {
                low_concurrency: 1,
                normal_concurrency: 1,
                high_concurrency: 1,
                ..CoprReadPoolConfig::default_for_test()
            },
            engine,
        ));

        let mut config = Config::default();
        config.lightlike_point_request_max_handle_duration =
            ReadableDuration::millis((PAYLOAD_SMALL + PAYLOAD_LARGE) as u64 * 2);

        let cm = ConcurrencyManager::new(1.into());
        let causet = node::<LmdbEngine>::new(&config, read_pool.handle(), cm);

        let (tx, rx) = std::sync::mpsc::channel();

        // A request that requests execution details.
        let mut req_with_exec_detail = ReqContext::default_for_test();
        req_with_exec_detail.context.set_handle_time(true);

        {
            let mut wait_time: i64 = 0;

            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(cop_timeshare::Response::default()),
                    PAYLOAD_SMALL as u64,
                )
                .into_boxed())
            });
            let resp_future_1 =
                causet.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let lightlikeer = tx.clone();
            thread::spawn(move || lightlikeer.lightlike(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Unary, error response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Err(box_err!("foo")), PAYLOAD_LARGE as u64)
                        .into_boxed(),
                )
            });
            let resp_future_2 =
                causet.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let lightlikeer = tx.clone();
            thread::spawn(move || lightlikeer.lightlike(vec![block_on(resp_future_2).unwrap()]).unwrap());
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_SMALL + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            wait_time += PAYLOAD_SMALL - SNAPSHOT_DURATION_MS;

            // Response 2
            let resp = &rx.recv().unwrap()[0];
            assert!(!resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }

        {
            // Test multi-stage tasks
            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration_yieldable(
                    Ok(cop_timeshare::Response::default()),
                    PAYLOAD_SMALL as u64,
                )
                .into_boxed())
            });
            let resp_future_1 =
                causet.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let lightlikeer = tx.clone();
            thread::spawn(move || lightlikeer.lightlike(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Unary, error response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration_yieldable(
                    Err(box_err!("foo")),
                    PAYLOAD_LARGE as u64,
                )
                .into_boxed())
            });
            let resp_future_2 =
                causet.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let lightlikeer = tx.clone();
            thread::spawn(move || lightlikeer.lightlike(vec![block_on(resp_future_2).unwrap()]).unwrap());
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_SMALL + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );

            // Response 2
            let resp = &rx.recv().unwrap()[0];
            assert!(!resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }

        {
            let mut wait_time: i64 = 0;

            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(cop_timeshare::Response::default()),
                    PAYLOAD_LARGE as u64,
                )
                .into_boxed())
            });
            let resp_future_1 =
                causet.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let lightlikeer = tx.clone();
            thread::spawn(move || lightlikeer.lightlike(vec![block_on(resp_future_1).unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Stream.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(StreamFixture::new_with_duration(
                    vec![
                        Ok(cop_timeshare::Response::default()),
                        Err(box_err!("foo")),
                        Ok(cop_timeshare::Response::default()),
                    ],
                    vec![
                        PAYLOAD_SMALL as u64,
                        PAYLOAD_LARGE as u64,
                        PAYLOAD_SMALL as u64,
                    ],
                )
                .into_boxed())
            });
            let resp_future_3 = causet
                .handle_stream_request(req_with_exec_detail, handler_builder)
                .unwrap();
            thread::spawn(move || {
                tx.lightlike(
                    block_on_stream(resp_future_3)
                        .collect::<Result<Vec<_>>>()
                        .unwrap(),
                )
                .unwrap()
            });

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            wait_time += PAYLOAD_LARGE - SNAPSHOT_DURATION_MS;

            // Response 2
            let resp = &rx.recv().unwrap();
            assert_eq!(resp.len(), 2);
            assert!(resp[0].get_other_error().is_empty());
            assert_ge!(
                resp[0]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[0]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_SMALL + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp[0].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[0].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );

            assert!(!resp[1].get_other_error().is_empty());
            assert_ge!(
                resp[1]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[1]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp[1].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[1].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }
    }

    #[test]
    fn test_check_memory_locks() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::from(build_read_pool_for_test(
            &CoprReadPoolConfig::default_for_test(),
            engine,
        ));
        let cm = ConcurrencyManager::new(1.into());
        let key = Key::from_raw(b"key");
        let guard = block_on(cm.lock_key(&key));
        guard.with_lock(|dagger| {
            *dagger = Some(txn_types::Dagger::new(
                LockType::Put,
                b"key".to_vec(),
                10.into(),
                100,
                Some(vec![]),
                0.into(),
                1,
                20.into(),
            ));
        });

        let config = Config {
            lightlike_point_check_memory_locks: true,
            ..Default::default()
        };
        let causet = node::<LmdbEngine>::new(&config, read_pool.handle(), cm);

        let mut req = cop_timeshare::Request::default();
        req.mut_context().set_isolation_level(IsolationLevel::Si);
        req.set_spacelike_ts(100);
        req.set_tp(REQ_TYPE_DAG);
        let mut key_cone = cop_timeshare::KeyCone::default();
        key_cone.set_spacelike(b"a".to_vec());
        key_cone.set_lightlike(b"z".to_vec());
        req.mut_cones().push(key_cone);
        let mut posetdag = PosetDagRequest::default();
        posetdag.mut_executors().push(FreeDaemon::default());
        req.set_data(posetdag.write_to_bytes().unwrap());

        let resp = block_on(causet.parse_and_handle_unary_request(req, None));
        assert_eq!(resp.get_daggered().get_key(), b"key");
    }
}
