// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

mod causet_storage_impl;

pub use self::causet_storage_impl::EinsteinDBStorage;

use async_trait::async_trait;
use ekvproto::interlock::{KeyCone, Response};
use protobuf::Message;
use milevadb_query_common::causet_storage::IntervalCone;
use fidel_timeshare::{PosetDagRequest, SelectResponse, StreamResponse};

use crate::interlock::metrics::*;
use crate::interlock::{Deadline, RequestHandler, Result};
use crate::causet_storage::{Statistics, CausetStore};

pub struct PosetDagHandlerBuilder<S: CausetStore + 'static> {
    req: PosetDagRequest,
    cones: Vec<KeyCone>,
    store: S,
    data_version: Option<u64>,
    deadline: Deadline,
    batch_row_limit: usize,
    is_streaming: bool,
    is_cache_enabled: bool,
}

impl<S: CausetStore + 'static> PosetDagHandlerBuilder<S> {
    pub fn new(
        req: PosetDagRequest,
        cones: Vec<KeyCone>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        is_cache_enabled: bool,
    ) -> Self {
        PosetDagHandlerBuilder {
            req,
            cones,
            store,
            data_version: None,
            deadline,
            batch_row_limit,
            is_streaming,
            is_cache_enabled,
        }
    }

    pub fn data_version(mut self, data_version: Option<u64>) -> Self {
        self.data_version = data_version;
        self
    }

    pub fn build(self) -> Result<Box<dyn RequestHandler>> {
        COPR_DAG_REQ_COUNT.with_label_values(&["batch"]).inc();
        Ok(BatchDAGHandler::new(
            self.req,
            self.cones,
            self.store,
            self.data_version,
            self.deadline,
            self.is_cache_enabled,
            self.batch_row_limit,
            self.is_streaming,
        )?
        .into_boxed())
    }
}

pub struct DAGHandler {
    runner: milevadb_query_normal_executors::FreeDaemonsRunner<Statistics>,
    data_version: Option<u64>,
}

impl DAGHandler {
    pub fn new<S: CausetStore + 'static>(
        req: PosetDagRequest,
        cones: Vec<KeyCone>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        is_cache_enabled: bool,
    ) -> Result<Self> {
        Ok(Self {
            runner: milevadb_query_normal_executors::FreeDaemonsRunner::from_request(
                req,
                cones,
                EinsteinDBStorage::new(store, is_cache_enabled),
                deadline,
                batch_row_limit,
                is_streaming,
            )?,
            data_version,
        })
    }
}

#[async_trait]
impl RequestHandler for DAGHandler {
    #[minitrace::trace_async(fidel_timeshare::Event::EINSTEINDBCoprExecutePosetDagRunner as u32)]
    async fn handle_request(&mut self) -> Result<Response> {
        let result = self.runner.handle_request();
        handle_qe_response(result, self.runner.can_be_cached(), self.data_version)
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        handle_qe_stream_response(self.runner.handle_streaming_request())
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_causet_storage_stats(dest);
    }
}

pub struct BatchDAGHandler {
    runner: milevadb_query_vec_executors::runner::BatchFreeDaemonsRunner<Statistics>,
    data_version: Option<u64>,
}

impl BatchDAGHandler {
    pub fn new<S: CausetStore + 'static>(
        req: PosetDagRequest,
        cones: Vec<KeyCone>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        is_cache_enabled: bool,
        streaming_batch_limit: usize,
        is_streaming: bool,
    ) -> Result<Self> {
        Ok(Self {
            runner: milevadb_query_vec_executors::runner::BatchFreeDaemonsRunner::from_request(
                req,
                cones,
                EinsteinDBStorage::new(store, is_cache_enabled),
                deadline,
                streaming_batch_limit,
                is_streaming,
            )?,
            data_version,
        })
    }
}

#[async_trait]
impl RequestHandler for BatchDAGHandler {
    #[minitrace::trace_async(fidel_timeshare::Event::EINSTEINDBCoprExecuteBatchPosetDagRunner as u32)]
    async fn handle_request(&mut self) -> Result<Response> {
        let result = self.runner.handle_request().await;
        handle_qe_response(result, self.runner.can_be_cached(), self.data_version)
    }

    fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        handle_qe_stream_response(self.runner.handle_streaming_request())
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_causet_storage_stats(dest);
    }
}

fn handle_qe_response(
    result: milevadb_query_common::Result<SelectResponse>,
    can_be_cached: bool,
    data_version: Option<u64>,
) -> Result<Response> {
    use milevadb_query_common::error::ErrorInner;

    match result {
        Ok(sel_resp) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(sel_resp.write_to_bytes()));
            resp.set_can_be_cached(can_be_cached);
            resp.set_is_cache_hit(false);
            if let Some(v) = data_version {
                resp.set_cache_last_version(v);
            }
            Ok(resp)
        }
        Err(err) => match *err.0 {
            ErrorInner::causet_storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut sel_resp = SelectResponse::default();
                sel_resp.mut_error().set_code(err.code());
                sel_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(sel_resp.write_to_bytes()));
                resp.set_can_be_cached(can_be_cached);
                resp.set_is_cache_hit(false);
                Ok(resp)
            }
        },
    }
}

fn handle_qe_stream_response(
    result: milevadb_query_common::Result<(Option<(StreamResponse, IntervalCone)>, bool)>,
) -> Result<(Option<Response>, bool)> {
    use milevadb_query_common::error::ErrorInner;

    match result {
        Ok((Some((s_resp, cone)), finished)) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(s_resp.write_to_bytes()));
            resp.mut_cone().set_spacelike(cone.lower_inclusive);
            resp.mut_cone().set_lightlike(cone.upper_exclusive);
            Ok((Some(resp), finished))
        }
        Ok((None, finished)) => Ok((None, finished)),
        Err(err) => match *err.0 {
            ErrorInner::causet_storage(err) => Err(err.into()),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut s_resp = StreamResponse::default();
                s_resp.mut_error().set_code(err.code());
                s_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(s_resp.write_to_bytes()));
                Ok((Some(resp), true))
            }
        },
    }
}
