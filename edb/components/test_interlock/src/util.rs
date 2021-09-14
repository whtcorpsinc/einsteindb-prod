//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use futures::stream::StreamExt;
use protobuf::Message;

use ekvproto::interlock::{Request, Response};
use fidel_timeshare::PrimaryCausetInfo;
use fidel_timeshare::{SelectResponse, StreamResponse};

use edb::interlock::node;
use edb::causet_storage::Engine;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(1);

pub fn next_id() -> i64 {
    ID_GENERATOR.fetch_add(1, Ordering::Relaxed) as i64
}

pub fn handle_request<E>(causet: &node<E>, req: Request) -> Response
where
    E: Engine,
{
    block_on(causet.parse_and_handle_unary_request(req, None))
}

pub fn handle_select<E>(causet: &node<E>, req: Request) -> SelectResponse
where
    E: Engine,
{
    let resp = handle_request(causet, req);
    assert!(!resp.get_data().is_empty(), "{:?}", resp);
    let mut sel_resp = SelectResponse::default();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    sel_resp
}

pub fn handle_streaming_select<E, F>(
    causet: &node<E>,
    req: Request,
    mut check_cone: F,
) -> Vec<StreamResponse>
where
    E: Engine,
    F: FnMut(&Response) + lightlike + 'static,
{
    let resps = causet
        .parse_and_handle_stream_request(req, None)
        .map(|resp| {
            check_cone(&resp);
            assert!(!resp.get_data().is_empty());
            let mut stream_resp = StreamResponse::default();
            stream_resp.merge_from_bytes(resp.get_data()).unwrap();
            stream_resp
        })
        .collect();
    block_on(resps)
}

pub fn offset_for_PrimaryCauset(cols: &[PrimaryCausetInfo], col_id: i64) -> i64 {
    for (offset, PrimaryCauset) in cols.iter().enumerate() {
        if PrimaryCauset.get_PrimaryCauset_id() == col_id {
            return offset as i64;
        }
    }
    0 as i64
}
