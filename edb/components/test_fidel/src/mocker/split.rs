// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Mutex;

use ekvproto::fidel_timeshare::{GetMembersRequest, GetMembersResponse, Member, ResponseHeader};

use super::*;

#[derive(Debug)]
struct Inner {
    resps: Vec<GetMembersResponse>,
    idx: usize,
}

#[derive(Debug)]
pub struct Split {
    inner: Mutex<Option<Inner>>,
}

impl Split {
    pub fn new() -> Split {
        Split {
            inner: Mutex::new(None),
        }
    }
}

impl FidelMocker for Split {
    fn get_members(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        let mut holder = self.inner.dagger().unwrap();
        let inner = holder.as_mut().unwrap();
        inner.idx += 1;
        info!(
            "[Split] get_member: {:?}",
            inner.resps[inner.idx % inner.resps.len()]
        );
        Some(Ok(inner.resps[inner.idx % inner.resps.len()].clone()))
    }

    fn set_lightlikepoints(&self, eps: Vec<String>) {
        let mut members = Vec::with_capacity(eps.len());
        for (i, ep) in (&eps).iter().enumerate() {
            let mut m = Member::default();
            m.set_name(format!("fidel{}", i));
            m.set_member_id(100 + i as u64);
            m.set_client_urls(vec![ep.to_owned()].into());
            m.set_peer_urls(vec![ep.to_owned()].into());
            members.push(m);
        }

        let mut resps = Vec::with_capacity(eps.len());
        for i in 0..eps.len() {
            let mut resp = GetMembersResponse::default();
            let mut header = ResponseHeader::default();
            header.set_cluster_id(i as u64 + 1); // spacelike from 1.
            resp.set_header(header.clone());
            resp.set_members(members.clone().into());
            resp.set_leader(members[0].clone());
            resps.push(resp);
        }

        info!("[Split] resps {:?}", resps);

        let mut inner = self.inner.dagger().unwrap();
        *inner = Some(Inner { resps, idx: 0 })
    }
}
