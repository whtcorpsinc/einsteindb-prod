// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::Mutex;
use std::time::{Duration, Instant};

use ekvproto::fidel_timeshare::*;

use super::*;

pub const LEADER_INTERVAL_SEC: u64 = 2;

#[derive(Debug)]
struct Roulette {
    ts: Instant,
    idx: usize,
}

#[derive(Debug)]
struct Inner {
    resps: Vec<GetMembersResponse>,
    r: Roulette,
}

#[derive(Debug)]
pub struct LeaderChange {
    inner: Mutex<Inner>,
}

impl LeaderChange {
    pub fn new() -> LeaderChange {
        LeaderChange {
            inner: Mutex::new(Inner {
                resps: vec![],
                r: Roulette {
                    ts: Instant::now(),
                    idx: 0,
                },
            }),
        }
    }

    pub fn get_leader_interval() -> Duration {
        Duration::from_secs(LEADER_INTERVAL_SEC)
    }
}

const DEAD_ID: u64 = 1000;
const DEAD_NAME: &str = "walking_dead";
const DEAD_URL: &str = "127.0.0.1:65534";

impl FidelMocker for LeaderChange {
    fn get_members(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        let mut inner = self.inner.dagger().unwrap();
        let now = Instant::now();
        if now - inner.r.ts > LeaderChange::get_leader_interval() {
            inner.r.idx += 1;
            inner.r.ts = now;
            return Some(Err("not leader".to_owned()));
        }

        info!(
            "[LeaderChange] get_members: {:?}",
            inner.resps[inner.r.idx % inner.resps.len()]
        );
        Some(Ok(inner.resps[inner.r.idx % inner.resps.len()].clone()))
    }

    fn get_brane_by_id(&self, _: &GetBraneByIdRequest) -> Option<Result<GetBraneResponse>> {
        let mut inner = self.inner.dagger().unwrap();
        let now = Instant::now();
        if now.duration_since(inner.r.ts) > LeaderChange::get_leader_interval() {
            inner.r.idx += 1;
            inner.r.ts = now;
            debug!(
                "[LeaderChange] change leader to {:?}",
                inner.resps[inner.r.idx % inner.resps.len()].get_leader()
            );
        }

        Some(Err("not leader".to_owned()))
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

        // A dead FIDel
        let mut m = Member::default();
        m.set_member_id(DEAD_ID);
        m.set_name(DEAD_NAME.to_owned());
        m.set_client_urls(vec![DEAD_URL.to_owned()].into());
        m.set_peer_urls(vec![DEAD_URL.to_owned()].into());
        members.push(m);

        let mut header = ResponseHeader::default();
        header.set_cluster_id(1);

        let mut resps = Vec::with_capacity(eps.len());
        for (i, _) in (&eps).iter().enumerate() {
            let mut resp = GetMembersResponse::default();
            resp.set_header(header.clone());
            resp.set_members(members.clone().into());
            resp.set_leader(members[i].clone());
            resps.push(resp);
        }

        info!("[LeaerChange] set_lightlikepoints {:?}", resps);
        let mut inner = self.inner.dagger().unwrap();
        inner.resps = resps;
    }
}
