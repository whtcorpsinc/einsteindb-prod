// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use fidel_client::{Config, RpcClient};
use security::{SecurityConfig, SecurityManager};
use einsteindb_util::config::ReadableDuration;

use std::sync::Arc;

pub fn new_config(eps: Vec<(String, u16)>) -> Config {
    let mut causetg = Config::default();
    causetg.lightlikepoints = eps
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();
    causetg
}

pub fn new_client(eps: Vec<(String, u16)>, mgr: Option<Arc<SecurityManager>>) -> RpcClient {
    let causetg = new_config(eps);
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&causetg, mgr).unwrap()
}

pub fn new_client_with_ufidelate_interval(
    eps: Vec<(String, u16)>,
    mgr: Option<Arc<SecurityManager>>,
    interval: ReadableDuration,
) -> RpcClient {
    let mut causetg = new_config(eps);
    causetg.ufidelate_interval = interval;
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&causetg, mgr).unwrap()
}
