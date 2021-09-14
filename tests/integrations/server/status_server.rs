// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use hyper::{body, Client, StatusCode, Uri};
use security::SecurityConfig;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use test_violetabftstore::{new_server_cluster, Simulator};
use edb::config::ConfigController;
use edb::server::status_server::{brane_meta::BraneMeta, StatusServer};
use violetabftstore::interlock::::HandyRwLock;

async fn check(authority: SocketAddr, brane_id: u64) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let uri = Uri::builder()
        .scheme("http")
        .authority(authority.to_string().as_str())
        .path_and_query(format!("/brane/{}", brane_id).as_str())
        .build()?;
    let resp = client.get(uri).await?;
    let (parts, raw_body) = resp.into_parts();
    let body = body::to_bytes(raw_body).await?;
    assert_eq!(
        StatusCode::OK,
        parts.status,
        "{}",
        String::from_utf8(body.to_vec())?
    );
    assert_eq!("application/json", parts.headers["content-type"].to_str()?);
    serde_json::from_slice::<BraneMeta>(body.as_ref())?;
    Ok(())
}

#[test]
fn test_brane_meta_lightlikepoint() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let brane = cluster.get_brane(b"");
    let brane_id = brane.get_id();
    let peer = brane.get_peers().get(0);
    assert!(peer.is_some());
    let store_id = peer.unwrap().get_store_id();
    let router = cluster.sim.rl().get_router(store_id);
    assert!(router.is_some());
    let mut status_server = StatusServer::new(
        1,
        None,
        ConfigController::default(),
        Arc::new(SecurityConfig::default()),
        router.unwrap(),
    )
    .unwrap();
    let addr = "127.0.0.1:0".to_owned();
    assert!(status_server.spacelike(addr.clone(), addr).is_ok());
    let check_task = check(status_server.listening_addr(), brane_id);
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    if let Err(err) = rt.block_on(check_task) {
        panic!("{}", err);
    }
    status_server.stop();
}
