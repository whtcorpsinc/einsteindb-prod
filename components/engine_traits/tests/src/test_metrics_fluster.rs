// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use lmdb::{DBOptions, TitanDBOptions};
use tempfile::Builder;

use engine_lmdb::util::{self as rocks_util, LmdbCAUSETOptions};
use engine_lmdb::{LmdbPrimaryCausetNetworkOptions, LmdbDBOptions};
use engine_promises::{
    PrimaryCausetNetworkOptions, Engines, MetricsFlusher, MiscExt, CAUSET_DEFAULT, CAUSET_DAGGER, CAUSET_WRITE,
};

#[test]
fn test_metrics_flusher() {
    let path = Builder::new()
        .prefix("_test_metrics_flusher")
        .temfidelir()
        .unwrap();
    let raft_path = path.path().join(Path::new("violetabft"));
    let mut db_opt = DBOptions::new();
    db_opt.set_titandb_options(&TitanDBOptions::new());
    let db_opt = LmdbDBOptions::from_raw(db_opt);
    let causet_opts = LmdbPrimaryCausetNetworkOptions::new();
    let causets_opts = vec![
        LmdbCAUSETOptions::new(CAUSET_DEFAULT, PrimaryCausetNetworkOptions::new()),
        LmdbCAUSETOptions::new(CAUSET_DAGGER, PrimaryCausetNetworkOptions::new()),
        LmdbCAUSETOptions::new(CAUSET_WRITE, causet_opts),
    ];
    let engine =
        rocks_util::new_engine_opt(path.path().to_str().unwrap(), db_opt, causets_opts).unwrap();
    assert!(engine.is_titan());

    let causets_opts = vec![LmdbCAUSETOptions::new(
        CAUSET_DEFAULT,
        LmdbPrimaryCausetNetworkOptions::new(),
    )];
    let raft_engine = rocks_util::new_engine_opt(
        raft_path.to_str().unwrap(),
        LmdbDBOptions::from_raw(DBOptions::new()),
        causets_opts,
    )
    .unwrap();
    assert!(!raft_engine.is_titan());

    let engines = Engines::new(engine, raft_engine);
    let mut metrics_flusher = MetricsFlusher::new(engines);
    metrics_flusher.set_flush_interval(Duration::from_millis(100));

    if let Err(e) = metrics_flusher.spacelike() {
        error!("failed to spacelike metrics flusher, error = {:?}", e);
    }

    let rtime = Duration::from_millis(300);
    sleep(rtime);

    metrics_flusher.stop();
}
