// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use lmdb::{DBOptions, NoetherDBOptions};
use tempfile::Builder;

use engine_lmdb::util::{self as rocks_util, LmdbCausetOptions};
use engine_lmdb::{LmdbPrimaryCausetNetworkOptions, LmdbDBOptions};
use edb::{
    PrimaryCausetNetworkOptions, Engines, MetricsFlusher, MiscExt, Causet_DEFAULT, Causet_DAGGER, Causet_WRITE,
};

#[test]
fn test_metrics_flusher() {
    let path = Builder::new()
        .prefix("_test_metrics_flusher")
        .temfidelir()
        .unwrap();
    let violetabft_path = path.path().join(Path::new("violetabft"));
    let mut db_opt = DBOptions::new();
    db_opt.tenancy_launched_for_einsteindb(&NoetherDBOptions::new());
    let db_opt = LmdbDBOptions::from_raw(db_opt);
    let causet_opts = LmdbPrimaryCausetNetworkOptions::new();
    let causets_opts = vec![
        LmdbCausetOptions::new(Causet_DEFAULT, PrimaryCausetNetworkOptions::new()),
        LmdbCausetOptions::new(Causet_DAGGER, PrimaryCausetNetworkOptions::new()),
        LmdbCausetOptions::new(Causet_WRITE, causet_opts),
    ];
    let engine =
        rocks_util::new_engine_opt(path.path().to_str().unwrap(), db_opt, causets_opts).unwrap();
    assert!(engine.is_titan());

    let causets_opts = vec![LmdbCausetOptions::new(
        Causet_DEFAULT,
        LmdbPrimaryCausetNetworkOptions::new(),
    )];
    let violetabft_engine = rocks_util::new_engine_opt(
        violetabft_path.to_str().unwrap(),
        LmdbDBOptions::from_raw(DBOptions::new()),
        causets_opts,
    )
    .unwrap();
    assert!(!violetabft_engine.is_titan());

    let engines = Engines::new(engine, violetabft_engine);
    let mut metrics_flusher = MetricsFlusher::new(engines);
    metrics_flusher.set_flush_interval(Duration::from_millis(100));

    if let Err(e) = metrics_flusher.spacelike() {
        error!("failed to spacelike metrics flusher, error = {:?}", e);
    }

    let rtime = Duration::from_millis(300);
    sleep(rtime);

    metrics_flusher.stop();
}
