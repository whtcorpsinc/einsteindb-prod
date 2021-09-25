// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::f64::INFINITY;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use engine_lmdb::raw::{IngestExternalFileOptions, WriBlock};
use engine_lmdb::util::get_causet_handle;
use engine_lmdb::util::new_temp_engine;
use engine_lmdb::LmdbEngine;
use engine_lmdb::{Compat, LmdbSnapshot, LmdbSstWriterBuilder};
use edb::{
    CompactExt, Engines, CausetEngine, MiscExt, SstWriter, SstWriterBuilder, ALL_CausetS, Causet_DEFAULT,
    Causet_WRITE,
};
use tuplespaceInstanton::data_key;
use ekvproto::meta_timeshare::{Peer, Brane};
use violetabftstore::store::BraneSnapshot;
use violetabftstore::store::{apply_sst_causet_file, build_sst_causet_file};
use tempfile::Builder;
use test_violetabftstore::*;
use edb::config::EINSTEINDBConfig;
use edb::causet_storage::tail_pointer::ScannerBuilder;
use edb::causet_storage::txn::Scanner;
use violetabftstore::interlock::::config::{ReadableDuration, ReadableSize};
use violetabftstore::interlock::::time::Limiter;
use txn_types::{Key, Write, WriteType};

#[test]
fn test_turnoff_titan() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.lmdb.defaultcauset.disable_auto_compactions = true;
    cluster.causet.lmdb.defaultcauset.num_levels = 1;
    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    cluster.run();
    assert_eq!(cluster.must_get(b"k1"), None);

    let size = 5;
    for i in 0..size {
        assert!(cluster
            .put(
                format!("k{:02}0", i).as_bytes(),
                format!("v{}", i).as_bytes(),
            )
            .is_ok());
    }
    cluster.must_flush_causet(Causet_DEFAULT, true);
    for i in 0..size {
        assert!(cluster
            .put(
                format!("k{:02}1", i).as_bytes(),
                format!("v{}", i).as_bytes(),
            )
            .is_ok());
    }
    cluster.must_flush_causet(Causet_DEFAULT, true);
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        assert_eq!(
            db.get_property_int(&"lmdb.num-files-at-level0").unwrap(),
            2
        );
        assert_eq!(
            db.get_property_int(&"lmdb.num-files-at-level1").unwrap(),
            0
        );
        assert_eq!(
            db.get_property_int(&"lmdb.titandb.num-live-blob-file")
                .unwrap(),
            2
        );
        assert_eq!(
            db.get_property_int(&"lmdb.titandb.num-obsolete-blob-file")
                .unwrap(),
            0
        );
    }
    cluster.shutdown();

    // try reopen db when titan isn't properly turned off.
    configure_for_disable_titan(&mut cluster);
    assert!(cluster.pre_spacelike_check().is_err());

    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    assert!(cluster.pre_spacelike_check().is_ok());
    cluster.spacelike().unwrap();
    assert_eq!(cluster.must_get(b"k1"), None);
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        let handle = get_causet_handle(&db, Causet_DEFAULT).unwrap();
        let mut opt = Vec::new();
        opt.push(("blob_run_mode", "kFallback"));
        assert!(db.set_options_causet(handle, &opt).is_ok());
    }
    cluster.compact_data();
    let mut all_check_pass = true;
    for _ in 0..10 {
        // wait for gc completes.
        sleep_ms(10);
        all_check_pass = true;
        for i in cluster.get_node_ids().into_iter() {
            let db = cluster.get_engine(i);
            if db.get_property_int(&"lmdb.num-files-at-level0").unwrap() != 0 {
                all_check_pass = false;
                break;
            }
            if db.get_property_int(&"lmdb.num-files-at-level1").unwrap() != 1 {
                all_check_pass = false;
                break;
            }
            if db
                .get_property_int(&"lmdb.titandb.num-live-blob-file")
                .unwrap()
                != 0
            {
                all_check_pass = false;
                break;
            }
        }
        if all_check_pass {
            break;
        }
    }
    if !all_check_pass {
        panic!("unexpected titan gc results");
    }
    cluster.shutdown();

    configure_for_disable_titan(&mut cluster);
    // wait till files are purged, timeout set to purge_obsolete_files_period.
    for _ in 1..100 {
        sleep_ms(10);
        if cluster.pre_spacelike_check().is_ok() {
            return;
        }
    }
    assert!(cluster.pre_spacelike_check().is_ok());
}

#[test]
fn test_delete_files_in_cone_for_titan() {
    let path = Builder::new()
        .prefix("test-titan-delete-files-in-cone")
        .temfidelir()
        .unwrap();

    // Set configs and create engines
    let mut causet = EINSTEINDBConfig::default();
    let cache = causet.causet_storage.block_cache.build_shared_cache();
    causet.lmdb.titan.enabled = true;
    causet.lmdb.titan.disable_gc = true;
    causet.lmdb.titan.purge_obsolete_files_period = ReadableDuration::secs(1);
    causet.lmdb.defaultcauset.disable_auto_compactions = true;
    // Disable dynamic_level_bytes, otherwise SST files would be ingested to L0.
    causet.lmdb.defaultcauset.dynamic_level_bytes = false;
    causet.lmdb.defaultcauset.titan.min_gc_batch_size = ReadableSize(0);
    causet.lmdb.defaultcauset.titan.discardable_ratio = 0.4;
    causet.lmdb.defaultcauset.titan.sample_ratio = 1.0;
    causet.lmdb.defaultcauset.titan.min_blob_size = ReadableSize(0);
    let kv_db_opts = causet.lmdb.build_opt();
    let kv_causets_opts = causet.lmdb.build_causet_opts(&cache);

    let violetabft_path = path.path().join(Path::new("titan"));
    let engines = Engines::new(
        LmdbEngine::from_db(Arc::new(
            engine_lmdb::raw_util::new_engine(
                path.path().to_str().unwrap(),
                Some(kv_db_opts),
                ALL_CausetS,
                Some(kv_causets_opts),
            )
            .unwrap(),
        )),
        LmdbEngine::from_db(Arc::new(
            engine_lmdb::raw_util::new_engine(
                violetabft_path.to_str().unwrap(),
                None,
                &[Causet_DEFAULT],
                None,
            )
            .unwrap(),
        )),
    );

    // Write some tail_pointer tuplespaceInstanton and values into db
    // default_causet : a_7, b_7
    // write_causet : a_8, b_8
    let spacelike_ts = 7.into();
    let commit_ts = 8.into();
    let write = Write::new(WriteType::Put, spacelike_ts, None);
    let db = &engines.kv.as_inner();
    let default_causet = db.causet_handle(Causet_DEFAULT).unwrap();
    let write_causet = db.causet_handle(Causet_WRITE).unwrap();
    db.put_causet(
        &default_causet,
        &data_key(Key::from_raw(b"a").applightlike_ts(spacelike_ts).as_encoded()),
        b"a_value",
    )
    .unwrap();
    db.put_causet(
        &write_causet,
        &data_key(Key::from_raw(b"a").applightlike_ts(commit_ts).as_encoded()),
        &write.as_ref().to_bytes(),
    )
    .unwrap();
    db.put_causet(
        &default_causet,
        &data_key(Key::from_raw(b"b").applightlike_ts(spacelike_ts).as_encoded()),
        b"b_value",
    )
    .unwrap();
    db.put_causet(
        &write_causet,
        &data_key(Key::from_raw(b"b").applightlike_ts(commit_ts).as_encoded()),
        &write.as_ref().to_bytes(),
    )
    .unwrap();

    // Flush and compact the kvs into L6.
    db.flush(true).unwrap();
    db.c().compact_files_in_cone(None, None, None).unwrap();
    let value = db.get_property_int(&"lmdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int(&"lmdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Delete one tail_pointer kvs we have written above.
    // Here we make the kvs on the L5 by ingesting SST.
    let sst_file_path = Path::new(db.path()).join("for_ingest.sst");
    let mut writer = LmdbSstWriterBuilder::new()
        .build(&sst_file_path.to_str().unwrap())
        .unwrap();
    writer
        .delete(&data_key(
            Key::from_raw(b"a").applightlike_ts(spacelike_ts).as_encoded(),
        ))
        .unwrap();
    writer.finish().unwrap();
    let mut opts = IngestExternalFileOptions::new();
    opts.move_files(true);
    db.ingest_external_file_causet(&default_causet, &opts, &[sst_file_path.to_str().unwrap()])
        .unwrap();

    // Now the LSM structure of default causet is:
    // L5: [delete(a_7)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the cones of two SST files are overlapped.
    //
    // There is one blob file in Noether
    // blob1: (a_7, a_value), (b_7, b_value)
    let value = db.get_property_int(&"lmdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int(&"lmdb.num-files-at-level5").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int(&"lmdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Used to trigger titan gc
    let db = &engines.kv.as_inner();
    db.put(b"1", b"1").unwrap();
    db.flush(true).unwrap();
    db.put(b"2", b"2").unwrap();
    db.flush(true).unwrap();
    db.c()
        .compact_files_in_cone(Some(b"0"), Some(b"3"), Some(1))
        .unwrap();

    // Now the LSM structure of default causet is:
    // memBlock: [put(b_7, blob4)] (because of Noether GC)
    // L0: [put(1, blob2), put(2, blob3)]
    // L5: [delete(a_7)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the cones of two SST files are overlapped.
    //
    // There is four blob files in Noether
    // blob1: (a_7, a_value), (b_7, b_value)
    // blob2: (1, 1)
    // blob3: (2, 2)
    // blob4: (b_7, b_value)
    let value = db.get_property_int(&"lmdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int(&"lmdb.num-files-at-level1").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int(&"lmdb.num-files-at-level5").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int(&"lmdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Wait Noether to purge obsolete files
    thread::sleep(Duration::from_secs(2));
    // Now the LSM structure of default causet is:
    // memBlock: [put(b_7, blob4)] (because of Noether GC)
    // L0: [put(1, blob2), put(2, blob3)]
    // L5: [delete(a_7)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the cones of two SST files are overlapped.
    //
    // There is three blob files in Noether
    // blob2: (1, 1)
    // blob3: (2, 2)
    // blob4: (b_7, b_value)

    // `delete_files_in_cone` may expose some old tuplespaceInstanton.
    // For Noether it may encounter `missing blob file` in `delete_all_in_cone`,
    // so we set key_only for Noether.
    engines
        .kv
        .delete_all_files_in_cone(
            &data_key(Key::from_raw(b"a").as_encoded()),
            &data_key(Key::from_raw(b"b").as_encoded()),
        )
        .unwrap();
    engines
        .kv
        .delete_all_in_cone(
            &data_key(Key::from_raw(b"a").as_encoded()),
            &data_key(Key::from_raw(b"b").as_encoded()),
            false,
        )
        .unwrap();

    // Now the LSM structure of default causet is:
    // memBlock: [put(b_7, blob4)] (because of Noether GC)
    // L0: [put(1, blob2), put(2, blob3)]
    // L6: [put(a_7, blob1), put(b_7, blob1)]
    // the cones of two SST files are overlapped.
    //
    // There is three blob files in Noether
    // blob2: (1, 1)
    // blob3: (2, 2)
    // blob4: (b_7, b_value)
    let value = db.get_property_int(&"lmdb.num-files-at-level0").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int(&"lmdb.num-files-at-level1").unwrap();
    assert_eq!(value, 1);
    let value = db.get_property_int(&"lmdb.num-files-at-level5").unwrap();
    assert_eq!(value, 0);
    let value = db.get_property_int(&"lmdb.num-files-at-level6").unwrap();
    assert_eq!(value, 1);

    // Generate a snapshot
    let default_sst_file_path = path.path().join("default.sst");
    let write_sst_file_path = path.path().join("write.sst");
    let limiter = Limiter::new(INFINITY);
    build_sst_causet_file::<LmdbEngine>(
        &default_sst_file_path.to_str().unwrap(),
        &engines.kv,
        &engines.kv.snapshot(),
        Causet_DEFAULT,
        b"",
        b"{",
        &limiter,
    )
    .unwrap();
    build_sst_causet_file::<LmdbEngine>(
        &write_sst_file_path.to_str().unwrap(),
        &engines.kv,
        &engines.kv.snapshot(),
        Causet_WRITE,
        b"",
        b"{",
        &limiter,
    )
    .unwrap();

    // Apply the snapshot to other DB.
    let dir1 = Builder::new()
        .prefix("test-snap-causet-db-apply")
        .temfidelir()
        .unwrap();
    let engines1 = new_temp_engine(&dir1);
    apply_sst_causet_file(
        &default_sst_file_path.to_str().unwrap(),
        &engines1.kv,
        Causet_DEFAULT,
    )
    .unwrap();
    apply_sst_causet_file(
        &write_sst_file_path.to_str().unwrap(),
        &engines1.kv,
        Causet_WRITE,
    )
    .unwrap();

    // Do scan on other DB.
    let mut r = Brane::default();
    r.mut_peers().push(Peer::default());
    r.set_spacelike_key(b"a".to_vec());
    r.set_lightlike_key(b"z".to_vec());
    let snapshot = BraneSnapshot::<LmdbSnapshot>::from_raw(engines1.kv.clone(), r);
    let mut scanner = ScannerBuilder::new(snapshot, 10.into(), false)
        .cone(Some(Key::from_raw(b"a")), None)
        .build()
        .unwrap();
    assert_eq!(
        scanner.next().unwrap(),
        Some((Key::from_raw(b"b"), b"b_value".to_vec())),
    );
}
