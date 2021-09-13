//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use engine_lmdb::raw::{CompactOptions, WriBlock, DB};
use edb::{Causet_DEFAULT, Causet_DAGGER};
use test_violetabftstore::*;

fn init_db_with_sst_files(db: &DB, level: i32, n: u8) {
    let mut opts = CompactOptions::new();
    opts.set_change_level(true);
    opts.set_target_level(level);
    for causet_name in &[Causet_DEFAULT, Causet_DAGGER] {
        let handle = db.causet_handle(causet_name).unwrap();
        // Each SST file has only one kv.
        for i in 0..n {
            let k = tuplespaceInstanton::data_key(&[i]);
            db.put_causet(handle, &k, &k).unwrap();
            db.flush_causet(handle, true).unwrap();
            db.compact_cone_causet_opt(handle, &opts, None, None);
        }
    }
}

fn check_db_files_at_level(db: &DB, level: i32, num_files: u64) {
    for causet_name in &[Causet_DEFAULT, Causet_DAGGER] {
        let handle = db.causet_handle(causet_name).unwrap();
        let name = format!("lmdb.num-files-at-level{}", level);
        let value = db.get_property_int_causet(handle, &name).unwrap();
        if value != num_files {
            panic!(
                "causet {} level {} should have {} files, got {}",
                causet_name, level, num_files, value
            );
        }
    }
}

fn check_kv_in_all_causets(db: &DB, i: u8, found: bool) {
    for causet_name in &[Causet_DEFAULT, Causet_DAGGER] {
        let handle = db.causet_handle(causet_name).unwrap();
        let k = tuplespaceInstanton::data_key(&[i]);
        let v = db.get_causet(handle, &k).unwrap();
        if found {
            assert_eq!(v.unwrap(), &k);
        } else {
            assert!(v.is_none());
        }
    }
}

fn test_clear_stale_data<T: Simulator>(cluster: &mut Cluster<T>) {
    // Disable compaction at level 0.
    cluster
        .causet
        .lmdb
        .defaultcauset
        .level0_file_num_compaction_trigger = 100;
    cluster
        .causet
        .lmdb
        .writecauset
        .level0_file_num_compaction_trigger = 100;
    cluster
        .causet
        .lmdb
        .lockcauset
        .level0_file_num_compaction_trigger = 100;
    cluster
        .causet
        .lmdb
        .violetabftcauset
        .level0_file_num_compaction_trigger = 100;

    cluster.run();

    let n = 6;
    // Choose one node.
    let node_id = *cluster.get_node_ids().iter().next().unwrap();
    let db = cluster.get_engine(node_id);

    // Split into `n` branes.
    for i in 0..n {
        let brane = cluster.get_brane(&[i]);
        cluster.must_split(&brane, &[i + 1]);
    }

    // Generate `n` files in db at level 6.
    let level = 6;
    init_db_with_sst_files(&db, level, n);
    check_db_files_at_level(&db, level, u64::from(n));
    for i in 0..n {
        check_kv_in_all_causets(&db, i, true);
    }

    // Remove some peers from the node.
    cluster.fidel_client.disable_default_operator();
    for i in 0..n {
        if i % 2 == 0 {
            continue;
        }
        let brane = cluster.get_brane(&[i]);
        let peer = find_peer(&brane, node_id).unwrap().clone();
        cluster.fidel_client.must_remove_peer(brane.get_id(), peer);
    }

    // Respacelike the node.
    cluster.stop_node(node_id);
    cluster.run_node(node_id).unwrap();

    // TuplespaceInstanton in removed peers should not exist.
    for i in 0..n {
        check_kv_in_all_causets(&db, i, i % 2 == 0);
    }
    check_db_files_at_level(&db, level, u64::from(n) / 2);
}

#[test]
fn test_server_clear_stale_data() {
    let mut cluster = new_server_cluster(0, 3);
    test_clear_stale_data(&mut cluster);
}
