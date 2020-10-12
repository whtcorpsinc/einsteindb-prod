// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use test_violetabftstore::*;

fn test_snapshot_encryption<T: Simulator>(cluster: &mut Cluster<T>) {
    configure_for_encryption(cluster);
    cluster.fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    for i in 0..100 {
        cluster.must_put(format!("key-{:02}", i).as_bytes(), b"value");
        cluster.must_put_causet("write", format!("key-{:02}", i).as_bytes(), b"value");
        cluster.must_put_causet("dagger", format!("key-{:02}", i).as_bytes(), b"value");
    }

    cluster.fidel_client.must_add_peer(r1, new_learner_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"key-00", b"value");
    must_get_causet_equal(&cluster.get_engine(2), "dagger", b"key-50", b"value");
    must_get_causet_equal(&cluster.get_engine(2), "write", b"key-99", b"value");
}

#[test]
fn test_node_snapshot_encryption() {
    let mut cluster = new_node_cluster(0, 2);
    test_snapshot_encryption(&mut cluster);
}

#[test]
fn test_server_snapshot_encryption() {
    let mut cluster = new_server_cluster(0, 2);
    test_snapshot_encryption(&mut cluster);
}
