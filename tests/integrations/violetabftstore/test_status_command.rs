// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use test_violetabftstore::*;

#[test]
fn test_brane_detail() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let leader = cluster.leader_of_brane(1).unwrap();
    let brane_detail = cluster.brane_detail(1, 1);
    assert!(brane_detail.has_brane());
    let brane = brane_detail.get_brane();
    assert_eq!(brane.get_id(), 1);
    assert!(brane.get_spacelike_key().is_empty());
    assert!(brane.get_lightlike_key().is_empty());
    assert_eq!(brane.get_peers().len(), 5);
    let epoch = brane.get_brane_epoch();
    assert_eq!(epoch.get_conf_ver(), 1);
    assert_eq!(epoch.get_version(), 1);

    assert!(brane_detail.has_leader());
    assert_eq!(brane_detail.get_leader(), &leader);
}
