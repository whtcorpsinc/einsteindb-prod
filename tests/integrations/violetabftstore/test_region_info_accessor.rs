//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use tuplespaceInstanton::data_lightlike_key;
use ekvproto::meta_timeshare::Brane;
use violetabft::StateRole;
use violetabftstore::interlock::{BraneInfo, BraneInfoAccessor};
use violetabftstore::store::util::{find_peer, new_peer};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use test_violetabftstore::{configure_for_merge, new_node_cluster, Cluster, NodeCluster};
use violetabftstore::interlock::::HandyRwLock;

fn dump(c: &BraneInfoAccessor) -> Vec<(Brane, StateRole)> {
    let (branes, brane_cones) = c.debug_dump();

    assert_eq!(branes.len(), brane_cones.len());

    let mut res = Vec::new();
    for (lightlike_key, id) in brane_cones {
        let BraneInfo { ref brane, role } = branes[&id];
        assert_eq!(lightlike_key, data_lightlike_key(brane.get_lightlike_key()));
        assert_eq!(id, brane.get_id());
        res.push((brane.clone(), role));
    }

    res
}

fn check_brane_cones(branes: &[(Brane, StateRole)], cones: &[(&[u8], &[u8])]) {
    assert_eq!(branes.len(), cones.len());
    branes
        .iter()
        .zip(cones.iter())
        .for_each(|((r, _), (spacelike_key, lightlike_key))| {
            assert_eq!(r.get_spacelike_key(), *spacelike_key);
            assert_eq!(r.get_lightlike_key(), *lightlike_key);
        })
}

fn test_brane_info_accessor_impl(cluster: &mut Cluster<NodeCluster>, c: &BraneInfoAccessor) {
    for i in 0..9 {
        let k = format!("k{}", i).into_bytes();
        let v = format!("v{}", i).into_bytes();
        cluster.must_put(&k, &v);
    }

    let fidel_client = Arc::clone(&cluster.fidel_client);

    let init_branes = dump(c);
    check_brane_cones(&init_branes, &[(&b""[..], &b""[..])]);
    assert_eq!(init_branes[0].0, cluster.get_brane(b"k1"));

    // Split
    {
        let r1 = cluster.get_brane(b"k1");
        cluster.must_split(&r1, b"k1");
        let r2 = cluster.get_brane(b"k4");
        cluster.must_split(&r2, b"k4");
        let r3 = cluster.get_brane(b"k2");
        cluster.must_split(&r3, b"k2");
        let r4 = cluster.get_brane(b"k3");
        cluster.must_split(&r4, b"k3");
    }

    let split_branes = dump(c);
    check_brane_cones(
        &split_branes,
        &[
            (&b""[..], &b"k1"[..]),
            (b"k1", b"k2"),
            (b"k2", b"k3"),
            (b"k3", b"k4"),
            (b"k4", b""),
        ],
    );
    for (ref brane, _) in &split_branes {
        if brane.get_id() == init_branes[0].0.get_id() {
            assert_ne!(
                brane.get_brane_epoch(),
                init_branes[0].0.get_brane_epoch()
            );
        }
    }

    // Merge from left to right
    fidel_client.must_merge(split_branes[1].0.get_id(), split_branes[2].0.get_id());
    let merge_branes = dump(&c);
    check_brane_cones(
        &merge_branes,
        &[
            (&b""[..], &b"k1"[..]),
            (b"k1", b"k3"),
            (b"k3", b"k4"),
            (b"k4", b""),
        ],
    );

    // Merge from right to left
    fidel_client.must_merge(merge_branes[2].0.get_id(), merge_branes[1].0.get_id());
    let mut merge_branes_2 = dump(&c);
    check_brane_cones(
        &merge_branes_2,
        &[(&b""[..], &b"k1"[..]), (b"k1", b"k4"), (b"k4", b"")],
    );

    // Add peer
    let (brane1, role1) = merge_branes_2.remove(1);
    assert_eq!(role1, StateRole::Leader);
    assert_eq!(brane1.get_peers().len(), 1);
    assert_eq!(brane1.get_peers()[0].get_store_id(), 1);

    fidel_client.must_add_peer(brane1.get_id(), new_peer(2, 100));
    let (brane2, role2) = dump(c).remove(1);
    assert_eq!(role2, StateRole::Leader);
    assert_eq!(brane2.get_peers().len(), 2);
    assert!(find_peer(&brane2, 1).is_some());
    assert!(find_peer(&brane2, 2).is_some());

    // Change leader
    fidel_client.transfer_leader(brane2.get_id(), find_peer(&brane2, 2).unwrap().clone());
    let mut brane3 = Brane::default();
    let mut role3 = StateRole::default();
    // Wait for transfer leader finish
    for _ in 0..100 {
        let r = dump(c).remove(1);
        brane3 = r.0;
        role3 = r.1;
        if role3 == StateRole::Follower {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(role3, StateRole::Follower);

    // Remove peer
    check_brane_cones(
        &dump(c),
        &[(&b""[..], &b"k1"[..]), (b"k1", b"k4"), (b"k4", b"")],
    );

    fidel_client.must_remove_peer(brane3.get_id(), find_peer(&brane3, 1).unwrap().clone());

    let mut branes_after_removing = Vec::new();
    // It seems brane_info_accessor is a little delayed than violetabftstore...
    for _ in 0..100 {
        branes_after_removing = dump(c);
        if branes_after_removing.len() == 2 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    check_brane_cones(
        &branes_after_removing,
        &[(&b""[..], &b"k1"[..]), (b"k4", b"")],
    );
}

#[test]
fn test_node_cluster_brane_info_accessor() {
    let mut cluster = new_node_cluster(1, 3);
    configure_for_merge(&mut cluster);

    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    // Create a BraneInfoAccessor on node 1
    let (tx, rx) = channel();
    cluster
        .sim
        .wl()
        .post_create_interlock_host(Box::new(move |id, host| {
            if id == 1 {
                let c = BraneInfoAccessor::new(host);
                tx.lightlike(c).unwrap();
            }
        }));
    cluster.run_conf_change();
    let c = rx.recv().unwrap();
    c.spacelike();
    // We only created it on the node whose id == 1 so we shouldn't receive more than one item.
    assert!(rx.try_recv().is_err());

    test_brane_info_accessor_impl(&mut cluster, &c);

    drop(cluster);
    c.stop();
}
