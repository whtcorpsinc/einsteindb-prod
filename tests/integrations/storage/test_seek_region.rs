//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use violetabftstore::interlock::{BraneInfoAccessor, BraneInfoProvider};
use test_violetabftstore::*;
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::HandyRwLock;

fn test_seek_brane_impl<T: Simulator, R: BraneInfoProvider>(
    mut cluster: Cluster<T>,
    brane_info_providers: HashMap<u64, R>,
) {
    for i in 0..15 {
        let i = i + b'0';
        let key = vec![b'k', i];
        let value = vec![b'v', i];
        cluster.must_put(&key, &value);
    }

    let lightlike_tuplespaceInstanton = vec![
        b"k1".to_vec(),
        b"k3".to_vec(),
        b"k5".to_vec(),
        b"k7".to_vec(),
        b"k9".to_vec(),
        b"".to_vec(),
    ];

    let spacelike_tuplespaceInstanton = vec![
        b"".to_vec(),
        b"k1".to_vec(),
        b"k3".to_vec(),
        b"k5".to_vec(),
        b"k7".to_vec(),
        b"k9".to_vec(),
    ];

    let mut branes = Vec::new();

    for mut key in lightlike_tuplespaceInstanton.iter().take(lightlike_tuplespaceInstanton.len() - 1).map(Vec::clone) {
        let brane = cluster.get_brane(&key);
        cluster.must_split(&brane, &key);

        key[1] -= 1;
        let brane = cluster.get_brane(&key);
        branes.push(brane);
    }
    branes.push(cluster.get_brane(b"k9"));

    assert_eq!(branes.len(), lightlike_tuplespaceInstanton.len());
    assert_eq!(branes.len(), spacelike_tuplespaceInstanton.len());
    for i in 0..branes.len() {
        assert_eq!(branes[i].spacelike_key, spacelike_tuplespaceInstanton[i]);
        assert_eq!(branes[i].lightlike_key, lightlike_tuplespaceInstanton[i]);
    }

    // Wait for violetabftstore to fidelio branes
    thread::sleep(Duration::from_secs(2));

    for node_id in cluster.get_node_ids() {
        let engine = &brane_info_providers[&node_id];

        // Test traverse all branes
        let key = b"".to_vec();
        let (tx, rx) = channel();
        let tx_ = tx.clone();
        engine
            .seek_brane(
                &key,
                Box::new(move |infos| {
                    tx_.lightlike(infos.map(|i| i.brane.clone()).collect()).unwrap();
                }),
            )
            .unwrap();
        let sought_branes: Vec<_> = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(sought_branes, branes);

        // Test lightlike_key is exclusive
        let (tx, rx) = channel();
        let tx_ = tx.clone();
        engine
            .seek_brane(
                b"k1",
                Box::new(move |infos| tx_.lightlike(infos.next().unwrap().brane.clone()).unwrap()),
            )
            .unwrap();
        let brane = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(brane, branes[1]);

        // Test seek from non-spacelikeing key
        let tx_ = tx.clone();
        engine
            .seek_brane(
                b"k6\xff\xff\xff\xff\xff",
                Box::new(move |infos| tx_.lightlike(infos.next().unwrap().brane.clone()).unwrap()),
            )
            .unwrap();
        let brane = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(brane, branes[3]);
        let tx_ = tx.clone();
        engine
            .seek_brane(
                b"\xff\xff\xff\xff\xff\xff\xff\xff",
                Box::new(move |infos| tx_.lightlike(infos.next().unwrap().brane.clone()).unwrap()),
            )
            .unwrap();
        let brane = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(brane, branes[5]);
    }
}

#[test]
fn test_brane_collection_seek_brane() {
    let mut cluster = new_node_cluster(0, 3);

    let (tx, rx) = channel();
    cluster
        .sim
        .wl()
        .post_create_interlock_host(Box::new(move |id, host| {
            let p = BraneInfoAccessor::new(host);
            p.spacelike();
            tx.lightlike((id, p)).unwrap()
        }));

    cluster.run();
    let brane_info_providers: HashMap<_, _> = rx.try_iter().collect();
    assert_eq!(brane_info_providers.len(), 3);

    test_seek_brane_impl(cluster, brane_info_providers.clone());

    for (_, p) in brane_info_providers {
        p.stop();
    }
}
