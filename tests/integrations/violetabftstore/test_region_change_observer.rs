//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use ekvproto::meta_timeshare::Brane;
use violetabft::StateRole;
use violetabftstore::interlock::{
    BoxBraneChangeSemaphore, Interlock, SemaphoreContext, BraneChangeEvent, BraneChangeSemaphore,
};
use violetabftstore::store::util::{find_peer, new_peer};
use std::mem;
use std::sync::mpsc::{channel, sync_channel, Receiver, Synclightlikeer};
use std::sync::Arc;
use std::time::Duration;
use test_violetabftstore::{new_node_cluster, Cluster, NodeCluster};
use violetabftstore::interlock::::HandyRwLock;

#[derive(Clone)]
struct TestSemaphore {
    lightlikeer: Synclightlikeer<(Brane, BraneChangeEvent)>,
}

impl Interlock for TestSemaphore {}

impl BraneChangeSemaphore for TestSemaphore {
    fn on_brane_changed(
        &self,
        ctx: &mut SemaphoreContext<'_>,
        event: BraneChangeEvent,
        _: StateRole,
    ) {
        self.lightlikeer.lightlike((ctx.brane().clone(), event)).unwrap();
    }
}

fn test_brane_change_semaphore_impl(mut cluster: Cluster<NodeCluster>) {
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let receiver: Receiver<(Brane, BraneChangeEvent)>;
    let r1;
    {
        let (tx, rx) = channel();

        cluster
            .sim
            .wl()
            .post_create_interlock_host(Box::new(move |id, host| {
                if id == 1 {
                    let (lightlikeer, receiver) = sync_channel(10);
                    host.registry.register_brane_change_semaphore(
                        1,
                        BoxBraneChangeSemaphore::new(TestSemaphore { lightlikeer }),
                    );
                    tx.lightlike(receiver).unwrap();
                }
            }));
        r1 = cluster.run_conf_change();

        // Only one node has node_id = 1
        receiver = rx.recv().unwrap();
        rx.try_recv().unwrap_err();
    }

    // Init branes
    let init_brane_event = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(init_brane_event.1, BraneChangeEvent::Create);
    assert_eq!(init_brane_event.0.get_id(), r1);
    assert_eq!(init_brane_event.0.get_peers().len(), 1);

    // Change peer
    fidel_client.must_add_peer(r1, new_peer(2, 10));
    let add_peer_event = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(add_peer_event.1, BraneChangeEvent::fidelio);
    assert_eq!(add_peer_event.0.get_id(), r1);
    assert_eq!(add_peer_event.0.get_peers().len(), 2);
    assert_ne!(
        add_peer_event.0.get_brane_epoch(),
        init_brane_event.0.get_brane_epoch()
    );

    // Split
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");
    cluster.must_split(&add_peer_event.0, b"k2");
    let mut split_fidelio = receiver.recv().unwrap();
    let mut split_create = receiver.recv().unwrap();
    // We should receive an `fidelio` and a `Create`. The order of them is not important.
    if split_fidelio.1 != BraneChangeEvent::fidelio {
        mem::swap(&mut split_fidelio, &mut split_create);
    }
    // No more events
    receiver.try_recv().unwrap_err();
    assert_eq!(split_fidelio.1, BraneChangeEvent::fidelio);
    assert_eq!(split_fidelio.0.get_id(), r1);
    assert_ne!(
        split_fidelio.0.get_brane_epoch(),
        add_peer_event.0.get_brane_epoch()
    );
    let r2 = split_create.0.get_id();
    assert_ne!(r2, r1);
    assert_eq!(split_create.1, BraneChangeEvent::Create);
    if split_fidelio.0.get_spacelike_key().is_empty() {
        assert_eq!(split_fidelio.0.get_lightlike_key(), b"k2");
        assert_eq!(split_create.0.get_spacelike_key(), b"k2");
        assert!(split_create.0.get_lightlike_key().is_empty());
    } else {
        assert_eq!(split_fidelio.0.get_spacelike_key(), b"k2");
        assert!(split_fidelio.0.get_lightlike_key().is_empty());
        assert!(split_create.0.get_spacelike_key().is_empty());
        assert_eq!(split_create.0.get_lightlike_key(), b"k2");
    }

    // Merge
    fidel_client.must_merge(split_fidelio.0.get_id(), split_create.0.get_id());
    // An `fidelio` produced by PrepareMerge. Ignore it.
    assert_eq!(receiver.recv().unwrap().1, BraneChangeEvent::fidelio);
    let mut merge_fidelio = receiver.recv().unwrap();
    let mut merge_destroy = receiver.recv().unwrap();
    // We should receive an `fidelio` and a `Destroy`. The order of them is not important.
    if merge_fidelio.1 != BraneChangeEvent::fidelio {
        mem::swap(&mut merge_fidelio, &mut merge_destroy);
    }
    // No more events
    receiver.try_recv().unwrap_err();
    assert_eq!(merge_fidelio.1, BraneChangeEvent::fidelio);
    assert!(merge_fidelio.0.get_spacelike_key().is_empty());
    assert!(merge_fidelio.0.get_lightlike_key().is_empty());
    assert_eq!(merge_destroy.1, BraneChangeEvent::Destroy);
    if merge_fidelio.0.get_id() == split_fidelio.0.get_id() {
        assert_eq!(merge_destroy.0.get_id(), split_create.0.get_id());
        assert_ne!(
            merge_fidelio.0.get_brane_epoch(),
            split_fidelio.0.get_brane_epoch()
        );
    } else {
        assert_eq!(merge_fidelio.0.get_id(), split_create.0.get_id());
        assert_eq!(merge_destroy.0.get_id(), split_fidelio.0.get_id());
        assert_ne!(
            merge_fidelio.0.get_brane_epoch(),
            split_create.0.get_brane_epoch()
        );
    }

    // Move out from this node
    // After last time calling "must_add_peer", this brane must have two peers
    assert_eq!(merge_fidelio.0.get_peers().len(), 2);
    let r = merge_fidelio.0.get_id();

    fidel_client.must_remove_peer(r, find_peer(&merge_fidelio.0, 1).unwrap().clone());

    let remove_peer_fidelio = receiver.recv().unwrap();
    // After being removed from the brane's peers, an fidelio is triggered at first.
    assert_eq!(remove_peer_fidelio.1, BraneChangeEvent::fidelio);
    assert!(find_peer(&remove_peer_fidelio.0, 1).is_none());

    let remove_peer_destroy = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(remove_peer_destroy.1, BraneChangeEvent::Destroy);
    assert_eq!(remove_peer_destroy.0.get_id(), r);

    fidel_client.must_add_peer(r, new_peer(1, 2333));
    let add_peer_event = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(add_peer_event.1, BraneChangeEvent::Create);
    assert_eq!(add_peer_event.0.get_id(), r);
    assert_eq!(find_peer(&add_peer_event.0, 1).unwrap().get_id(), 2333);

    // No more messages
    receiver.recv_timeout(Duration::from_secs(1)).unwrap_err();
}

#[test]
fn test_brane_change_semaphore() {
    let cluster = new_node_cluster(1, 3);
    test_brane_change_semaphore_impl(cluster);
}
