// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use engine_lmdb::LmdbSnapshot;
use violetabft::evioletabft_timeshare::MessageType;
use violetabftstore::store::*;
use std::time::*;
use test_violetabftstore::*;

/// Allow lost situation.
#[derive(PartialEq, Eq, Clone, Copy)]
enum DataLost {
    /// The leader loses commit index.
    ///
    /// A leader can't lost both the committed entries and commit index
    /// at the same time.
    LeaderCommit,
    /// A follower loses commit index.
    FollowerCommit,
    /// All the nodes loses data.
    ///
    /// Typically, both leader and followers lose commit index.
    AllLost,
}

fn test<A, C>(cluster: &mut Cluster<NodeCluster>, action: A, check: C, mode: DataLost)
where
    A: FnOnce(&mut Cluster<NodeCluster>),
    C: FnOnce(&mut Cluster<NodeCluster>),
{
    let filter = match mode {
        DataLost::AllLost | DataLost::LeaderCommit => BranePacketFilter::new(1, 1)
            .msg_type(MessageType::MsgApplightlikeResponse)
            .direction(Direction::Recv),
        DataLost::FollowerCommit => BranePacketFilter::new(1, 3)
            .msg_type(MessageType::MsgApplightlikeResponse)
            .direction(Direction::Recv),
    };
    cluster.add_lightlike_filter(CloneFilterFactory(filter));
    let last_index = cluster.violetabft_local_state(1, 1).get_last_index();
    action(cluster);
    cluster.wait_last_index(1, 1, last_index + 1, Duration::from_secs(3));
    let mut snaps = vec![];
    snaps.push((1, LmdbSnapshot::new(cluster.get_violetabft_engine(1))));
    if mode == DataLost::AllLost {
        cluster.wait_last_index(1, 2, last_index + 1, Duration::from_secs(3));
        snaps.push((2, LmdbSnapshot::new(cluster.get_violetabft_engine(2))));
        cluster.wait_last_index(1, 3, last_index + 1, Duration::from_secs(3));
        snaps.push((3, LmdbSnapshot::new(cluster.get_violetabft_engine(3))));
    }
    cluster.clear_lightlike_filters();
    check(cluster);
    for (id, _) in &snaps {
        cluster.stop_node(*id);
    }
    // Simulate data lost in violetabft causet.
    for (id, snap) in &snaps {
        cluster.restore_violetabft(1, *id, snap);
    }
    for (id, _) in &snaps {
        cluster.run_node(*id).unwrap();
    }

    if mode == DataLost::LeaderCommit || mode == DataLost::AllLost {
        cluster.must_transfer_leader(1, new_peer(1, 1));
    }
}

/// Test whether system can recover from mismatched violetabft state and apply state.
///
/// If EinsteinDB is not shutdown gracefully, apply state may go ahead of violetabft
/// state. EinsteinDB should be able to recognize the situation and spacelike normally.
fn test_early_apply(mode: DataLost) {
    let mut cluster = new_node_cluster(0, 3);
    cluster.causet.violetabft_store.early_apply = true;
    cluster.fidel_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run();
    if mode == DataLost::LeaderCommit || mode == DataLost::AllLost {
        cluster.must_transfer_leader(1, new_peer(1, 1));
    } else {
        cluster.must_transfer_leader(1, new_peer(3, 3));
    }
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    test(
        &mut cluster,
        |c| {
            c.async_put(b"k2", b"v2").unwrap();
        },
        |c| must_get_equal(&c.get_engine(1), b"k2", b"v2"),
        mode,
    );
    let brane = cluster.get_brane(b"");
    test(
        &mut cluster,
        |c| {
            c.split_brane(&brane, b"k2", Callback::None);
        },
        |c| c.wait_brane_split(&brane),
        mode,
    );
    if mode != DataLost::LeaderCommit && mode != DataLost::AllLost {
        test(
            &mut cluster,
            |c| {
                c.async_remove_peer(1, new_peer(1, 1)).unwrap();
            },
            |c| must_get_none(&c.get_engine(1), b"k2"),
            mode,
        );
    }
}

/// Tests whether the cluster can recover from leader lost its commit index.
#[test]
fn test_leader_early_apply() {
    test_early_apply(DataLost::LeaderCommit)
}

/// Tests whether the cluster can recover from follower lost its commit index.
#[test]
fn test_follower_commit_early_apply() {
    test_early_apply(DataLost::FollowerCommit)
}

/// Tests whether the cluster can recover from all nodes lost their commit index.
#[test]
fn test_all_node_crash() {
    test_early_apply(DataLost::AllLost)
}

/// Tests if apply index inside violetabft is fideliod correctly.
///
/// If index is not fideliod, violetabft will reject to campaign on timeout.
#[test]
fn test_fidelio_internal_apply_index() {
    let mut cluster = new_node_cluster(0, 4);
    cluster.causet.violetabft_store.early_apply = true;
    cluster.fidel_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    let filter = BranePacketFilter::new(1, 3)
        .msg_type(MessageType::MsgApplightlikeResponse)
        .direction(Direction::Recv);
    cluster.add_lightlike_filter(CloneFilterFactory(filter));
    let last_index = cluster.violetabft_local_state(1, 1).get_last_index();
    cluster.async_remove_peer(1, new_peer(4, 4)).unwrap();
    cluster.async_put(b"k2", b"v2").unwrap();
    let mut snaps = vec![];
    for i in 1..3 {
        cluster.wait_last_index(1, i, last_index + 2, Duration::from_secs(3));
        snaps.push((i, LmdbSnapshot::new(cluster.get_violetabft_engine(1))));
    }
    cluster.clear_lightlike_filters();
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");

    // Simulate data lost in violetabft causet.
    for (id, snap) in &snaps {
        cluster.stop_node(*id);
        cluster.restore_violetabft(1, *id, &snap);
        cluster.run_node(*id).unwrap();
    }

    let brane = cluster.get_brane(b"k1");
    // Issues a heartbeat to followers so they will re-commit the logs.
    let resp = read_on_peer(
        &mut cluster,
        new_peer(3, 3),
        brane.clone(),
        b"k1",
        true,
        Duration::from_secs(3),
    )
    .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    cluster.stop_node(3);
    cluster.must_put(b"k3", b"v3");
}
