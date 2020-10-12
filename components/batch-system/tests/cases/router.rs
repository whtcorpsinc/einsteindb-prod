// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use batch_system::test_runner::*;
use batch_system::*;
use crossbeam::channel::*;
use std::sync::atomic::*;
use std::sync::Arc;
use std::time::Duration;
use einsteindb_util::mpsc;

fn counter_closure(counter: &Arc<AtomicUsize>) -> Message {
    let c = counter.clone();
    Message::Callback(Box::new(move |_: &mut Runner| {
        c.fetch_add(1, Ordering::SeqCst);
    }))
}

fn noop() -> Message {
    Message::Callback(Box::new(|_| ()))
}

fn unreachable() -> Message {
    Message::Callback(Box::new(|_: &mut Runner| unreachable!()))
}

#[test]
fn test_basic() {
    let (control_tx, mut control_fsm) = Runner::new(10);
    let (control_drop_tx, control_drop_rx) = mpsc::unbounded();
    control_fsm.slightlikeer = Some(control_drop_tx);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm);
    let builder = Builder::new();
    system.spawn("test".to_owned(), builder);

    // Missing mailbox should report error.
    match router.force_slightlike(1, unreachable()) {
        Err(SlightlikeError(_)) => (),
        Ok(_) => panic!("slightlike should fail"),
    }
    match router.slightlike(1, unreachable()) {
        Err(TrySlightlikeError::Disconnected(_)) => (),
        Ok(_) => panic!("slightlike should fail"),
        Err(TrySlightlikeError::Full(_)) => panic!("expect disconnected."),
    }

    let (tx, rx) = mpsc::unbounded();
    let router_ = router.clone();
    // Control mailbox should be connected.
    router
        .slightlike_control(Message::Callback(Box::new(move |_: &mut Runner| {
            let (slightlikeer, mut runner) = Runner::new(10);
            let (tx1, rx1) = mpsc::unbounded();
            runner.slightlikeer = Some(tx1);
            let mailbox = BasicMailbox::new(slightlikeer, runner);
            router_.register(1, mailbox);
            tx.slightlike(rx1).unwrap();
        })))
        .unwrap();
    let runner_drop_rx = rx.recv_timeout(Duration::from_secs(3)).unwrap();

    // Registered mailbox should be connected.
    router.force_slightlike(1, noop()).unwrap();
    router.slightlike(1, noop()).unwrap();

    // Slightlike should respect capacity limit, while force_slightlike not.
    let (tx, rx) = mpsc::unbounded();
    router
        .slightlike(
            1,
            Message::Callback(Box::new(move |_: &mut Runner| {
                rx.recv_timeout(Duration::from_secs(100)).unwrap();
            })),
        )
        .unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let sent_cnt = (0..)
        .take_while(|_| router.slightlike(1, counter_closure(&counter)).is_ok())
        .count();
    match router.slightlike(1, counter_closure(&counter)) {
        Err(TrySlightlikeError::Full(_)) => {}
        Err(TrySlightlikeError::Disconnected(_)) => panic!("mailbox should still be connected."),
        Ok(_) => panic!("slightlike should fail"),
    }
    router.force_slightlike(1, counter_closure(&counter)).unwrap();
    tx.slightlike(1).unwrap();
    // Flush.
    let (tx, rx) = mpsc::unbounded();
    router
        .force_slightlike(
            1,
            Message::Callback(Box::new(move |_: &mut Runner| {
                tx.slightlike(1).unwrap();
            })),
        )
        .unwrap();
    rx.recv_timeout(Duration::from_secs(100)).unwrap();

    let c = counter.load(Ordering::SeqCst);
    assert_eq!(c, sent_cnt + 1);

    // close should release resources.
    assert_eq!(runner_drop_rx.try_recv(), Err(TryRecvError::Empty));
    router.close(1);
    assert_eq!(
        runner_drop_rx.recv_timeout(Duration::from_secs(3)),
        Err(RecvTimeoutError::Disconnected)
    );
    match router.slightlike(1, unreachable()) {
        Err(TrySlightlikeError::Disconnected(_)) => (),
        Ok(_) => panic!("slightlike should fail."),
        Err(TrySlightlikeError::Full(_)) => panic!("slightlikeer should be closed"),
    }
    match router.force_slightlike(1, unreachable()) {
        Err(SlightlikeError(_)) => (),
        Ok(_) => panic!("slightlike should fail."),
    }
    assert_eq!(control_drop_rx.try_recv(), Err(TryRecvError::Empty));
    system.shutdown();
    assert_eq!(
        control_drop_rx.recv_timeout(Duration::from_secs(3)),
        Err(RecvTimeoutError::Disconnected)
    );
}
