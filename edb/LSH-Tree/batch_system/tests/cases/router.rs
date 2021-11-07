// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use batch_system::test_runner::*;
use batch_system::*;
use crossbeam::channel::*;
use std::sync::atomic::*;
use std::sync::Arc;
use std::time::Duration;
use violetabftstore::interlock::::mpsc;

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
    control_fsm.lightlikeer = Some(control_drop_tx);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm);
    let builder = Builder::new();
    system.spawn("test".to_owned(), builder);

    // Missing mailbox should report error.
    match router.force_lightlike(1, unreachable()) {
        Err(lightlikeError(_)) => (),
        Ok(_) => panic!("lightlike should fail"),
    }
    match router.lightlike(1, unreachable()) {
        Err(TrylightlikeError::Disconnected(_)) => (),
        Ok(_) => panic!("lightlike should fail"),
        Err(TrylightlikeError::Full(_)) => panic!("expect disconnected."),
    }

    let (tx, rx) = mpsc::unbounded();
    let router_ = router.clone();
    // Control mailbox should be connected.
    router
        .lightlike_control(Message::Callback(Box::new(move |_: &mut Runner| {
            let (lightlikeer, mut runner) = Runner::new(10);
            let (tx1, rx1) = mpsc::unbounded();
            runner.lightlikeer = Some(tx1);
            let mailbox = BasicMailbox::new(lightlikeer, runner);
            router_.register(1, mailbox);
            tx.lightlike(rx1).unwrap();
        })))
        .unwrap();
    let runner_drop_rx = rx.recv_timeout(Duration::from_secs(3)).unwrap();

    // Registered mailbox should be connected.
    router.force_lightlike(1, noop()).unwrap();
    router.lightlike(1, noop()).unwrap();

    // lightlike should respect capacity limit, while force_lightlike not.
    let (tx, rx) = mpsc::unbounded();
    router
        .lightlike(
            1,
            Message::Callback(Box::new(move |_: &mut Runner| {
                rx.recv_timeout(Duration::from_secs(100)).unwrap();
            })),
        )
        .unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let sent_cnt = (0..)
        .take_while(|_| router.lightlike(1, counter_closure(&counter)).is_ok())
        .count();
    match router.lightlike(1, counter_closure(&counter)) {
        Err(TrylightlikeError::Full(_)) => {}
        Err(TrylightlikeError::Disconnected(_)) => panic!("mailbox should still be connected."),
        Ok(_) => panic!("lightlike should fail"),
    }
    router.force_lightlike(1, counter_closure(&counter)).unwrap();
    tx.lightlike(1).unwrap();
    // Flush.
    let (tx, rx) = mpsc::unbounded();
    router
        .force_lightlike(
            1,
            Message::Callback(Box::new(move |_: &mut Runner| {
                tx.lightlike(1).unwrap();
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
    match router.lightlike(1, unreachable()) {
        Err(TrylightlikeError::Disconnected(_)) => (),
        Ok(_) => panic!("lightlike should fail."),
        Err(TrylightlikeError::Full(_)) => panic!("lightlikeer should be closed"),
    }
    match router.force_lightlike(1, unreachable()) {
        Err(lightlikeError(_)) => (),
        Ok(_) => panic!("lightlike should fail."),
    }
    assert_eq!(control_drop_rx.try_recv(), Err(TryRecvError::Empty));
    system.shutdown();
    assert_eq!(
        control_drop_rx.recv_timeout(Duration::from_secs(3)),
        Err(RecvTimeoutError::Disconnected)
    );
}
