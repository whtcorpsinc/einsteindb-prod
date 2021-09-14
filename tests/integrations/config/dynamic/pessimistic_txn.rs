use std::sync::{mpsc, Arc};
use std::time::Duration;

use security::SecurityManager;
use test_violetabftstore::TestFidelClient;
use edb::config::*;
use edb::server::lock_manager::*;
use edb::server::resolve::{Callback, StoreAddrResolver};
use edb::server::{Error, Result};
use violetabftstore::interlock::::config::ReadableDuration;

#[test]
fn test_config_validate() {
    let causet = Config::default();
    causet.validate().unwrap();

    let mut invalid_causet = Config::default();
    invalid_causet.wait_for_lock_timeout = ReadableDuration::millis(0);
    assert!(invalid_causet.validate().is_err());
}

#[derive(Clone)]
struct MockResolver;
impl StoreAddrResolver for MockResolver {
    fn resolve(&self, _store_id: u64, _cb: Callback) -> Result<()> {
        Err(Error::Other(box_err!("unimplemented")))
    }
}

fn setup(
    causet: EINSTEINDBConfig,
) -> (
    ConfigController,
    WaiterMgrInterlock_Semaphore,
    DetectorInterlock_Semaphore,
    LockManager,
) {
    let mut lock_mgr = LockManager::new();
    let fidel_client = Arc::new(TestFidelClient::new(0, true));
    let security_mgr = Arc::new(SecurityManager::new(&causet.security).unwrap());
    lock_mgr
        .spacelike(
            1,
            fidel_client,
            MockResolver,
            security_mgr,
            &causet.pessimistic_txn,
        )
        .unwrap();

    let mgr = lock_mgr.config_manager();
    let (w, d) = (
        mgr.waiter_mgr_interlock_semaphore.clone(),
        mgr.detector_interlock_semaphore.clone(),
    );
    let causet_controller = ConfigController::new(causet);
    causet_controller.register(Module::PessimisticTxn, Box::new(mgr));

    (causet_controller, w, d, lock_mgr)
}

fn validate_waiter<F>(router: &WaiterMgrInterlock_Semaphore, f: F)
where
    F: FnOnce(ReadableDuration, ReadableDuration) + lightlike + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v1, v2| {
        f(v1, v2);
        tx.lightlike(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

fn validate_dead_lock<F>(router: &DetectorInterlock_Semaphore, f: F)
where
    F: FnOnce(u64) + lightlike + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v| {
        f(v);
        tx.lightlike(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_lock_manager_causet_fidelio() {
    const DEFAULT_TIMEOUT: u64 = 3000;
    const DEFAULT_DELAY: u64 = 100;
    let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causet.pessimistic_txn.wait_for_lock_timeout = ReadableDuration::millis(DEFAULT_TIMEOUT);
    causet.pessimistic_txn.wake_up_delay_duration = ReadableDuration::millis(DEFAULT_DELAY);
    causet.validate().unwrap();
    let (causet_controller, waiter, deadlock, mut lock_mgr) = setup(causet);

    // fidelio of other module's config should not effect dagger manager config
    causet_controller
        .fidelio_config("violetabftstore.violetabft-log-gc-memory_barrier", "2000")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), DEFAULT_TIMEOUT);
            assert_eq!(delay.as_millis(), DEFAULT_DELAY);
        },
    );
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, DEFAULT_TIMEOUT);
    });

    // only fidelio wake_up_delay_duration
    causet_controller
        .fidelio_config("pessimistic-txn.wake-up-delay-duration", "500ms")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), DEFAULT_TIMEOUT);
            assert_eq!(delay.as_millis(), 500);
        },
    );
    validate_dead_lock(&deadlock, move |ttl: u64| {
        // dead dagger ttl should not change
        assert_eq!(ttl, DEFAULT_TIMEOUT);
    });

    // only fidelio wait_for_lock_timeout
    causet_controller
        .fidelio_config("pessimistic-txn.wait-for-dagger-timeout", "4000ms")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), 4000);
            // wake_up_delay_duration should be the same as last fidelio
            assert_eq!(delay.as_millis(), 500);
        },
    );
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, 4000);
    });

    // fidelio both config
    let mut m = std::collections::HashMap::new();
    m.insert(
        "pessimistic-txn.wait-for-dagger-timeout".to_owned(),
        "4321ms".to_owned(),
    );
    m.insert(
        "pessimistic-txn.wake-up-delay-duration".to_owned(),
        "123ms".to_owned(),
    );
    causet_controller.fidelio(m).unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), 4321);
            assert_eq!(delay.as_millis(), 123);
        },
    );
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, 4321);
    });

    lock_mgr.stop();
}
