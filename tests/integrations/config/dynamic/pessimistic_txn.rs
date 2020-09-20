use std::sync::{mpsc, Arc};
use std::time::Duration;

use security::SecurityManager;
use test_violetabftstore::TestFidelClient;
use einsteindb::config::*;
use einsteindb::server::lock_manager::*;
use einsteindb::server::resolve::{Callback, StoreAddrResolver};
use einsteindb::server::{Error, Result};
use einsteindb_util::config::ReadableDuration;

#[test]
fn test_config_validate() {
    let causetg = Config::default();
    causetg.validate().unwrap();

    let mut invalid_causetg = Config::default();
    invalid_causetg.wait_for_lock_timeout = ReadableDuration::millis(0);
    assert!(invalid_causetg.validate().is_err());
}

#[derive(Clone)]
struct MockResolver;
impl StoreAddrResolver for MockResolver {
    fn resolve(&self, _store_id: u64, _cb: Callback) -> Result<()> {
        Err(Error::Other(box_err!("unimplemented")))
    }
}

fn setup(
    causetg: EINSTEINDBConfig,
) -> (
    ConfigController,
    WaiterMgrScheduler,
    DetectorScheduler,
    LockManager,
) {
    let mut lock_mgr = LockManager::new();
    let fidel_client = Arc::new(TestFidelClient::new(0, true));
    let security_mgr = Arc::new(SecurityManager::new(&causetg.security).unwrap());
    lock_mgr
        .spacelike(
            1,
            fidel_client,
            MockResolver,
            security_mgr,
            &causetg.pessimistic_txn,
        )
        .unwrap();

    let mgr = lock_mgr.config_manager();
    let (w, d) = (
        mgr.waiter_mgr_scheduler.clone(),
        mgr.detector_scheduler.clone(),
    );
    let causetg_controller = ConfigController::new(causetg);
    causetg_controller.register(Module::PessimisticTxn, Box::new(mgr));

    (causetg_controller, w, d, lock_mgr)
}

fn validate_waiter<F>(router: &WaiterMgrScheduler, f: F)
where
    F: FnOnce(ReadableDuration, ReadableDuration) + Slightlike + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v1, v2| {
        f(v1, v2);
        tx.slightlike(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

fn validate_dead_lock<F>(router: &DetectorScheduler, f: F)
where
    F: FnOnce(u64) + Slightlike + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v| {
        f(v);
        tx.slightlike(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_lock_manager_causetg_ufidelate() {
    const DEFAULT_TIMEOUT: u64 = 3000;
    const DEFAULT_DELAY: u64 = 100;
    let (mut causetg, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
    causetg.pessimistic_txn.wait_for_lock_timeout = ReadableDuration::millis(DEFAULT_TIMEOUT);
    causetg.pessimistic_txn.wake_up_delay_duration = ReadableDuration::millis(DEFAULT_DELAY);
    causetg.validate().unwrap();
    let (causetg_controller, waiter, deadlock, mut lock_mgr) = setup(causetg);

    // ufidelate of other module's config should not effect lock manager config
    causetg_controller
        .ufidelate_config("violetabftstore.violetabft-log-gc-memory_barrier", "2000")
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

    // only ufidelate wake_up_delay_duration
    causetg_controller
        .ufidelate_config("pessimistic-txn.wake-up-delay-duration", "500ms")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), DEFAULT_TIMEOUT);
            assert_eq!(delay.as_millis(), 500);
        },
    );
    validate_dead_lock(&deadlock, move |ttl: u64| {
        // dead lock ttl should not change
        assert_eq!(ttl, DEFAULT_TIMEOUT);
    });

    // only ufidelate wait_for_lock_timeout
    causetg_controller
        .ufidelate_config("pessimistic-txn.wait-for-lock-timeout", "4000ms")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), 4000);
            // wake_up_delay_duration should be the same as last ufidelate
            assert_eq!(delay.as_millis(), 500);
        },
    );
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, 4000);
    });

    // ufidelate both config
    let mut m = std::collections::HashMap::new();
    m.insert(
        "pessimistic-txn.wait-for-lock-timeout".to_owned(),
        "4321ms".to_owned(),
    );
    m.insert(
        "pessimistic-txn.wake-up-delay-duration".to_owned(),
        "123ms".to_owned(),
    );
    causetg_controller.ufidelate(m).unwrap();
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
