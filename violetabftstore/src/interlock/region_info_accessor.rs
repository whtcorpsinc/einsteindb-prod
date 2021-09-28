//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Unbounded};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use super::metrics::*;
use super::{
    BoxBraneChangeSemaphore, BoxRoleSemaphore, Interlock, InterlockHost, SemaphoreContext,
    BraneChangeEvent, BraneChangeSemaphore, Result, RoleSemaphore,
};
use edb::CausetEngine;
use tuplespaceInstanton::{data_lightlike_key, data_key};
use ekvproto::meta_timeshare::Brane;
use violetabft::StateRole;
use violetabftstore::interlock::::collections::HashMap;
use violetabftstore::interlock::::timer::Timer;
use violetabftstore::interlock::::worker::{Builder as WorkerBuilder, Runnable, RunnableWithTimer, Interlock_Semaphore, Worker};

/// `BraneInfoAccessor` is used to collect all branes' information on this EinsteinDB into a collection
/// so that other parts of EinsteinDB can get brane information from it. It registers a semaphore to
/// violetabftstore, which is named `BraneEventListener`. When the events that we are interested in
/// happen (such as creating and deleting branes), `BraneEventListener` simply lightlikes the events
/// through a channel.
/// In the mean time, `BraneCollector` keeps fetching messages from the channel, and mutates
/// the collection according to the messages. When an accessor method of `BraneInfoAccessor` is
/// called, it also simply lightlikes a message to `BraneCollector`, and the result will be sent
/// back through as soon as it's finished.
/// In fact, the channel mentioned above is actually a `util::worker::Worker`.
///
/// **Caution**: Note that the information in `BraneInfoAccessor` is not perfectly precise. Some
/// branes may be temporarily absent while merging or splitting is in progress. Also,
/// `BraneInfoAccessor`'s information may lightly lag the actual branes on the EinsteinDB.

/// `VioletaBftStoreEvent` Represents events dispatched from violetabftstore interlock.
#[derive(Debug)]
pub enum VioletaBftStoreEvent {
    CreateBrane { brane: Brane, role: StateRole },
    fidelioBrane { brane: Brane, role: StateRole },
    DestroyBrane { brane: Brane },
    RoleChange { brane: Brane, role: StateRole },
}

impl VioletaBftStoreEvent {
    pub fn get_brane(&self) -> &Brane {
        match self {
            VioletaBftStoreEvent::CreateBrane { brane, .. }
            | VioletaBftStoreEvent::fidelioBrane { brane, .. }
            | VioletaBftStoreEvent::DestroyBrane { brane, .. }
            | VioletaBftStoreEvent::RoleChange { brane, .. } => brane,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BraneInfo {
    pub brane: Brane,
    pub role: StateRole,
}

impl BraneInfo {
    pub fn new(brane: Brane, role: StateRole) -> Self {
        Self { brane, role }
    }
}

type BranesMap = HashMap<u64, BraneInfo>;
type BraneConesMap = BTreeMap<Vec<u8>, u64>;

pub type Callback<T> = Box<dyn FnOnce(T) + lightlike>;
pub type SeekBraneCallback = Box<dyn FnOnce(&mut dyn Iteron<Item = &BraneInfo>) + lightlike>;

/// `BraneInfoAccessor` has its own thread. Queries and fidelios are done by lightlikeing commands to the
/// thread.
pub enum BraneInfoQuery {
    VioletaBftStoreEvent(VioletaBftStoreEvent),
    SeekBrane {
        from: Vec<u8>,
        callback: SeekBraneCallback,
    },
    FindBraneById {
        brane_id: u64,
        callback: Callback<Option<BraneInfo>>,
    },
    /// Gets all contents from the collection. Only used for testing.
    DebugDump(mpsc::lightlikeer<(BranesMap, BraneConesMap)>),
}

impl Display for BraneInfoQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            BraneInfoQuery::VioletaBftStoreEvent(e) => write!(f, "VioletaBftStoreEvent({:?})", e),
            BraneInfoQuery::SeekBrane { from, .. } => {
                write!(f, "SeekBrane(from: {})", hex::encode_upper(from))
            }
            BraneInfoQuery::FindBraneById { brane_id, .. } => {
                write!(f, "FindBraneById(brane_id: {})", brane_id)
            }
            BraneInfoQuery::DebugDump(_) => write!(f, "DebugDump"),
        }
    }
}

/// `BraneEventListener` implements semaphore promises. It simply lightlike the events that we are interested in
/// through the `interlock_semaphore`.
#[derive(Clone)]
struct BraneEventListener {
    interlock_semaphore: Interlock_Semaphore<BraneInfoQuery>,
}

impl Interlock for BraneEventListener {}

impl BraneChangeSemaphore for BraneEventListener {
    fn on_brane_changed(
        &self,
        context: &mut SemaphoreContext<'_>,
        event: BraneChangeEvent,
        role: StateRole,
    ) {
        let brane = context.brane().clone();
        let event = match event {
            BraneChangeEvent::Create => VioletaBftStoreEvent::CreateBrane { brane, role },
            BraneChangeEvent::fidelio => VioletaBftStoreEvent::fidelioBrane { brane, role },
            BraneChangeEvent::Destroy => VioletaBftStoreEvent::DestroyBrane { brane },
        };
        self.interlock_semaphore
            .schedule(BraneInfoQuery::VioletaBftStoreEvent(event))
            .unwrap();
    }
}

impl RoleSemaphore for BraneEventListener {
    fn on_role_change(&self, context: &mut SemaphoreContext<'_>, role: StateRole) {
        let brane = context.brane().clone();
        let event = VioletaBftStoreEvent::RoleChange { brane, role };
        self.interlock_semaphore
            .schedule(BraneInfoQuery::VioletaBftStoreEvent(event))
            .unwrap();
    }
}

/// Creates an `BraneEventListener` and register it to given interlock host.
fn register_brane_event_listener(
    host: &mut InterlockHost<impl CausetEngine>,
    interlock_semaphore: Interlock_Semaphore<BraneInfoQuery>,
) {
    let listener = BraneEventListener { interlock_semaphore };

    host.registry
        .register_role_semaphore(1, BoxRoleSemaphore::new(listener.clone()));
    host.registry
        .register_brane_change_semaphore(1, BoxBraneChangeSemaphore::new(listener));
}

/// `BraneCollector` is the place where we hold all brane information we collected, and the
/// underlying runner of `BraneInfoAccessor`. It listens on events sent by the `BraneEventListener` and
/// keeps information of all branes. Role of each brane are also tracked.
pub struct BraneCollector {
    // HashMap: brane_id -> (Brane, State)
    branes: BranesMap,
    // BTreeMap: data_lightlike_key -> brane_id
    brane_cones: BraneConesMap,
}

impl BraneCollector {
    pub fn new() -> Self {
        Self {
            branes: HashMap::default(),
            brane_cones: BTreeMap::default(),
        }
    }

    pub fn create_brane(&mut self, brane: Brane, role: StateRole) {
        let lightlike_key = data_lightlike_key(brane.get_lightlike_key());
        let brane_id = brane.get_id();

        // Create new brane
        self.brane_cones.insert(lightlike_key, brane_id);

        // TODO: Should we set it follower?
        assert!(
            self.branes
                .insert(brane_id, BraneInfo::new(brane, role))
                .is_none(),
            "trying to create new brane {} but it already exists.",
            brane_id
        );
    }

    fn fidelio_brane(&mut self, brane: Brane) {
        let existing_brane_info = self.branes.get_mut(&brane.get_id()).unwrap();

        let old_brane = &mut existing_brane_info.brane;
        assert_eq!(old_brane.get_id(), brane.get_id());

        // If the lightlike_key changed, the old entry in `brane_cones` should be removed.
        if old_brane.get_lightlike_key() != brane.get_lightlike_key() {
            // The brane's lightlike_key has changed.
            // Remove the old entry in `self.brane_cones`.
            let old_lightlike_key = data_lightlike_key(old_brane.get_lightlike_key());

            let old_id = self.brane_cones.remove(&old_lightlike_key).unwrap();
            assert_eq!(old_id, brane.get_id());

            // Insert new entry to `brane_cones`.
            let lightlike_key = data_lightlike_key(brane.get_lightlike_key());
            assert!(self
                .brane_cones
                .insert(lightlike_key, brane.get_id())
                .is_none());
        }

        // If the brane already exists, fidelio it and keep the original role.
        *old_brane = brane;
    }

    fn handle_create_brane(&mut self, brane: Brane, role: StateRole) {
        // During tests, we found that the `Create` event may arrive multiple times. And when we
        // receive an `fidelio` message, the brane may have been deleted for some reason. So we
        // handle it according to whether the brane exists in the collection.
        if self.branes.contains_key(&brane.get_id()) {
            info!(
                "trying to create brane but it already exists, try to fidelio it";
                "brane_id" => brane.get_id(),
            );
            self.fidelio_brane(brane);
        } else {
            self.create_brane(brane, role);
        }
    }

    fn handle_fidelio_brane(&mut self, brane: Brane, role: StateRole) {
        if self.branes.contains_key(&brane.get_id()) {
            self.fidelio_brane(brane);
        } else {
            info!(
                "trying to fidelio brane but it doesn't exist, try to create it";
                "brane_id" => brane.get_id(),
            );
            self.create_brane(brane, role);
        }
    }

    fn handle_destroy_brane(&mut self, brane: Brane) {
        if let Some(removed_brane_info) = self.branes.remove(&brane.get_id()) {
            let removed_brane = removed_brane_info.brane;
            assert_eq!(removed_brane.get_id(), brane.get_id());

            let lightlike_key = data_lightlike_key(removed_brane.get_lightlike_key());

            let removed_id = self.brane_cones.remove(&lightlike_key).unwrap();
            assert_eq!(removed_id, brane.get_id());
        } else {
            // It's possible that the brane is already removed because it's lightlike_key is used by
            // another newer brane.
            debug!(
                "destroying brane but it doesn't exist";
                "brane_id" => brane.get_id(),
            )
        }
    }

    fn handle_role_change(&mut self, brane: Brane, new_role: StateRole) {
        let brane_id = brane.get_id();

        if let Some(r) = self.branes.get_mut(&brane_id) {
            r.role = new_role;
            return;
        }

        warn!(
            "role change on brane but the brane doesn't exist. create it.";
            "brane_id" => brane_id,
        );
        self.create_brane(brane, new_role);
    }

    /// Determines whether `brane_to_check`'s epoch is stale compared to `current`'s epoch
    #[inline]
    fn is_brane_epoch_stale(&self, brane_to_check: &Brane, current: &Brane) -> bool {
        let epoch = brane_to_check.get_brane_epoch();
        let current_epoch = current.get_brane_epoch();

        // Only compare conf_ver when they have the same version.
        // When a brane A merges brane B, brane B may have a greater conf_ver. Then, the new
        // merged brane meta has larger version but smaller conf_ver than the original B's. In this
        // case, the incoming brane meta has a smaller conf_ver but is not stale.
        epoch.get_version() < current_epoch.get_version()
            || (epoch.get_version() == current_epoch.get_version()
                && epoch.get_conf_ver() < current_epoch.get_conf_ver())
    }

    /// For all branes whose cone overlaps with the given `brane` or brane_id is the same as
    /// `brane`'s, checks whether the given `brane`'s epoch is not older than theirs.
    ///
    /// Returns false if the given `brane` is stale, which means, at least one brane above has
    /// newer epoch.
    /// If the given `brane` is not stale, all other branes in the collection that overlaps with
    /// the given `brane` must be stale. Returns true in this case, and if `clear_branes_in_cone`
    /// is true, those out-of-date branes will be removed from the collection.
    fn check_brane_cone(&mut self, brane: &Brane, clear_branes_in_cone: bool) -> bool {
        if let Some(brane_with_same_id) = self.branes.get(&brane.get_id()) {
            if self.is_brane_epoch_stale(brane, &brane_with_same_id.brane) {
                return false;
            }
        }

        let mut stale_branes_in_cone = vec![];

        for (key, id) in self
            .brane_cones
            .cone((Excluded(data_key(brane.get_spacelike_key())), Unbounded))
        {
            if *id == brane.get_id() {
                continue;
            }

            let current_brane = &self.branes[id].brane;
            if !brane.get_lightlike_key().is_empty()
                && current_brane.get_spacelike_key() >= brane.get_lightlike_key()
            {
                // This and following branes are not overlapping with `brane`.
                break;
            }

            if self.is_brane_epoch_stale(brane, current_brane) {
                return false;
            }
            // They are impossible to equal, or they cannot overlap.
            assert_ne!(
                brane.get_brane_epoch().get_version(),
                current_brane.get_brane_epoch().get_version()
            );
            // Remove it since it's a out-of-date brane info.
            if clear_branes_in_cone {
                stale_branes_in_cone.push((key.clone(), *id));
            }
        }

        // Remove all plightlikeing-remove branes
        for (key, id) in stale_branes_in_cone {
            self.branes.remove(&id).unwrap();
            self.brane_cones.remove(&key).unwrap();
        }

        true
    }

    pub fn handle_seek_brane(&self, from_key: Vec<u8>, callback: SeekBraneCallback) {
        let from_key = data_key(&from_key);
        let mut iter = self
            .brane_cones
            .cone((Excluded(from_key), Unbounded))
            .map(|(_, brane_id)| &self.branes[brane_id]);
        callback(&mut iter)
    }

    pub fn handle_find_brane_by_id(&self, brane_id: u64, callback: Callback<Option<BraneInfo>>) {
        callback(self.branes.get(&brane_id).cloned());
    }

    fn handle_violetabftstore_event(&mut self, event: VioletaBftStoreEvent) {
        {
            let brane = event.get_brane();
            if brane.get_brane_epoch().get_version() == 0 {
                // Ignore messages with version 0.
                // In violetabftstore `Peer::replicate`, the brane meta's fields are all initialized with
                // default value except brane_id. So if there is more than one brane replicating
                // when the EinsteinDB just spacelikes, the assertion "Any two brane with different ids and
                // overlapping cones must have different version" fails.
                //
                // Since 0 is actually an invalid value of version, we can simply ignore the
                // messages with version 0. The brane will be created later when the brane's epoch
                // is properly set and an fidelio message was sent.
                return;
            }
            if !self.check_brane_cone(brane, true) {
                debug!(
                    "Received stale event";
                    "event" => ?event,
                );
                return;
            }
        }

        match event {
            VioletaBftStoreEvent::CreateBrane { brane, role } => {
                self.handle_create_brane(brane, role);
            }
            VioletaBftStoreEvent::fidelioBrane { brane, role } => {
                self.handle_fidelio_brane(brane, role);
            }
            VioletaBftStoreEvent::DestroyBrane { brane } => {
                self.handle_destroy_brane(brane);
            }
            VioletaBftStoreEvent::RoleChange { brane, role } => {
                self.handle_role_change(brane, role);
            }
        }
    }
}

impl Runnable for BraneCollector {
    type Task = BraneInfoQuery;

    fn run(&mut self, task: BraneInfoQuery) {
        match task {
            BraneInfoQuery::VioletaBftStoreEvent(event) => {
                self.handle_violetabftstore_event(event);
            }
            BraneInfoQuery::SeekBrane { from, callback } => {
                self.handle_seek_brane(from, callback);
            }
            BraneInfoQuery::FindBraneById {
                brane_id,
                callback,
            } => {
                self.handle_find_brane_by_id(brane_id, callback);
            }
            BraneInfoQuery::DebugDump(tx) => {
                tx.lightlike((self.branes.clone(), self.brane_cones.clone()))
                    .unwrap();
            }
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

impl RunnableWithTimer for BraneCollector {
    type TimeoutTask = ();

    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let mut count = 0;
        let mut leader = 0;
        for r in self.branes.values() {
            count += 1;
            if r.role == StateRole::Leader {
                leader += 1;
            }
        }
        REGION_COUNT_GAUGE_VEC
            .with_label_values(&["brane"])
            .set(count);
        REGION_COUNT_GAUGE_VEC
            .with_label_values(&["leader"])
            .set(leader);
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
    }
}

/// `BraneInfoAccessor` keeps all brane information separately from violetabftstore itself.
#[derive(Clone)]
pub struct BraneInfoAccessor {
    worker: Arc<Mutex<Worker<BraneInfoQuery>>>,
    interlock_semaphore: Interlock_Semaphore<BraneInfoQuery>,
}

impl BraneInfoAccessor {
    /// Creates a new `BraneInfoAccessor` and register to `host`.
    /// `BraneInfoAccessor` doesn't need, and should not be created more than once. If it's needed
    /// in different places, just clone it, and their contents are shared.
    pub fn new(host: &mut InterlockHost<impl CausetEngine>) -> Self {
        let worker = WorkerBuilder::new("brane-collector-worker").create();
        let interlock_semaphore = worker.interlock_semaphore();

        register_brane_event_listener(host, interlock_semaphore.clone());

        Self {
            worker: Arc::new(Mutex::new(worker)),
            interlock_semaphore,
        }
    }

    /// Starts the `BraneInfoAccessor`. It should be spacelikeed before violetabftstore.
    pub fn spacelike(&self) {
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
        self.worker
            .dagger()
            .unwrap()
            .spacelike_with_timer(BraneCollector::new(), timer)
            .unwrap();
    }

    /// Stops the `BraneInfoAccessor`. It should be stopped after violetabftstore.
    pub fn stop(&self) {
        self.worker.dagger().unwrap().stop().unwrap().join().unwrap();
    }

    /// Gets all content from the collection. Only used for testing.
    pub fn debug_dump(&self) -> (BranesMap, BraneConesMap) {
        let (tx, rx) = mpsc::channel();
        self.interlock_semaphore
            .schedule(BraneInfoQuery::DebugDump(tx))
            .unwrap();
        rx.recv().unwrap()
    }
}

pub trait BraneInfoProvider: lightlike + Clone + 'static {
    /// Get a Iteron of branes that contains `from` or have tuplespaceInstanton larger than `from`, and invoke
    /// the callback to process the result.
    fn seek_brane(&self, _from: &[u8], _callback: SeekBraneCallback) -> Result<()> {
        unimplemented!()
    }

    fn find_brane_by_id(
        &self,
        _reigon_id: u64,
        _callback: Callback<Option<BraneInfo>>,
    ) -> Result<()> {
        unimplemented!()
    }
}

impl BraneInfoProvider for BraneInfoAccessor {
    fn seek_brane(&self, from: &[u8], callback: SeekBraneCallback) -> Result<()> {
        let msg = BraneInfoQuery::SeekBrane {
            from: from.to_vec(),
            callback,
        };
        self.interlock_semaphore
            .schedule(msg)
            .map_err(|e| box_err!("failed to lightlike request to brane collector: {:?}", e))
    }

    fn find_brane_by_id(
        &self,
        brane_id: u64,
        callback: Callback<Option<BraneInfo>>,
    ) -> Result<()> {
        let msg = BraneInfoQuery::FindBraneById {
            brane_id,
            callback,
        };
        self.interlock_semaphore
            .schedule(msg)
            .map_err(|e| box_err!("failed to lightlike request to brane collector: {:?}", e))
    }
}

#[causet(test)]
mod tests {
    use super::*;

    fn new_brane(id: u64, spacelike_key: &[u8], lightlike_key: &[u8], version: u64) -> Brane {
        let mut brane = Brane::default();
        brane.set_id(id);
        brane.set_spacelike_key(spacelike_key.to_vec());
        brane.set_lightlike_key(lightlike_key.to_vec());
        brane.mut_brane_epoch().set_version(version);
        brane
    }

    fn brane_with_conf(
        id: u64,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
        version: u64,
        conf_ver: u64,
    ) -> Brane {
        let mut brane = new_brane(id, spacelike_key, lightlike_key, version);
        brane.mut_brane_epoch().set_conf_ver(conf_ver);
        brane
    }

    fn check_collection(c: &BraneCollector, branes: &[(Brane, StateRole)]) {
        let brane_cones: Vec<_> = branes
            .iter()
            .map(|(r, _)| (data_lightlike_key(r.get_lightlike_key()), r.get_id()))
            .collect();

        let mut is_branes_equal = c.branes.len() == branes.len();

        if is_branes_equal {
            for (expect_brane, expect_role) in branes {
                is_branes_equal = is_branes_equal
                    && c.branes.get(&expect_brane.get_id()).map_or(
                        false,
                        |BraneInfo { brane, role }| {
                            expect_brane == brane && expect_role == role
                        },
                    );

                if !is_branes_equal {
                    break;
                }
            }
        }
        if !is_branes_equal {
            panic!("branes: expect {:?}, but got {:?}", branes, c.branes);
        }

        let mut is_cones_equal = c.brane_cones.len() == brane_cones.len();
        is_cones_equal = is_cones_equal
            && c.brane_cones.iter().zip(brane_cones.iter()).all(
                |((actual_key, actual_id), (expect_key, expect_id))| {
                    actual_key == expect_key && actual_id == expect_id
                },
            );
        if !is_cones_equal {
            panic!(
                "brane_cones: expect {:?}, but got {:?}",
                brane_cones, c.brane_cones
            );
        }
    }

    /// Adds a set of branes to an empty collection and check if it's successfully loaded.
    fn must_load_branes(c: &mut BraneCollector, branes: &[Brane]) {
        assert!(c.branes.is_empty());
        assert!(c.brane_cones.is_empty());

        for brane in branes {
            must_create_brane(c, &brane, StateRole::Follower);
        }

        let expected_branes: Vec<_> = branes
            .iter()
            .map(|r| (r.clone(), StateRole::Follower))
            .collect();
        check_collection(&c, &expected_branes);
    }

    fn must_create_brane(c: &mut BraneCollector, brane: &Brane, role: StateRole) {
        assert!(c.branes.get(&brane.get_id()).is_none());

        c.handle_violetabftstore_event(VioletaBftStoreEvent::CreateBrane {
            brane: brane.clone(),
            role,
        });

        assert_eq!(&c.branes[&brane.get_id()].brane, brane);
        assert_eq!(
            c.brane_cones[&data_lightlike_key(brane.get_lightlike_key())],
            brane.get_id()
        );
    }

    fn must_fidelio_brane(c: &mut BraneCollector, brane: &Brane, role: StateRole) {
        let old_lightlike_key = c
            .branes
            .get(&brane.get_id())
            .map(|r| r.brane.get_lightlike_key().to_vec());

        c.handle_violetabftstore_event(VioletaBftStoreEvent::fidelioBrane {
            brane: brane.clone(),
            role,
        });

        if let Some(r) = c.branes.get(&brane.get_id()) {
            assert_eq!(r.brane, *brane);
            assert_eq!(
                c.brane_cones[&data_lightlike_key(brane.get_lightlike_key())],
                brane.get_id()
            );
        } else {
            let another_brane_id = c.brane_cones[&data_lightlike_key(brane.get_lightlike_key())];
            let version = c.branes[&another_brane_id]
                .brane
                .get_brane_epoch()
                .get_version();
            assert!(brane.get_brane_epoch().get_version() < version);
        }
        // If lightlike_key is fideliod and the brane_id corresponding to the `old_lightlike_key` doesn't equals
        // to `brane_id`, it shouldn't be removed since it was used by another brane.
        if let Some(old_lightlike_key) = old_lightlike_key {
            if old_lightlike_key.as_slice() != brane.get_lightlike_key() {
                assert!(c
                    .brane_cones
                    .get(&data_lightlike_key(&old_lightlike_key))
                    .map_or(true, |id| *id != brane.get_id()));
            }
        }
    }

    fn must_destroy_brane(c: &mut BraneCollector, brane: Brane) {
        let id = brane.get_id();
        let lightlike_key = c.branes.get(&id).map(|r| r.brane.get_lightlike_key().to_vec());

        c.handle_violetabftstore_event(VioletaBftStoreEvent::DestroyBrane { brane });

        assert!(c.branes.get(&id).is_none());
        // If the brane_id corresponding to the lightlike_key doesn't equals to `id`, it shouldn't be
        // removed since it was used by another brane.
        if let Some(lightlike_key) = lightlike_key {
            assert!(c
                .brane_cones
                .get(&data_lightlike_key(&lightlike_key))
                .map_or(true, |r| *r != id));
        }
    }

    fn must_change_role(c: &mut BraneCollector, brane: &Brane, role: StateRole) {
        c.handle_violetabftstore_event(VioletaBftStoreEvent::RoleChange {
            brane: brane.clone(),
            role,
        });

        if let Some(r) = c.branes.get(&brane.get_id()) {
            assert_eq!(r.role, role);
        }
    }

    #[test]
    fn test_ignore_invalid_version() {
        let mut c = BraneCollector::new();

        c.handle_violetabftstore_event(VioletaBftStoreEvent::CreateBrane {
            brane: new_brane(1, b"k1", b"k3", 0),
            role: StateRole::Follower,
        });
        c.handle_violetabftstore_event(VioletaBftStoreEvent::fidelioBrane {
            brane: new_brane(2, b"k2", b"k4", 0),
            role: StateRole::Follower,
        });
        c.handle_violetabftstore_event(VioletaBftStoreEvent::RoleChange {
            brane: new_brane(1, b"k1", b"k2", 0),
            role: StateRole::Leader,
        });

        check_collection(&c, &[]);
    }

    #[test]
    fn test_epoch_stale_check() {
        let branes = &[
            brane_with_conf(1, b"", b"k1", 10, 10),
            brane_with_conf(2, b"k1", b"k2", 20, 10),
            brane_with_conf(3, b"k3", b"k4", 10, 20),
            brane_with_conf(4, b"k4", b"k5", 20, 20),
            brane_with_conf(5, b"k6", b"k7", 10, 20),
            brane_with_conf(6, b"k7", b"", 20, 10),
        ];

        let mut c = BraneCollector::new();
        must_load_branes(&mut c, branes);

        assert!(c.check_brane_cone(&brane_with_conf(1, b"", b"k1", 10, 10), false));
        assert!(c.check_brane_cone(&brane_with_conf(1, b"", b"k1", 12, 10), false));
        assert!(c.check_brane_cone(&brane_with_conf(1, b"", b"k1", 10, 12), false));
        assert!(!c.check_brane_cone(&brane_with_conf(1, b"", b"k1", 8, 10), false));
        assert!(!c.check_brane_cone(&brane_with_conf(1, b"", b"k1", 10, 8), false));

        assert!(c.check_brane_cone(&brane_with_conf(3, b"k3", b"k4", 12, 20), false));
        assert!(!c.check_brane_cone(&brane_with_conf(3, b"k3", b"k4", 8, 20), false));

        assert!(c.check_brane_cone(&brane_with_conf(6, b"k7", b"", 20, 12), false));
        assert!(!c.check_brane_cone(&brane_with_conf(6, b"k7", b"", 20, 8), false));

        assert!(!c.check_brane_cone(&brane_with_conf(1, b"k7", b"", 15, 10), false));
        assert!(!c.check_brane_cone(&brane_with_conf(1, b"", b"", 19, 10), false));

        assert!(c.check_brane_cone(&brane_with_conf(7, b"k2", b"k3", 1, 1), false));

        must_fidelio_brane(
            &mut c,
            &brane_with_conf(1, b"", b"k1", 100, 100),
            StateRole::Follower,
        );
        must_fidelio_brane(
            &mut c,
            &brane_with_conf(6, b"k7", b"", 100, 100),
            StateRole::Follower,
        );
        assert!(c.check_brane_cone(&brane_with_conf(2, b"k1", b"k7", 30, 30), false));
        assert!(c.check_brane_cone(&brane_with_conf(2, b"k11", b"k61", 30, 30), false));
        assert!(!c.check_brane_cone(&brane_with_conf(2, b"k0", b"k7", 30, 30), false));
        assert!(!c.check_brane_cone(&brane_with_conf(2, b"k1", b"k8", 30, 30), false));

        must_fidelio_brane(
            &mut c,
            &brane_with_conf(2, b"k1", b"k2", 100, 100),
            StateRole::Follower,
        );
        must_fidelio_brane(
            &mut c,
            &brane_with_conf(5, b"k6", b"k7", 100, 100),
            StateRole::Follower,
        );
        assert!(c.check_brane_cone(&brane_with_conf(3, b"k2", b"k6", 30, 30), false));
        assert!(c.check_brane_cone(&brane_with_conf(3, b"k21", b"k51", 30, 30), false));
        assert!(c.check_brane_cone(&brane_with_conf(3, b"k3", b"k5", 30, 30), false));
        assert!(!c.check_brane_cone(&brane_with_conf(3, b"k11", b"k6", 30, 30), false));
        assert!(!c.check_brane_cone(&brane_with_conf(3, b"k2", b"k61", 30, 30), false));
    }

    #[test]
    fn test_clear_overlapped_branes() {
        let init_branes = vec![
            new_brane(1, b"", b"k1", 1),
            new_brane(2, b"k1", b"k2", 1),
            new_brane(3, b"k3", b"k4", 1),
            new_brane(4, b"k4", b"k5", 1),
            new_brane(5, b"k6", b"k7", 1),
            new_brane(6, b"k7", b"", 1),
        ];

        let mut c = BraneCollector::new();
        must_load_branes(&mut c, &init_branes);
        let mut branes: Vec<_> = init_branes
            .iter()
            .map(|brane| (brane.clone(), StateRole::Follower))
            .collect();

        c.check_brane_cone(&new_brane(7, b"k2", b"k3", 2), true);
        check_collection(&c, &branes);

        c.check_brane_cone(&new_brane(7, b"k31", b"k32", 2), true);
        // Remove brane 3
        branes.remove(2);
        check_collection(&c, &branes);

        c.check_brane_cone(&new_brane(7, b"k3", b"k5", 2), true);
        // Remove brane 4
        branes.remove(2);
        check_collection(&c, &branes);

        c.check_brane_cone(&new_brane(7, b"k11", b"k61", 2), true);
        // Remove brane 2 and brane 5
        branes.remove(1);
        branes.remove(1);
        check_collection(&c, &branes);

        c.check_brane_cone(&new_brane(7, b"", b"", 2), true);
        // Remove all
        check_collection(&c, &[]);

        // Test that the brane with the same id will be kept in the collection
        c = BraneCollector::new();
        must_load_branes(&mut c, &init_branes);

        c.check_brane_cone(&new_brane(3, b"k1", b"k7", 2), true);
        check_collection(
            &c,
            &[
                (init_branes[0].clone(), StateRole::Follower),
                (init_branes[2].clone(), StateRole::Follower),
                (init_branes[5].clone(), StateRole::Follower),
            ],
        );

        c.check_brane_cone(&new_brane(1, b"", b"", 2), true);
        check_collection(&c, &[(init_branes[0].clone(), StateRole::Follower)]);
    }

    #[test]
    fn test_basic_ufidelating() {
        let mut c = BraneCollector::new();
        let init_branes = &[
            new_brane(1, b"", b"k1", 1),
            new_brane(2, b"k1", b"k9", 1),
            new_brane(3, b"k9", b"", 1),
        ];

        must_load_branes(&mut c, init_branes);

        // lightlike_key changed
        must_fidelio_brane(&mut c, &new_brane(2, b"k2", b"k8", 2), StateRole::Follower);
        // lightlike_key changed (previous lightlike_key is empty)
        must_fidelio_brane(
            &mut c,
            &new_brane(3, b"k9", b"k99", 2),
            StateRole::Follower,
        );
        // lightlike_key not changed
        must_fidelio_brane(&mut c, &new_brane(1, b"k0", b"k1", 2), StateRole::Follower);
        check_collection(
            &c,
            &[
                (new_brane(1, b"k0", b"k1", 2), StateRole::Follower),
                (new_brane(2, b"k2", b"k8", 2), StateRole::Follower),
                (new_brane(3, b"k9", b"k99", 2), StateRole::Follower),
            ],
        );

        must_change_role(
            &mut c,
            &new_brane(1, b"k0", b"k1", 2),
            StateRole::Candidate,
        );
        must_create_brane(&mut c, &new_brane(5, b"k99", b"", 2), StateRole::Follower);
        must_change_role(&mut c, &new_brane(2, b"k2", b"k8", 2), StateRole::Leader);
        must_fidelio_brane(&mut c, &new_brane(2, b"k3", b"k7", 3), StateRole::Leader);
        must_create_brane(&mut c, &new_brane(4, b"k1", b"k3", 3), StateRole::Follower);
        check_collection(
            &c,
            &[
                (new_brane(1, b"k0", b"k1", 2), StateRole::Candidate),
                (new_brane(4, b"k1", b"k3", 3), StateRole::Follower),
                (new_brane(2, b"k3", b"k7", 3), StateRole::Leader),
                (new_brane(3, b"k9", b"k99", 2), StateRole::Follower),
                (new_brane(5, b"k99", b"", 2), StateRole::Follower),
            ],
        );

        must_destroy_brane(&mut c, new_brane(4, b"k1", b"k3", 3));
        must_destroy_brane(&mut c, new_brane(3, b"k9", b"k99", 2));
        check_collection(
            &c,
            &[
                (new_brane(1, b"k0", b"k1", 2), StateRole::Candidate),
                (new_brane(2, b"k3", b"k7", 3), StateRole::Leader),
                (new_brane(5, b"k99", b"", 2), StateRole::Follower),
            ],
        );
    }

    /// Simulates splitting a brane into 3 branes, and the brane with old id will be the
    /// `derive_index`-th brane of them. The events are triggered in order indicated by `seq`.
    /// This is to ensure the collection is correct, no matter what the events' order to happen is.
    /// Values in `seq` and of `derive_index` spacelike from 1.
    fn test_split_impl(derive_index: usize, seq: &[usize]) {
        let mut c = BraneCollector::new();
        let init_branes = &[
            new_brane(1, b"", b"k1", 1),
            new_brane(2, b"k1", b"k9", 1),
            new_brane(3, b"k9", b"", 1),
        ];
        must_load_branes(&mut c, init_branes);

        let mut final_branes = vec![
            new_brane(1, b"", b"k1", 1),
            new_brane(4, b"k1", b"k3", 2),
            new_brane(5, b"k3", b"k6", 2),
            new_brane(6, b"k6", b"k9", 2),
            new_brane(3, b"k9", b"", 1),
        ];
        // `derive_index` spacelikes from 1
        final_branes[derive_index].set_id(2);

        for idx in seq {
            if *idx == derive_index {
                must_fidelio_brane(&mut c, &final_branes[*idx], StateRole::Follower);
            } else {
                must_create_brane(&mut c, &final_branes[*idx], StateRole::Follower);
            }
        }

        let final_branes = final_branes
            .into_iter()
            .map(|r| (r, StateRole::Follower))
            .collect::<Vec<_>>();
        check_collection(&c, &final_branes);
    }

    #[test]
    fn test_split() {
        let indices = &[1, 2, 3];
        let orders = &[
            &[1, 2, 3],
            &[1, 3, 2],
            &[2, 1, 3],
            &[2, 3, 1],
            &[3, 1, 2],
            &[3, 2, 1],
        ];

        for index in indices {
            for order in orders {
                test_split_impl(*index, *order);
            }
        }
    }

    fn test_merge_impl(to_left: bool, fidelio_first: bool) {
        let mut c = BraneCollector::new();
        let init_branes = &[
            brane_with_conf(1, b"", b"k1", 1, 1),
            brane_with_conf(2, b"k1", b"k2", 1, 100),
            brane_with_conf(3, b"k2", b"k3", 1, 1),
            brane_with_conf(4, b"k3", b"", 1, 100),
        ];
        must_load_branes(&mut c, init_branes);

        let (mut ufidelating_brane, destroying_brane) = if to_left {
            (init_branes[1].clone(), init_branes[2].clone())
        } else {
            (init_branes[2].clone(), init_branes[1].clone())
        };
        ufidelating_brane.set_spacelike_key(b"k1".to_vec());
        ufidelating_brane.set_lightlike_key(b"k3".to_vec());
        ufidelating_brane.mut_brane_epoch().set_version(2);

        if fidelio_first {
            must_fidelio_brane(&mut c, &ufidelating_brane, StateRole::Follower);
            must_destroy_brane(&mut c, destroying_brane);
        } else {
            must_destroy_brane(&mut c, destroying_brane);
            must_fidelio_brane(&mut c, &ufidelating_brane, StateRole::Follower);
        }

        let final_branes = &[
            (brane_with_conf(1, b"", b"k1", 1, 1), StateRole::Follower),
            (ufidelating_brane, StateRole::Follower),
            (brane_with_conf(4, b"k3", b"", 1, 100), StateRole::Follower),
        ];
        check_collection(&c, final_branes);
    }

    #[test]
    fn test_merge() {
        test_merge_impl(false, false);
        test_merge_impl(false, true);
        test_merge_impl(true, false);
        test_merge_impl(true, true);
    }

    #[test]
    fn test_extreme_cases() {
        let mut c = BraneCollector::new();
        let init_branes = &[
            new_brane(1, b"", b"k1", 1),
            new_brane(2, b"k1", b"k9", 1),
            new_brane(3, b"k9", b"", 1),
        ];
        must_load_branes(&mut c, init_branes);

        // While splitting, brane 4 created but brane 2 still has an `fidelio` event which haven't
        // been handled.
        must_create_brane(&mut c, &new_brane(4, b"k5", b"k9", 2), StateRole::Follower);
        must_fidelio_brane(&mut c, &new_brane(2, b"k1", b"k9", 1), StateRole::Follower);
        must_change_role(&mut c, &new_brane(2, b"k1", b"k9", 1), StateRole::Leader);
        must_fidelio_brane(&mut c, &new_brane(2, b"k1", b"k5", 2), StateRole::Leader);
        // TODO: In fact, brane 2's role should be follower. However because it's previous state was
        // removed while creating ufidelating brane 4, it can't be successfully fideliod. Fortunately
        // this case may hardly happen so it can be fixed later.
        check_collection(
            &c,
            &[
                (new_brane(1, b"", b"k1", 1), StateRole::Follower),
                (new_brane(2, b"k1", b"k5", 2), StateRole::Leader),
                (new_brane(4, b"k5", b"k9", 2), StateRole::Follower),
                (new_brane(3, b"k9", b"", 1), StateRole::Follower),
            ],
        );

        // While merging, brane 2 expanded and covered brane 4 (and their lightlike key become the same)
        // but brane 4 still has an `fidelio` event which haven't been handled.
        must_fidelio_brane(&mut c, &new_brane(2, b"k1", b"k9", 3), StateRole::Leader);
        must_fidelio_brane(&mut c, &new_brane(4, b"k5", b"k9", 2), StateRole::Follower);
        must_change_role(&mut c, &new_brane(4, b"k5", b"k9", 2), StateRole::Leader);
        must_destroy_brane(&mut c, new_brane(4, b"k5", b"k9", 2));
        check_collection(
            &c,
            &[
                (new_brane(1, b"", b"k1", 1), StateRole::Follower),
                (new_brane(2, b"k1", b"k9", 3), StateRole::Leader),
                (new_brane(3, b"k9", b"", 1), StateRole::Follower),
            ],
        );
    }
}
