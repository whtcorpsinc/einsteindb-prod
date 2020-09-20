// Copyright 2020 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

use engine_promises::{CfName, KvEngine};
use ekvproto::metapb::Brane;
use ekvproto::fidelpb::CheckPolicy;
use ekvproto::raft_cmdpb::{ComputeHashRequest, VioletaBftCmdRequest};

use super::*;
use crate::store::CasualRouter;

struct Entry<T> {
    priority: u32,
    observer: T,
}

impl<T: Clone> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            priority: self.priority,
            observer: self.observer.clone(),
        }
    }
}

pub trait ClonableObserver: 'static + Slightlike {
    type Ob: ?Sized + Slightlike;
    fn inner(&self) -> &Self::Ob;
    fn inner_mut(&mut self) -> &mut Self::Ob;
    fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Slightlike>;
}

macro_rules! impl_box_observer {
    ($name:ident, $ob: ident, $wrapper: ident) => {
        pub struct $name(Box<dyn ClonableObserver<Ob = dyn $ob> + Slightlike>);
        impl $name {
            pub fn new<T: 'static + $ob + Clone>(observer: T) -> $name {
                $name(Box::new($wrapper { inner: observer }))
            }
        }
        impl Clone for $name {
            fn clone(&self) -> $name {
                $name((**self).box_clone())
            }
        }
        impl Deref for $name {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob> + Slightlike>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob> + Slightlike> {
                &self.0
            }
        }

        struct $wrapper<T: $ob + Clone> {
            inner: T,
        }
        impl<T: 'static + $ob + Clone> ClonableObserver for $wrapper<T> {
            type Ob = dyn $ob;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Slightlike> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                })
            }
        }
    };
}

// This is the same as impl_box_observer_g except $ob has a typaram
macro_rules! impl_box_observer_g {
    ($name:ident, $ob: ident, $wrapper: ident) => {
        pub struct $name<E: KvEngine>(Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Slightlike>);
        impl<E: KvEngine + 'static + Slightlike> $name<E> {
            pub fn new<T: 'static + $ob<E> + Clone>(observer: T) -> $name<E> {
                $name(Box::new($wrapper {
                    inner: observer,
                    _phantom: PhantomData,
                }))
            }
        }
        impl<E: KvEngine + 'static> Clone for $name<E> {
            fn clone(&self) -> $name<E> {
                $name((**self).box_clone())
            }
        }
        impl<E: KvEngine> Deref for $name<E> {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Slightlike>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Slightlike> {
                &self.0
            }
        }

        struct $wrapper<E: KvEngine, T: $ob<E> + Clone> {
            inner: T,
            _phantom: PhantomData<E>,
        }
        impl<E: KvEngine + 'static + Slightlike, T: 'static + $ob<E> + Clone> ClonableObserver
            for $wrapper<E, T>
        {
            type Ob = dyn $ob<E>;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Slightlike> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                    _phantom: PhantomData,
                })
            }
        }
    };
}

impl_box_observer!(BoxAdminObserver, AdminObserver, WrappedAdminObserver);
impl_box_observer!(BoxQueryObserver, QueryObserver, WrappedQueryObserver);
impl_box_observer!(
    BoxApplySnapshotObserver,
    ApplySnapshotObserver,
    WrappedApplySnapshotObserver
);
impl_box_observer_g!(
    BoxSplitCheckObserver,
    SplitCheckObserver,
    WrappedSplitCheckObserver
);
impl_box_observer!(BoxRoleObserver, RoleObserver, WrappedRoleObserver);
impl_box_observer!(
    BoxBraneChangeObserver,
    BraneChangeObserver,
    WrappedBraneChangeObserver
);
impl_box_observer_g!(BoxCmdObserver, CmdObserver, WrappedCmdObserver);
impl_box_observer_g!(
    BoxConsistencyCheckObserver,
    ConsistencyCheckObserver,
    WrappedConsistencyCheckObserver
);

/// Registry contains all registered interlocks.
#[derive(Clone)]
pub struct Registry<E>
where
    E: KvEngine + 'static,
{
    admin_observers: Vec<Entry<BoxAdminObserver>>,
    query_observers: Vec<Entry<BoxQueryObserver>>,
    apply_snapshot_observers: Vec<Entry<BoxApplySnapshotObserver>>,
    split_check_observers: Vec<Entry<BoxSplitCheckObserver<E>>>,
    consistency_check_observers: Vec<Entry<BoxConsistencyCheckObserver<E>>>,
    role_observers: Vec<Entry<BoxRoleObserver>>,
    brane_change_observers: Vec<Entry<BoxBraneChangeObserver>>,
    cmd_observers: Vec<Entry<BoxCmdObserver<E>>>,
    // TODO: add lightlikepoint
}

impl<E: KvEngine> Default for Registry<E> {
    fn default() -> Registry<E> {
        Registry {
            admin_observers: Default::default(),
            query_observers: Default::default(),
            apply_snapshot_observers: Default::default(),
            split_check_observers: Default::default(),
            consistency_check_observers: Default::default(),
            role_observers: Default::default(),
            brane_change_observers: Default::default(),
            cmd_observers: Default::default(),
        }
    }
}

macro_rules! push {
    ($p:expr, $t:ident, $vec:expr) => {
        $t.inner().spacelike();
        let e = Entry {
            priority: $p,
            observer: $t,
        };
        let vec = &mut $vec;
        vec.push(e);
        vec.sort_by(|l, r| l.priority.cmp(&r.priority));
    };
}

impl<E: KvEngine> Registry<E> {
    pub fn register_admin_observer(&mut self, priority: u32, ao: BoxAdminObserver) {
        push!(priority, ao, self.admin_observers);
    }

    pub fn register_query_observer(&mut self, priority: u32, qo: BoxQueryObserver) {
        push!(priority, qo, self.query_observers);
    }

    pub fn register_apply_snapshot_observer(
        &mut self,
        priority: u32,
        aso: BoxApplySnapshotObserver,
    ) {
        push!(priority, aso, self.apply_snapshot_observers);
    }

    pub fn register_split_check_observer(&mut self, priority: u32, sco: BoxSplitCheckObserver<E>) {
        push!(priority, sco, self.split_check_observers);
    }

    pub fn register_consistency_check_observer(
        &mut self,
        priority: u32,
        cco: BoxConsistencyCheckObserver<E>,
    ) {
        push!(priority, cco, self.consistency_check_observers);
    }

    pub fn register_role_observer(&mut self, priority: u32, ro: BoxRoleObserver) {
        push!(priority, ro, self.role_observers);
    }

    pub fn register_brane_change_observer(&mut self, priority: u32, rlo: BoxBraneChangeObserver) {
        push!(priority, rlo, self.brane_change_observers);
    }

    pub fn register_cmd_observer(&mut self, priority: u32, rlo: BoxCmdObserver<E>) {
        push!(priority, rlo, self.cmd_observers);
    }
}

/// A macro that loops over all observers and returns early when error is found or
/// bypass is set. `try_loop_ob` is expected to be used for hook that returns a `Result`.
macro_rules! try_loop_ob {
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _res, $r, $obs, $hook, $($args)*)
    };
}

/// A macro that loops over all observers and returns early when bypass is set.
///
/// Using a macro so we don't need to write tests for every observers.
macro_rules! loop_ob {
    // Execute a hook, return early if error is found.
    (_exec _res, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)?
    };
    // Execute a hook.
    (_exec _tup, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)
    };
    // When the try loop finishes successfully, the value to be returned.
    (_done _res) => {
        Ok(())
    };
    // When the loop finishes successfully, the value to be returned.
    (_done _tup) => {{}};
    // Actual implementation of the for loop.
    (_imp $res_type:tt, $r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {{
        let mut ctx = ObserverContext::new($r);
        for o in $obs {
            loop_ob!(_exec $res_type, o.observer, $hook, &mut ctx, $($args)*);
            if ctx.bypass {
                break;
            }
        }
        loop_ob!(_done $res_type)
    }};
    // Loop over all observers and return early when bypass is set.
    // This macro is expected to be used for hook that returns `()`.
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _tup, $r, $obs, $hook, $($args)*)
    };
}

/// Admin and invoke all interlocks.
#[derive(Clone)]
pub struct InterlockHost<E>
where
    E: KvEngine + 'static,
{
    pub registry: Registry<E>,
}

impl<E: KvEngine> Default for InterlockHost<E>
where
    E: 'static,
{
    fn default() -> Self {
        InterlockHost {
            registry: Default::default(),
        }
    }
}

impl<E: KvEngine> InterlockHost<E> {
    pub fn new<C: CasualRouter<E> + Clone + Slightlike + 'static>(ch: C) -> InterlockHost<E> {
        let mut registry = Registry::default();
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(ch.clone())),
        );
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(TuplespaceInstantonCheckObserver::new(ch)),
        );
        // TableCheckObserver has higher priority than SizeCheckObserver.
        registry.register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        registry.register_split_check_observer(
            400,
            BoxSplitCheckObserver::new(TableCheckObserver::default()),
        );
        InterlockHost { registry }
    }

    /// Call all propose hooks until bypass is set to true.
    pub fn pre_propose(&self, brane: &Brane, req: &mut VioletaBftCmdRequest) -> Result<()> {
        if !req.has_admin_request() {
            let query = req.mut_requests();
            let mut vec_query = mem::take(query).into();
            let result = try_loop_ob!(
                brane,
                &self.registry.query_observers,
                pre_propose_query,
                &mut vec_query,
            );
            *query = vec_query.into();
            result
        } else {
            let admin = req.mut_admin_request();
            try_loop_ob!(
                brane,
                &self.registry.admin_observers,
                pre_propose_admin,
                admin
            )
        }
    }

    /// Call all pre apply hook until bypass is set to true.
    pub fn pre_apply(&self, brane: &Brane, req: &VioletaBftCmdRequest) {
        if !req.has_admin_request() {
            let query = req.get_requests();
            loop_ob!(
                brane,
                &self.registry.query_observers,
                pre_apply_query,
                query,
            );
        } else {
            let admin = req.get_admin_request();
            loop_ob!(
                brane,
                &self.registry.admin_observers,
                pre_apply_admin,
                admin
            );
        }
    }

    pub fn post_apply(&self, brane: &Brane, cmd: &mut Cmd) {
        if !cmd.response.has_admin_response() {
            loop_ob!(
                brane,
                &self.registry.query_observers,
                post_apply_query,
                cmd,
            );
        } else {
            let admin = cmd.response.mut_admin_response();
            loop_ob!(
                brane,
                &self.registry.admin_observers,
                post_apply_admin,
                admin
            );
        }
    }

    pub fn post_apply_plain_kvs_from_snapshot(
        &self,
        brane: &Brane,
        causet: CfName,
        kv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        loop_ob!(
            brane,
            &self.registry.apply_snapshot_observers,
            apply_plain_kvs,
            causet,
            kv_pairs
        );
    }

    pub fn post_apply_sst_from_snapshot(&self, brane: &Brane, causet: CfName, path: &str) {
        loop_ob!(
            brane,
            &self.registry.apply_snapshot_observers,
            apply_sst,
            causet,
            path
        );
    }

    pub fn new_split_checker_host<'a>(
        &self,
        causetg: &'a Config,
        brane: &Brane,
        engine: &E,
        auto_split: bool,
        policy: CheckPolicy,
    ) -> SplitCheckerHost<'a, E> {
        let mut host = SplitCheckerHost::new(auto_split, causetg);
        loop_ob!(
            brane,
            &self.registry.split_check_observers,
            add_checker,
            &mut host,
            engine,
            policy
        );
        host
    }

    pub fn on_prepropose_compute_hash(&self, req: &mut ComputeHashRequest) {
        for observer in &self.registry.consistency_check_observers {
            let observer = observer.observer.inner();
            if observer.ufidelate_context(req.mut_context()) {
                break;
            }
        }
    }

    pub fn on_compute_hash(
        &self,
        brane: &Brane,
        context: &[u8],
        snap: E::Snapshot,
    ) -> Result<Vec<(Vec<u8>, u32)>> {
        let mut hashes = Vec::new();
        let (mut reader, context_len) = (context, context.len());
        for observer in &self.registry.consistency_check_observers {
            let observer = observer.observer.inner();
            let old_len = reader.len();
            let hash = match box_try!(observer.compute_hash(brane, &mut reader, &snap)) {
                Some(hash) => hash,
                None => break,
            };
            let new_len = reader.len();
            let ctx = context[context_len - old_len..context_len - new_len].to_vec();
            hashes.push((ctx, hash));
        }
        Ok(hashes)
    }

    pub fn on_role_change(&self, brane: &Brane, role: StateRole) {
        loop_ob!(brane, &self.registry.role_observers, on_role_change, role);
    }

    pub fn on_brane_changed(&self, brane: &Brane, event: BraneChangeEvent, role: StateRole) {
        loop_ob!(
            brane,
            &self.registry.brane_change_observers,
            on_brane_changed,
            event,
            role
        );
    }

    pub fn prepare_for_apply(&self, observe_id: ObserveID, brane_id: u64) {
        for cmd_ob in &self.registry.cmd_observers {
            cmd_ob
                .observer
                .inner()
                .on_prepare_for_apply(observe_id, brane_id);
        }
    }

    pub fn on_apply_cmd(&self, observe_id: ObserveID, brane_id: u64, cmd: Cmd) {
        assert!(
            !self.registry.cmd_observers.is_empty(),
            "CmdObserver is not registered"
        );
        for i in 0..self.registry.cmd_observers.len() - 1 {
            self.registry
                .cmd_observers
                .get(i)
                .unwrap()
                .observer
                .inner()
                .on_apply_cmd(observe_id, brane_id, cmd.clone())
        }
        self.registry
            .cmd_observers
            .last()
            .unwrap()
            .observer
            .inner()
            .on_apply_cmd(observe_id, brane_id, cmd)
    }

    pub fn on_flush_apply(&self, engine: E) {
        if self.registry.cmd_observers.is_empty() {
            return;
        }

        for i in 0..self.registry.cmd_observers.len() - 1 {
            self.registry
                .cmd_observers
                .get(i)
                .unwrap()
                .observer
                .inner()
                .on_flush_apply(engine.clone())
        }
        self.registry
            .cmd_observers
            .last()
            .unwrap()
            .observer
            .inner()
            .on_flush_apply(engine)
    }

    pub fn shutdown(&self) {
        for entry in &self.registry.admin_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.query_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.split_check_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.cmd_observers {
            entry.observer.inner().stop();
        }
    }
}

#[causetg(test)]
mod tests {
    use crate::interlock::*;
    use std::sync::atomic::*;
    use std::sync::Arc;

    use engine_panic::PanicEngine;
    use ekvproto::metapb::Brane;
    use ekvproto::raft_cmdpb::{
        AdminRequest, AdminResponse, VioletaBftCmdRequest, VioletaBftCmdResponse, Request,
    };

    #[derive(Clone, Default)]
    struct TestInterlock {
        bypass: Arc<AtomicBool>,
        called: Arc<AtomicUsize>,
        return_err: Arc<AtomicBool>,
    }

    impl Interlock for TestInterlock {}

    impl AdminObserver for TestInterlock {
        fn pre_propose_admin(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut AdminRequest,
        ) -> Result<()> {
            self.called.fetch_add(1, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &AdminRequest) {
            self.called.fetch_add(2, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &mut AdminResponse) {
            self.called.fetch_add(3, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl QueryObserver for TestInterlock {
        fn pre_propose_query(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut Vec<Request>,
        ) -> Result<()> {
            self.called.fetch_add(4, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &[Request]) {
            self.called.fetch_add(5, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &mut Cmd) {
            self.called.fetch_add(6, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RoleObserver for TestInterlock {
        fn on_role_change(&self, ctx: &mut ObserverContext<'_>, _: StateRole) {
            self.called.fetch_add(7, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl BraneChangeObserver for TestInterlock {
        fn on_brane_changed(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: BraneChangeEvent,
            _: StateRole,
        ) {
            self.called.fetch_add(8, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl ApplySnapshotObserver for TestInterlock {
        fn apply_plain_kvs(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: CfName,
            _: &[(Vec<u8>, Vec<u8>)],
        ) {
            self.called.fetch_add(9, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn apply_sst(&self, ctx: &mut ObserverContext<'_>, _: CfName, _: &str) {
            self.called.fetch_add(10, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl CmdObserver<PanicEngine> for TestInterlock {
        fn on_prepare_for_apply(&self, _: ObserveID, _: u64) {
            self.called.fetch_add(11, Ordering::SeqCst);
        }
        fn on_apply_cmd(&self, _: ObserveID, _: u64, _: Cmd) {
            self.called.fetch_add(12, Ordering::SeqCst);
        }
        fn on_flush_apply(&self, _: PanicEngine) {
            self.called.fetch_add(13, Ordering::SeqCst);
        }
    }

    macro_rules! assert_all {
        ($target:expr, $expect:expr) => {{
            for (c, e) in ($target).iter().zip($expect) {
                assert_eq!(c.load(Ordering::SeqCst), *e);
            }
        }};
    }

    macro_rules! set_all {
        ($target:expr, $v:expr) => {{
            for v in $target {
                v.store($v, Ordering::SeqCst);
            }
        }};
    }

    #[test]
    fn test_trigger_right_hook() {
        let mut host = InterlockHost::<PanicEngine>::default();
        let ob = TestInterlock::default();
        host.registry
            .register_admin_observer(1, BoxAdminObserver::new(ob.clone()));
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(ob.clone()));
        host.registry
            .register_apply_snapshot_observer(1, BoxApplySnapshotObserver::new(ob.clone()));
        host.registry
            .register_role_observer(1, BoxRoleObserver::new(ob.clone()));
        host.registry
            .register_brane_change_observer(1, BoxBraneChangeObserver::new(ob.clone()));
        host.registry
            .register_cmd_observer(1, BoxCmdObserver::new(ob.clone()));
        let brane = Brane::default();
        let mut admin_req = VioletaBftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        host.pre_propose(&brane, &mut admin_req).unwrap();
        assert_all!(&[&ob.called], &[1]);
        host.pre_apply(&brane, &admin_req);
        assert_all!(&[&ob.called], &[3]);
        let mut admin_resp = VioletaBftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        host.post_apply(&brane, &mut Cmd::new(0, admin_req, admin_resp));
        assert_all!(&[&ob.called], &[6]);

        let mut query_req = VioletaBftCmdRequest::default();
        query_req.set_requests(vec![Request::default()].into());
        host.pre_propose(&brane, &mut query_req).unwrap();
        assert_all!(&[&ob.called], &[10]);
        host.pre_apply(&brane, &query_req);
        assert_all!(&[&ob.called], &[15]);
        let query_resp = VioletaBftCmdResponse::default();
        host.post_apply(&brane, &mut Cmd::new(0, query_req, query_resp));
        assert_all!(&[&ob.called], &[21]);

        host.on_role_change(&brane, StateRole::Leader);
        assert_all!(&[&ob.called], &[28]);

        host.on_brane_changed(&brane, BraneChangeEvent::Create, StateRole::Follower);
        assert_all!(&[&ob.called], &[36]);

        host.post_apply_plain_kvs_from_snapshot(&brane, "default", &[]);
        assert_all!(&[&ob.called], &[45]);
        host.post_apply_sst_from_snapshot(&brane, "default", "");
        assert_all!(&[&ob.called], &[55]);
        let observe_id = ObserveID::new();
        host.prepare_for_apply(observe_id, 0);
        assert_all!(&[&ob.called], &[66]);
        host.on_apply_cmd(
            observe_id,
            0,
            Cmd::new(0, VioletaBftCmdRequest::default(), VioletaBftCmdResponse::default()),
        );
        assert_all!(&[&ob.called], &[78]);
        host.on_flush_apply(PanicEngine);
        assert_all!(&[&ob.called], &[91]);
    }

    #[test]
    fn test_order() {
        let mut host = InterlockHost::<PanicEngine>::default();

        let ob1 = TestInterlock::default();
        host.registry
            .register_admin_observer(3, BoxAdminObserver::new(ob1.clone()));
        host.registry
            .register_query_observer(3, BoxQueryObserver::new(ob1.clone()));
        let ob2 = TestInterlock::default();
        host.registry
            .register_admin_observer(2, BoxAdminObserver::new(ob2.clone()));
        host.registry
            .register_query_observer(2, BoxQueryObserver::new(ob2.clone()));

        let brane = Brane::default();
        let mut admin_req = VioletaBftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        let mut admin_resp = VioletaBftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        let query_req = VioletaBftCmdRequest::default();
        let query_resp = VioletaBftCmdResponse::default();

        let cases = vec![(0, admin_req, admin_resp), (3, query_req, query_resp)];

        for (base_score, mut req, resp) in cases {
            set_all!(&[&ob1.return_err, &ob2.return_err], false);
            set_all!(&[&ob1.called, &ob2.called], 0);
            set_all!(&[&ob1.bypass, &ob2.bypass], true);

            host.pre_propose(&brane, &mut req).unwrap();

            // less means more.
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score + 1]);

            host.pre_apply(&brane, &req);
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score * 2 + 3]);

            host.post_apply(&brane, &mut Cmd::new(0, req.clone(), resp.clone()));
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score * 3 + 6]);

            set_all!(&[&ob2.bypass], false);
            set_all!(&[&ob2.called], 0);

            host.pre_propose(&brane, &mut req).unwrap();

            assert_all!(
                &[&ob1.called, &ob2.called],
                &[base_score + 1, base_score + 1]
            );

            set_all!(&[&ob1.called, &ob2.called], 0);

            // when return error, following interlock should not be run.
            set_all!(&[&ob2.return_err], true);
            host.pre_propose(&brane, &mut req).unwrap_err();
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score + 1]);
        }
    }
}
