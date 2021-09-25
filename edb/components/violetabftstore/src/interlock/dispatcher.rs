// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

use edb::{CfName, CausetEngine};
use ekvproto::meta_timeshare::Brane;
use ekvproto::fidel_timeshare::CheckPolicy;
use ekvproto::violetabft_cmd_timeshare::{ComputeHashRequest, VioletaBftCmdRequest};

use super::*;
use crate::store::CasualRouter;

struct Entry<T> {
    priority: u32,
    semaphore: T,
}

impl<T: Clone> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            priority: self.priority,
            semaphore: self.semaphore.clone(),
        }
    }
}

pub trait ClonableSemaphore: 'static + lightlike {
    type Ob: ?Sized + lightlike;
    fn inner(&self) -> &Self::Ob;
    fn causet_set_mut(&mut self) -> &mut Self::Ob;
    fn box_clone(&self) -> Box<dyn ClonableSemaphore<Ob = Self::Ob> + lightlike>;
}

macro_rules! impl_box_semaphore {
    ($name:ident, $ob: ident, $wrapper: ident) => {
        pub struct $name(Box<dyn ClonableSemaphore<Ob = dyn $ob> + lightlike>);
        impl $name {
            pub fn new<T: 'static + $ob + Clone>(semaphore: T) -> $name {
                $name(Box::new($wrapper { inner: semaphore }))
            }
        }
        impl Clone for $name {
            fn clone(&self) -> $name {
                $name((**self).box_clone())
            }
        }
        impl Deref for $name {
            type Target = Box<dyn ClonableSemaphore<Ob = dyn $ob> + lightlike>;

            fn deref(&self) -> &Box<dyn ClonableSemaphore<Ob = dyn $ob> + lightlike> {
                &self.0
            }
        }

        struct $wrapper<T: $ob + Clone> {
            inner: T,
        }
        impl<T: 'static + $ob + Clone> ClonableSemaphore for $wrapper<T> {
            type Ob = dyn $ob;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn causet_set_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableSemaphore<Ob = Self::Ob> + lightlike> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                })
            }
        }
    };
}

// This is the same as impl_box_semaphore_g except $ob has a typaram
macro_rules! impl_box_semaphore_g {
    ($name:ident, $ob: ident, $wrapper: ident) => {
        pub struct $name<E: CausetEngine>(Box<dyn ClonableSemaphore<Ob = dyn $ob<E>> + lightlike>);
        impl<E: CausetEngine + 'static + lightlike> $name<E> {
            pub fn new<T: 'static + $ob<E> + Clone>(semaphore: T) -> $name<E> {
                $name(Box::new($wrapper {
                    inner: semaphore,
                    _phantom: PhantomData,
                }))
            }
        }
        impl<E: CausetEngine + 'static> Clone for $name<E> {
            fn clone(&self) -> $name<E> {
                $name((**self).box_clone())
            }
        }
        impl<E: CausetEngine> Deref for $name<E> {
            type Target = Box<dyn ClonableSemaphore<Ob = dyn $ob<E>> + lightlike>;

            fn deref(&self) -> &Box<dyn ClonableSemaphore<Ob = dyn $ob<E>> + lightlike> {
                &self.0
            }
        }

        struct $wrapper<E: CausetEngine, T: $ob<E> + Clone> {
            inner: T,
            _phantom: PhantomData<E>,
        }
        impl<E: CausetEngine + 'static + lightlike, T: 'static + $ob<E> + Clone> ClonableSemaphore
            for $wrapper<E, T>
        {
            type Ob = dyn $ob<E>;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn causet_set_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableSemaphore<Ob = Self::Ob> + lightlike> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                    _phantom: PhantomData,
                })
            }
        }
    };
}

impl_box_semaphore!(BoxAdminSemaphore, AdminSemaphore, WrappedAdminSemaphore);
impl_box_semaphore!(BoxQuerySemaphore, QuerySemaphore, WrappedQuerySemaphore);
impl_box_semaphore!(
    BoxApplySnapshotSemaphore,
    ApplySnapshotSemaphore,
    WrappedApplySnapshotSemaphore
);
impl_box_semaphore_g!(
    BoxSplitCheckSemaphore,
    SplitCheckSemaphore,
    WrappedSplitCheckSemaphore
);
impl_box_semaphore!(BoxRoleSemaphore, RoleSemaphore, WrappedRoleSemaphore);
impl_box_semaphore!(
    BoxBraneChangeSemaphore,
    BraneChangeSemaphore,
    WrappedBraneChangeSemaphore
);
impl_box_semaphore_g!(BoxCmdSemaphore, CmdSemaphore, WrappedCmdSemaphore);
impl_box_semaphore_g!(
    BoxConsistencyCheckSemaphore,
    ConsistencyCheckSemaphore,
    WrappedConsistencyCheckSemaphore
);

/// Registry contains all registered interlocks.
#[derive(Clone)]
pub struct Registry<E>
where
    E: CausetEngine + 'static,
{
    admin_semaphores: Vec<Entry<BoxAdminSemaphore>>,
    query_semaphores: Vec<Entry<BoxQuerySemaphore>>,
    apply_snapshot_semaphores: Vec<Entry<BoxApplySnapshotSemaphore>>,
    split_check_semaphores: Vec<Entry<BoxSplitCheckSemaphore<E>>>,
    consistency_check_semaphores: Vec<Entry<BoxConsistencyCheckSemaphore<E>>>,
    role_semaphores: Vec<Entry<BoxRoleSemaphore>>,
    brane_change_semaphores: Vec<Entry<BoxBraneChangeSemaphore>>,
    cmd_semaphores: Vec<Entry<BoxCmdSemaphore<E>>>,
    // TODO: add lightlikepoint
}

impl<E: CausetEngine> Default for Registry<E> {
    fn default() -> Registry<E> {
        Registry {
            admin_semaphores: Default::default(),
            query_semaphores: Default::default(),
            apply_snapshot_semaphores: Default::default(),
            split_check_semaphores: Default::default(),
            consistency_check_semaphores: Default::default(),
            role_semaphores: Default::default(),
            brane_change_semaphores: Default::default(),
            cmd_semaphores: Default::default(),
        }
    }
}

macro_rules! push {
    ($p:expr, $t:ident, $vec:expr) => {
        $t.inner().spacelike();
        let e = Entry {
            priority: $p,
            semaphore: $t,
        };
        let vec = &mut $vec;
        vec.push(e);
        vec.sort_by(|l, r| l.priority.cmp(&r.priority));
    };
}

impl<E: CausetEngine> Registry<E> {
    pub fn register_admin_semaphore(&mut self, priority: u32, ao: BoxAdminSemaphore) {
        push!(priority, ao, self.admin_semaphores);
    }

    pub fn register_query_semaphore(&mut self, priority: u32, qo: BoxQuerySemaphore) {
        push!(priority, qo, self.query_semaphores);
    }

    pub fn register_apply_snapshot_semaphore(
        &mut self,
        priority: u32,
        aso: BoxApplySnapshotSemaphore,
    ) {
        push!(priority, aso, self.apply_snapshot_semaphores);
    }

    pub fn register_split_check_semaphore(&mut self, priority: u32, sco: BoxSplitCheckSemaphore<E>) {
        push!(priority, sco, self.split_check_semaphores);
    }

    pub fn register_consistency_check_semaphore(
        &mut self,
        priority: u32,
        cco: BoxConsistencyCheckSemaphore<E>,
    ) {
        push!(priority, cco, self.consistency_check_semaphores);
    }

    pub fn register_role_semaphore(&mut self, priority: u32, ro: BoxRoleSemaphore) {
        push!(priority, ro, self.role_semaphores);
    }

    pub fn register_brane_change_semaphore(&mut self, priority: u32, rlo: BoxBraneChangeSemaphore) {
        push!(priority, rlo, self.brane_change_semaphores);
    }

    pub fn register_cmd_semaphore(&mut self, priority: u32, rlo: BoxCmdSemaphore<E>) {
        push!(priority, rlo, self.cmd_semaphores);
    }
}

/// A macro that loops over all semaphores and returns early when error is found or
/// bypass is set. `try_loop_ob` is expected to be used for hook that returns a `Result`.
macro_rules! try_loop_ob {
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _res, $r, $obs, $hook, $($args)*)
    };
}

/// A macro that loops over all semaphores and returns early when bypass is set.
///
/// Using a macro so we don't need to write tests for every semaphores.
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
        let mut ctx = SemaphoreContext::new($r);
        for o in $obs {
            loop_ob!(_exec $res_type, o.semaphore, $hook, &mut ctx, $($args)*);
            if ctx.bypass {
                break;
            }
        }
        loop_ob!(_done $res_type)
    }};
    // Loop over all semaphores and return early when bypass is set.
    // This macro is expected to be used for hook that returns `()`.
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _tup, $r, $obs, $hook, $($args)*)
    };
}

/// Admin and invoke all interlocks.
#[derive(Clone)]
pub struct InterlockHost<E>
where
    E: CausetEngine + 'static,
{
    pub registry: Registry<E>,
}

impl<E: CausetEngine> Default for InterlockHost<E>
where
    E: 'static,
{
    fn default() -> Self {
        InterlockHost {
            registry: Default::default(),
        }
    }
}

impl<E: CausetEngine> InterlockHost<E> {
    pub fn new<C: CasualRouter<E> + Clone + lightlike + 'static>(ch: C) -> InterlockHost<E> {
        let mut registry = Registry::default();
        registry.register_split_check_semaphore(
            200,
            BoxSplitCheckSemaphore::new(SizeCheckSemaphore::new(ch.clone())),
        );
        registry.register_split_check_semaphore(
            200,
            BoxSplitCheckSemaphore::new(TuplespaceInstantonCheckSemaphore::new(ch)),
        );
        // BlockCheckSemaphore has higher priority than SizeCheckSemaphore.
        registry.register_split_check_semaphore(100, BoxSplitCheckSemaphore::new(HalfCheckSemaphore));
        registry.register_split_check_semaphore(
            400,
            BoxSplitCheckSemaphore::new(BlockCheckSemaphore::default()),
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
                &self.registry.query_semaphores,
                pre_propose_query,
                &mut vec_query,
            );
            *query = vec_query.into();
            result
        } else {
            let admin = req.mut_admin_request();
            try_loop_ob!(
                brane,
                &self.registry.admin_semaphores,
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
                &self.registry.query_semaphores,
                pre_apply_query,
                query,
            );
        } else {
            let admin = req.get_admin_request();
            loop_ob!(
                brane,
                &self.registry.admin_semaphores,
                pre_apply_admin,
                admin
            );
        }
    }

    pub fn post_apply(&self, brane: &Brane, cmd: &mut Cmd) {
        if !cmd.response.has_admin_response() {
            loop_ob!(
                brane,
                &self.registry.query_semaphores,
                post_apply_query,
                cmd,
            );
        } else {
            let admin = cmd.response.mut_admin_response();
            loop_ob!(
                brane,
                &self.registry.admin_semaphores,
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
            &self.registry.apply_snapshot_semaphores,
            apply_plain_kvs,
            causet,
            kv_pairs
        );
    }

    pub fn post_apply_sst_from_snapshot(&self, brane: &Brane, causet: CfName, path: &str) {
        loop_ob!(
            brane,
            &self.registry.apply_snapshot_semaphores,
            apply_sst,
            causet,
            path
        );
    }

    pub fn new_split_checker_host<'a>(
        &self,
        causet: &'a Config,
        brane: &Brane,
        engine: &E,
        auto_split: bool,
        policy: CheckPolicy,
    ) -> SplitCheckerHost<'a, E> {
        let mut host = SplitCheckerHost::new(auto_split, causet);
        loop_ob!(
            brane,
            &self.registry.split_check_semaphores,
            add_checker,
            &mut host,
            engine,
            policy
        );
        host
    }

    pub fn on_prepropose_compute_hash(&self, req: &mut ComputeHashRequest) {
        for semaphore in &self.registry.consistency_check_semaphores {
            let semaphore = semaphore.semaphore.inner();
            if semaphore.fidelio_context(req.mut_context()) {
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
        for semaphore in &self.registry.consistency_check_semaphores {
            let semaphore = semaphore.semaphore.inner();
            let old_len = reader.len();
            let hash = match box_try!(semaphore.compute_hash(brane, &mut reader, &snap)) {
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
        loop_ob!(brane, &self.registry.role_semaphores, on_role_change, role);
    }

    pub fn on_brane_changed(&self, brane: &Brane, event: BraneChangeEvent, role: StateRole) {
        loop_ob!(
            brane,
            &self.registry.brane_change_semaphores,
            on_brane_changed,
            event,
            role
        );
    }

    pub fn prepare_for_apply(&self, observe_id: ObserveID, brane_id: u64) {
        for cmd_ob in &self.registry.cmd_semaphores {
            cmd_ob
                .semaphore
                .inner()
                .on_prepare_for_apply(observe_id, brane_id);
        }
    }

    pub fn on_apply_cmd(&self, observe_id: ObserveID, brane_id: u64, cmd: Cmd) {
        assert!(
            !self.registry.cmd_semaphores.is_empty(),
            "CmdSemaphore is not registered"
        );
        for i in 0..self.registry.cmd_semaphores.len() - 1 {
            self.registry
                .cmd_semaphores
                .get(i)
                .unwrap()
                .semaphore
                .inner()
                .on_apply_cmd(observe_id, brane_id, cmd.clone())
        }
        self.registry
            .cmd_semaphores
            .last()
            .unwrap()
            .semaphore
            .inner()
            .on_apply_cmd(observe_id, brane_id, cmd)
    }

    pub fn on_flush_apply(&self, engine: E) {
        if self.registry.cmd_semaphores.is_empty() {
            return;
        }

        for i in 0..self.registry.cmd_semaphores.len() - 1 {
            self.registry
                .cmd_semaphores
                .get(i)
                .unwrap()
                .semaphore
                .inner()
                .on_flush_apply(engine.clone())
        }
        self.registry
            .cmd_semaphores
            .last()
            .unwrap()
            .semaphore
            .inner()
            .on_flush_apply(engine)
    }

    pub fn shutdown(&self) {
        for entry in &self.registry.admin_semaphores {
            entry.semaphore.inner().stop();
        }
        for entry in &self.registry.query_semaphores {
            entry.semaphore.inner().stop();
        }
        for entry in &self.registry.split_check_semaphores {
            entry.semaphore.inner().stop();
        }
        for entry in &self.registry.cmd_semaphores {
            entry.semaphore.inner().stop();
        }
    }
}

#[causet(test)]
mod tests {
    use crate::interlock::*;
    use std::sync::atomic::*;
    use std::sync::Arc;

    use engine_panic::PanicEngine;
    use ekvproto::meta_timeshare::Brane;
    use ekvproto::violetabft_cmd_timeshare::{
        AdminRequest, AdminResponse, VioletaBftCmdRequest, VioletaBftCmdResponse, Request,
    };

    #[derive(Clone, Default)]
    struct TestInterlock {
        bypass: Arc<AtomicBool>,
        called: Arc<AtomicUsize>,
        return_err: Arc<AtomicBool>,
    }

    impl Interlock for TestInterlock {}

    impl AdminSemaphore for TestInterlock {
        fn pre_propose_admin(
            &self,
            ctx: &mut SemaphoreContext<'_>,
            _: &mut AdminRequest,
        ) -> Result<()> {
            self.called.fetch_add(1, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_admin(&self, ctx: &mut SemaphoreContext<'_>, _: &AdminRequest) {
            self.called.fetch_add(2, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_admin(&self, ctx: &mut SemaphoreContext<'_>, _: &mut AdminResponse) {
            self.called.fetch_add(3, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl QuerySemaphore for TestInterlock {
        fn pre_propose_query(
            &self,
            ctx: &mut SemaphoreContext<'_>,
            _: &mut Vec<Request>,
        ) -> Result<()> {
            self.called.fetch_add(4, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_query(&self, ctx: &mut SemaphoreContext<'_>, _: &[Request]) {
            self.called.fetch_add(5, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_query(&self, ctx: &mut SemaphoreContext<'_>, _: &mut Cmd) {
            self.called.fetch_add(6, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RoleSemaphore for TestInterlock {
        fn on_role_change(&self, ctx: &mut SemaphoreContext<'_>, _: StateRole) {
            self.called.fetch_add(7, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl BraneChangeSemaphore for TestInterlock {
        fn on_brane_changed(
            &self,
            ctx: &mut SemaphoreContext<'_>,
            _: BraneChangeEvent,
            _: StateRole,
        ) {
            self.called.fetch_add(8, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl ApplySnapshotSemaphore for TestInterlock {
        fn apply_plain_kvs(
            &self,
            ctx: &mut SemaphoreContext<'_>,
            _: CfName,
            _: &[(Vec<u8>, Vec<u8>)],
        ) {
            self.called.fetch_add(9, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn apply_sst(&self, ctx: &mut SemaphoreContext<'_>, _: CfName, _: &str) {
            self.called.fetch_add(10, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl CmdSemaphore<PanicEngine> for TestInterlock {
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
            .register_admin_semaphore(1, BoxAdminSemaphore::new(ob.clone()));
        host.registry
            .register_query_semaphore(1, BoxQuerySemaphore::new(ob.clone()));
        host.registry
            .register_apply_snapshot_semaphore(1, BoxApplySnapshotSemaphore::new(ob.clone()));
        host.registry
            .register_role_semaphore(1, BoxRoleSemaphore::new(ob.clone()));
        host.registry
            .register_brane_change_semaphore(1, BoxBraneChangeSemaphore::new(ob.clone()));
        host.registry
            .register_cmd_semaphore(1, BoxCmdSemaphore::new(ob.clone()));
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
            .register_admin_semaphore(3, BoxAdminSemaphore::new(ob1.clone()));
        host.registry
            .register_query_semaphore(3, BoxQuerySemaphore::new(ob1.clone()));
        let ob2 = TestInterlock::default();
        host.registry
            .register_admin_semaphore(2, BoxAdminSemaphore::new(ob2.clone()));
        host.registry
            .register_query_semaphore(2, BoxQuerySemaphore::new(ob2.clone()));

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
