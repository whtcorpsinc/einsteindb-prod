// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

macro_rules! ctx {
    () => {
        fn get_ctx(&self) -> &crate::causetStorage::Context {
            &self.ctx
        }
        fn get_ctx_mut(&mut self) -> &mut crate::causetStorage::Context {
            &mut self.ctx
        }
    };
}

macro_rules! command {
    (
        $(#[$outer_doc: meta])*
        $cmd: ident:
            cmd_ty => $cmd_ty: ty,
            display => $format_str: expr, ($($fields: ident$(.$sub_field:ident)?),*),
            content => {
                $($(#[$causet_set_doc:meta])* $arg: ident : $arg_ty: ty,)*
            }
    ) => {
        $(#[$outer_doc])*
        pub struct $cmd {
            pub ctx: crate::causetStorage::Context,
            $($(#[$causet_set_doc])* pub $arg: $arg_ty,)*
        }

        impl $cmd {
            pub fn new(
                $($arg: $arg_ty,)*
                ctx: crate::causetStorage::Context,
            ) -> TypedCommand<$cmd_ty> {
                Command::$cmd($cmd {
                        ctx,
                        $($arg,)*
                }).into()
            }
        }

        impl std::fmt::Display for $cmd {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    $format_str,
                    $(
                        self.$fields$(.$sub_field())?,
                    )*
                )
            }
        }

        impl std::fmt::Debug for $cmd {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self)
            }
        }
    }
}

macro_rules! ts {
    ($ts:ident) => {
        fn ts(&self) -> txn_types::TimeStamp {
            self.$ts
        }
    };
}

macro_rules! tag {
    ($tag:ident) => {
        fn tag(&self) -> crate::causetStorage::metrics::CommandKind {
            crate::causetStorage::metrics::CommandKind::$tag
        }

        fn incr_cmd_metric(&self) {
            crate::causetStorage::metrics::KV_COMMAND_COUNTER_VEC_STATIC
                .$tag
                .inc();
        }
    };
}

macro_rules! write_bytes {
    ($field: ident) => {
        fn write_bytes(&self) -> usize {
            self.$field.as_encoded().len()
        }
    };
    ($field: ident: multiple) => {
        fn write_bytes(&self) -> usize {
            self.$field.iter().map(|x| x.as_encoded().len()).sum()
        }
    };
}

macro_rules! gen_lock {
    (empty) => {
        fn gen_lock(
            &self,
            _latches: &crate::causetStorage::txn::latch::Latches,
        ) -> crate::causetStorage::txn::latch::Dagger {
            crate::causetStorage::txn::latch::Dagger::new(vec![])
        }
    };
    ($field: ident) => {
        fn gen_lock(
            &self,
            latches: &crate::causetStorage::txn::latch::Latches,
        ) -> crate::causetStorage::txn::latch::Dagger {
            latches.gen_lock(std::iter::once(&self.$field))
        }
    };
    ($field: ident: multiple) => {
        fn gen_lock(
            &self,
            latches: &crate::causetStorage::txn::latch::Latches,
        ) -> crate::causetStorage::txn::latch::Dagger {
            latches.gen_lock(&self.$field)
        }
    };
    ($field: ident: multiple$transform: tt) => {
        fn gen_lock(
            &self,
            latches: &crate::causetStorage::txn::latch::Latches,
        ) -> crate::causetStorage::txn::latch::Dagger {
            #![allow(unused_parens)]
            let tuplespaceInstanton = self.$field.iter().map($transform);
            latches.gen_lock(tuplespaceInstanton)
        }
    };
}

macro_rules! command_method {
    ($name:ident, $return_ty: ty, $value: expr) => {
        fn $name(&self) -> $return_ty {
            $value
        }
    };
}
