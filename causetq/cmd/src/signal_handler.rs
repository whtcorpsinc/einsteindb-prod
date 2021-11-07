// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

pub use self::imp::wait_for_signal;

#[causet(unix)]
mod imp {
    use engine_lmdb::LmdbEngine;
    use edb::{Engines, MiscExt, VioletaBftEngine};
    use libc::c_int;
    use nix::sys::signal::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2};
    use signal::trap::Trap;
    use violetabftstore::interlock::::metrics;

    #[allow(dead_code)]
    pub fn wait_for_signal<ER: VioletaBftEngine>(engines: Option<Engines<LmdbEngine, ER>>) {
        let trap = Trap::trap(&[SIGTERM, SIGINT, SIGHUP, SIGUSR1, SIGUSR2]);
        for sig in trap {
            match sig {
                SIGTERM | SIGINT | SIGHUP => {
                    info!("receive signal {}, stopping server...", sig as c_int);
                    break;
                }
                SIGUSR1 => {
                    // Use SIGUSR1 to log metrics.
                    info!("{}", metrics::dump());
                    if let Some(ref engines) = engines {
                        info!("{:?}", MiscExt::dump_stats(&engines.kv));
                        info!("{:?}", VioletaBftEngine::dump_stats(&engines.violetabft));
                    }
                }
                // TODO: handle more signal
                _ => unreachable!(),
            }
        }
    }
}

#[causet(not(unix))]
mod imp {
    use engine_lmdb::LmdbEngine;
    use edb::Engines;

    pub fn wait_for_signal(_: Option<Engines<LmdbEngine, LmdbEngine>>) {}
}
