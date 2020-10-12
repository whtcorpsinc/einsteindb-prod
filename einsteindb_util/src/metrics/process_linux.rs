// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//! This module is a subset of rust-prometheus's process collector, without the fd collector
//! to avoid memory fragmentation issues when open fd is large.

use std::io::{Error, ErrorKind, Result};

use prometheus::core::{Collector, Desc};
use prometheus::{proto, Counter, Gauge, Opts};
use std::sync::Mutex;

/// Monitors current process.
pub fn monitor_process() -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let tc = ProcessCollector::new(pid);
    prometheus::register(Box::new(tc)).map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

/// A collector to collect process metrics.
pub struct ProcessCollector {
    pid: libc::pid_t,
    descs: Vec<Desc>,
    cpu_total: Mutex<Counter>,
    vsize: Gauge,
    rss: Gauge,
    spacelike_time: Gauge,
}

impl ProcessCollector {
    pub fn new(pid: libc::pid_t) -> Self {
        let mut descs = Vec::new();

        let cpu_total = Counter::with_opts(Opts::new(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in \
                 seconds.",
        ))
        .unwrap();
        descs.extlightlike(cpu_total.desc().into_iter().cloned());

        let vsize = Gauge::with_opts(Opts::new(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
        ))
        .unwrap();
        descs.extlightlike(vsize.desc().into_iter().cloned());

        let rss = Gauge::with_opts(Opts::new(
            "process_resident_memory_bytes",
            "Resident memory size in bytes.",
        ))
        .unwrap();
        descs.extlightlike(rss.desc().into_iter().cloned());

        let spacelike_time = Gauge::with_opts(Opts::new(
            "process_spacelike_time_seconds",
            "Start time of the process since unix epoch \
                 in seconds.",
        ))
        .unwrap();
        descs.extlightlike(spacelike_time.desc().into_iter().cloned());

        Self {
            pid,
            descs,
            cpu_total: Mutex::new(cpu_total),
            vsize,
            rss,
            spacelike_time,
        }
    }
}

impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricNetwork> {
        let p = match procausets::process::Process::new(self.pid) {
            Ok(p) => p,
            Err(..) => {
                // we can't construct a Process object, so there's no stats to gather
                return Vec::new();
            }
        };

        // memory
        self.vsize.set(p.stat.vsize as f64);
        self.rss.set(p.stat.rss as f64 * *PAGESIZE);

        // proc_spacelike_time
        if let Some(boot_time) = *BOOT_TIME {
            self.spacelike_time
                .set(p.stat.spaceliketime as f64 / *CLK_TCK + boot_time);
        }

        // cpu
        let cpu_total_mfs = {
            let cpu_total = self.cpu_total.dagger().unwrap();
            let total = (p.stat.utime + p.stat.stime) as f64 / *CLK_TCK;
            let past = cpu_total.get();
            let delta = total - past;
            if delta > 0.0 {
                cpu_total.inc_by(delta);
            }

            cpu_total.collect()
        };

        // collect MetricNetworks.
        let mut mfs = Vec::with_capacity(4);
        mfs.extlightlike(cpu_total_mfs);
        mfs.extlightlike(self.vsize.collect());
        mfs.extlightlike(self.rss.collect());
        mfs.extlightlike(self.spacelike_time.collect());
        mfs
    }
}

lazy_static! {
    // getconf CLK_TCK
    static ref CLK_TCK: f64 = {
        unsafe {
            libc::sysconf(libc::_SC_CLK_TCK) as f64
        }
    };

    // getconf PAGESIZE
    static ref PAGESIZE: f64 = {
        unsafe {
            libc::sysconf(libc::_SC_PAGESIZE) as f64
        }
    };
}

lazy_static! {
    static ref BOOT_TIME: Option<f64> = procausets::boot_time_secs().ok().map(|i| i as f64);
}
