// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::thread;
use std::time::Duration;

use prometheus::*;

#[causet(target_os = "linux")]
mod threads_linux;
#[causet(target_os = "linux")]
pub use self::threads_linux::{cpu_total, get_thread_ids, monitor_threads, ThreadInfoStatistics};

#[causet(target_os = "linux")]
mod process_linux;
#[causet(target_os = "linux")]
pub use self::process_linux::monitor_process;

#[causet(not(target_os = "linux"))]
mod threads_dummy;
#[causet(not(target_os = "linux"))]
pub use self::threads_dummy::{monitor_threads, ThreadInfoStatistics};

#[causet(not(target_os = "linux"))]
mod process_dummy;
#[causet(not(target_os = "linux"))]
pub use self::process_dummy::monitor_process;

pub use self::allocator_metrics::monitor_allocator_stats;

pub mod allocator_metrics;

pub use self::metrics_reader::HistogramReader;

mod metrics_reader;

/// Runs a background Prometheus client.
pub fn run_prometheus(
    interval: Duration,
    address: &str,
    job: &str,
) -> Option<thread::JoinHandle<()>> {
    if interval == Duration::from_secs(0) {
        return None;
    }

    let job = job.to_owned();
    let address = address.to_owned();
    let handler = thread::Builder::new()
        .name("promepusher".to_owned())
        .spawn(move || {
            edb_alloc::add_thread_memory_accessor();
            loop {
                let metric_families = prometheus::gather();

                let res = prometheus::push_metrics(
                    &job,
                    prometheus::hostname_grouping_key(),
                    &address,
                    metric_families,
                    None,
                );
                if let Err(e) = res {
                    error!("fail to push metrics"; "err" => ?e);
                }

                thread::sleep(interval);
            }
        })
        .unwrap();

    Some(handler)
}

pub fn dump() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    for mf in metric_families {
        if let Err(e) = encoder.encode(&[mf], &mut buffer) {
            warn!("prometheus encoding error"; "err" => ?e);
        }
    }
    String::from_utf8(buffer).unwrap()
}

lazy_static! {
    pub static ref CRITICAL_ERROR: IntCounterVec = register_int_counter_vec!(
        "edb_critical_error_total",
        "Counter of critical error.",
        &["type"]
    )
    .unwrap();
}
