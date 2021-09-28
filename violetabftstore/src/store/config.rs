// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Duration;
use std::u64;
use time::Duration as TimeDuration;

use crate::{interlock, Result};
use batch_system::Config as BatchSystemConfig;
use configuration::{ConfigChange, ConfigManager, ConfigValue, Configuration};
use engine_lmdb::config as rocks_config;
use engine_lmdb::PerfLevel;
use violetabftstore::interlock::::config::{ReadableDuration, ReadableSize, VersionTrack};

lazy_static! {
    pub static ref CONFIG_VIOLETABFTSTORE_GAUGE: prometheus::GaugeVec = register_gauge_vec!(
        "edb_config_violetabftstore",
        "Config information of violetabftstore",
        &["name"]
    )
    .unwrap();
}

with_prefix!(prefix_apply "apply-");
with_prefix!(prefix_store "store-");
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    // minimizes disruption when a partitioned node rejoins the cluster by using a two phase election.
    #[config(skip)]
    pub prevote: bool,
    #[config(skip)]
    pub violetabftdb_path: String,

    // store capacity. 0 means no limit.
    #[config(skip)]
    pub capacity: ReadableSize,

    // violetabft_base_tick_interval is a base tick interval (ms).
    #[config(hidden)]
    pub violetabft_base_tick_interval: ReadableDuration,
    #[config(hidden)]
    pub violetabft_heartbeat_ticks: usize,
    #[config(hidden)]
    pub violetabft_election_timeout_ticks: usize,
    #[config(hidden)]
    pub violetabft_min_election_timeout_ticks: usize,
    #[config(hidden)]
    pub violetabft_max_election_timeout_ticks: usize,
    #[config(hidden)]
    pub violetabft_max_size_per_msg: ReadableSize,
    #[config(hidden)]
    pub violetabft_max_inflight_msgs: usize,
    // When the entry exceed the max size, reject to propose it.
    pub violetabft_entry_max_size: ReadableSize,

    // Interval to gc unnecessary violetabft log (ms).
    pub violetabft_log_gc_tick_interval: ReadableDuration,
    // A memory_barrier to gc stale violetabft log, must >= 1.
    pub violetabft_log_gc_memory_barrier: u64,
    // When entry count exceed this value, gc will be forced trigger.
    pub violetabft_log_gc_count_limit: u64,
    // When the approximate size of violetabft log entries exceed this value,
    // gc will be forced trigger.
    pub violetabft_log_gc_size_limit: ReadableSize,
    // Old VioletaBft logs could be reserved if `violetabft_log_gc_memory_barrier` is not reached.
    // GC them after ticks `violetabft_log_reserve_max_ticks` times.
    #[doc(hidden)]
    #[config(hidden)]
    pub violetabft_log_reserve_max_ticks: usize,
    // Old logs in VioletaBft engine needs to be purged peridically.
    pub violetabft_engine_purge_interval: ReadableDuration,
    // When a peer is not responding for this time, leader will not keep entry cache for it.
    pub violetabft_entry_cache_life_time: ReadableDuration,
    // When a peer is newly added, reject transferring leader to the peer for a while.
    pub violetabft_reject_transfer_leader_duration: ReadableDuration,

    // Interval (ms) to check brane whether need to be split or not.
    pub split_brane_check_tick_interval: ReadableDuration,
    /// When size change of brane exceed the diff since last check, it
    /// will be checked again whether it should be split.
    pub brane_split_check_diff: ReadableSize,
    /// Interval (ms) to check whether spacelike compaction for a brane.
    pub brane_compact_check_interval: ReadableDuration,
    /// Number of branes for each time checking.
    pub brane_compact_check_step: u64,
    /// Minimum number of tombstones to trigger manual compaction.
    pub brane_compact_min_tombstones: u64,
    /// Minimum percentage of tombstones to trigger manual compaction.
    /// Should between 1 and 100.
    pub brane_compact_tombstones_percent: u64,
    pub fidel_heartbeat_tick_interval: ReadableDuration,
    pub fidel_store_heartbeat_tick_interval: ReadableDuration,
    pub snap_mgr_gc_tick_interval: ReadableDuration,
    pub snap_gc_timeout: ReadableDuration,
    pub lock_causet_compact_interval: ReadableDuration,
    pub lock_causet_compact_bytes_memory_barrier: ReadableSize,

    #[config(skip)]
    pub notify_capacity: usize,
    pub messages_per_tick: usize,

    /// When a peer is not active for max_peer_down_duration,
    /// the peer is considered to be down and is reported to FIDel.
    pub max_peer_down_duration: ReadableDuration,

    /// If the leader of a peer is missing for longer than max_leader_missing_duration,
    /// the peer would ask fidel to confirm whether it is valid in any brane.
    /// If the peer is stale and is not valid in any brane, it will destroy itself.
    pub max_leader_missing_duration: ReadableDuration,
    /// Similar to the max_leader_missing_duration, instead it will log warnings and
    /// try to alert monitoring systems, if there is any.
    pub abnormal_leader_missing_duration: ReadableDuration,
    pub peer_stale_state_check_interval: ReadableDuration,

    #[config(hidden)]
    pub leader_transfer_max_log_lag: u64,

    #[config(skip)]
    pub snap_apply_batch_size: ReadableSize,

    // Interval (ms) to check brane whether the data is consistent.
    pub consistency_check_interval: ReadableDuration,

    #[config(hidden)]
    pub report_brane_flow_interval: ReadableDuration,

    // The lease provided by a successfully proposed and applied entry.
    pub violetabft_store_max_leader_lease: ReadableDuration,

    // Right brane derive origin brane id when split.
    #[config(hidden)]
    pub right_derive_when_split: bool,

    pub allow_remove_leader: bool,

    /// Max log gap allowed to propose merge.
    #[config(hidden)]
    pub merge_max_log_gap: u64,
    /// Interval to re-propose merge.
    pub merge_check_tick_interval: ReadableDuration,

    #[config(hidden)]
    pub use_delete_cone: bool,

    pub cleanup_import_sst_interval: ReadableDuration,

    /// Maximum size of every local read task batch.
    pub local_read_batch_size: u64,

    #[config(skip)]
    #[serde(flatten, with = "prefix_apply")]
    pub apply_batch_system: BatchSystemConfig,

    #[config(skip)]
    #[serde(flatten, with = "prefix_store")]
    pub store_batch_system: BatchSystemConfig,

    #[config(skip)]
    pub future_poll_size: usize,
    #[config(hidden)]
    pub hibernate_branes: bool,
    pub hibernate_timeout: ReadableDuration,
    #[config(hidden)]
    pub early_apply: bool,
    #[doc(hidden)]
    #[config(hidden)]
    pub dev_assert: bool,
    #[config(hidden)]
    pub apply_yield_duration: ReadableDuration,

    // Deprecated! These configuration has been moved to Interlock.
    // They are preserved for compatibility check.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[config(skip)]
    pub brane_max_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[config(skip)]
    pub brane_split_size: ReadableSize,
    // Deprecated! The time to clean stale peer safely can be decided based on Lmdb snapshot sequence number.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[config(skip)]
    pub clean_stale_peer_delay: ReadableDuration,
    #[serde(with = "rocks_config::perf_level_serde")]
    #[config(skip)]
    pub perf_level: PerfLevel,
}

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(interlock::config::SPLIT_SIZE_MB);
        Config {
            prevote: true,
            violetabftdb_path: String::new(),
            capacity: ReadableSize(0),
            violetabft_base_tick_interval: ReadableDuration::secs(1),
            violetabft_heartbeat_ticks: 2,
            violetabft_election_timeout_ticks: 10,
            violetabft_min_election_timeout_ticks: 0,
            violetabft_max_election_timeout_ticks: 0,
            violetabft_max_size_per_msg: ReadableSize::mb(1),
            violetabft_max_inflight_msgs: 256,
            violetabft_entry_max_size: ReadableSize::mb(8),
            violetabft_log_gc_tick_interval: ReadableDuration::secs(10),
            violetabft_log_gc_memory_barrier: 50,
            // Assume the average size of entries is 1k.
            violetabft_log_gc_count_limit: split_size * 3 / 4 / ReadableSize::kb(1),
            violetabft_log_gc_size_limit: split_size * 3 / 4,
            violetabft_log_reserve_max_ticks: 6,
            violetabft_engine_purge_interval: ReadableDuration::secs(10),
            violetabft_entry_cache_life_time: ReadableDuration::secs(30),
            violetabft_reject_transfer_leader_duration: ReadableDuration::secs(3),
            split_brane_check_tick_interval: ReadableDuration::secs(10),
            brane_split_check_diff: split_size / 16,
            brane_compact_check_interval: ReadableDuration::minutes(5),
            brane_compact_check_step: 100,
            brane_compact_min_tombstones: 10000,
            brane_compact_tombstones_percent: 30,
            fidel_heartbeat_tick_interval: ReadableDuration::minutes(1),
            fidel_store_heartbeat_tick_interval: ReadableDuration::secs(10),
            notify_capacity: 40960,
            snap_mgr_gc_tick_interval: ReadableDuration::minutes(1),
            snap_gc_timeout: ReadableDuration::hours(4),
            messages_per_tick: 4096,
            max_peer_down_duration: ReadableDuration::minutes(5),
            max_leader_missing_duration: ReadableDuration::hours(2),
            abnormal_leader_missing_duration: ReadableDuration::minutes(10),
            peer_stale_state_check_interval: ReadableDuration::minutes(5),
            leader_transfer_max_log_lag: 10,
            snap_apply_batch_size: ReadableSize::mb(10),
            lock_causet_compact_interval: ReadableDuration::minutes(10),
            lock_causet_compact_bytes_memory_barrier: ReadableSize::mb(256),
            // Disable consistency check by default as it will hurt performance.
            // We should turn on this only in our tests.
            consistency_check_interval: ReadableDuration::secs(0),
            report_brane_flow_interval: ReadableDuration::minutes(1),
            violetabft_store_max_leader_lease: ReadableDuration::secs(9),
            right_derive_when_split: true,
            allow_remove_leader: false,
            merge_max_log_gap: 10,
            merge_check_tick_interval: ReadableDuration::secs(10),
            use_delete_cone: false,
            cleanup_import_sst_interval: ReadableDuration::minutes(10),
            local_read_batch_size: 1024,
            apply_batch_system: BatchSystemConfig::default(),
            store_batch_system: BatchSystemConfig::default(),
            future_poll_size: 1,
            hibernate_branes: true,
            hibernate_timeout: ReadableDuration::minutes(10),
            early_apply: true,
            dev_assert: false,
            apply_yield_duration: ReadableDuration::millis(500),

            // They are preserved for compatibility check.
            brane_max_size: ReadableSize(0),
            brane_split_size: ReadableSize(0),
            clean_stale_peer_delay: ReadableDuration::minutes(0),
            perf_level: PerfLevel::Disable,
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn violetabft_store_max_leader_lease(&self) -> TimeDuration {
        TimeDuration::from_std(self.violetabft_store_max_leader_lease.0).unwrap()
    }

    pub fn violetabft_heartbeat_interval(&self) -> Duration {
        self.violetabft_base_tick_interval.0 * self.violetabft_heartbeat_ticks as u32
    }

    pub fn validate(&mut self) -> Result<()> {
        if self.violetabft_heartbeat_ticks == 0 {
            return Err(box_err!("heartbeat tick must greater than 0"));
        }

        if self.violetabft_election_timeout_ticks != 10 {
            warn!(
                "Election timeout ticks needs to be same across all the cluster, \
                 otherwise it may lead to inconsistency."
            );
        }

        if self.violetabft_election_timeout_ticks <= self.violetabft_heartbeat_ticks {
            return Err(box_err!(
                "election tick must be greater than heartbeat tick"
            ));
        }

        if self.violetabft_min_election_timeout_ticks == 0 {
            self.violetabft_min_election_timeout_ticks = self.violetabft_election_timeout_ticks;
        }

        if self.violetabft_max_election_timeout_ticks == 0 {
            self.violetabft_max_election_timeout_ticks = self.violetabft_election_timeout_ticks * 2;
        }

        if self.violetabft_min_election_timeout_ticks < self.violetabft_election_timeout_ticks
            || self.violetabft_min_election_timeout_ticks >= self.violetabft_max_election_timeout_ticks
        {
            return Err(box_err!(
                "invalid timeout cone [{}, {}) for timeout {}",
                self.violetabft_min_election_timeout_ticks,
                self.violetabft_max_election_timeout_ticks,
                self.violetabft_election_timeout_ticks
            ));
        }

        if self.violetabft_log_gc_memory_barrier < 1 {
            return Err(box_err!(
                "violetabft log gc memory_barrier must >= 1, not {}",
                self.violetabft_log_gc_memory_barrier
            ));
        }

        if self.violetabft_log_gc_size_limit.0 == 0 {
            return Err(box_err!("violetabft log gc size limit should large than 0."));
        }

        let election_timeout =
            self.violetabft_base_tick_interval.as_millis() * self.violetabft_election_timeout_ticks as u64;
        let lease = self.violetabft_store_max_leader_lease.as_millis() as u64;
        if election_timeout < lease {
            return Err(box_err!(
                "election timeout {} ms is less than lease {} ms",
                election_timeout,
                lease
            ));
        }

        if self.merge_max_log_gap >= self.violetabft_log_gc_count_limit {
            return Err(box_err!(
                "merge log gap {} should be less than log gc limit {}.",
                self.merge_max_log_gap,
                self.violetabft_log_gc_count_limit
            ));
        }

        if self.merge_check_tick_interval.as_millis() == 0 {
            return Err(box_err!("violetabftstore.merge-check-tick-interval can't be 0."));
        }

        let stale_state_check = self.peer_stale_state_check_interval.as_millis() as u64;
        if stale_state_check < election_timeout * 2 {
            return Err(box_err!(
                "peer stale state check interval {} ms is less than election timeout x 2 {} ms",
                stale_state_check,
                election_timeout * 2
            ));
        }

        if self.leader_transfer_max_log_lag < 10 {
            return Err(box_err!(
                "violetabftstore.leader-transfer-max-log-lag should be >= 10."
            ));
        }

        let abnormal_leader_missing = self.abnormal_leader_missing_duration.as_millis() as u64;
        if abnormal_leader_missing < stale_state_check {
            return Err(box_err!(
                "abnormal leader missing {} ms is less than peer stale state check interval {} ms",
                abnormal_leader_missing,
                stale_state_check
            ));
        }

        let max_leader_missing = self.max_leader_missing_duration.as_millis() as u64;
        if max_leader_missing < abnormal_leader_missing {
            return Err(box_err!(
                "max leader missing {} ms is less than abnormal leader missing {} ms",
                max_leader_missing,
                abnormal_leader_missing
            ));
        }

        if self.brane_compact_tombstones_percent < 1
            || self.brane_compact_tombstones_percent > 100
        {
            return Err(box_err!(
                "brane-compact-tombstones-percent must between 1 and 100, current value is {}",
                self.brane_compact_tombstones_percent
            ));
        }

        if self.local_read_batch_size == 0 {
            return Err(box_err!("local-read-batch-size must be greater than 0"));
        }

        if self.apply_batch_system.pool_size == 0 {
            return Err(box_err!("apply-pool-size should be greater than 0"));
        }
        if self.apply_batch_system.max_batch_size == 0 {
            return Err(box_err!("apply-max-batch-size should be greater than 0"));
        }
        if self.store_batch_system.pool_size == 0 {
            return Err(box_err!("store-pool-size should be greater than 0"));
        }
        if self.store_batch_system.max_batch_size == 0 {
            return Err(box_err!("store-max-batch-size should be greater than 0"));
        }
        if self.future_poll_size == 0 {
            return Err(box_err!("future-poll-size should be greater than 0."));
        }
        Ok(())
    }

    pub fn write_into_metrics(&self) {
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["prevote"])
            .set((self.prevote as i32).into());

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["capacity"])
            .set(self.capacity.0 as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_base_tick_interval"])
            .set(self.violetabft_base_tick_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_heartbeat_ticks"])
            .set(self.violetabft_heartbeat_ticks as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_election_timeout_ticks"])
            .set(self.violetabft_election_timeout_ticks as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_min_election_timeout_ticks"])
            .set(self.violetabft_min_election_timeout_ticks as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_max_election_timeout_ticks"])
            .set(self.violetabft_max_election_timeout_ticks as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_max_size_per_msg"])
            .set(self.violetabft_max_size_per_msg.0 as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_max_inflight_msgs"])
            .set(self.violetabft_max_inflight_msgs as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_entry_max_size"])
            .set(self.violetabft_entry_max_size.0 as f64);

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_log_gc_tick_interval"])
            .set(self.violetabft_log_gc_tick_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_log_gc_memory_barrier"])
            .set(self.violetabft_log_gc_memory_barrier as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_log_gc_count_limit"])
            .set(self.violetabft_log_gc_count_limit as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_log_gc_size_limit"])
            .set(self.violetabft_log_gc_size_limit.0 as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_log_reserve_max_ticks"])
            .set(self.violetabft_log_reserve_max_ticks as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_engine_purge_interval"])
            .set(self.violetabft_engine_purge_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_entry_cache_life_time"])
            .set(self.violetabft_entry_cache_life_time.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_reject_transfer_leader_duration"])
            .set(self.violetabft_reject_transfer_leader_duration.as_secs() as f64);

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["split_brane_check_tick_interval"])
            .set(self.split_brane_check_tick_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["brane_split_check_diff"])
            .set(self.brane_split_check_diff.0 as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["brane_compact_check_interval"])
            .set(self.brane_compact_check_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["brane_compact_check_step"])
            .set(self.brane_compact_check_step as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["brane_compact_min_tombstones"])
            .set(self.brane_compact_min_tombstones as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["brane_compact_tombstones_percent"])
            .set(self.brane_compact_tombstones_percent as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["fidel_heartbeat_tick_interval"])
            .set(self.fidel_heartbeat_tick_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["fidel_store_heartbeat_tick_interval"])
            .set(self.fidel_store_heartbeat_tick_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["snap_mgr_gc_tick_interval"])
            .set(self.snap_mgr_gc_tick_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["snap_gc_timeout"])
            .set(self.snap_gc_timeout.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["lock_causet_compact_interval"])
            .set(self.lock_causet_compact_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["lock_causet_compact_bytes_memory_barrier"])
            .set(self.lock_causet_compact_bytes_memory_barrier.0 as f64);

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["notify_capacity"])
            .set(self.notify_capacity as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["messages_per_tick"])
            .set(self.messages_per_tick as f64);

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["max_peer_down_duration"])
            .set(self.max_peer_down_duration.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["max_leader_missing_duration"])
            .set(self.max_leader_missing_duration.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["abnormal_leader_missing_duration"])
            .set(self.abnormal_leader_missing_duration.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["peer_stale_state_check_interval"])
            .set(self.peer_stale_state_check_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["leader_transfer_max_log_lag"])
            .set(self.leader_transfer_max_log_lag as f64);

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["snap_apply_batch_size"])
            .set(self.snap_apply_batch_size.0 as f64);

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["consistency_check_interval_seconds"])
            .set(self.consistency_check_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["report_brane_flow_interval"])
            .set(self.report_brane_flow_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["violetabft_store_max_leader_lease"])
            .set(self.violetabft_store_max_leader_lease.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["right_derive_when_split"])
            .set((self.right_derive_when_split as i32).into());
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["allow_remove_leader"])
            .set((self.allow_remove_leader as i32).into());

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["merge_max_log_gap"])
            .set(self.merge_max_log_gap as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["merge_check_tick_interval"])
            .set(self.merge_check_tick_interval.as_secs() as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["use_delete_cone"])
            .set((self.use_delete_cone as i32).into());
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["cleanup_import_sst_interval"])
            .set(self.cleanup_import_sst_interval.as_secs() as f64);

        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["local_read_batch_size"])
            .set(self.local_read_batch_size as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["apply_max_batch_size"])
            .set(self.apply_batch_system.max_batch_size as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["apply_pool_size"])
            .set(self.apply_batch_system.pool_size as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["store_max_batch_size"])
            .set(self.store_batch_system.max_batch_size as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["store_pool_size"])
            .set(self.store_batch_system.pool_size as f64);
        CONFIG_VIOLETABFTSTORE_GAUGE
            .with_label_values(&["future_poll_size"])
            .set(self.future_poll_size as f64);
    }

    fn write_change_into_metrics(change: ConfigChange) {
        for (name, value) in change {
            if let Ok(v) = match value {
                ConfigValue::F64(v) => Ok(v),
                ConfigValue::U64(v) => Ok(v as f64),
                ConfigValue::Size(v) => Ok(v as f64),
                ConfigValue::Usize(v) => Ok(v as f64),
                ConfigValue::Bool(v) => Ok((v as i32).into()),
                ConfigValue::Duration(v) => Ok((v / 1000) as f64), // millis -> secs
                _ => Err(()),
            } {
                CONFIG_VIOLETABFTSTORE_GAUGE
                    .with_label_values(&[name.as_str()])
                    .set(v);
            }
        }
    }
}

pub struct VioletaBftstoreConfigManager(pub Arc<VersionTrack<Config>>);

impl ConfigManager for VioletaBftstoreConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0.fidelio(move |causet: &mut Config| causet.fidelio(change));
        }
        info!(
            "violetabftstore config changed";
            "change" => ?change,
        );
        Config::write_change_into_metrics(change);
        Ok(())
    }
}

impl std::ops::Deref for VioletaBftstoreConfigManager {
    type Target = Arc<VersionTrack<Config>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let mut causet = Config::new();
        causet.validate().unwrap();
        assert_eq!(
            causet.violetabft_min_election_timeout_ticks,
            causet.violetabft_election_timeout_ticks
        );
        assert_eq!(
            causet.violetabft_max_election_timeout_ticks,
            causet.violetabft_election_timeout_ticks * 2
        );

        causet.violetabft_heartbeat_ticks = 0;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.violetabft_election_timeout_ticks = 10;
        causet.violetabft_heartbeat_ticks = 10;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.violetabft_min_election_timeout_ticks = 5;
        causet.validate().unwrap_err();
        causet.violetabft_min_election_timeout_ticks = 25;
        causet.validate().unwrap_err();
        causet.violetabft_min_election_timeout_ticks = 10;
        causet.validate().unwrap();

        causet.violetabft_heartbeat_ticks = 11;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.violetabft_log_gc_memory_barrier = 0;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.violetabft_log_gc_size_limit = ReadableSize(0);
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.violetabft_base_tick_interval = ReadableDuration::secs(1);
        causet.violetabft_election_timeout_ticks = 10;
        causet.violetabft_store_max_leader_lease = ReadableDuration::secs(20);
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.violetabft_log_gc_count_limit = 100;
        causet.merge_max_log_gap = 110;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.merge_check_tick_interval = ReadableDuration::secs(0);
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.violetabft_base_tick_interval = ReadableDuration::secs(1);
        causet.violetabft_election_timeout_ticks = 10;
        causet.peer_stale_state_check_interval = ReadableDuration::secs(5);
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.peer_stale_state_check_interval = ReadableDuration::minutes(2);
        causet.abnormal_leader_missing_duration = ReadableDuration::minutes(1);
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.abnormal_leader_missing_duration = ReadableDuration::minutes(2);
        causet.max_leader_missing_duration = ReadableDuration::minutes(1);
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.local_read_batch_size = 0;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.apply_batch_system.max_batch_size = 0;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.apply_batch_system.pool_size = 0;
        assert!(causet.validate().is_err());

        causet = Config::new();
        causet.future_poll_size = 0;
        assert!(causet.validate().is_err());
    }
}
