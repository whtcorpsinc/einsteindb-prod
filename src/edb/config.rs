// Copyright 2020 WHTCORPS INC Project Authors. Licensed

//! Configuration for the entire server.
//!
//! EinsteinDB is configured through the `EINSTEINDBConfig` type, which is in turn
//! made up of many other configuration types.

use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::i32;
use std::io::Error as IoError;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::usize;

use configuration::{ConfigChange, ConfigManager, ConfigValue, Configuration, Result as CfgResult};
use engine_lmdb::raw::{
    BlockBasedOptions, Cache, PrimaryCausetNetworkOptions, CompactionFilterFactory, CompactionPriority,
    DBCompactionStyle, DBCompressionType, DBOptions, DBRateLimiterMode, DBRecoveryMode,
    LRUCacheOptions, NoetherDBOptions,
};

use crate::import::Config as ImportConfig;
use crate::server::gc_worker::GcConfig;
use crate::server::gc_worker::WriteCompactionFilterFactory;
use crate::server::lock_manager::Config as PessimisticTxnConfig;
use crate::server::Config as ServerConfig;
use crate::server::CONFIG_LMDB_GAUGE;
use crate::causet_storage::config::{Config as StorageConfig, DEFAULT_DATA_DIR, DEFAULT_LMDB_SUB_DIR};
use engine_lmdb::config::{self as rocks_config, BlobRunMode, CompressionType, LogLevel};
use engine_lmdb::properties::MvccPropertiesCollectorFactory;
use engine_lmdb::raw_util::CausetOptions;
use engine_lmdb::util::{
    FixedPrefixSliceTransform, FixedSuffixSliceTransform, NoopSliceTransform,
};
use engine_lmdb::{
    VioletaBftDBLogger, ConePropertiesCollectorFactory, LmdbEngine, LmdbEventListener, LmdbdbLogger,
    DEFAULT_PROP_KEYS_INDEX_DISTANCE, DEFAULT_PROP_SIZE_INDEX_DISTANCE,
};
use edb::{CausetHandleExt, PrimaryCausetNetworkOptions as PrimaryCausetNetworkOptionsTrait, DBOptionsExt};
use edb::{Causet_DEFAULT, Causet_DAGGER, Causet_VIOLETABFT, Causet_VER_DEFAULT, Causet_WRITE};
use tuplespaceInstanton::brane_violetabft_prefix_len;
use fidel_client::Config as FidelConfig;
use violetabft_log_engine::VioletaBftEngineConfig as RawVioletaBftEngineConfig;
use violetabft_log_engine::VioletaBftLogEngine;
use violetabftstore::interlock::Config as CopConfig;
use violetabftstore::store::Config as VioletaBftstoreConfig;
use violetabftstore::store::SplitConfig;
use security::SecurityConfig;
use violetabftstore::interlock::::config::{
    self, LogFormat, OptionReadableSize, ReadableDuration, ReadableSize, TomlWriter, GB, MB,
};
use violetabftstore::interlock::::sys::sys_quota::SysQuota;
use violetabftstore::interlock::::time::duration_to_sec;
use violetabftstore::interlock::::yatp_pool;

const LOCKCauset_MIN_MEM: usize = 256 * MB as usize;
const LOCKCauset_MAX_MEM: usize = GB as usize;
const VIOLETABFT_MIN_MEM: usize = 256 * MB as usize;
const VIOLETABFT_MAX_MEM: usize = 2 * GB as usize;
const LAST_CONFIG_FILE: &str = "last_edb.toml";
const TMP_CONFIG_FILE: &str = "tmp_edb.toml";
const MAX_BLOCK_SIZE: usize = 32 * MB as usize;

fn memory_mb_for_causet(is_violetabft_db: bool, causet: &str) -> usize {
    let total_mem = SysQuota::new().memory_limit_in_bytes();
    let (ratio, min, max) = match (is_violetabft_db, causet) {
        (true, Causet_DEFAULT) => (0.02, VIOLETABFT_MIN_MEM, VIOLETABFT_MAX_MEM),
        (false, Causet_DEFAULT) => (0.25, 0, usize::MAX),
        (false, Causet_DAGGER) => (0.02, LOCKCauset_MIN_MEM, LOCKCauset_MAX_MEM),
        (false, Causet_WRITE) => (0.15, 0, usize::MAX),
        (false, Causet_VER_DEFAULT) => (0.25, 0, usize::MAX),
        _ => unreachable!(),
    };
    let mut size = (total_mem as f64 * ratio) as usize;
    if size < min {
        size = min;
    } else if size > max {
        size = max;
    }
    size / MB as usize
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct NoetherCfConfig {
    #[config(skip)]
    pub min_blob_size: ReadableSize,
    #[config(skip)]
    pub blob_file_compression: CompressionType,
    #[config(skip)]
    pub blob_cache_size: ReadableSize,
    #[config(skip)]
    pub min_gc_batch_size: ReadableSize,
    #[config(skip)]
    pub max_gc_batch_size: ReadableSize,
    #[config(skip)]
    pub discardable_ratio: f64,
    #[config(skip)]
    pub sample_ratio: f64,
    #[config(skip)]
    pub merge_small_file_memory_barrier: ReadableSize,
    pub blob_run_mode: BlobRunMode,
    #[config(skip)]
    pub level_merge: bool,
    #[config(skip)]
    pub cone_merge: bool,
    #[config(skip)]
    pub max_sorted_runs: i32,
    #[config(skip)]
    pub gc_merge_rewrite: bool,
}

impl Default for NoetherCfConfig {
    fn default() -> Self {
        Self {
            min_blob_size: ReadableSize::kb(1), // disable titan default
            blob_file_compression: CompressionType::Lz4,
            blob_cache_size: ReadableSize::mb(0),
            min_gc_batch_size: ReadableSize::mb(16),
            max_gc_batch_size: ReadableSize::mb(64),
            discardable_ratio: 0.5,
            sample_ratio: 0.1,
            merge_small_file_memory_barrier: ReadableSize::mb(8),
            blob_run_mode: BlobRunMode::Normal,
            level_merge: false,
            cone_merge: true,
            max_sorted_runs: 20,
            gc_merge_rewrite: false,
        }
    }
}

impl NoetherCfConfig {
    fn build_opts(&self) -> NoetherDBOptions {
        let mut opts = NoetherDBOptions::new();
        opts.set_min_blob_size(self.min_blob_size.0 as u64);
        opts.set_blob_file_compression(self.blob_file_compression.into());
        opts.set_blob_cache(self.blob_cache_size.0 as usize, -1, false, 0.0);
        opts.set_min_gc_batch_size(self.min_gc_batch_size.0 as u64);
        opts.set_max_gc_batch_size(self.max_gc_batch_size.0 as u64);
        opts.set_discardable_ratio(self.discardable_ratio);
        opts.set_sample_ratio(self.sample_ratio);
        opts.set_merge_small_file_memory_barrier(self.merge_small_file_memory_barrier.0 as u64);
        opts.set_blob_run_mode(self.blob_run_mode.into());
        opts.set_level_merge(self.level_merge);
        opts.set_cone_merge(self.cone_merge);
        opts.set_max_sorted_runs(self.max_sorted_runs);
        opts.set_gc_merge_rewrite(self.gc_merge_rewrite);
        opts
    }
}

fn get_background_job_limit(
    default_background_jobs: i32,
    default_sub_compactions: u32,
    default_background_gc: i32,
) -> (i32, u32, i32) {
    let cpu_num = SysQuota::new().cpu_cores_quota();
    // At the minimum, we should have two background jobs: one for flush and one for compaction.
    // Otherwise, the number of background jobs should not exceed cpu_num - 1.
    // By default, lmdb assign (max_background_jobs / 4) threads dedicated for flush, and
    // the rest shared by flush and compaction.
    let max_background_jobs: i32 =
        cmp::max(2, cmp::min(default_background_jobs, (cpu_num - 1.0) as i32));
    // Cap max_sub_compactions to allow at least two compactions.
    let max_compactions = max_background_jobs - max_background_jobs / 4;
    let max_sub_compactions: u32 = cmp::max(
        1,
        cmp::min(default_sub_compactions, (max_compactions - 1) as u32),
    );
    // Maximum background GC threads for Noether
    let max_background_gc: i32 = cmp::min(default_background_gc, cpu_num as i32);

    (max_background_jobs, max_sub_compactions, max_background_gc)
}

macro_rules! causet_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            #[config(skip)]
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            #[config(skip)]
            pub disable_block_cache: bool,
            #[config(skip)]
            pub cache_index_and_filter_blocks: bool,
            #[config(skip)]
            pub pin_l0_filter_and_index_blocks: bool,
            #[config(skip)]
            pub use_bloom_filter: bool,
            #[config(skip)]
            pub optimize_filters_for_hits: bool,
            #[config(skip)]
            pub whole_key_filtering: bool,
            #[config(skip)]
            pub bloom_filter_bits_per_key: i32,
            #[config(skip)]
            pub block_based_bloom_filter: bool,
            #[config(skip)]
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "rocks_config::compression_type_level_serde")]
            #[config(skip)]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            #[config(skip)]
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "rocks_config::compaction_pri_serde")]
            #[config(skip)]
            pub compaction_pri: CompactionPriority,
            #[config(skip)]
            pub dynamic_level_bytes: bool,
            #[config(skip)]
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "rocks_config::compaction_style_serde")]
            #[config(skip)]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub soft_plightlikeing_compaction_bytes_limit: ReadableSize,
            pub hard_plightlikeing_compaction_bytes_limit: ReadableSize,
            #[config(skip)]
            pub force_consistency_checks: bool,
            #[config(skip)]
            pub prop_size_index_distance: u64,
            #[config(skip)]
            pub prop_tuplespaceInstanton_index_distance: u64,
            #[config(skip)]
            pub enable_doubly_skiplist: bool,
            #[config(submodule)]
            pub titan: NoetherCfConfig,
        }

        impl $name {
            fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.block_size.0 as usize > MAX_BLOCK_SIZE {
                    return Err(format!(
                        "invalid block-size {} for {}, exceed max size {}",
                        self.block_size.0,
                        stringify!($name),
                        MAX_BLOCK_SIZE
                    )
                    .into());
                }
                Ok(())
            }
        }
    };
}

macro_rules! write_into_metrics {
    ($causet:expr, $tag:expr, $metrics:expr) => {{
        $metrics
            .with_label_values(&[$tag, "block_size"])
            .set($causet.block_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "block_cache_size"])
            .set($causet.block_cache_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "disable_block_cache"])
            .set(($causet.disable_block_cache as i32).into());

        $metrics
            .with_label_values(&[$tag, "cache_index_and_filter_blocks"])
            .set(($causet.cache_index_and_filter_blocks as i32).into());
        $metrics
            .with_label_values(&[$tag, "pin_l0_filter_and_index_blocks"])
            .set(($causet.pin_l0_filter_and_index_blocks as i32).into());

        $metrics
            .with_label_values(&[$tag, "use_bloom_filter"])
            .set(($causet.use_bloom_filter as i32).into());
        $metrics
            .with_label_values(&[$tag, "optimize_filters_for_hits"])
            .set(($causet.optimize_filters_for_hits as i32).into());
        $metrics
            .with_label_values(&[$tag, "whole_key_filtering"])
            .set(($causet.whole_key_filtering as i32).into());
        $metrics
            .with_label_values(&[$tag, "bloom_filter_bits_per_key"])
            .set($causet.bloom_filter_bits_per_key.into());
        $metrics
            .with_label_values(&[$tag, "block_based_bloom_filter"])
            .set(($causet.block_based_bloom_filter as i32).into());

        $metrics
            .with_label_values(&[$tag, "read_amp_bytes_per_bit"])
            .set($causet.read_amp_bytes_per_bit.into());
        $metrics
            .with_label_values(&[$tag, "write_buffer_size"])
            .set($causet.write_buffer_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "max_write_buffer_number"])
            .set($causet.max_write_buffer_number.into());
        $metrics
            .with_label_values(&[$tag, "min_write_buffer_number_to_merge"])
            .set($causet.min_write_buffer_number_to_merge.into());
        $metrics
            .with_label_values(&[$tag, "max_bytes_for_level_base"])
            .set($causet.max_bytes_for_level_base.0 as f64);
        $metrics
            .with_label_values(&[$tag, "target_file_size_base"])
            .set($causet.target_file_size_base.0 as f64);
        $metrics
            .with_label_values(&[$tag, "level0_file_num_compaction_trigger"])
            .set($causet.level0_file_num_compaction_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_slowdown_writes_trigger"])
            .set($causet.level0_slowdown_writes_trigger.into());
        $metrics
            .with_label_values(&[$tag, "level0_stop_writes_trigger"])
            .set($causet.level0_stop_writes_trigger.into());
        $metrics
            .with_label_values(&[$tag, "max_compaction_bytes"])
            .set($causet.max_compaction_bytes.0 as f64);
        $metrics
            .with_label_values(&[$tag, "dynamic_level_bytes"])
            .set(($causet.dynamic_level_bytes as i32).into());
        $metrics
            .with_label_values(&[$tag, "num_levels"])
            .set($causet.num_levels.into());
        $metrics
            .with_label_values(&[$tag, "max_bytes_for_level_multiplier"])
            .set($causet.max_bytes_for_level_multiplier.into());

        $metrics
            .with_label_values(&[$tag, "disable_auto_compactions"])
            .set(($causet.disable_auto_compactions as i32).into());
        $metrics
            .with_label_values(&[$tag, "soft_plightlikeing_compaction_bytes_limit"])
            .set($causet.soft_plightlikeing_compaction_bytes_limit.0 as f64);
        $metrics
            .with_label_values(&[$tag, "hard_plightlikeing_compaction_bytes_limit"])
            .set($causet.hard_plightlikeing_compaction_bytes_limit.0 as f64);
        $metrics
            .with_label_values(&[$tag, "force_consistency_checks"])
            .set(($causet.force_consistency_checks as i32).into());
        $metrics
            .with_label_values(&[$tag, "enable_doubly_skiplist"])
            .set(($causet.enable_doubly_skiplist as i32).into());
        $metrics
            .with_label_values(&[$tag, "titan_min_blob_size"])
            .set($causet.titan.min_blob_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_blob_cache_size"])
            .set($causet.titan.blob_cache_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_min_gc_batch_size"])
            .set($causet.titan.min_gc_batch_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_max_gc_batch_size"])
            .set($causet.titan.max_gc_batch_size.0 as f64);
        $metrics
            .with_label_values(&[$tag, "titan_discardable_ratio"])
            .set($causet.titan.discardable_ratio);
        $metrics
            .with_label_values(&[$tag, "titan_sample_ratio"])
            .set($causet.titan.sample_ratio);
        $metrics
            .with_label_values(&[$tag, "titan_merge_small_file_memory_barrier"])
            .set($causet.titan.merge_small_file_memory_barrier.0 as f64);
    }};
}

macro_rules! build_causet_opt {
    ($opt:ident, $cache:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        if let Some(cache) = $cache {
            block_base_opts.set_block_cache(cache);
        } else {
            let mut cache_opts = LRUCacheOptions::new();
            cache_opts.set_capacity($opt.block_cache_size.0 as usize);
            block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
        }
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts
            .set_pin_l0_filter_and_index_blocks_in_cache($opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter(
                $opt.bloom_filter_bits_per_key,
                $opt.block_based_bloom_filter,
            );
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_block_based_Block_factory(&block_base_opts);
        causet_opts.set_num_levels($opt.num_levels);
        assert!($opt.compression_per_level.len() >= $opt.num_levels as usize);
        let compression_per_level = $opt.compression_per_level[..$opt.num_levels as usize].to_vec();
        causet_opts.compression_per_level(compression_per_level.as_slice());
        causet_opts.set_write_buffer_size($opt.write_buffer_size.0);
        causet_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        causet_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        causet_opts.set_max_bytes_for_level_base($opt.max_bytes_for_level_base.0);
        causet_opts.set_target_file_size_base($opt.target_file_size_base.0);
        causet_opts.set_level_zero_file_num_compaction_trigger($opt.level0_file_num_compaction_trigger);
        causet_opts.set_level_zero_slowdown_writes_trigger($opt.level0_slowdown_writes_trigger);
        causet_opts.set_level_zero_stop_writes_trigger($opt.level0_stop_writes_trigger);
        causet_opts.set_max_compaction_bytes($opt.max_compaction_bytes.0);
        causet_opts.compaction_priority($opt.compaction_pri);
        causet_opts.set_level_compaction_dynamic_level_bytes($opt.dynamic_level_bytes);
        causet_opts.set_max_bytes_for_level_multiplier($opt.max_bytes_for_level_multiplier);
        causet_opts.set_compaction_style($opt.compaction_style);
        causet_opts.set_disable_auto_compactions($opt.disable_auto_compactions);
        causet_opts.set_soft_plightlikeing_compaction_bytes_limit($opt.soft_plightlikeing_compaction_bytes_limit.0);
        causet_opts.set_hard_plightlikeing_compaction_bytes_limit($opt.hard_plightlikeing_compaction_bytes_limit.0);
        causet_opts.set_optimize_filters_for_hits($opt.optimize_filters_for_hits);
        causet_opts.set_force_consistency_checks($opt.force_consistency_checks);
        if $opt.enable_doubly_skiplist {
            causet_opts.set_doubly_skiplist();
        }
        causet_opts
    }};
}

causet_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        DefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_causet(false, Causet_DEFAULT) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(64),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan: NoetherCfConfig::default(),
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> PrimaryCausetNetworkOptions {
        let mut causet_opts = build_causet_opt!(self, cache);
        let f = Box::new(ConePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_tuplespaceInstanton_index_distance: self.prop_tuplespaceInstanton_index_distance,
        });
        causet_opts.add_Block_properties_collector_factory("edb.cone-properties-collector", f);
        causet_opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        causet_opts
    }
}

causet_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        // Setting blob_run_mode=read_only effectively disable Noether.
        let mut titan = NoetherCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        WriteCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_causet(false, Causet_WRITE) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_key_filtering: false,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(64),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> PrimaryCausetNetworkOptions {
        let mut causet_opts = build_causet_opt!(self, cache);
        // Prefix extractor(trim the timestamp at tail) for write causet.
        let e = Box::new(FixedSuffixSliceTransform::new(8));
        causet_opts
            .set_prefix_extractor("FixedSuffixSliceTransform", e)
            .unwrap();
        // Create prefix bloom filter for memBlock.
        causet_opts.set_memBlock_prefix_bloom_size_ratio(0.1);
        // Collects user defined properties.
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.tail_pointer-properties-collector", f);
        let f = Box::new(ConePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_tuplespaceInstanton_index_distance: self.prop_tuplespaceInstanton_index_distance,
        });
        causet_opts.add_Block_properties_collector_factory("edb.cone-properties-collector", f);
        causet_opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        causet_opts
            .set_compaction_filter_factory(
                "write_compaction_filter_factory",
                Box::new(WriteCompactionFilterFactory {}) as Box<dyn CompactionFilterFactory>,
            )
            .unwrap();
        causet_opts
    }
}

causet_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        // Setting blob_run_mode=read_only effectively disable Noether.
        let mut titan = NoetherCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(memory_mb_for_causet(false, Causet_DAGGER) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(32),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(64),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> PrimaryCausetNetworkOptions {
        let mut causet_opts = build_causet_opt!(self, cache);
        let f = Box::new(NoopSliceTransform);
        causet_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        let f = Box::new(ConePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_tuplespaceInstanton_index_distance: self.prop_tuplespaceInstanton_index_distance,
        });
        causet_opts.add_Block_properties_collector_factory("edb.cone-properties-collector", f);
        causet_opts.set_memBlock_prefix_bloom_size_ratio(0.1);
        causet_opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        causet_opts
    }
}

causet_config!(VioletaBftCfConfig);

impl Default for VioletaBftCfConfig {
    fn default() -> VioletaBftCfConfig {
        // Setting blob_run_mode=read_only effectively disable Noether.
        let mut titan = NoetherCfConfig::default();
        titan.blob_run_mode = BlobRunMode::ReadOnly;
        VioletaBftCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(128),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(64),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan,
        }
    }
}

impl VioletaBftCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> PrimaryCausetNetworkOptions {
        let mut causet_opts = build_causet_opt!(self, cache);
        let f = Box::new(NoopSliceTransform);
        causet_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        causet_opts.set_memBlock_prefix_bloom_size_ratio(0.1);
        causet_opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        causet_opts
    }
}

causet_config!(VersionCfConfig);

impl Default for VersionCfConfig {
    fn default() -> VersionCfConfig {
        VersionCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_causet(false, Causet_VER_DEFAULT) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(64),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan: NoetherCfConfig::default(),
        }
    }
}

impl VersionCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> PrimaryCausetNetworkOptions {
        let mut causet_opts = build_causet_opt!(self, cache);
        let f = Box::new(ConePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_tuplespaceInstanton_index_distance: self.prop_tuplespaceInstanton_index_distance,
        });
        causet_opts.add_Block_properties_collector_factory("edb.cone-properties-collector", f);
        causet_opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        causet_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
// Note that Noether is still an experimental feature. Once enabled, it can't fall back.
// Forced fallback may result in data loss.
pub struct NoetherDBConfig {
    pub enabled: bool,
    pub dirname: String,
    pub disable_gc: bool,
    pub max_background_gc: i32,
    // The value of this field will be truncated to seconds.
    pub purge_obsolete_files_period: ReadableDuration,
}

impl Default for NoetherDBConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dirname: "".to_owned(),
            disable_gc: false,
            max_background_gc: 4,
            purge_obsolete_files_period: ReadableDuration::secs(10),
        }
    }
}

impl NoetherDBConfig {
    fn build_opts(&self) -> NoetherDBOptions {
        let mut opts = NoetherDBOptions::new();
        opts.set_dirname(&self.dirname);
        opts.set_disable_background_gc(self.disable_gc);
        opts.set_max_background_gc(self.max_background_gc);
        opts.set_purge_obsolete_files_period(self.purge_obsolete_files_period.as_secs() as usize);
        opts
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[config(skip)]
    pub info_log_level: LogLevel,
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[config(skip)]
    pub wal_dir: String,
    #[config(skip)]
    pub wal_ttl_seconds: u64,
    #[config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    #[config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[config(skip)]
    pub enable_statistics: bool,
    #[config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    #[config(skip)]
    pub info_log_max_size: ReadableSize,
    #[config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[config(skip)]
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    #[config(skip)]
    pub rate_limiter_refill_period: ReadableDuration,
    #[serde(with = "rocks_config::rate_limiter_mode_serde")]
    #[config(skip)]
    pub rate_limiter_mode: DBRateLimiterMode,
    #[config(skip)]
    pub auto_tuned: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[config(skip)]
    pub max_sub_compactions: u32,
    pub wriBlock_file_max_buffer_size: ReadableSize,
    #[config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[config(skip)]
    pub enable_pipelined_write: bool,
    #[config(skip)]
    pub enable_multi_batch_write: bool,
    #[config(skip)]
    pub enable_unordered_write: bool,
    #[config(submodule)]
    pub defaultcauset: DefaultCfConfig,
    #[config(submodule)]
    pub writecauset: WriteCfConfig,
    #[config(submodule)]
    pub lockcauset: LockCfConfig,
    #[config(submodule)]
    pub violetabftcauset: VioletaBftCfConfig,
    #[config(submodule)]
    pub ver_defaultcauset: VersionCfConfig,
    #[config(skip)]
    pub titan: NoetherDBConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        let (max_background_jobs, max_sub_compactions, max_background_gc) =
            get_background_job_limit(8, 3, 4);
        let mut titan_config = NoetherDBConfig::default();
        titan_config.max_background_gc = max_background_gc;
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs,
            max_manifest_file_size: ReadableSize::mb(128),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_level: LogLevel::Info,
            rate_bytes_per_sec: ReadableSize::kb(0),
            rate_limiter_refill_period: ReadableDuration::millis(100),
            rate_limiter_mode: DBRateLimiterMode::WriteOnly,
            auto_tuned: false,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions,
            wriBlock_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            enable_multi_batch_write: true,
            enable_unordered_write: false,
            defaultcauset: DefaultCfConfig::default(),
            writecauset: WriteCfConfig::default(),
            lockcauset: LockCfConfig::default(),
            violetabftcauset: VioletaBftCfConfig::default(),
            ver_defaultcauset: VersionCfConfig::default(),
            titan: titan_config,
        }
    }
}

impl DbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        if self.rate_bytes_per_sec.0 > 0 {
            opts.set_ratelimiter_with_auto_tuned(
                self.rate_bytes_per_sec.0 as i64,
                (self.rate_limiter_refill_period.as_millis() * 1000) as i64,
                self.rate_limiter_mode,
                self.auto_tuned,
            );
        }

        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_wriBlock_file_max_buffer_size(self.wriBlock_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(
            (self.enable_pipelined_write || self.enable_multi_batch_write)
                && !self.enable_unordered_write,
        );
        opts.enable_multi_batch_write(self.enable_multi_batch_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.add_event_listener(LmdbEventListener::new("kv"));
        opts.set_info_log(LmdbdbLogger::default());
        opts.set_info_log_level(self.info_log_level.into());
        if self.titan.enabled {
            opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        }
        opts
    }

    pub fn build_causet_opts(&self, cache: &Option<Cache>) -> Vec<CausetOptions<'_>> {
        vec![
            CausetOptions::new(Causet_DEFAULT, self.defaultcauset.build_opt(cache)),
            CausetOptions::new(Causet_DAGGER, self.lockcauset.build_opt(cache)),
            CausetOptions::new(Causet_WRITE, self.writecauset.build_opt(cache)),
            // TODO: remove Causet_VIOLETABFT.
            CausetOptions::new(Causet_VIOLETABFT, self.violetabftcauset.build_opt(cache)),
            CausetOptions::new(Causet_VER_DEFAULT, self.ver_defaultcauset.build_opt(cache)),
        ]
    }

    pub fn build_causet_opts_v2(&self, cache: &Option<Cache>) -> Vec<CausetOptions<'_>> {
        vec![
            CausetOptions::new(Causet_DEFAULT, self.defaultcauset.build_opt(cache)),
            CausetOptions::new(Causet_DAGGER, self.lockcauset.build_opt(cache)),
            CausetOptions::new(Causet_WRITE, self.writecauset.build_opt(cache)),
            CausetOptions::new(Causet_VIOLETABFT, self.violetabftcauset.build_opt(cache)),
            CausetOptions::new(Causet_VER_DEFAULT, self.ver_defaultcauset.build_opt(cache)),
        ]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcauset.validate()?;
        self.lockcauset.validate()?;
        self.writecauset.validate()?;
        self.violetabftcauset.validate()?;
        self.ver_defaultcauset.validate()?;
        self.titan.validate()?;
        if self.enable_unordered_write {
            if self.titan.enabled {
                return Err("Lmdb.unordered_write does not support Noether".into());
            }
            if self.enable_pipelined_write || self.enable_multi_batch_write {
                return Err("pipelined_write is not compatible with unordered_write".into());
            }
        }
        Ok(())
    }

    fn write_into_metrics(&self) {
        write_into_metrics!(self.defaultcauset, Causet_DEFAULT, CONFIG_LMDB_GAUGE);
        write_into_metrics!(self.lockcauset, Causet_DAGGER, CONFIG_LMDB_GAUGE);
        write_into_metrics!(self.writecauset, Causet_WRITE, CONFIG_LMDB_GAUGE);
        write_into_metrics!(self.violetabftcauset, Causet_VIOLETABFT, CONFIG_LMDB_GAUGE);
        write_into_metrics!(self.ver_defaultcauset, Causet_VER_DEFAULT, CONFIG_LMDB_GAUGE);
    }
}

causet_config!(VioletaBftDefaultCfConfig);

impl Default for VioletaBftDefaultCfConfig {
    fn default() -> VioletaBftDefaultCfConfig {
        VioletaBftDefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_causet(true, Causet_DEFAULT) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(64),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: true,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            titan: NoetherCfConfig::default(),
        }
    }
}

impl VioletaBftDefaultCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> PrimaryCausetNetworkOptions {
        let mut causet_opts = build_causet_opt!(self, cache);
        let f = Box::new(FixedPrefixSliceTransform::new(brane_violetabft_prefix_len()));
        causet_opts
            .set_memBlock_insert_hint_prefix_extractor("VioletaBftPrefixSliceTransform", f)
            .unwrap();
        causet_opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        causet_opts
    }
}

// Lmdb Env associate thread pools of multiple instances from the same process.
// When construct Options, options.env is set to same singleton Env::Default() object.
// So total max_background_jobs = max(lmdb.max_background_jobs, violetabftdb.max_background_jobs)
// But each instance will limit their background jobs according to their own max_background_jobs
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct VioletaBftDbConfig {
    #[serde(with = "rocks_config::recovery_mode_serde")]
    #[config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[config(skip)]
    pub wal_dir: String,
    #[config(skip)]
    pub wal_ttl_seconds: u64,
    #[config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    #[config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[config(skip)]
    pub enable_statistics: bool,
    #[config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    #[config(skip)]
    pub info_log_max_size: ReadableSize,
    #[config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[config(skip)]
    pub info_log_dir: String,
    #[config(skip)]
    pub info_log_level: LogLevel,
    #[config(skip)]
    pub max_sub_compactions: u32,
    pub wriBlock_file_max_buffer_size: ReadableSize,
    #[config(skip)]
    pub use_direct_io_for_flush_and_compaction: bool,
    #[config(skip)]
    pub enable_pipelined_write: bool,
    #[config(skip)]
    pub enable_unordered_write: bool,
    #[config(skip)]
    pub allow_concurrent_memBlock_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[config(submodule)]
    pub defaultcauset: VioletaBftDefaultCfConfig,
    #[config(skip)]
    pub titan: NoetherDBConfig,
}

impl Default for VioletaBftDbConfig {
    fn default() -> VioletaBftDbConfig {
        let (max_background_jobs, max_sub_compactions, max_background_gc) =
            get_background_job_limit(4, 2, 4);
        let mut titan_config = NoetherDBConfig::default();
        titan_config.max_background_gc = max_background_gc;
        VioletaBftDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs,
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_level: LogLevel::Info,
            max_sub_compactions,
            wriBlock_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            enable_unordered_write: false,
            allow_concurrent_memBlock_write: true,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            defaultcauset: VioletaBftDefaultCfConfig::default(),
            titan: titan_config,
        }
    }
}

impl VioletaBftDbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_compaction_readahead_size(self.compaction_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        opts.set_info_log(VioletaBftDBLogger::default());
        opts.set_info_log_level(self.info_log_level.into());
        opts.set_max_subcompactions(self.max_sub_compactions);
        opts.set_wriBlock_file_max_buffer_size(self.wriBlock_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_compaction(
            self.use_direct_io_for_flush_and_compaction,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.allow_concurrent_memBlock_write(self.allow_concurrent_memBlock_write);
        opts.add_event_listener(LmdbEventListener::new("violetabft"));
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        // TODO maybe create a new env for violetabft engine
        if self.titan.enabled {
            opts.tenancy_launched_for_einsteindb(&self.titan.build_opts());
        }

        opts
    }

    pub fn build_causet_opts(&self, cache: &Option<Cache>) -> Vec<CausetOptions<'_>> {
        vec![CausetOptions::new(Causet_DEFAULT, self.defaultcauset.build_opt(cache))]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultcauset.validate()?;
        if self.enable_unordered_write {
            if self.titan.enabled {
                return Err("violetabftdb: unordered_write is not compatible with Noether".into());
            }
            if self.enable_pipelined_write {
                return Err(
                    "violetabftdb: pipelined_write is not compatible with unordered_write".into(),
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
#[serde(default, rename_all = "kebab-case")]
pub struct VioletaBftEngineConfig {
    pub enable: bool,
    #[serde(flatten)]
    config: RawVioletaBftEngineConfig,
}

impl VioletaBftEngineConfig {
    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.config.validate().map_err(|e| Box::new(e))?;
        Ok(())
    }

    pub fn config(&self) -> RawVioletaBftEngineConfig {
        self.config.clone()
    }

    pub fn mut_config(&mut self) -> &mut RawVioletaBftEngineConfig {
        &mut self.config
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DBType {
    Kv,
    VioletaBft,
}

pub struct DBConfigManger {
    db: LmdbEngine,
    db_type: DBType,
    shared_block_cache: bool,
}

impl DBConfigManger {
    pub fn new(db: LmdbEngine, db_type: DBType, shared_block_cache: bool) -> Self {
        DBConfigManger {
            db,
            db_type,
            shared_block_cache,
        }
    }
}

impl DBConfigManger {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.db.set_db_options(opts)?;
        Ok(())
    }

    fn set_causet_config(&self, causet: &str, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.validate_causet(causet)?;
        let handle = self.db.causet_handle(causet)?;
        self.db.set_options_causet(handle, opts)?;
        // Write config to metric
        for (causet_name, causet_value) in opts {
            let causet_value = match causet_value {
                v if *v == "true" => Ok(1f64),
                v if *v == "false" => Ok(0f64),
                v => v.parse::<f64>(),
            };
            if let Ok(v) = causet_value {
                CONFIG_LMDB_GAUGE
                    .with_label_values(&[causet, causet_name])
                    .set(v);
            }
        }
        Ok(())
    }

    fn set_block_cache_size(&self, causet: &str, size: ReadableSize) -> Result<(), Box<dyn Error>> {
        self.validate_causet(causet)?;
        if self.shared_block_cache {
            return Err("shared block cache is enabled, change cache size through \
                 block-cache.capacity in causet_storage module instead"
                .into());
        }
        let handle = self.db.causet_handle(causet)?;
        let opt = self.db.get_options_causet(handle);
        opt.serialize_capacity(size.0)?;
        // Write config to metric
        CONFIG_LMDB_GAUGE
            .with_label_values(&[causet, "block_cache_size"])
            .set(size.0 as f64);
        Ok(())
    }

    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> Result<(), Box<dyn Error>> {
        let mut opt = self.db.as_inner().get_db_options();
        opt.set_rate_bytes_per_sec(rate_bytes_per_sec)?;
        Ok(())
    }

    fn validate_causet(&self, causet: &str) -> Result<(), Box<dyn Error>> {
        match (self.db_type, causet) {
            (DBType::Kv, Causet_DEFAULT)
            | (DBType::Kv, Causet_WRITE)
            | (DBType::Kv, Causet_DAGGER)
            | (DBType::Kv, Causet_VIOLETABFT)
            | (DBType::Kv, Causet_VER_DEFAULT)
            | (DBType::VioletaBft, Causet_DEFAULT) => Ok(()),
            _ => Err(format!("invalid causet {:?} for db {:?}", causet, self.db_type).into()),
        }
    }
}

impl ConfigManager for DBConfigManger {
    fn dispatch(&mut self, change: ConfigChange) -> Result<(), Box<dyn Error>> {
        let change_str = format!("{:?}", change);
        let mut change: Vec<(String, ConfigValue)> = change.into_iter().collect();
        let causet_config = change.drain_filter(|(name, _)| name.lightlikes_with("causet"));
        for (causet_name, causet_change) in causet_config {
            if let ConfigValue::Module(mut causet_change) = causet_change {
                // defaultcauset -> default
                let causet_name = &causet_name[..(causet_name.len() - 2)];
                if let Some(v) = causet_change.remove("block_cache_size") {
                    // currently we can't modify block_cache_size via set_options_causet
                    self.set_block_cache_size(&causet_name, v.into())?;
                }
                if let Some(ConfigValue::Module(titan_change)) = causet_change.remove("titan") {
                    for (name, value) in titan_change {
                        causet_change.insert(name, value);
                    }
                }
                if !causet_change.is_empty() {
                    let causet_change = config_value_to_string(causet_change.into_iter().collect());
                    let causet_change_slice = config_to_slice(&causet_change);
                    self.set_causet_config(&causet_name, &causet_change_slice)?;
                }
            }
        }

        if let Some(rate_bytes_config) = change
            .drain_filter(|(name, _)| name == "rate_bytes_per_sec")
            .next()
        {
            let rate_bytes_per_sec: ReadableSize = rate_bytes_config.1.into();
            self.set_rate_bytes_per_sec(rate_bytes_per_sec.0 as i64)?;
        }

        if !change.is_empty() {
            let change = config_value_to_string(change);
            let change_slice = config_to_slice(&change);
            self.set_db_config(&change_slice)?;
        }
        info!(
            "lmdb config changed";
            "db" => ?self.db_type,
            "change" => change_str
        );
        Ok(())
    }
}

fn config_to_slice(config_change: &[(String, String)]) -> Vec<(&str, &str)> {
    config_change
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

// Convert `ConfigValue` to formatted String that can pass to `DB::set_db_options`
fn config_value_to_string(config_change: Vec<(String, ConfigValue)>) -> Vec<(String, String)> {
    config_change
        .into_iter()
        .filter_map(|(name, value)| {
            let v = match value {
                d @ ConfigValue::Duration(_) => {
                    let d: ReadableDuration = d.into();
                    Some(d.as_secs().to_string())
                }
                s @ ConfigValue::Size(_) => {
                    let s: ReadableSize = s.into();
                    Some(s.0.to_string())
                }
                s @ ConfigValue::OptionSize(_) => {
                    let s: OptionReadableSize = s.into();
                    s.0.map(|v| v.0.to_string())
                }
                ConfigValue::Module(_) => unreachable!(),
                v => Some(format!("{}", v)),
            };
            v.map(|v| (name, v))
        })
        .collect()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MetricConfig {
    pub interval: ReadableDuration,
    pub address: String,
    pub job: String,
}

impl Default for MetricConfig {
    fn default() -> MetricConfig {
        MetricConfig {
            interval: ReadableDuration::secs(15),
            address: "".to_owned(),
            job: "edb".to_owned(),
        }
    }
}

pub mod log_level_serde {
    use serde::{
        de::{Error, Unexpected},
        Deserialize, Deserializer, Serialize, Serializer,
    };
    use slog::Level;
    use violetabftstore::interlock::::logger::{get_level_by_string, get_string_by_level};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        get_level_by_string(&string)
            .ok_or_else(|| D::Error::invalid_value(Unexpected::Str(&string), &"a valid log level"))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(value: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        get_string_by_level(*value).serialize(serializer)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct UnifiedReadPoolConfig {
    pub min_thread_count: usize,
    pub max_thread_count: usize,
    pub stack_size: ReadableSize,
    pub max_tasks_per_worker: usize,
    // FIXME: Add more configs when they are effective in yatp
}

impl UnifiedReadPoolConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.min_thread_count == 0 {
            return Err("readpool.unified.min-thread-count should be > 0"
                .to_string()
                .into());
        }
        if self.max_thread_count < self.min_thread_count {
            return Err(
                "readpool.unified.max-thread-count should be >= readpool.unified.min-thread-count"
                    .to_string()
                    .into(),
            );
        }
        if self.stack_size.0 < ReadableSize::mb(2).0 {
            return Err("readpool.unified.stack-size should be >= 2mb"
                .to_string()
                .into());
        }
        if self.max_tasks_per_worker <= 1 {
            return Err("readpool.unified.max-tasks-per-worker should be > 1"
                .to_string()
                .into());
        }
        Ok(())
    }
}

const UNIFIED_READPOOL_MIN_CONCURRENCY: usize = 4;

// FIXME: Use macros to generate it if yatp is used elsewhere besides readpool.
impl Default for UnifiedReadPoolConfig {
    fn default() -> UnifiedReadPoolConfig {
        let cpu_num = SysQuota::new().cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(UNIFIED_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            min_thread_count: 1,
            max_thread_count: concurrency,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
            max_tasks_per_worker: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
        }
    }
}

#[causet(test)]
mod unified_read_pool_tests {
    use super::*;

    #[test]
    fn test_validate() {
        let causet = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            stack_size: ReadableSize::mb(2),
            max_tasks_per_worker: 2000,
        };
        assert!(causet.validate().is_ok());

        let invalid_causet = UnifiedReadPoolConfig {
            min_thread_count: 0,
            ..causet
        };
        assert!(invalid_causet.validate().is_err());

        let invalid_causet = UnifiedReadPoolConfig {
            min_thread_count: 2,
            max_thread_count: 1,
            ..causet
        };
        assert!(invalid_causet.validate().is_err());

        let invalid_causet = UnifiedReadPoolConfig {
            stack_size: ReadableSize::mb(1),
            ..causet
        };
        assert!(invalid_causet.validate().is_err());

        let invalid_causet = UnifiedReadPoolConfig {
            max_tasks_per_worker: 1,
            ..causet
        };
        assert!(invalid_causet.validate().is_err());
    }
}

macro_rules! readpool_config {
    ($struct_name:ident, $test_mod_name:ident, $display_name:expr) => {
        #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $struct_name {
            pub use_unified_pool: Option<bool>,
            pub high_concurrency: usize,
            pub normal_concurrency: usize,
            pub low_concurrency: usize,
            pub max_tasks_per_worker_high: usize,
            pub max_tasks_per_worker_normal: usize,
            pub max_tasks_per_worker_low: usize,
            pub stack_size: ReadableSize,
        }

        impl $struct_name {
            /// Builds configurations for low, normal and high priority pools.
            pub fn to_yatp_pool_configs(&self) -> Vec<yatp_pool::Config> {
                vec![
                    yatp_pool::Config {
                        workers: self.low_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_low,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.normal_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_normal,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.high_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_high,
                        stack_size: self.stack_size.0 as usize,
                    },
                ]
            }

            pub fn default_for_test() -> Self {
                Self {
                    use_unified_pool: None,
                    high_concurrency: 2,
                    normal_concurrency: 2,
                    low_concurrency: 2,
                    max_tasks_per_worker_high: 2000,
                    max_tasks_per_worker_normal: 2000,
                    max_tasks_per_worker_low: 2000,
                    stack_size: ReadableSize::mb(1),
                }
            }

            pub fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.use_unified_pool() {
                    return Ok(());
                }
                if self.high_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.high-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.normal_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.normal-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.low_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.low-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.stack_size.0 < ReadableSize::mb(MIN_READPOOL_STACK_SIZE_MB).0 {
                    return Err(format!(
                        "readpool.{}.stack-size should be >= {}mb",
                        $display_name, MIN_READPOOL_STACK_SIZE_MB
                    )
                    .into());
                }
                if self.max_tasks_per_worker_high <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-high should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_normal <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-normal should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_low <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-low should be > 1",
                        $display_name
                    )
                    .into());
                }

                Ok(())
            }
        }

        #[causet(test)]
        mod $test_mod_name {
            use super::*;

            #[test]
            fn test_validate() {
                let causet = $struct_name::default();
                assert!(causet.validate().is_ok());

                let mut invalid_causet = causet.clone();
                invalid_causet.high_concurrency = 0;
                assert!(invalid_causet.validate().is_err());

                let mut invalid_causet = causet.clone();
                invalid_causet.normal_concurrency = 0;
                assert!(invalid_causet.validate().is_err());

                let mut invalid_causet = causet.clone();
                invalid_causet.low_concurrency = 0;
                assert!(invalid_causet.validate().is_err());

                let mut invalid_causet = causet.clone();
                invalid_causet.stack_size = ReadableSize::mb(1);
                assert!(invalid_causet.validate().is_err());

                let mut invalid_causet = causet.clone();
                invalid_causet.max_tasks_per_worker_high = 0;
                assert!(invalid_causet.validate().is_err());
                invalid_causet.max_tasks_per_worker_high = 1;
                assert!(invalid_causet.validate().is_err());
                invalid_causet.max_tasks_per_worker_high = 100;
                assert!(causet.validate().is_ok());

                let mut invalid_causet = causet.clone();
                invalid_causet.max_tasks_per_worker_normal = 0;
                assert!(invalid_causet.validate().is_err());
                invalid_causet.max_tasks_per_worker_normal = 1;
                assert!(invalid_causet.validate().is_err());
                invalid_causet.max_tasks_per_worker_normal = 100;
                assert!(causet.validate().is_ok());

                let mut invalid_causet = causet.clone();
                invalid_causet.max_tasks_per_worker_low = 0;
                assert!(invalid_causet.validate().is_err());
                invalid_causet.max_tasks_per_worker_low = 1;
                assert!(invalid_causet.validate().is_err());
                invalid_causet.max_tasks_per_worker_low = 100;
                assert!(causet.validate().is_ok());

                let mut invalid_but_unified = causet.clone();
                invalid_but_unified.use_unified_pool = Some(true);
                invalid_but_unified.low_concurrency = 0;
                assert!(invalid_but_unified.validate().is_ok());
            }
        }
    };
}

const DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY: usize = 4;
const DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual spacelikeed. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
const DEFAULT_READPOOL_MAX_TASKS_PER_WORKER: usize = 2 * 1000;

const MIN_READPOOL_STACK_SIZE_MB: u64 = 2;
const DEFAULT_READPOOL_STACK_SIZE_MB: u64 = 10;

readpool_config!(StorageReadPoolConfig, causet_storage_read_pool_test, "causet_storage");

impl Default for StorageReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::new().cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.5) as usize;
        concurrency = cmp::max(DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY, concurrency);
        concurrency = cmp::min(DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

impl StorageReadPoolConfig {
    pub fn use_unified_pool(&self) -> bool {
        // The causet_storage module does not use the unified pool by default.
        self.use_unified_pool.unwrap_or(false)
    }

    pub fn adjust_use_unified_pool(&mut self) {
        if self.use_unified_pool.is_none() {
            // The causet_storage module does not use the unified pool by default.
            info!("readpool.causet_storage.use-unified-pool is not set, set to false by default");
            self.use_unified_pool = Some(false);
        }
    }
}

const DEFAULT_INTERLOCK_READPOOL_MIN_CONCURRENCY: usize = 2;

readpool_config!(
    CoprReadPoolConfig,
    interlock_read_pool_test,
    "interlock"
);

impl Default for CoprReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::new().cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(DEFAULT_INTERLOCK_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

impl CoprReadPoolConfig {
    pub fn use_unified_pool(&self) -> bool {
        // The interlock module uses the unified pool unless it has customized configurations.
        self.use_unified_pool
            .unwrap_or_else(|| *self == Default::default())
    }

    pub fn adjust_use_unified_pool(&mut self) {
        if self.use_unified_pool.is_none() {
            // The interlock module uses the unified pool unless it has customized configurations.
            if *self == Default::default() {
                info!("readpool.interlock.use-unified-pool is not set, set to true by default");
                self.use_unified_pool = Some(true);
            } else {
                info!("readpool.interlock.use-unified-pool is not set, set to false because there are other customized configurations");
                self.use_unified_pool = Some(false);
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    pub unified: UnifiedReadPoolConfig,
    pub causet_storage: StorageReadPoolConfig,
    pub interlock: CoprReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn is_unified_pool_enabled(&self) -> bool {
        self.causet_storage.use_unified_pool() || self.interlock.use_unified_pool()
    }

    pub fn adjust_use_unified_pool(&mut self) {
        self.causet_storage.adjust_use_unified_pool();
        self.interlock.adjust_use_unified_pool();
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.is_unified_pool_enabled() {
            self.unified.validate()?;
        }
        self.causet_storage.validate()?;
        self.interlock.validate()?;
        Ok(())
    }
}

#[causet(test)]
mod readpool_tests {
    use super::*;

    #[test]
    fn test_unified_disabled() {
        // Allow invalid yatp config when yatp is not used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            stack_size: ReadableSize::mb(0),
            max_tasks_per_worker: 0,
        };
        assert!(unified.validate().is_err());
        let causet_storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(causet_storage.validate().is_ok());
        let interlock = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(interlock.validate().is_ok());
        let causet = ReadPoolConfig {
            unified,
            causet_storage,
            interlock,
        };
        assert!(!causet.is_unified_pool_enabled());
        assert!(causet.validate().is_ok());

        // causet_storage and interlock config must be valid when yatp is not used.
        let unified = UnifiedReadPoolConfig::default();
        assert!(unified.validate().is_ok());
        let causet_storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            high_concurrency: 0,
            ..Default::default()
        };
        assert!(causet_storage.validate().is_err());
        let interlock = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        let invalid_causet = ReadPoolConfig {
            unified,
            causet_storage,
            interlock,
        };
        assert!(!invalid_causet.is_unified_pool_enabled());
        assert!(invalid_causet.validate().is_err());
    }

    #[test]
    fn test_unified_enabled() {
        // Yatp config must be valid when yatp is used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            ..Default::default()
        };
        assert!(unified.validate().is_err());
        let causet_storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(causet_storage.validate().is_ok());
        let interlock = CoprReadPoolConfig::default();
        assert!(interlock.validate().is_ok());
        let mut causet = ReadPoolConfig {
            unified,
            causet_storage,
            interlock,
        };
        causet.adjust_use_unified_pool();
        assert!(causet.is_unified_pool_enabled());
        assert!(causet.validate().is_err());
    }

    #[test]
    fn test_is_unified() {
        let causet_storage = StorageReadPoolConfig::default();
        assert!(!causet_storage.use_unified_pool());
        let interlock = CoprReadPoolConfig::default();
        assert!(interlock.use_unified_pool());

        let mut causet = ReadPoolConfig {
            causet_storage,
            interlock,
            ..Default::default()
        };
        assert!(causet.is_unified_pool_enabled());

        causet.interlock.use_unified_pool = Some(false);
        assert!(!causet.is_unified_pool_enabled());
    }

    #[test]
    fn test_partially_unified() {
        let causet_storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!causet_storage.use_unified_pool());
        let interlock = CoprReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(interlock.use_unified_pool());
        let mut causet = ReadPoolConfig {
            causet_storage,
            interlock,
            ..Default::default()
        };
        assert!(causet.is_unified_pool_enabled());
        assert!(causet.validate().is_err());
        causet.causet_storage.low_concurrency = 1;
        assert!(causet.validate().is_ok());

        let causet_storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(causet_storage.use_unified_pool());
        let interlock = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!interlock.use_unified_pool());
        let mut causet = ReadPoolConfig {
            causet_storage,
            interlock,
            ..Default::default()
        };
        assert!(causet.is_unified_pool_enabled());
        assert!(causet.validate().is_err());
        causet.interlock.low_concurrency = 1;
        assert!(causet.validate().is_ok());
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BackupConfig {
    pub num_threads: usize,
}

impl BackupConfig {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.num_threads == 0 {
            return Err("backup.num_threads cannot be 0".into());
        }
        Ok(())
    }
}

impl Default for BackupConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::new().cpu_cores_quota();
        Self {
            // use at most 75% of vCPU by default
            num_threads: (cpu_num * 0.75).clamp(1.0, 32.0) as usize,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct causet_contextConfig {
    pub min_ts_interval: ReadableDuration,
    pub old_value_cache_size: usize,
}

impl Default for causet_contextConfig {
    fn default() -> Self {
        Self {
            min_ts_interval: ReadableDuration::secs(1),
            old_value_cache_size: 1024,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EINSTEINDBConfig {
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[config(hidden)]
    pub causet_path: String,

    #[config(skip)]
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,

    #[config(skip)]
    pub log_file: String,

    #[config(skip)]
    pub log_format: LogFormat,

    #[config(skip)]
    pub slow_log_file: String,

    #[config(skip)]
    pub slow_log_memory_barrier: ReadableDuration,

    #[config(skip)]
    pub log_rotation_timespan: ReadableDuration,

    #[config(skip)]
    pub log_rotation_size: ReadableSize,

    #[config(hidden)]
    pub panic_when_unexpected_key_or_data: bool,

    #[config(skip)]
    pub readpool: ReadPoolConfig,

    #[config(skip)]
    pub server: ServerConfig,

    #[config(submodule)]
    pub causet_storage: StorageConfig,

    #[config(skip)]
    pub fidel: FidelConfig,

    #[config(hidden)]
    pub metric: MetricConfig,

    #[config(submodule)]
    #[serde(rename = "violetabftstore")]
    pub violetabft_store: VioletaBftstoreConfig,

    #[config(submodule)]
    pub interlock: CopConfig,

    #[config(submodule)]
    pub lmdb: DbConfig,

    #[config(submodule)]
    pub violetabftdb: VioletaBftDbConfig,

    #[config(skip)]
    pub violetabft_engine: VioletaBftEngineConfig,

    #[config(skip)]
    pub security: SecurityConfig,

    #[config(skip)]
    pub import: ImportConfig,

    #[config(submodule)]
    pub backup: BackupConfig,

    #[config(submodule)]
    pub pessimistic_txn: PessimisticTxnConfig,

    #[config(submodule)]
    pub gc: GcConfig,

    #[config(submodule)]
    pub split: SplitConfig,

    #[config(submodule)]
    pub causet_context: causet_contextConfig,
}

impl Default for EINSTEINDBConfig {
    fn default() -> EINSTEINDBConfig {
        EINSTEINDBConfig {
            causet_path: "".to_owned(),
            log_level: slog::Level::Info,
            log_file: "".to_owned(),
            log_format: LogFormat::Text,
            slow_log_file: "".to_owned(),
            slow_log_memory_barrier: ReadableDuration::secs(1),
            log_rotation_timespan: ReadableDuration::hours(24),
            log_rotation_size: ReadableSize::mb(300),
            panic_when_unexpected_key_or_data: false,
            readpool: ReadPoolConfig::default(),
            server: ServerConfig::default(),
            metric: MetricConfig::default(),
            violetabft_store: VioletaBftstoreConfig::default(),
            interlock: CopConfig::default(),
            fidel: FidelConfig::default(),
            lmdb: DbConfig::default(),
            violetabftdb: VioletaBftDbConfig::default(),
            violetabft_engine: VioletaBftEngineConfig::default(),
            causet_storage: StorageConfig::default(),
            security: SecurityConfig::default(),
            import: ImportConfig::default(),
            backup: BackupConfig::default(),
            pessimistic_txn: PessimisticTxnConfig::default(),
            gc: GcConfig::default(),
            split: SplitConfig::default(),
            causet_context: causet_contextConfig::default(),
        }
    }
}

impl EINSTEINDBConfig {
    // TODO: change to validate(&self)
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.readpool.validate()?;
        self.causet_storage.validate()?;

        self.violetabft_store.brane_split_check_diff = self.interlock.brane_split_size / 16;

        if self.causet_path.is_empty() {
            self.causet_path = Path::new(&self.causet_storage.data_dir)
                .join(LAST_CONFIG_FILE)
                .to_str()
                .unwrap()
                .to_owned();
        }

        if !self.violetabft_engine.enable {
            let default_violetabftdb_path =
                config::canonicalize_sub_path(&self.causet_storage.data_dir, "violetabft")?;
            if self.violetabft_store.violetabftdb_path.is_empty() {
                self.violetabft_store.violetabftdb_path = default_violetabftdb_path;
            } else if self.violetabft_store.violetabftdb_path != default_violetabftdb_path {
                self.violetabft_store.violetabftdb_path =
                    config::canonicalize_path(&self.violetabft_store.violetabftdb_path)?;
            }
        } else {
            let default_er_path =
                config::canonicalize_sub_path(&self.causet_storage.data_dir, "violetabft-engine")?;
            if self.violetabft_engine.config.dir.is_empty() {
                self.violetabft_engine.config.dir = default_er_path;
            } else if self.violetabft_engine.config.dir != default_er_path {
                self.violetabft_engine.config.dir =
                    config::canonicalize_path(&self.violetabft_engine.config.dir)?;
            }
        }

        let kv_db_path =
            config::canonicalize_sub_path(&self.causet_storage.data_dir, DEFAULT_LMDB_SUB_DIR)?;

        if kv_db_path == self.violetabft_store.violetabftdb_path {
            return Err("violetabft_store.violetabftdb_path can not same with causet_storage.data_dir/db".into());
        }
        if !self.violetabft_engine.enable {
            if LmdbEngine::exists(&kv_db_path)
                && !LmdbEngine::exists(&self.violetabft_store.violetabftdb_path)
            {
                return Err("default lmdb exist, buf violetabftdb not exist".into());
            }
            if !LmdbEngine::exists(&kv_db_path)
                && LmdbEngine::exists(&self.violetabft_store.violetabftdb_path)
            {
                return Err("default lmdb not exist, buf violetabftdb exist".into());
            }
        } else {
            if LmdbEngine::exists(&kv_db_path)
                && !VioletaBftLogEngine::exists(&self.violetabft_engine.config.dir)
            {
                return Err("default lmdb exist, buf violetabft engine not exist".into());
            }
            if !LmdbEngine::exists(&kv_db_path)
                && VioletaBftLogEngine::exists(&self.violetabft_engine.config.dir)
            {
                return Err("default lmdb not exist, buf violetabft engine exist".into());
            }
        }

        // Check blob file dir is empty when titan is disabled
        if !self.lmdb.titan.enabled {
            let titandb_path = if self.lmdb.titan.dirname.is_empty() {
                Path::new(&kv_db_path).join("titandb")
            } else {
                Path::new(&self.lmdb.titan.dirname).to_path_buf()
            };
            if let Err(e) =
                violetabftstore::interlock::::config::check_data_dir_empty(titandb_path.to_str().unwrap(), "blob")
            {
                return Err(format!(
                    "check: titandb-data-dir-empty; err: \"{}\"; \
                     hint: You have disabled titan when its data directory is not empty. \
                     To properly shutdown titan, please enter fallback blob-run-mode and \
                     wait till titandb files are all safely ingested.",
                    e
                )
                .into());
            }
        }

        let expect_keepalive = self.violetabft_store.violetabft_heartbeat_interval() * 2;
        if expect_keepalive > self.server.grpc_keepalive_time.0 {
            return Err(format!(
                "grpc_keepalive_time is too small, it should not less than the double of \
                 violetabft tick interval (>= {})",
                duration_to_sec(expect_keepalive)
            )
            .into());
        }

        self.lmdb.validate()?;
        self.violetabftdb.validate()?;
        self.violetabft_engine.validate()?;
        self.server.validate()?;
        self.violetabft_store.validate()?;
        self.fidel.validate()?;
        self.interlock.validate()?;
        self.security.validate()?;
        self.import.validate()?;
        self.backup.validate()?;
        self.pessimistic_txn.validate()?;
        self.gc.validate()?;
        Ok(())
    }

    pub fn compatible_adjust(&mut self) {
        let default_violetabft_store = VioletaBftstoreConfig::default();
        let default_interlock = CopConfig::default();
        if self.violetabft_store.brane_max_size != default_violetabft_store.brane_max_size {
            warn!(
                "deprecated configuration, \
                 violetabftstore.brane-max-size has been moved to interlock"
            );
            if self.interlock.brane_max_size == default_interlock.brane_max_size {
                warn!(
                    "override interlock.brane-max-size with violetabftstore.brane-max-size, {:?}",
                    self.violetabft_store.brane_max_size
                );
                self.interlock.brane_max_size = self.violetabft_store.brane_max_size;
            }
            self.violetabft_store.brane_max_size = default_violetabft_store.brane_max_size;
        }
        if self.violetabft_store.brane_split_size != default_violetabft_store.brane_split_size {
            warn!(
                "deprecated configuration, \
                 violetabftstore.brane-split-size has been moved to interlock",
            );
            if self.interlock.brane_split_size == default_interlock.brane_split_size {
                warn!(
                    "override interlock.brane-split-size with violetabftstore.brane-split-size, {:?}",
                    self.violetabft_store.brane_split_size
                );
                self.interlock.brane_split_size = self.violetabft_store.brane_split_size;
            }
            self.violetabft_store.brane_split_size = default_violetabft_store.brane_split_size;
        }
        if self.server.lightlike_point_concurrency.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.lightlike-point-concurrency", "readpool.interlock.xxx-concurrency",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.interlock.xxx-concurrency",
                "server.lightlike-point-concurrency",
                self.server.lightlike_point_concurrency
            );
            let concurrency = self.server.lightlike_point_concurrency.take().unwrap();
            self.readpool.interlock.high_concurrency = concurrency;
            self.readpool.interlock.normal_concurrency = concurrency;
            self.readpool.interlock.low_concurrency = concurrency;
        }
        if self.server.lightlike_point_stack_size.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.lightlike-point-stack-size", "readpool.interlock.stack-size",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.interlock.stack-size",
                "server.lightlike-point-stack-size",
                self.server.lightlike_point_stack_size
            );
            self.readpool.interlock.stack_size = self.server.lightlike_point_stack_size.take().unwrap();
        }
        if self.server.lightlike_point_max_tasks.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "server.lightlike-point-max-tasks", "readpool.interlock.max-tasks-per-worker-xxx",
            );
            // Note:
            // Our `lightlike_point_max_tasks` is mostly mistakenly configured, so we don't override
            // new configuration using old values.
            self.server.lightlike_point_max_tasks = None;
        }
        if self.violetabft_store.clean_stale_peer_delay.as_secs() > 0 {
            warn!(
                "deprecated configuration, {} is no longer used and ignored.",
                "violetabft_store.clean_stale_peer_delay",
            );
        }
        // When shared block cache is enabled, if its capacity is set, it overrides individual
        // block cache sizes. Otherwise use the sum of block cache size of all PrimaryCauset families
        // as the shared cache size.
        let cache_causet = &mut self.causet_storage.block_cache;
        if cache_causet.shared && cache_causet.capacity.0.is_none() {
            cache_causet.capacity.0 = Some(ReadableSize {
                0: self.lmdb.defaultcauset.block_cache_size.0
                    + self.lmdb.writecauset.block_cache_size.0
                    + self.lmdb.lockcauset.block_cache_size.0
                    + self.violetabftdb.defaultcauset.block_cache_size.0,
            });
        }

        self.readpool.adjust_use_unified_pool();
    }

    pub fn check_critical_causet_with(&self, last_causet: &Self) -> Result<(), String> {
        if last_causet.lmdb.wal_dir != self.lmdb.wal_dir {
            return Err(format!(
                "db wal_dir have been changed, former db wal_dir is '{}', \
                 current db wal_dir is '{}', please guarantee all data wal logs \
                 have been moved to destination directory.",
                last_causet.lmdb.wal_dir, self.lmdb.wal_dir
            ));
        }

        if last_causet.violetabftdb.wal_dir != self.violetabftdb.wal_dir {
            return Err(format!(
                "violetabftdb wal_dir have been changed, former violetabftdb wal_dir is '{}', \
                 current violetabftdb wal_dir is '{}', please guarantee all violetabft wal logs \
                 have been moved to destination directory.",
                last_causet.violetabftdb.wal_dir, self.lmdb.wal_dir
            ));
        }

        if last_causet.causet_storage.data_dir != self.causet_storage.data_dir {
            // In edb 3.0 the default value of causet_storage.data-dir changed
            // from "" to "./"
            let using_default_after_upgrade =
                last_causet.causet_storage.data_dir.is_empty() && self.causet_storage.data_dir == DEFAULT_DATA_DIR;

            if !using_default_after_upgrade {
                return Err(format!(
                    "causet_storage data dir have been changed, former data dir is {}, \
                     current data dir is {}, please check if it is expected.",
                    last_causet.causet_storage.data_dir, self.causet_storage.data_dir
                ));
            }
        }

        if last_causet.violetabft_store.violetabftdb_path != self.violetabft_store.violetabftdb_path {
            return Err(format!(
                "violetabft dir have been changed, former violetabft dir is '{}', \
                 current violetabft dir is '{}', please check if it is expected.",
                last_causet.violetabft_store.violetabftdb_path, self.violetabft_store.violetabftdb_path
            ));
        }
        if last_causet.violetabft_engine.enable
            && self.violetabft_engine.enable
            && last_causet.violetabft_engine.config.dir != self.violetabft_engine.config.dir
        {
            return Err(format!(
                "violetabft engine dir have been changed, former is '{}', \
                 current is '{}', please check if it is expected.",
                last_causet.violetabft_engine.config.dir, self.violetabft_engine.config.dir
            ));
        }
        if last_causet.violetabft_engine.enable && !self.violetabft_engine.enable {
            return Err("violetabft engine can't be disabled after switched on.".to_owned());
        }

        Ok(())
    }

    pub fn from_file(path: &Path, unrecognized_tuplespaceInstanton: Option<&mut Vec<String>>) -> Self {
        (|| -> Result<Self, Box<dyn Error>> {
            let s = fs::read_to_string(path)?;
            let mut deserializer = toml::Deserializer::new(&s);
            let mut causet = if let Some(tuplespaceInstanton) = unrecognized_tuplespaceInstanton {
                serde_ignored::deserialize(&mut deserializer, |key| tuplespaceInstanton.push(key.to_string()))
            } else {
                <EINSTEINDBConfig as serde::Deserialize>::deserialize(&mut deserializer)
            }?;
            deserializer.lightlike()?;
            causet.causet_path = path.display().to_string();
            Ok(causet)
        })()
        .unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                path.display(),
                e
            );
        })
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), IoError> {
        let content = ::toml::to_string(&self).unwrap();
        let mut f = fs::File::create(&path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;

        Ok(())
    }

    pub fn write_into_metrics(&self) {
        self.violetabft_store.write_into_metrics();
        self.lmdb.write_into_metrics();
    }

    pub fn with_tmp() -> Result<(EINSTEINDBConfig, tempfile::TempDir), IoError> {
        let tmp = tempfile::temfidelir()?;
        let mut causet = EINSTEINDBConfig::default();
        causet.causet_storage.data_dir = tmp.path().display().to_string();
        causet.causet_path = tmp.path().join(LAST_CONFIG_FILE).display().to_string();
        Ok((causet, tmp))
    }
}

/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_edb.toml`,
/// compares key configuration items and fails if they are not
/// identical.
pub fn check_critical_config(config: &EINSTEINDBConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    if let Some(mut causet) = get_last_config(&config.causet_storage.data_dir) {
        causet.compatible_adjust();
        let _ = causet.validate();
        config.check_critical_causet_with(&causet)?;
    }
    Ok(())
}

fn get_last_config(data_dir: &str) -> Option<EINSTEINDBConfig> {
    let store_path = Path::new(data_dir);
    let last_causet_path = store_path.join(LAST_CONFIG_FILE);
    if last_causet_path.exists() {
        return Some(EINSTEINDBConfig::from_file(&last_causet_path, None));
    }
    None
}

/// Persists config to `last_edb.toml`
pub fn persist_config(config: &EINSTEINDBConfig) -> Result<(), String> {
    let store_path = Path::new(&config.causet_storage.data_dir);
    let last_causet_path = store_path.join(LAST_CONFIG_FILE);
    let tmp_causet_path = store_path.join(TMP_CONFIG_FILE);

    let same_as_last_causet = fs::read_to_string(&last_causet_path).map_or(false, |last_causet| {
        toml::to_string(&config).unwrap() == last_causet
    });
    if same_as_last_causet {
        return Ok(());
    }

    // Create parent directory if missing.
    if let Err(e) = fs::create_dir_all(&store_path) {
        return Err(format!(
            "create parent directory '{}' failed: {}",
            store_path.to_str().unwrap(),
            e
        ));
    }

    // Persist current configurations to temporary file.
    if let Err(e) = config.write_to_file(&tmp_causet_path) {
        return Err(format!(
            "persist config to '{}' failed: {}",
            tmp_causet_path.to_str().unwrap(),
            e
        ));
    }

    // Rename temporary file to last config file.
    if let Err(e) = fs::rename(&tmp_causet_path, &last_causet_path) {
        return Err(format!(
            "rename config file from '{}' to '{}' failed: {}",
            tmp_causet_path.to_str().unwrap(),
            last_causet_path.to_str().unwrap(),
            e
        ));
    }

    Ok(())
}

pub fn write_config<P: AsRef<Path>>(path: P, content: &[u8]) -> CfgResult<()> {
    let tmp_causet_path = match path.as_ref().parent() {
        Some(p) => p.join(TMP_CONFIG_FILE),
        None => {
            return Err(format!(
                "failed to get parent path of config file: {}",
                path.as_ref().display()
            )
            .into())
        }
    };
    {
        let mut f = fs::File::create(&tmp_causet_path)?;
        f.write_all(content)?;
        f.sync_all()?;
    }
    fs::rename(&tmp_causet_path, &path)?;
    Ok(())
}

lazy_static! {
    pub static ref EINSTEINDBCONFIG_TYPED: ConfigChange = EINSTEINDBConfig::default().typed();
}

fn to_config_change(change: HashMap<String, String>) -> CfgResult<ConfigChange> {
    fn helper(
        mut fields: Vec<String>,
        dst: &mut ConfigChange,
        typed: &ConfigChange,
        value: String,
    ) -> CfgResult<()> {
        if let Some(field) = fields.pop() {
            let f = if field == "violetabftstore" {
                "violetabft_store".to_owned()
            } else {
                field.replace("-", "_")
            };
            return match typed.get(&f) {
                None => Err(format!("unexpect fields: {}", field).into()),
                Some(ConfigValue::Skip) => {
                    Err(format!("config {} can not be changed", field).into())
                }
                Some(ConfigValue::Module(m)) => {
                    if let ConfigValue::Module(n_dst) = dst
                        .entry(f)
                        .or_insert_with(|| ConfigValue::Module(HashMap::new()))
                    {
                        return helper(fields, n_dst, m, value);
                    }
                    panic!("unexpect config value");
                }
                Some(v) => {
                    if fields.is_empty() {
                        return match to_change_value(&value, v) {
                            Err(_) => Err(format!("failed to parse: {}", value).into()),
                            Ok(v) => {
                                dst.insert(f, v);
                                Ok(())
                            }
                        };
                    }
                    let c: Vec<_> = fields.into_iter().rev().collect();
                    Err(format!("unexpect fields: {}", c[..].join(".")).into())
                }
            };
        }
        Ok(())
    }
    let mut res = HashMap::new();
    for (name, value) in change {
        let fields: Vec<_> = name.as_str().split('.').collect();
        let fields: Vec<_> = fields.into_iter().map(|s| s.to_owned()).rev().collect();
        helper(fields, &mut res, &EINSTEINDBCONFIG_TYPED, value)?;
    }
    Ok(res)
}

fn to_change_value(v: &str, typed: &ConfigValue) -> CfgResult<ConfigValue> {
    let v = v.trim_matches('\"');
    let res = match typed {
        ConfigValue::Duration(_) => ConfigValue::from(v.parse::<ReadableDuration>()?),
        ConfigValue::Size(_) => ConfigValue::from(v.parse::<ReadableSize>()?),
        ConfigValue::OptionSize(_) => {
            ConfigValue::from(OptionReadableSize(Some(v.parse::<ReadableSize>()?)))
        }
        ConfigValue::U64(_) => ConfigValue::from(v.parse::<u64>()?),
        ConfigValue::F64(_) => ConfigValue::from(v.parse::<f64>()?),
        ConfigValue::U32(_) => ConfigValue::from(v.parse::<u32>()?),
        ConfigValue::I32(_) => ConfigValue::from(v.parse::<i32>()?),
        ConfigValue::Usize(_) => ConfigValue::from(v.parse::<usize>()?),
        ConfigValue::Bool(_) => ConfigValue::from(v.parse::<bool>()?),
        ConfigValue::BlobRunMode(_) => ConfigValue::from(v.parse::<BlobRunMode>()?),
        ConfigValue::String(_) => ConfigValue::String(v.to_owned()),
        _ => unreachable!(),
    };
    Ok(res)
}

fn to_toml_encode(change: HashMap<String, String>) -> CfgResult<HashMap<String, String>> {
    fn helper(mut fields: Vec<String>, typed: &ConfigChange) -> CfgResult<bool> {
        if let Some(field) = fields.pop() {
            let f = if field == "violetabftstore" {
                "violetabft_store".to_owned()
            } else {
                field.replace("-", "_")
            };
            match typed.get(&f) {
                None | Some(ConfigValue::Skip) => {
                    Err(format!("failed to get field: {}", field).into())
                }
                Some(ConfigValue::Module(m)) => helper(fields, m),
                Some(c) => {
                    if !fields.is_empty() {
                        return Err(format!("unexpect fields: {:?}", fields).into());
                    }
                    match c {
                        ConfigValue::Duration(_)
                        | ConfigValue::Size(_)
                        | ConfigValue::OptionSize(_)
                        | ConfigValue::String(_)
                        | ConfigValue::BlobRunMode(_) => Ok(true),
                        _ => Ok(false),
                    }
                }
            }
        } else {
            Err("failed to get field".to_owned().into())
        }
    };
    let mut dst = HashMap::new();
    for (name, value) in change {
        let fields: Vec<_> = name.as_str().split('.').collect();
        let fields: Vec<_> = fields.into_iter().map(|s| s.to_owned()).rev().collect();
        if helper(fields, &EINSTEINDBCONFIG_TYPED)? {
            dst.insert(name.replace("_", "-"), format!("\"{}\"", value));
        } else {
            dst.insert(name.replace("_", "-"), value);
        }
    }
    Ok(dst)
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Module {
    Readpool,
    Server,
    Metric,
    VioletaBftstore,
    Interlock,
    Fidel,
    Lmdbdb,
    VioletaBftdb,
    VioletaBftEngine,
    causet_storage,
    Security,
    Encryption,
    Import,
    Backup,
    PessimisticTxn,
    Gc,
    Split,
    Unknown(String),
}

impl From<&str> for Module {
    fn from(m: &str) -> Module {
        match m {
            "readpool" => Module::Readpool,
            "server" => Module::Server,
            "metric" => Module::Metric,
            "violetabft_store" => Module::VioletaBftstore,
            "interlock" => Module::Interlock,
            "fidel" => Module::Fidel,
            "split" => Module::Split,
            "lmdb" => Module::Lmdbdb,
            "violetabftdb" => Module::VioletaBftdb,
            "violetabft_engine" => Module::VioletaBftEngine,
            "causet_storage" => Module::causet_storage,
            "security" => Module::Security,
            "import" => Module::Import,
            "backup" => Module::Backup,
            "pessimistic_txn" => Module::PessimisticTxn,
            "gc" => Module::Gc,
            n => Module::Unknown(n.to_owned()),
        }
    }
}

/// ConfigController use to register each module's config manager,
/// and dispatch the change of config to corresponding managers or
/// return the change if the incoming change is invalid.
#[derive(Default, Clone)]
pub struct ConfigController {
    inner: Arc<RwLock<ConfigInner>>,
}

#[derive(Default)]
struct ConfigInner {
    current: EINSTEINDBConfig,
    config_mgrs: HashMap<Module, Box<dyn ConfigManager>>,
}

impl ConfigController {
    pub fn new(current: EINSTEINDBConfig) -> Self {
        ConfigController {
            inner: Arc::new(RwLock::new(ConfigInner {
                current,
                config_mgrs: HashMap::new(),
            })),
        }
    }

    pub fn fidelio(&self, change: HashMap<String, String>) -> CfgResult<()> {
        let diff = to_config_change(change.clone())?;
        {
            let mut incoming = self.get_current();
            incoming.fidelio(diff.clone());
            incoming.validate()?;
        }
        let mut inner = self.inner.write().unwrap();
        let mut to_fidelio = HashMap::with_capacity(diff.len());
        for (name, change) in diff.into_iter() {
            match change {
                ConfigValue::Module(change) => {
                    // fidelio a submodule's config only if changes had been sucessfully
                    // dispatched to corresponding config manager, to avoid dispatch change twice
                    if let Some(mgr) = inner.config_mgrs.get_mut(&Module::from(name.as_str())) {
                        if let Err(e) = mgr.dispatch(change.clone()) {
                            inner.current.fidelio(to_fidelio);
                            return Err(e);
                        }
                    }
                    to_fidelio.insert(name, ConfigValue::Module(change));
                }
                _ => {
                    let _ = to_fidelio.insert(name, change);
                }
            }
        }
        debug!("all config change had been dispatched"; "change" => ?to_fidelio);
        inner.current.fidelio(to_fidelio);
        // Write change to the config file
        let content = {
            let change = to_toml_encode(change)?;
            let src = if Path::new(&inner.current.causet_path).exists() {
                fs::read_to_string(&inner.current.causet_path)?
            } else {
                String::new()
            };
            let mut t = TomlWriter::new();
            t.write_change(src, change);
            t.finish()
        };
        write_config(&inner.current.causet_path, &content)?;
        Ok(())
    }

    pub fn fidelio_config(&self, name: &str, value: &str) -> CfgResult<()> {
        let mut m = HashMap::new();
        m.insert(name.to_owned(), value.to_owned());
        self.fidelio(m)
    }

    pub fn register(&self, module: Module, causet_mgr: Box<dyn ConfigManager>) {
        let mut inner = self.inner.write().unwrap();
        if inner.config_mgrs.insert(module.clone(), causet_mgr).is_some() {
            warn!("config manager for module {:?} already registered", module)
        }
    }

    pub fn get_current(&self) -> EINSTEINDBConfig {
        self.inner.read().unwrap().current.clone()
    }
}

#[causet(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::causet_storage::config::StorageConfigManger;
    use engine_lmdb::raw_util::new_engine_opt;
    use edb::DBOptions as DBOptionsTrait;
    use violetabft_log_engine::RecoveryMode;
    use slog::Level;
    use std::sync::Arc;

    #[test]
    fn test_check_critical_causet_with() {
        let mut edb_causet = EINSTEINDBConfig::default();
        let mut last_causet = EINSTEINDBConfig::default();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_ok());

        edb_causet.lmdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_err());

        last_causet.lmdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_ok());

        edb_causet.violetabftdb.wal_dir = "/violetabft/wal_dir".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_err());

        last_causet.violetabftdb.wal_dir = "/violetabft/wal_dir".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_ok());

        edb_causet.causet_storage.data_dir = "/data1".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_err());

        last_causet.causet_storage.data_dir = "/data1".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_ok());

        edb_causet.violetabft_store.violetabftdb_path = "/violetabft_path".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_err());

        last_causet.violetabft_store.violetabftdb_path = "/violetabft_path".to_owned();
        assert!(edb_causet.check_critical_causet_with(&last_causet).is_ok());
    }

    #[test]
    fn test_last_causet_modified() {
        let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
        let store_path = Path::new(&causet.causet_storage.data_dir);
        let last_causet_path = store_path.join(LAST_CONFIG_FILE);

        causet.write_to_file(&last_causet_path).unwrap();

        let mut last_causet_metadata = last_causet_path.metadata().unwrap();
        let first_modified = last_causet_metadata.modified().unwrap();

        // not write to file when config is the equivalent of last one.
        assert!(persist_config(&causet).is_ok());
        last_causet_metadata = last_causet_path.metadata().unwrap();
        assert_eq!(last_causet_metadata.modified().unwrap(), first_modified);

        // write to file when config is the inequivalent of last one.
        causet.log_level = slog::Level::Warning;
        assert!(persist_config(&causet).is_ok());
        last_causet_metadata = last_causet_path.metadata().unwrap();
        assert_ne!(last_causet_metadata.modified().unwrap(), first_modified);
    }

    #[test]
    fn test_persist_causet() {
        let dir = Builder::new().prefix("test_persist_causet").temfidelir().unwrap();
        let path_buf = dir.path().join(LAST_CONFIG_FILE);
        let file = path_buf.as_path();
        let (s1, s2) = ("/xxx/wal_dir".to_owned(), "/yyy/wal_dir".to_owned());

        let mut edb_causet = EINSTEINDBConfig::default();

        edb_causet.lmdb.wal_dir = s1.clone();
        edb_causet.violetabftdb.wal_dir = s2.clone();
        edb_causet.write_to_file(file).unwrap();
        let causet_from_file = EINSTEINDBConfig::from_file(file, None);
        assert_eq!(causet_from_file.lmdb.wal_dir, s1);
        assert_eq!(causet_from_file.violetabftdb.wal_dir, s2);

        // write critical config when exist.
        edb_causet.lmdb.wal_dir = s2.clone();
        edb_causet.violetabftdb.wal_dir = s1.clone();
        edb_causet.write_to_file(file).unwrap();
        let causet_from_file = EINSTEINDBConfig::from_file(file, None);
        assert_eq!(causet_from_file.lmdb.wal_dir, s2);
        assert_eq!(causet_from_file.violetabftdb.wal_dir, s1);
    }

    #[test]
    fn test_create_parent_dir_if_missing() {
        let root_path = Builder::new()
            .prefix("test_create_parent_dir_if_missing")
            .temfidelir()
            .unwrap();
        let path = root_path.path().join("not_exist_dir");

        let mut edb_causet = EINSTEINDBConfig::default();
        edb_causet.causet_storage.data_dir = path.as_path().to_str().unwrap().to_owned();
        assert!(persist_config(&edb_causet).is_ok());
    }

    #[test]
    fn test_keepalive_check() {
        let mut edb_causet = EINSTEINDBConfig::default();
        edb_causet.fidel.lightlikepoints = vec!["".to_owned()];
        let dur = edb_causet.violetabft_store.violetabft_heartbeat_interval();
        edb_causet.server.grpc_keepalive_time = ReadableDuration(dur);
        assert!(edb_causet.validate().is_err());
        edb_causet.server.grpc_keepalive_time = ReadableDuration(dur * 2);
        edb_causet.validate().unwrap();
    }

    #[test]
    fn test_block_size() {
        let mut edb_causet = EINSTEINDBConfig::default();
        edb_causet.fidel.lightlikepoints = vec!["".to_owned()];
        edb_causet.lmdb.defaultcauset.block_size = ReadableSize::gb(10);
        edb_causet.lmdb.lockcauset.block_size = ReadableSize::gb(10);
        edb_causet.lmdb.writecauset.block_size = ReadableSize::gb(10);
        edb_causet.lmdb.violetabftcauset.block_size = ReadableSize::gb(10);
        edb_causet.violetabftdb.defaultcauset.block_size = ReadableSize::gb(10);
        assert!(edb_causet.validate().is_err());
        edb_causet.lmdb.defaultcauset.block_size = ReadableSize::kb(10);
        edb_causet.lmdb.lockcauset.block_size = ReadableSize::kb(10);
        edb_causet.lmdb.writecauset.block_size = ReadableSize::kb(10);
        edb_causet.lmdb.violetabftcauset.block_size = ReadableSize::kb(10);
        edb_causet.violetabftdb.defaultcauset.block_size = ReadableSize::kb(10);
        edb_causet.validate().unwrap();
    }

    #[test]
    fn test_parse_log_level() {
        #[derive(Serialize, Deserialize, Debug)]
        struct LevelHolder {
            #[serde(with = "log_level_serde")]
            v: Level,
        }

        let legal_cases = vec![
            ("critical", Level::Critical),
            ("error", Level::Error),
            ("warning", Level::Warning),
            ("debug", Level::Debug),
            ("trace", Level::Trace),
            ("info", Level::Info),
        ];
        for (serialized, deserialized) in legal_cases {
            let holder = LevelHolder { v: deserialized };
            let res_string = toml::to_string(&holder).unwrap();
            let exp_string = format!("v = \"{}\"\n", serialized);
            assert_eq!(res_string, exp_string);
            let res_value: LevelHolder = toml::from_str(&exp_string).unwrap();
            assert_eq!(res_value.v, deserialized);
        }

        let compatibility_cases = vec![("warn", Level::Warning)];
        for (serialized, deserialized) in compatibility_cases {
            let variant_string = format!("v = \"{}\"\n", serialized);
            let res_value: LevelHolder = toml::from_str(&variant_string).unwrap();
            assert_eq!(res_value.v, deserialized);
        }

        let illegal_cases = vec!["foobar", ""];
        for case in illegal_cases {
            let string = format!("v = \"{}\"\n", case);
            toml::from_str::<LevelHolder>(&string).unwrap_err();
        }
    }

    #[test]
    fn test_to_config_change() {
        assert_eq!(
            to_change_value("10h", &ConfigValue::Duration(0)).unwrap(),
            ConfigValue::from(ReadableDuration::hours(10))
        );
        assert_eq!(
            to_change_value("100MB", &ConfigValue::Size(0)).unwrap(),
            ConfigValue::from(ReadableSize::mb(100))
        );
        assert_eq!(
            to_change_value("10000", &ConfigValue::U64(0)).unwrap(),
            ConfigValue::from(10000u64)
        );

        let old = EINSTEINDBConfig::default();
        let mut incoming = EINSTEINDBConfig::default();
        incoming.interlock.brane_split_tuplespaceInstanton = 10000;
        incoming.gc.max_write_bytes_per_sec = ReadableSize::mb(100);
        incoming.lmdb.defaultcauset.block_cache_size = ReadableSize::mb(500);
        let diff = old.diff(&incoming);
        let mut change = HashMap::new();
        change.insert(
            "interlock.brane-split-tuplespaceInstanton".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "lmdb.defaultcauset.block-cache-size".to_owned(),
            "500MB".to_owned(),
        );
        let res = to_config_change(change).unwrap();
        assert_eq!(diff, res);

        // illegal cases
        let cases = vec![
            // wrong value type
            ("gc.max-write-bytes-per-sec".to_owned(), "10s".to_owned()),
            (
                "pessimistic-txn.wait-for-dagger-timeout".to_owned(),
                "1MB".to_owned(),
            ),
            // missing or unknown config fields
            ("xxx.yyy".to_owned(), "12".to_owned()),
            (
                "lmdb.defaultcauset.block-cache-size.xxx".to_owned(),
                "50MB".to_owned(),
            ),
            ("lmdb.xxx.block-cache-size".to_owned(), "50MB".to_owned()),
            ("lmdb.block-cache-size".to_owned(), "50MB".to_owned()),
            // not support change config
            (
                "violetabftstore.violetabft-heartbeat-ticks".to_owned(),
                "100".to_owned(),
            ),
            ("violetabftstore.prevote".to_owned(), "false".to_owned()),
        ];
        for (name, value) in cases {
            let mut change = HashMap::new();
            change.insert(name, value);
            assert!(to_config_change(change).is_err());
        }
    }

    #[test]
    fn test_to_toml_encode() {
        let mut change = HashMap::new();
        change.insert(
            "violetabftstore.fidel-heartbeat-tick-interval".to_owned(),
            "1h".to_owned(),
        );
        change.insert(
            "interlock.brane-split-tuplespaceInstanton".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "lmdb.defaultcauset.titan.blob-run-mode".to_owned(),
            "read-only".to_owned(),
        );
        let res = to_toml_encode(change).unwrap();
        assert_eq!(
            res.get("violetabftstore.fidel-heartbeat-tick-interval"),
            Some(&"\"1h\"".to_owned())
        );
        assert_eq!(
            res.get("interlock.brane-split-tuplespaceInstanton"),
            Some(&"10000".to_owned())
        );
        assert_eq!(
            res.get("gc.max-write-bytes-per-sec"),
            Some(&"\"100MB\"".to_owned())
        );
        assert_eq!(
            res.get("lmdb.defaultcauset.titan.blob-run-mode"),
            Some(&"\"read-only\"".to_owned())
        );
    }

    fn new_engines(causet: EINSTEINDBConfig) -> (LmdbEngine, ConfigController) {
        let engine = LmdbEngine::from_db(Arc::new(
            new_engine_opt(
                &causet.causet_storage.data_dir,
                causet.lmdb.build_opt(),
                causet.lmdb
                    .build_causet_opts(&causet.causet_storage.block_cache.build_shared_cache()),
            )
            .unwrap(),
        ));

        let (shared, causet_controller) = (causet.causet_storage.block_cache.shared, ConfigController::new(causet));
        causet_controller.register(
            Module::Lmdbdb,
            Box::new(DBConfigManger::new(engine.clone(), DBType::Kv, shared)),
        );
        causet_controller.register(
            Module::causet_storage,
            Box::new(StorageConfigManger::new(engine.clone(), shared)),
        );
        (engine, causet_controller)
    }

    #[test]
    fn test_change_lmdb_config() {
        let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
        causet.lmdb.max_background_jobs = 2;
        causet.lmdb.defaultcauset.disable_auto_compactions = false;
        causet.lmdb.defaultcauset.target_file_size_base = ReadableSize::mb(64);
        causet.lmdb.defaultcauset.block_cache_size = ReadableSize::mb(8);
        causet.lmdb.rate_bytes_per_sec = ReadableSize::mb(64);
        causet.causet_storage.block_cache.shared = false;
        causet.validate().unwrap();
        let (db, causet_controller) = new_engines(causet);

        // fidelio max_background_jobs
        let db_opts = db.get_db_options();
        assert_eq!(db_opts.get_max_background_jobs(), 2);

        causet_controller
            .fidelio_config("lmdb.max-background-jobs", "8")
            .unwrap();
        assert_eq!(db.get_db_options().get_max_background_jobs(), 8);

        // fidelio rate_bytes_per_sec
        let db_opts = db.get_db_options();
        assert_eq!(
            db_opts.get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(64).0 as i64
        );

        causet_controller
            .fidelio_config("lmdb.rate-bytes-per-sec", "128MB")
            .unwrap();
        assert_eq!(
            db.get_db_options().get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(128).0 as i64
        );

        // fidelio some configs on default causet
        let defaultcauset = db.causet_handle(Causet_DEFAULT).unwrap();
        let causet_opts = db.get_options_causet(defaultcauset);
        assert_eq!(causet_opts.get_disable_auto_compactions(), false);
        assert_eq!(causet_opts.get_target_file_size_base(), ReadableSize::mb(64).0);
        assert_eq!(causet_opts.pull_upper_bound_release_buffer(), ReadableSize::mb(8).0);

        let mut change = HashMap::new();
        change.insert(
            "lmdb.defaultcauset.disable-auto-compactions".to_owned(),
            "true".to_owned(),
        );
        change.insert(
            "lmdb.defaultcauset.target-file-size-base".to_owned(),
            "32MB".to_owned(),
        );
        change.insert(
            "lmdb.defaultcauset.block-cache-size".to_owned(),
            "256MB".to_owned(),
        );
        causet_controller.fidelio(change).unwrap();

        let causet_opts = db.get_options_causet(defaultcauset);
        assert_eq!(causet_opts.get_disable_auto_compactions(), true);
        assert_eq!(causet_opts.get_target_file_size_base(), ReadableSize::mb(32).0);
        assert_eq!(causet_opts.pull_upper_bound_release_buffer(), ReadableSize::mb(256).0);

        // Can not fidelio block cache through causet_storage module
        // when shared block cache is disabled
        assert!(causet_controller
            .fidelio_config("causet_storage.block-cache.capacity", "512MB")
            .is_err());
    }

    #[test]
    fn test_change_shared_block_cache() {
        let (mut causet, _dir) = EINSTEINDBConfig::with_tmp().unwrap();
        causet.causet_storage.block_cache.shared = true;
        causet.validate().unwrap();
        let (db, causet_controller) = new_engines(causet);

        // Can not fidelio shared block cache through lmdb module
        assert!(causet_controller
            .fidelio_config("lmdb.defaultcauset.block-cache-size", "256MB")
            .is_err());

        causet_controller
            .fidelio_config("causet_storage.block-cache.capacity", "256MB")
            .unwrap();

        let defaultcauset = db.causet_handle(Causet_DEFAULT).unwrap();
        let defaultcauset_opts = db.get_options_causet(defaultcauset);
        assert_eq!(
            defaultcauset_opts.pull_upper_bound_release_buffer(),
            ReadableSize::mb(256).0
        );
    }

    #[test]
    fn test_dispatch_titan_blob_run_mode_config() {
        let mut causet = EINSTEINDBConfig::default();
        let mut incoming = causet.clone();
        causet.lmdb.defaultcauset.titan.blob_run_mode = BlobRunMode::Normal;
        incoming.lmdb.defaultcauset.titan.blob_run_mode = BlobRunMode::Fallback;

        let diff = causet
            .lmdb
            .defaultcauset
            .titan
            .diff(&incoming.lmdb.defaultcauset.titan);
        assert_eq!(diff.len(), 1);

        let diff = config_value_to_string(diff.into_iter().collect());
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0.as_str(), "blob_run_mode");
        assert_eq!(diff[0].1.as_str(), "kFallback");
    }

    #[test]
    fn test_compatible_adjust_validate_equal() {
        // After calling many time of `compatible_adjust` and `validate` should has
        // the same effect as calling `compatible_adjust` and `validate` one time
        let mut c = EINSTEINDBConfig::default();
        let mut causet = c.clone();
        c.compatible_adjust();
        c.validate().unwrap();

        for _ in 0..10 {
            causet.compatible_adjust();
            causet.validate().unwrap();
            assert_eq!(c, causet);
        }
    }

    #[test]
    fn test_readpool_compatible_adjust_config() {
        let content = r#"
        [readpool.causet_storage]
        [readpool.interlock]
        "#;
        let mut causet: EINSTEINDBConfig = toml::from_str(content).unwrap();
        causet.compatible_adjust();
        assert_eq!(causet.readpool.causet_storage.use_unified_pool, Some(false));
        assert_eq!(causet.readpool.interlock.use_unified_pool, Some(true));

        let content = r#"
        [readpool.causet_storage]
        stack-size = "10MB"
        [readpool.interlock]
        normal-concurrency = 1
        "#;
        let mut causet: EINSTEINDBConfig = toml::from_str(content).unwrap();
        causet.compatible_adjust();
        assert_eq!(causet.readpool.causet_storage.use_unified_pool, Some(false));
        assert_eq!(causet.readpool.interlock.use_unified_pool, Some(false));
    }

    #[test]
    fn test_unrecognized_config_tuplespaceInstanton() {
        let mut temp_config_file = tempfile::NamedTempFile::new().unwrap();
        let temp_config_writer = temp_config_file.as_file_mut();
        temp_config_writer
            .write_all(
                br#"
                    log-level = "debug"
                    log-fmt = "json"
                    [readpool.unified]
                    min-threads-count = 5
                    stack-size = "20MB"
                    [import]
                    num_threads = 4
                    [gcc]
                    batch-tuplespaceInstanton = 1024
                    [[security.encryption.master-tuplespaceInstanton]]
                    type = "file"
                "#,
            )
            .unwrap();
        temp_config_writer.sync_data().unwrap();

        let mut unrecognized_tuplespaceInstanton = Vec::new();
        let _ = EINSTEINDBConfig::from_file(temp_config_file.path(), Some(&mut unrecognized_tuplespaceInstanton));

        assert_eq!(
            unrecognized_tuplespaceInstanton,
            vec![
                "log-fmt".to_owned(),
                "readpool.unified.min-threads-count".to_owned(),
                "import.num_threads".to_owned(),
                "gcc".to_owned(),
                "security.encryption.master-tuplespaceInstanton".to_owned(),
            ],
        );
    }

    #[test]
    fn test_violetabft_engine() {
        let content = r#"
            [violetabft-engine]
            enable = true
            recovery_mode = "tolerate-corrupted-tail-records"
            bytes-per-sync = "64KB"
            purge-memory_barrier = "1GB"
        "#;
        let causet: EINSTEINDBConfig = toml::from_str(content).unwrap();
        assert!(causet.violetabft_engine.enable);
        let config = &causet.violetabft_engine.config;
        assert_eq!(
            config.recovery_mode,
            RecoveryMode::TolerateCorruptedTailRecords
        );
        assert_eq!(config.bytes_per_sync.0, ReadableSize::kb(64).0);
        assert_eq!(config.purge_memory_barrier.0, ReadableSize::gb(1).0);
    }

    #[test]
    fn test_violetabft_engine_dir() {
        let content = r#"
            [violetabft-engine]
            enable = true
        "#;
        let mut causet: EINSTEINDBConfig = toml::from_str(content).unwrap();
        causet.validate().unwrap();
        assert_eq!(
            causet.violetabft_engine.config.dir,
            config::canonicalize_sub_path(&causet.causet_storage.data_dir, "violetabft-engine").unwrap()
        );
    }
}
