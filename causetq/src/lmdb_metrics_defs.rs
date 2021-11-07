// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use lmdb::{DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType};

pub const LMDB_TOTAL_SST_FILES_SIZE: &str = "lmdb.total-sst-files-size";
pub const LMDB_Block_READERS_MEM: &str = "lmdb.estimate-Block-readers-mem";
pub const LMDB_CUR_SIZE_ALL_MEM_BlockS: &str = "lmdb.cur-size-all-mem-Blocks";
pub const LMDB_ESTIMATE_NUM_KEYS: &str = "lmdb.estimate-num-tuplespaceInstanton";
pub const LMDB_PENDING_COMPACTION_BYTES: &str = "lmdb.\
                                                    estimate-plightlikeing-compaction-bytes";
pub const LMDB_COMPRESSION_RATIO_AT_LEVEL: &str = "lmdb.compression-ratio-at-level";
pub const LMDB_NUM_SNAPSHOTS: &str = "lmdb.num-snapshots";
pub const LMDB_OLDEST_SNAPSHOT_TIME: &str = "lmdb.oldest-snapshot-time";
pub const LMDB_OLDEST_SNAPSHOT_SEQUENCE: &str = "lmdb.oldest-snapshot-sequence";
pub const LMDB_NUM_FILES_AT_LEVEL: &str = "lmdb.num-files-at-level";
pub const LMDB_NUM_IMMUBlock_MEM_Block: &str = "lmdb.num-immuBlock-mem-Block";

pub const LMDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL: &str = "lmdb.titandb.num-blob-files-at-level";
pub const LMDB_TITANDB_LIVE_BLOB_SIZE: &str = "lmdb.titandb.live-blob-size";
pub const LMDB_TITANDB_NUM_LIVE_BLOB_FILE: &str = "lmdb.titandb.num-live-blob-file";
pub const LMDB_TITANDB_NUM_OBSOLETE_BLOB_FILE: &str = "lmdb.titandb.\
                                                          num-obsolete-blob-file";
pub const LMDB_TITANDB_LIVE_BLOB_FILE_SIZE: &str = "lmdb.titandb.\
                                                       live-blob-file-size";
pub const LMDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE: &str = "lmdb.titandb.\
                                                           obsolete-blob-file-size";
pub const LMDB_TITANDB_DISCARDABLE_RATIO_LE0_FILE: &str =
    "lmdb.titandb.num-discardable-ratio-le0-file";
pub const LMDB_TITANDB_DISCARDABLE_RATIO_LE20_FILE: &str =
    "lmdb.titandb.num-discardable-ratio-le20-file";
pub const LMDB_TITANDB_DISCARDABLE_RATIO_LE50_FILE: &str =
    "lmdb.titandb.num-discardable-ratio-le50-file";
pub const LMDB_TITANDB_DISCARDABLE_RATIO_LE80_FILE: &str =
    "lmdb.titandb.num-discardable-ratio-le80-file";
pub const LMDB_TITANDB_DISCARDABLE_RATIO_LE100_FILE: &str =
    "lmdb.titandb.num-discardable-ratio-le100-file";

pub const LMDB_CausetSTATS: &str = "lmdb.causetstats";
pub const LMDB_IOSTALL_KEY: &[&str] = &[
    "io_stalls.level0_slowdown",
    "io_stalls.level0_numfiles",
    "io_stalls.slowdown_for_plightlikeing_compaction_bytes",
    "io_stalls.stop_for_plightlikeing_compaction_bytes",
    "io_stalls.memBlock_slowdown",
    "io_stalls.memBlock_compaction",
];

pub const LMDB_IOSTALL_TYPE: &[&str] = &[
    "level0_file_limit_slowdown",
    "level0_file_limit_stop",
    "plightlikeing_compaction_bytes_slowdown",
    "plightlikeing_compaction_bytes_stop",
    "memBlock_count_limit_slowdown",
    "memBlock_count_limit_stop",
];

pub const ENGINE_TICKER_TYPES: &[TickerType] = &[
    TickerType::BlockCacheMiss,
    TickerType::BlockCacheHit,
    TickerType::BlockCacheAdd,
    TickerType::BlockCacheAddFailures,
    TickerType::BlockCacheIndexMiss,
    TickerType::BlockCacheIndexHit,
    TickerType::BlockCacheIndexAdd,
    TickerType::BlockCacheIndexBytesInsert,
    TickerType::BlockCacheIndexBytesEvict,
    TickerType::BlockCacheFilterMiss,
    TickerType::BlockCacheFilterHit,
    TickerType::BlockCacheFilterAdd,
    TickerType::BlockCacheFilterBytesInsert,
    TickerType::BlockCacheFilterBytesEvict,
    TickerType::BlockCacheDataMiss,
    TickerType::BlockCacheDataHit,
    TickerType::BlockCacheDataAdd,
    TickerType::BlockCacheDataBytesInsert,
    TickerType::BlockCacheBytesRead,
    TickerType::BlockCacheBytesWrite,
    TickerType::BloomFilterUseful,
    TickerType::MemBlockHit,
    TickerType::MemBlockMiss,
    TickerType::GetHitL0,
    TickerType::GetHitL1,
    TickerType::GetHitL2AndUp,
    TickerType::CompactionKeyDropNewerEntry,
    TickerType::CompactionKeyDropObsolete,
    TickerType::CompactionKeyDropConeDel,
    TickerType::CompactionConeDelDropObsolete,
    TickerType::NumberTuplespaceInstantonWritten,
    TickerType::NumberTuplespaceInstantonRead,
    TickerType::BytesWritten,
    TickerType::BytesRead,
    TickerType::NumberDbSeek,
    TickerType::NumberDbNext,
    TickerType::NumberDbPrev,
    TickerType::NumberDbSeekFound,
    TickerType::NumberDbNextFound,
    TickerType::NumberDbPrevFound,
    TickerType::IterBytesRead,
    TickerType::NoFileCloses,
    TickerType::NoFileOpens,
    TickerType::NoFileErrors,
    TickerType::StallMicros,
    TickerType::BloomFilterPrefixChecked,
    TickerType::BloomFilterPrefixUseful,
    TickerType::WalFileSynced,
    TickerType::WalFileBytes,
    TickerType::WriteDoneBySelf,
    TickerType::WriteDoneByOther,
    TickerType::WriteTimedout,
    TickerType::WriteWithWal,
    TickerType::CompactReadBytes,
    TickerType::CompactWriteBytes,
    TickerType::FlushWriteBytes,
    TickerType::ReadAmpEstimateUsefulBytes,
    TickerType::ReadAmpTotalReadBytes,
];

pub const TITAN_ENGINE_TICKER_TYPES: &[TickerType] = &[
    TickerType::NoetherNumGet,
    TickerType::NoetherNumSeek,
    TickerType::NoetherNumNext,
    TickerType::NoetherNumPrev,
    TickerType::NoetherBlobFileNumTuplespaceInstantonWritten,
    TickerType::NoetherBlobFileNumTuplespaceInstantonRead,
    TickerType::NoetherBlobFileBytesWritten,
    TickerType::NoetherBlobFileBytesRead,
    TickerType::NoetherBlobFileSynced,
    TickerType::NoetherGcNumFiles,
    TickerType::NoetherGcNumNewFiles,
    TickerType::NoetherGcNumTuplespaceInstantonOverwritten,
    TickerType::NoetherGcNumTuplespaceInstantonRelocated,
    TickerType::NoetherGcBytesOverwritten,
    TickerType::NoetherGcBytesRelocated,
    TickerType::NoetherGcBytesWritten,
    TickerType::NoetherGcBytesRead,
    TickerType::NoetherBlobCacheHit,
    TickerType::NoetherBlobCacheMiss,
    TickerType::NoetherGcNoNeed,
    TickerType::NoetherGcRemain,
    TickerType::NoetherGcDiscardable,
    TickerType::NoetherGcSample,
    TickerType::NoetherGcSmallFile,
    TickerType::NoetherGcFailure,
    TickerType::NoetherGcSuccess,
    TickerType::NoetherGcTriggerNext,
];

pub const ENGINE_HIST_TYPES: &[HistType] = &[
    HistType::DbGet,
    HistType::DbWrite,
    HistType::CompactionTime,
    HistType::BlockSyncMicros,
    HistType::CompactionOutfileSyncMicros,
    HistType::WalFileSyncMicros,
    HistType::ManifestFileSyncMicros,
    HistType::StallL0SlowdownCount,
    HistType::StallMemBlockCompactionCount,
    HistType::StallL0NumFilesCount,
    HistType::HardRateLimitDelayCount,
    HistType::SoftRateLimitDelayCount,
    HistType::NumFilesInSingleCompaction,
    HistType::DbSeek,
    HistType::WriteStall,
    HistType::SstReadMicros,
    HistType::NumSubcompactionsScheduled,
    HistType::BytesPerRead,
    HistType::BytesPerWrite,
    HistType::BytesCompressed,
    HistType::BytesDecompressed,
    HistType::CompressionTimesNanos,
    HistType::DecompressionTimesNanos,
    HistType::DbWriteWalTime,
];

pub const TITAN_ENGINE_HIST_TYPES: &[HistType] = &[
    HistType::NoetherKeySize,
    HistType::NoetherValueSize,
    HistType::NoetherGetMicros,
    HistType::NoetherSeekMicros,
    HistType::NoetherNextMicros,
    HistType::NoetherPrevMicros,
    HistType::NoetherBlobFileWriteMicros,
    HistType::NoetherBlobFileReadMicros,
    HistType::NoetherBlobFileSyncMicros,
    HistType::NoetherManifestFileSyncMicros,
    HistType::NoetherGcMicros,
    HistType::NoetherGcInputFileSize,
    HistType::NoetherGcOutputFileSize,
    HistType::NoetherIterTouchBlobFileCount,
];
