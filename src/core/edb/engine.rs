// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::*;

/// An EinsteinDB key-value store
pub trait CausetEngine:
    Peekable
    + SyncMuBlock
    + Iterable
    + WriteBatchExt
    + DBOptionsExt
    + CausetNamesExt
    + CausetHandleExt
    + ImportExt
    + SstExt
    + BlockPropertiesExt
    + CompactExt
    + ConePropertiesExt
    + MiscExt
    + lightlike
    + Sync
    + Clone
    + Debug
    + 'static
{
    /// A consistent read-only snapshot of the database
    type Snapshot: Snapshot;

    /// Create a snapshot
    fn snapshot(&self) -> Self::Snapshot;

    /// Syncs any writes to disk
    fn sync(&self) -> Result<()>;

    /// Flush metrics to prometheus
    ///
    /// `instance` is the label of the metric to flush.
    fn flush_metrics(&self, _instance: &str) {}

    /// Reset internal statistics
    fn reset_statistics(&self) {}

    /// Cast to a concrete engine type
    ///
    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;
}
