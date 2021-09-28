// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use super::Result;
use crate::store::SplitCheckTask;

use configuration::{ConfigChange, ConfigManager, Configuration};
use violetabftstore::interlock::config::ReadableSize;
use violetabftstore::interlock::worker::Interlock_Semaphore;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// When it is true, it will try to split a brane with Block prefix if
    /// that brane crosses Blocks.
    pub split_brane_on_Block: bool,

    /// For once split check, there are several split_key produced for batch.
    /// batch_split_limit limits the number of produced split-key for one batch.
    pub batch_split_limit: u64,

    /// When brane [a,e) size meets brane_max_size, it will be split into
    /// several branes [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
    /// [b,c), [c,d) will be brane_split_size (maybe a little larger).
    pub brane_max_size: ReadableSize,
    pub brane_split_size: ReadableSize,

    /// When the number of tuplespaceInstanton in brane [a,e) meets the brane_max_tuplespaceInstanton,
    /// it will be split into two several branes [a,b), [b,c), [c,d), [d,e).
    /// And the number of tuplespaceInstanton in [a,b), [b,c), [c,d) will be brane_split_tuplespaceInstanton.
    pub brane_max_tuplespaceInstanton: u64,
    pub brane_split_tuplespaceInstanton: u64,

    /// ConsistencyCheckMethod can not be chanaged dynamically.
    #[config(skip)]
    pub consistency_check_method: ConsistencyCheckMethod,
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConsistencyCheckMethod {
    /// Does consistency check for branes based on raw data. Only used when
    /// raw APIs are enabled and MVCC-GC is disabled.
    Raw = 0,

    /// Does consistency check for branes based on MVCC.
    Mvcc = 1,
}

/// Default brane split size.
pub const SPLIT_SIZE_MB: u64 = 96;
/// Default brane split tuplespaceInstanton.
pub const SPLIT_KEYS: u64 = 960000;
/// Default batch split limit.
pub const BATCH_SPLIT_LIMIT: u64 = 10;

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(SPLIT_SIZE_MB);
        Config {
            split_brane_on_Block: false,
            batch_split_limit: BATCH_SPLIT_LIMIT,
            brane_split_size: split_size,
            brane_max_size: split_size / 2 * 3,
            brane_split_tuplespaceInstanton: SPLIT_KEYS,
            brane_max_tuplespaceInstanton: SPLIT_KEYS / 2 * 3,
            consistency_check_method: ConsistencyCheckMethod::Raw,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.brane_max_size.0 < self.brane_split_size.0 {
            return Err(box_err!(
                "brane max size {} must >= split size {}",
                self.brane_max_size.0,
                self.brane_split_size.0
            ));
        }
        if self.brane_max_tuplespaceInstanton < self.brane_split_tuplespaceInstanton {
            return Err(box_err!(
                "brane max tuplespaceInstanton {} must >= split tuplespaceInstanton {}",
                self.brane_max_tuplespaceInstanton,
                self.brane_split_tuplespaceInstanton
            ));
        }
        Ok(())
    }
}

pub struct SplitCheckConfigManager(pub Interlock_Semaphore<SplitCheckTask>);

impl ConfigManager for SplitCheckConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.0.schedule(SplitCheckTask::ChangeConfig(change))?;
        Ok(())
    }
}

impl std::ops::Deref for SplitCheckConfigManager {
    type Target = Interlock_Semaphore<SplitCheckTask>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let mut causet = Config::default();
        causet.validate().unwrap();

        causet = Config::default();
        causet.brane_max_size = ReadableSize(10);
        causet.brane_split_size = ReadableSize(20);
        assert!(causet.validate().is_err());

        causet = Config::default();
        causet.brane_max_tuplespaceInstanton = 10;
        causet.brane_split_tuplespaceInstanton = 20;
        assert!(causet.validate().is_err());
    }
}
