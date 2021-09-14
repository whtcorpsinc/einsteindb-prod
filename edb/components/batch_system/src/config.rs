// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use violetabftstore::interlock::::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: usize,
    pub pool_size: usize,
    pub reschedule_duration: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_batch_size: 256,
            pool_size: 2,
            reschedule_duration: ReadableDuration::secs(5),
        }
    }
}
