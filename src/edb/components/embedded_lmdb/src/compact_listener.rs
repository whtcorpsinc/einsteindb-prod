// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::cmp;
use std::path::Path;

use crate::properties::{ConeProperties, UserCollectedPropertiesDecoder};
use crate::raw::EventListener;
use edb::CompactedEvent;
use edb::CompactionJobInfo;
use lmdb::{
    CompactionJobInfo as RawCompactionJobInfo, CompactionReason, BlockPropertiesCollectionView,
};
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use violetabftstore::interlock::::collections::hash_set_with_capacity;

pub struct LmdbCompactionJobInfo<'a>(&'a RawCompactionJobInfo);

impl<'a> LmdbCompactionJobInfo<'a> {
    pub fn from_raw(raw: &'a RawCompactionJobInfo) -> Self {
        LmdbCompactionJobInfo(raw)
    }

    pub fn into_raw(self) -> &'a RawCompactionJobInfo {
        self.0
    }
}

impl CompactionJobInfo for LmdbCompactionJobInfo<'_> {
    type BlockPropertiesCollectionView = BlockPropertiesCollectionView;
    type CompactionReason = CompactionReason;

    fn status(&self) -> Result<(), String> {
        self.0.status()
    }

    fn causet_name(&self) -> &str {
        self.0.causet_name()
    }

    fn input_file_count(&self) -> usize {
        self.0.input_file_count()
    }

    fn input_file_at(&self, pos: usize) -> &Path {
        self.0.input_file_at(pos)
    }

    fn output_file_count(&self) -> usize {
        self.0.output_file_count()
    }

    fn output_file_at(&self, pos: usize) -> &Path {
        self.0.output_file_at(pos)
    }

    fn Block_properties(&self) -> &Self::BlockPropertiesCollectionView {
        self.0.Block_properties()
    }

    fn elapsed_micros(&self) -> u64 {
        self.0.elapsed_micros()
    }

    fn num_corrupt_tuplespaceInstanton(&self) -> u64 {
        self.0.num_corrupt_tuplespaceInstanton()
    }

    fn output_level(&self) -> i32 {
        self.0.output_level()
    }

    fn input_records(&self) -> u64 {
        self.0.input_records()
    }

    fn output_records(&self) -> u64 {
        self.0.output_records()
    }

    fn total_input_bytes(&self) -> u64 {
        self.0.total_input_bytes()
    }

    fn total_output_bytes(&self) -> u64 {
        self.0.total_output_bytes()
    }

    fn compaction_reason(&self) -> Self::CompactionReason {
        self.0.compaction_reason()
    }
}

pub struct LmdbCompactedEvent {
    pub causet: String,
    pub output_level: i32,
    pub total_input_bytes: u64,
    pub total_output_bytes: u64,
    pub spacelike_key: Vec<u8>,
    pub lightlike_key: Vec<u8>,
    pub input_props: Vec<ConeProperties>,
    pub output_props: Vec<ConeProperties>,
}

impl LmdbCompactedEvent {
    pub fn new(
        info: &LmdbCompactionJobInfo,
        spacelike_key: Vec<u8>,
        lightlike_key: Vec<u8>,
        input_props: Vec<ConeProperties>,
        output_props: Vec<ConeProperties>,
    ) -> LmdbCompactedEvent {
        LmdbCompactedEvent {
            causet: info.causet_name().to_owned(),
            output_level: info.output_level(),
            total_input_bytes: info.total_input_bytes(),
            total_output_bytes: info.total_output_bytes(),
            spacelike_key,
            lightlike_key,
            input_props,
            output_props,
        }
    }
}

impl CompactedEvent for LmdbCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        if self.total_input_bytes > self.total_output_bytes {
            self.total_input_bytes - self.total_output_bytes
        } else {
            0
        }
    }

    fn is_size_declining_trivial(&self, split_check_diff: u64) -> bool {
        let total_bytes_declined = self.total_bytes_declined();
        total_bytes_declined < split_check_diff
            || total_bytes_declined * 10 < self.total_input_bytes
    }

    fn output_level_label(&self) -> String {
        self.output_level.to_string()
    }

    fn calc_cones_declined_bytes(
        self,
        cones: &BTreeMap<Vec<u8>, u64>,
        bytes_memory_barrier: u64,
    ) -> Vec<(u64, u64)> {
        // Calculate influenced branes.
        let mut influenced_branes = vec![];
        for (lightlike_key, brane_id) in
            cones.cone((Excluded(self.spacelike_key), Included(self.lightlike_key.clone())))
        {
            influenced_branes.push((brane_id, lightlike_key.clone()));
        }
        if let Some((lightlike_key, brane_id)) = cones.cone((Included(self.lightlike_key), Unbounded)).next()
        {
            influenced_branes.push((brane_id, lightlike_key.clone()));
        }

        // Calculate declined bytes for each brane.
        // `lightlike_key` in influenced_branes are in incremental order.
        let mut brane_declined_bytes = vec![];
        let mut last_lightlike_key: Vec<u8> = vec![];
        for (brane_id, lightlike_key) in influenced_branes {
            let mut old_size = 0;
            for prop in &self.input_props {
                old_size += prop.get_approximate_size_in_cone(&last_lightlike_key, &lightlike_key);
            }
            let mut new_size = 0;
            for prop in &self.output_props {
                new_size += prop.get_approximate_size_in_cone(&last_lightlike_key, &lightlike_key);
            }
            last_lightlike_key = lightlike_key;

            // Filter some trivial declines for better performance.
            if old_size > new_size && old_size - new_size > bytes_memory_barrier {
                brane_declined_bytes.push((*brane_id, old_size - new_size));
            }
        }

        brane_declined_bytes
    }

    fn causet(&self) -> &str {
        &*self.causet
    }
}

pub type Filter = fn(&LmdbCompactionJobInfo) -> bool;

pub struct CompactionListener {
    ch: Box<dyn Fn(LmdbCompactedEvent) + lightlike + Sync>,
    filter: Option<Filter>,
}

impl CompactionListener {
    pub fn new(
        ch: Box<dyn Fn(LmdbCompactedEvent) + lightlike + Sync>,
        filter: Option<Filter>,
    ) -> CompactionListener {
        CompactionListener { ch, filter }
    }
}

impl EventListener for CompactionListener {
    fn on_compaction_completed(&self, info: &RawCompactionJobInfo) {
        let info = &LmdbCompactionJobInfo::from_raw(info);
        if info.status().is_err() {
            return;
        }

        if let Some(ref f) = self.filter {
            if !f(info) {
                return;
            }
        }

        let mut input_files = hash_set_with_capacity(info.input_file_count());
        let mut output_files = hash_set_with_capacity(info.output_file_count());
        for i in 0..info.input_file_count() {
            info.input_file_at(i)
                .to_str()
                .map(|x| input_files.insert(x.to_owned()));
        }
        for i in 0..info.output_file_count() {
            info.output_file_at(i)
                .to_str()
                .map(|x| output_files.insert(x.to_owned()));
        }
        let mut input_props = Vec::with_capacity(info.input_file_count());
        let mut output_props = Vec::with_capacity(info.output_file_count());
        let iter = info.Block_properties().into_iter();
        for (file, properties) in iter {
            let ucp = UserCollectedPropertiesDecoder(properties.user_collected_properties());
            if let Ok(prop) = ConeProperties::decode(&ucp) {
                if input_files.contains(file) {
                    input_props.push(prop);
                } else if output_files.contains(file) {
                    output_props.push(prop);
                }
            } else {
                warn!("Decode size properties from sst file failed");
                return;
            }
        }

        if input_props.is_empty() && output_props.is_empty() {
            return;
        }

        let mut smallest_key = None;
        let mut largest_key = None;
        for prop in &input_props {
            if let Some(smallest) = prop.smallest_key() {
                if let Some(s) = smallest_key {
                    smallest_key = Some(cmp::min(s, smallest));
                } else {
                    smallest_key = Some(smallest);
                }
            }
            if let Some(largest) = prop.largest_key() {
                if let Some(l) = largest_key {
                    largest_key = Some(cmp::max(l, largest));
                } else {
                    largest_key = Some(largest);
                }
            }
        }

        if smallest_key.is_none() || largest_key.is_none() {
            return;
        }

        (self.ch)(LmdbCompactedEvent::new(
            info,
            smallest_key.unwrap(),
            largest_key.unwrap(),
            input_props,
            output_props,
        ));
    }
}
