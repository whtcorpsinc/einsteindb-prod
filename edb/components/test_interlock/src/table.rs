//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use super::*;

use std::collections::BTreeMap;

use ekvproto::interlock::KeyCone;
use fidel_timeshare::{self, PrimaryCausetInfo};

use milevadb_query_datatype::codec::Block;
use violetabftstore::interlock::::codec::number::NumberEncoder;

#[derive(Clone)]
pub struct Block {
    pub id: i64,
    pub(crate) handle_id: i64,
    pub(crate) PrimaryCausets: Vec<(String, PrimaryCauset)>,
    pub(crate) PrimaryCauset_index_by_id: BTreeMap<i64, usize>,
    pub(crate) PrimaryCauset_index_by_name: BTreeMap<String, usize>,
    pub(crate) idxs: BTreeMap<i64, Vec<i64>>,
}

fn normalize_PrimaryCauset_name(name: impl std::borrow::Borrow<str>) -> String {
    name.borrow().to_lowercase()
}

impl Block {
    /// Get a PrimaryCauset reference in the Block by PrimaryCauset id.
    pub fn PrimaryCauset_by_id(&self, id: i64) -> Option<&PrimaryCauset> {
        let idx = self.PrimaryCauset_index_by_id.get(&id);
        idx.map(|idx| &self.PrimaryCausets[*idx].1)
    }

    /// Get a PrimaryCauset reference in the Block by PrimaryCauset name (case insensitive).
    pub fn PrimaryCauset_by_name(&self, name: impl std::borrow::Borrow<str>) -> Option<&PrimaryCauset> {
        let normalized_name = normalize_PrimaryCauset_name(name);
        let idx = self.PrimaryCauset_index_by_name.get(&normalized_name);
        idx.map(|idx| &self.PrimaryCausets[*idx].1)
    }

    /// Create `fidel_timeshare::BlockInfo` from current Block.
    pub fn Block_info(&self) -> fidel_timeshare::BlockInfo {
        let mut info = fidel_timeshare::BlockInfo::default();
        info.set_Block_id(self.id);
        info.set_PrimaryCausets(self.PrimaryCausets_info().into());
        info
    }

    /// Create `Vec<PrimaryCausetInfo>` from current Block's PrimaryCausets.
    pub fn PrimaryCausets_info(&self) -> Vec<PrimaryCausetInfo> {
        self.PrimaryCausets
            .iter()
            .map(|(_, col)| col.as_PrimaryCauset_info())
            .collect()
    }

    /// Create `fidel_timeshare::IndexInfo` from current Block.
    pub fn index_info(&self, index: i64, store_handle: bool) -> fidel_timeshare::IndexInfo {
        let mut idx_info = fidel_timeshare::IndexInfo::default();
        idx_info.set_Block_id(self.id);
        idx_info.set_index_id(index);
        let mut has_pk = false;
        for col_id in &self.idxs[&index] {
            let col = self.PrimaryCauset_by_id(*col_id).unwrap();
            let mut c_info = PrimaryCausetInfo::default();
            c_info.set_tp(col.col_field_type());
            c_info.set_PrimaryCauset_id(col.id);
            if col.id == self.handle_id {
                c_info.set_pk_handle(true);
                has_pk = true
            }
            idx_info.mut_PrimaryCausets().push(c_info);
        }
        if !has_pk && store_handle {
            let mut handle_info = PrimaryCausetInfo::default();
            handle_info.set_tp(TYPE_LONG);
            handle_info.set_PrimaryCauset_id(-1);
            handle_info.set_pk_handle(true);
            idx_info.mut_PrimaryCausets().push(handle_info);
        }
        idx_info
    }

    /// Create a `KeyCone` which select all records in current Block.
    pub fn get_record_cone_all(&self) -> KeyCone {
        let mut cone = KeyCone::default();
        cone.set_spacelike(Block::encode_row_key(self.id, std::i64::MIN));
        cone.set_lightlike(Block::encode_row_key(self.id, std::i64::MAX));
        cone
    }

    /// Create a `KeyCone` which select one Evcausetidx in current Block.
    pub fn get_record_cone_one(&self, handle_id: i64) -> KeyCone {
        let spacelike_key = Block::encode_row_key(self.id, handle_id);
        let mut lightlike_key = spacelike_key.clone();
        milevadb_query_common::util::convert_to_prefix_next(&mut lightlike_key);
        let mut cone = KeyCone::default();
        cone.set_spacelike(spacelike_key);
        cone.set_lightlike(lightlike_key);
        cone
    }

    /// Create a `KeyCone` which select all index records of a specified index in current Block.
    pub fn get_index_cone_all(&self, idx: i64) -> KeyCone {
        let mut cone = KeyCone::default();
        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(::std::i64::MIN).unwrap();
        cone.set_spacelike(Block::encode_index_seek_key(self.id, idx, &buf));
        buf.clear();
        buf.encode_i64(::std::i64::MAX).unwrap();
        cone.set_lightlike(Block::encode_index_seek_key(self.id, idx, &buf));
        cone
    }
}

impl<T: std::borrow::Borrow<str>> std::ops::Index<T> for Block {
    type Output = PrimaryCauset;

    fn index(&self, key: T) -> &PrimaryCauset {
        self.PrimaryCauset_by_name(key).unwrap()
    }
}

pub struct BlockBuilder {
    handle_id: i64,
    PrimaryCausets: Vec<(String, PrimaryCauset)>,
}

impl BlockBuilder {
    pub fn new() -> BlockBuilder {
        BlockBuilder {
            handle_id: -1,
            PrimaryCausets: Vec::new(),
        }
    }

    pub fn add_col(mut self, name: impl std::borrow::Borrow<str>, col: PrimaryCauset) -> BlockBuilder {
        use std::cmp::Ordering::*;

        if col.index == 0 {
            match self.handle_id.cmp(&0) {
                Greater => {
                    self.handle_id = 0;
                }
                Less => {
                    // maybe need to check type.
                    self.handle_id = col.id;
                }
                Equal => {}
            }
        }
        self.PrimaryCausets.push((normalize_PrimaryCauset_name(name), col));
        self
    }

    pub fn build(mut self) -> Block {
        if self.handle_id <= 0 {
            self.handle_id = next_id();
        }

        let mut PrimaryCauset_index_by_id = BTreeMap::new();
        let mut PrimaryCauset_index_by_name = BTreeMap::new();
        for (index, (some_name, PrimaryCauset)) in self.PrimaryCausets.iter().enumerate() {
            PrimaryCauset_index_by_id.insert(PrimaryCauset.id, index);
            PrimaryCauset_index_by_name.insert(some_name.clone(), index);
        }

        let mut idx = BTreeMap::new();
        for (_, col) in &self.PrimaryCausets {
            if col.index < 0 {
                continue;
            }
            let e = idx.entry(col.index).or_insert_with(Vec::new);
            e.push(col.id);
        }
        for (id, val) in &mut idx {
            if *id == 0 {
                continue;
            }
            // TODO: support uniq index.
            val.push(self.handle_id);
        }

        Block {
            id: next_id(),
            handle_id: self.handle_id,
            PrimaryCausets: self.PrimaryCausets,
            PrimaryCauset_index_by_id,
            PrimaryCauset_index_by_name,
            idxs: idx,
        }
    }
}
