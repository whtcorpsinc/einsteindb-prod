// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use edb::{DecodeProperties, IndexHandles};
use std::cmp;
use std::collections::HashMap;
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::u64;

use edb::CausetEngine;
use edb::Cone;
use edb::{IndexHandle, BlockProperties, BlockPropertiesCollection};
use lmdb::{
    DBEntryType, BlockPropertiesCollector, BlockPropertiesCollectorFactory, NoetherBlobIndex,
    UserCollectedProperties,
};
use violetabftstore::interlock::::codec::number::{self, NumberEncoder};
use violetabftstore::interlock::::codec::{Error, Result};
use txn_types::{Key, TimeStamp, Write, WriteType};

const PROP_TOTAL_SIZE: &str = "edb.total_size";
const PROP_SIZE_INDEX: &str = "edb.size_index";
const PROP_RANGE_INDEX: &str = "edb.cone_index";
pub const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;
pub const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;

fn get_entry_size(value: &[u8], entry_type: DBEntryType) -> std::result::Result<u64, ()> {
    match entry_type {
        DBEntryType::Put => Ok(value.len() as u64),
        DBEntryType::BlobIndex => match NoetherBlobIndex::decode(value) {
            Ok(index) => Ok(index.blob_size + value.len() as u64),
            Err(_) => Err(()),
        },
        _ => Err(()),
    }
}

// Deprecated. Only for compatible issue from v2.0 or older version.
#[derive(Debug, Default)]
pub struct SizeProperties {
    pub total_size: u64,
    pub index_handles: IndexHandles,
}

impl SizeProperties {
    pub fn encode(&self) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_TOTAL_SIZE, self.total_size);
        props.encode_handles(PROP_SIZE_INDEX, &self.index_handles);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<SizeProperties> {
        let mut res = SizeProperties::default();
        res.total_size = props.decode_u64(PROP_TOTAL_SIZE)?;
        res.index_handles = props.decode_handles(PROP_SIZE_INDEX)?;
        Ok(res)
    }
}

pub struct UserProperties(pub HashMap<Vec<u8>, Vec<u8>>);

impl Deref for UserProperties {
    type Target = HashMap<Vec<u8>, Vec<u8>>;
    fn deref(&self) -> &HashMap<Vec<u8>, Vec<u8>> {
        &self.0
    }
}

impl DerefMut for UserProperties {
    fn deref_mut(&mut self) -> &mut HashMap<Vec<u8>, Vec<u8>> {
        &mut self.0
    }
}

impl UserProperties {
    pub fn new() -> UserProperties {
        UserProperties(HashMap::new())
    }

    fn encode(&mut self, name: &str, value: Vec<u8>) {
        self.insert(name.as_bytes().to_owned(), value);
    }

    pub fn encode_u64(&mut self, name: &str, value: u64) {
        let mut buf = Vec::with_capacity(8);
        buf.encode_u64(value).unwrap();
        self.encode(name, buf);
    }

    pub fn encode_handles(&mut self, name: &str, handles: &IndexHandles) {
        self.encode(name, handles.encode())
    }
}

impl DecodeProperties for UserProperties {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.0.get(k.as_bytes()) {
            Some(v) => Ok(v.as_slice()),
            None => Err(Error::KeyNotFound),
        }
    }
}

// FIXME: This is a temporary hack to implement a foreign trait on a foreign
// type until the engine abstraction situation is straightened out.
pub struct UserCollectedPropertiesDecoder<'a>(pub &'a UserCollectedProperties);

impl<'a> DecodeProperties for UserCollectedPropertiesDecoder<'a> {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.0.get(k.as_bytes()) {
            Some(v) => Ok(v),
            None => Err(Error::KeyNotFound),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ConeOffsetKind {
    Size,
    TuplespaceInstanton,
}

#[derive(Debug, Default, Clone)]
pub struct ConeOffsets {
    pub size: u64,
    pub tuplespaceInstanton: u64,
}

impl ConeOffsets {
    fn get(&self, kind: ConeOffsetKind) -> u64 {
        match kind {
            ConeOffsetKind::TuplespaceInstanton => self.tuplespaceInstanton,
            ConeOffsetKind::Size => self.size,
        }
    }
}

#[derive(Debug, Default)]
pub struct ConeProperties {
    pub offsets: Vec<(Vec<u8>, ConeOffsets)>,
}

impl ConeProperties {
    pub fn get(&self, key: &[u8]) -> &ConeOffsets {
        let idx = self
            .offsets
            .binary_search_by_key(&key, |&(ref k, _)| k)
            .unwrap();
        &self.offsets[idx].1
    }

    pub fn encode(&self) -> UserProperties {
        let mut buf = Vec::with_capacity(1024);
        for (k, offsets) in &self.offsets {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extlightlike(k);
            buf.encode_u64(offsets.size).unwrap();
            buf.encode_u64(offsets.tuplespaceInstanton).unwrap();
        }
        let mut props = UserProperties::new();
        props.encode(PROP_RANGE_INDEX, buf);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<ConeProperties> {
        match ConeProperties::decode_from_cone_properties(props) {
            Ok(res) => return Ok(res),
            Err(e) => info!("decode to ConeProperties failed with err: {:?}, try to decode to SizeProperties, maybe upgrade from v2.0 or older version?", e),
        }
        SizeProperties::decode(props).map(|res| res.into())
    }

    fn decode_from_cone_properties<T: DecodeProperties>(props: &T) -> Result<ConeProperties> {
        let mut res = ConeProperties::default();
        let mut buf = props.decode(PROP_RANGE_INDEX)?;
        while !buf.is_empty() {
            let klen = number::decode_u64(&mut buf)?;
            let mut k = vec![0; klen as usize];
            buf.read_exact(&mut k)?;
            let mut offsets = ConeOffsets::default();
            offsets.size = number::decode_u64(&mut buf)?;
            offsets.tuplespaceInstanton = number::decode_u64(&mut buf)?;
            res.offsets.push((k, offsets));
        }
        Ok(res)
    }

    pub fn get_approximate_size_in_cone(&self, spacelike: &[u8], lightlike: &[u8]) -> u64 {
        self.get_approximate_distance_in_cone(ConeOffsetKind::Size, spacelike, lightlike)
    }

    pub fn get_approximate_tuplespaceInstanton_in_cone(&self, spacelike: &[u8], lightlike: &[u8]) -> u64 {
        self.get_approximate_distance_in_cone(ConeOffsetKind::TuplespaceInstanton, spacelike, lightlike)
    }

    fn get_approximate_distance_in_cone(
        &self,
        kind: ConeOffsetKind,
        spacelike: &[u8],
        lightlike: &[u8],
    ) -> u64 {
        assert!(spacelike <= lightlike);
        if spacelike == lightlike {
            return 0;
        }
        let spacelike_offset = match self.offsets.binary_search_by_key(&spacelike, |&(ref k, _)| k) {
            Ok(idx) => self.offsets[idx].1.get(kind),
            Err(next_idx) => {
                if next_idx == 0 {
                    0
                } else {
                    self.offsets[next_idx - 1].1.get(kind)
                }
            }
        };

        let lightlike_offset = match self.offsets.binary_search_by_key(&lightlike, |&(ref k, _)| k) {
            Ok(idx) => self.offsets[idx].1.get(kind),
            Err(next_idx) => {
                if next_idx == 0 {
                    0
                } else {
                    self.offsets[next_idx - 1].1.get(kind)
                }
            }
        };

        if lightlike_offset < spacelike_offset {
            panic!(
                "spacelike {:?} lightlike {:?} spacelike_offset {} lightlike_offset {}",
                spacelike, lightlike, spacelike_offset, lightlike_offset
            );
        }
        lightlike_offset - spacelike_offset
    }

    // equivalent to cone(Excluded(spacelike_key), Excluded(lightlike_key))
    pub fn take_excluded_cone(
        mut self,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
    ) -> Vec<(Vec<u8>, ConeOffsets)> {
        let spacelike_offset = match self
            .offsets
            .binary_search_by_key(&spacelike_key, |&(ref k, _)| k)
        {
            Ok(idx) => {
                if idx == self.offsets.len() - 1 {
                    return vec![];
                } else {
                    idx + 1
                }
            }
            Err(next_idx) => next_idx,
        };

        let lightlike_offset = match self.offsets.binary_search_by_key(&lightlike_key, |&(ref k, _)| k) {
            Ok(idx) => {
                if idx == 0 {
                    return vec![];
                } else {
                    idx - 1
                }
            }
            Err(next_idx) => {
                if next_idx == 0 {
                    return vec![];
                } else {
                    next_idx - 1
                }
            }
        };

        if spacelike_offset > lightlike_offset {
            return vec![];
        }

        self.offsets.drain(spacelike_offset..=lightlike_offset).collect()
    }

    pub fn smallest_key(&self) -> Option<Vec<u8>> {
        self.offsets.first().map(|(k, _)| k.to_owned())
    }

    pub fn largest_key(&self) -> Option<Vec<u8>> {
        self.offsets.last().map(|(k, _)| k.to_owned())
    }
}

impl From<SizeProperties> for ConeProperties {
    fn from(p: SizeProperties) -> ConeProperties {
        let mut res = ConeProperties::default();
        for (key, size_handle) in p.index_handles.into_map() {
            let mut cone = ConeOffsets::default();
            cone.size = size_handle.offset;
            res.offsets.push((key, cone));
        }
        res
    }
}

pub struct ConePropertiesCollector {
    props: ConeProperties,
    last_offsets: ConeOffsets,
    last_key: Vec<u8>,
    cur_offsets: ConeOffsets,
    prop_size_index_distance: u64,
    prop_tuplespaceInstanton_index_distance: u64,
}

impl Default for ConePropertiesCollector {
    fn default() -> Self {
        ConePropertiesCollector {
            props: ConeProperties::default(),
            last_offsets: ConeOffsets::default(),
            last_key: vec![],
            cur_offsets: ConeOffsets::default(),
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl ConePropertiesCollector {
    pub fn new(prop_size_index_distance: u64, prop_tuplespaceInstanton_index_distance: u64) -> Self {
        ConePropertiesCollector {
            prop_size_index_distance,
            prop_tuplespaceInstanton_index_distance,
            ..Default::default()
        }
    }

    fn size_in_last_cone(&self) -> u64 {
        self.cur_offsets.size - self.last_offsets.size
    }

    fn tuplespaceInstanton_in_last_cone(&self) -> u64 {
        self.cur_offsets.tuplespaceInstanton - self.last_offsets.tuplespaceInstanton
    }

    fn insert_new_point(&mut self, key: Vec<u8>) {
        self.last_offsets = self.cur_offsets.clone();
        self.props.offsets.push((key, self.cur_offsets.clone()));
    }
}

impl BlockPropertiesCollector for ConePropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        // size
        let size = match get_entry_size(value, entry_type) {
            Ok(entry_size) => key.len() as u64 + entry_size,
            Err(_) => return,
        };
        self.cur_offsets.size += size;
        // tuplespaceInstanton
        self.cur_offsets.tuplespaceInstanton += 1;
        // Add the spacelike key for convenience.
        if self.last_key.is_empty()
            || self.size_in_last_cone() >= self.prop_size_index_distance
            || self.tuplespaceInstanton_in_last_cone() >= self.prop_tuplespaceInstanton_index_distance
        {
            self.insert_new_point(key.to_owned());
        }
        self.last_key.clear();
        self.last_key.extlightlike_from_slice(key);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        if self.size_in_last_cone() > 0 || self.tuplespaceInstanton_in_last_cone() > 0 {
            let key = self.last_key.clone();
            self.insert_new_point(key);
        }
        self.props.encode().0
    }
}

pub struct ConePropertiesCollectorFactory {
    pub prop_size_index_distance: u64,
    pub prop_tuplespaceInstanton_index_distance: u64,
}

impl Default for ConePropertiesCollectorFactory {
    fn default() -> Self {
        ConePropertiesCollectorFactory {
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_tuplespaceInstanton_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl BlockPropertiesCollectorFactory for ConePropertiesCollectorFactory {
    fn create_Block_properties_collector(&mut self, _: u32) -> Box<dyn BlockPropertiesCollector> {
        Box::new(ConePropertiesCollector::new(
            self.prop_size_index_distance,
            self.prop_tuplespaceInstanton_index_distance,
        ))
    }
}

const PROP_NUM_ERRORS: &str = "edb.num_errors";
const PROP_MIN_TS: &str = "edb.min_ts";
const PROP_MAX_TS: &str = "edb.max_ts";
const PROP_NUM_ROWS: &str = "edb.num_rows";
const PROP_NUM_PUTS: &str = "edb.num_puts";
const PROP_NUM_DELETES: &str = "edb.num_deletes";
const PROP_NUM_VERSIONS: &str = "edb.num_versions";
const PROP_MAX_ROW_VERSIONS: &str = "edb.max_row_versions";
const PROP_ROWS_INDEX: &str = "edb.rows_index";
const PROP_ROWS_INDEX_DISTANCE: u64 = 10000;

#[derive(Clone, Debug, Default)]
pub struct MvccProperties {
    pub min_ts: TimeStamp,     // The minimal timestamp.
    pub max_ts: TimeStamp,     // The maximal timestamp.
    pub num_rows: u64,         // The number of events.
    pub num_puts: u64,         // The number of MVCC puts of all events.
    pub num_deletes: u64,      // The number of MVCC deletes of all events.
    pub num_versions: u64,     // The number of MVCC versions of all events.
    pub max_row_versions: u64, // The maximal number of MVCC versions of a single Evcausetidx.
}

impl MvccProperties {
    pub fn new() -> MvccProperties {
        MvccProperties {
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
            num_rows: 0,
            num_puts: 0,
            num_deletes: 0,
            num_versions: 0,
            max_row_versions: 0,
        }
    }

    pub fn add(&mut self, other: &MvccProperties) {
        self.min_ts = cmp::min(self.min_ts, other.min_ts);
        self.max_ts = cmp::max(self.max_ts, other.max_ts);
        self.num_rows += other.num_rows;
        self.num_puts += other.num_puts;
        self.num_deletes += other.num_deletes;
        self.num_versions += other.num_versions;
        self.max_row_versions = cmp::max(self.max_row_versions, other.max_row_versions);
    }

    pub fn encode(&self) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_MIN_TS, self.min_ts.into_inner());
        props.encode_u64(PROP_MAX_TS, self.max_ts.into_inner());
        props.encode_u64(PROP_NUM_ROWS, self.num_rows);
        props.encode_u64(PROP_NUM_PUTS, self.num_puts);
        props.encode_u64(PROP_NUM_DELETES, self.num_deletes);
        props.encode_u64(PROP_NUM_VERSIONS, self.num_versions);
        props.encode_u64(PROP_MAX_ROW_VERSIONS, self.max_row_versions);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<MvccProperties> {
        let mut res = MvccProperties::new();
        res.min_ts = props.decode_u64(PROP_MIN_TS)?.into();
        res.max_ts = props.decode_u64(PROP_MAX_TS)?.into();
        res.num_rows = props.decode_u64(PROP_NUM_ROWS)?;
        res.num_puts = props.decode_u64(PROP_NUM_PUTS)?;
        res.num_versions = props.decode_u64(PROP_NUM_VERSIONS)?;
        // To be compatible with old versions.
        res.num_deletes = props
            .decode_u64(PROP_NUM_DELETES)
            .unwrap_or_else(|_| res.num_versions - res.num_puts);
        res.max_row_versions = props.decode_u64(PROP_MAX_ROW_VERSIONS)?;
        Ok(res)
    }
}

pub struct MvccPropertiesCollector {
    props: MvccProperties,
    last_row: Vec<u8>,
    num_errors: u64,
    row_versions: u64,
    cur_index_handle: IndexHandle,
    row_index_handles: IndexHandles,
}

impl MvccPropertiesCollector {
    fn new() -> MvccPropertiesCollector {
        MvccPropertiesCollector {
            props: MvccProperties::new(),
            last_row: Vec::new(),
            num_errors: 0,
            row_versions: 0,
            cur_index_handle: IndexHandle::default(),
            row_index_handles: IndexHandles::new(),
        }
    }
}

impl BlockPropertiesCollector for MvccPropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        // TsFilter filters sst based on max_ts and min_ts during iterating.
        // To prevent seeing outdated (GC) records, we should consider
        // Lmdb delete entry type.
        if entry_type != DBEntryType::Put && entry_type != DBEntryType::Delete {
            return;
        }

        if !tuplespaceInstanton::validate_data_key(key) {
            self.num_errors += 1;
            return;
        }

        let (k, ts) = match Key::split_on_ts_for(key) {
            Ok((k, ts)) => (k, ts),
            Err(_) => {
                self.num_errors += 1;
                return;
            }
        };

        self.props.min_ts = cmp::min(self.props.min_ts, ts);
        self.props.max_ts = cmp::max(self.props.max_ts, ts);
        if entry_type == DBEntryType::Delete {
            // Empty value for delete entry type, skip following properties.
            return;
        }

        self.props.num_versions += 1;

        if k != self.last_row.as_slice() {
            self.props.num_rows += 1;
            self.row_versions = 1;
            self.last_row.clear();
            self.last_row.extlightlike(k);
        } else {
            self.row_versions += 1;
        }
        if self.row_versions > self.props.max_row_versions {
            self.props.max_row_versions = self.row_versions;
        }

        let write_type = match Write::parse_type(value) {
            Ok(v) => v,
            Err(_) => {
                self.num_errors += 1;
                return;
            }
        };

        match write_type {
            WriteType::Put => self.props.num_puts += 1,
            WriteType::Delete => self.props.num_deletes += 1,
            _ => {}
        }

        // Add new Evcausetidx.
        if self.row_versions == 1 {
            self.cur_index_handle.size += 1;
            self.cur_index_handle.offset += 1;
            if self.cur_index_handle.offset == 1
                || self.cur_index_handle.size >= PROP_ROWS_INDEX_DISTANCE
            {
                self.row_index_handles
                    .insert(self.last_row.clone(), self.cur_index_handle.clone());
                self.cur_index_handle.size = 0;
            }
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        // Insert last handle.
        if self.cur_index_handle.size > 0 {
            self.row_index_handles
                .insert(self.last_row.clone(), self.cur_index_handle.clone());
        }
        let mut res = self.props.encode();
        res.encode_u64(PROP_NUM_ERRORS, self.num_errors);
        res.encode_handles(PROP_ROWS_INDEX, &self.row_index_handles);
        res.0
    }
}

#[derive(Default)]
pub struct MvccPropertiesCollectorFactory {}

impl BlockPropertiesCollectorFactory for MvccPropertiesCollectorFactory {
    fn create_Block_properties_collector(&mut self, _: u32) -> Box<dyn BlockPropertiesCollector> {
        Box::new(MvccPropertiesCollector::new())
    }
}

pub fn get_cone_entries_and_versions<E>(
    engine: &E,
    causet: &E::CausetHandle,
    spacelike: &[u8],
    lightlike: &[u8],
) -> Option<(u64, u64)>
where
    E: CausetEngine,
{
    let cone = Cone::new(spacelike, lightlike);
    let collection = match engine.get_properties_of_Blocks_in_cone(causet, &[cone]) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if collection.is_empty() {
        return None;
    }

    // Aggregate total MVCC properties and total number entries.
    let mut props = MvccProperties::new();
    let mut num_entries = 0;
    for (_, v) in collection.iter() {
        let tail_pointer = match MvccProperties::decode(&v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        num_entries += v.num_entries();
        props.add(&tail_pointer);
    }

    Some((num_entries, props.num_versions))
}

#[causet(test)]
mod tests {
    use rand::Rng;

    use std::sync::Arc;

    use crate::raw::{PrimaryCausetNetworkOptions, DBOptions, WriBlock};
    use crate::raw::{DBEntryType, BlockPropertiesCollector};
    use tempfile::Builder;
    use test::Bencher;

    use crate::compat::Compat;
    use crate::raw_util::CausetOptions;
    use edb::CausetHandleExt;
    use edb::{Causet_WRITE, LARGE_CausetS};
    use txn_types::{Key, Write, WriteType};

    use super::*;

    #[allow(clippy::many_single_char_names)]
    #[test]
    fn test_cone_properties() {
        let cases = [
            ("a", 0, 1),
            // handle "a": size(size = 1, offset = 1),tuplespaceInstanton(1,1)
            ("b", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            ("c", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            ("d", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("e", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            // handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),tuplespaceInstanton(4,5)
            ("f", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            ("g", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("h", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8, 1),
            ("i", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4, 1),
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9),tuplespaceInstanton(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2, 1),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),tuplespaceInstanton(2,11)
            ("l", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            ("m", 0, DEFAULT_PROP_KEYS_INDEX_DISTANCE / 2),
            //handle "m": tuplespaceInstanton = DEFAULT_PROP_KEYS_INDEX_DISTANCE,offset = 11+DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("n", 1, DEFAULT_PROP_KEYS_INDEX_DISTANCE),
            //handle "n": tuplespaceInstanton = DEFAULT_PROP_KEYS_INDEX_DISTANCE, offset = 11+2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
            ("o", 1, 1),
            // handle　"o": tuplespaceInstanton = 1, offset = 12 + 2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
        ];

        let mut collector = ConePropertiesCollector::default();
        for &(k, vlen, count) in &cases {
            let v = vec![0; vlen as usize];
            for _ in 0..count {
                collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
            }
        }
        for &(k, vlen, _) in &cases {
            let v = vec![0; vlen as usize];
            collector.add(k.as_bytes(), &v, DBEntryType::Other, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = ConeProperties::decode(&result).unwrap();
        assert_eq!(props.smallest_key().unwrap(), cases[0].0.as_bytes());
        assert_eq!(
            props.largest_key().unwrap(),
            cases[cases.len() - 1].0.as_bytes()
        );
        assert_eq!(
            props.get_approximate_size_in_cone(b"", b"k"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11
        );
        assert_eq!(props.get_approximate_tuplespaceInstanton_in_cone(b"", b"k"), 11 as u64);

        assert_eq!(props.offsets.len(), 7);
        let a = props.get(b"a".as_ref());
        assert_eq!(a.size, 1);
        let e = props.get(b"e".as_ref());
        assert_eq!(e.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE + 5);
        let i = props.get(b"i".as_ref());
        assert_eq!(i.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 17 + 9);
        let k = props.get(b"k".as_ref());
        assert_eq!(k.size, DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11);
        let m = props.get(b"m".as_ref());
        assert_eq!(m.tuplespaceInstanton, 11 + DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let n = props.get(b"n".as_ref());
        assert_eq!(n.tuplespaceInstanton, 11 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let o = props.get(b"o".as_ref());
        assert_eq!(o.tuplespaceInstanton, 12 + 2 * DEFAULT_PROP_KEYS_INDEX_DISTANCE);
        let empty = ConeOffsets::default();
        let cases = [
            (" ", "k", k, &empty, 3),
            (" ", " ", &empty, &empty, 0),
            ("k", "k", k, k, 0),
            ("a", "k", k, a, 2),
            ("a", "i", i, a, 1),
            ("e", "h", e, e, 0),
            ("b", "h", e, a, 1),
            ("g", "g", i, i, 0),
        ];
        for &(spacelike, lightlike, lightlike_idx, spacelike_idx, count) in &cases {
            let props = ConeProperties::decode(&result).unwrap();
            let size = lightlike_idx.size - spacelike_idx.size;
            assert_eq!(
                props.get_approximate_size_in_cone(spacelike.as_bytes(), lightlike.as_bytes()),
                size
            );
            let tuplespaceInstanton = lightlike_idx.tuplespaceInstanton - spacelike_idx.tuplespaceInstanton;
            assert_eq!(
                props.get_approximate_tuplespaceInstanton_in_cone(spacelike.as_bytes(), lightlike.as_bytes()),
                tuplespaceInstanton
            );
            assert_eq!(
                props
                    .take_excluded_cone(spacelike.as_bytes(), lightlike.as_bytes())
                    .len(),
                count
            );
        }
    }

    #[test]
    fn test_cone_properties_with_blob_index() {
        let cases = [
            ("a", 0),
            // handle "a": size(size = 1, offset = 1),tuplespaceInstanton(1,1)
            ("b", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("c", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("d", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("e", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            // handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),tuplespaceInstanton(4,5)
            ("f", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            ("g", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("h", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8),
            ("i", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 4),
            // handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9),tuplespaceInstanton(4,5)
            ("j", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            ("k", DEFAULT_PROP_SIZE_INDEX_DISTANCE / 2),
            // handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),tuplespaceInstanton(2,11)
        ];

        let handles = ["a", "e", "i", "k"];

        let mut rng = rand::thread_rng();
        let mut collector = ConePropertiesCollector::default();
        let mut extra_value_size: u64 = 0;
        for &(k, vlen) in &cases {
            if handles.contains(&k) || rng.gen_cone(0, 2) == 0 {
                let v = vec![0; vlen as usize - extra_value_size as usize];
                extra_value_size = 0;
                collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
            } else {
                let mut blob_index = NoetherBlobIndex::default();
                blob_index.blob_size = vlen - extra_value_size;
                let v = blob_index.encode();
                extra_value_size = v.len() as u64;
                collector.add(k.as_bytes(), &v, DBEntryType::BlobIndex, 0, 0);
            }
        }
        let result = UserProperties(collector.finish());

        let props = ConeProperties::decode(&result).unwrap();
        assert_eq!(props.smallest_key().unwrap(), cases[0].0.as_bytes());
        assert_eq!(
            props.largest_key().unwrap(),
            cases[cases.len() - 1].0.as_bytes()
        );
        assert_eq!(
            props.get_approximate_size_in_cone(b"e", b"i"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 9 + 4
        );
        assert_eq!(
            props.get_approximate_size_in_cone(b"", b"k"),
            DEFAULT_PROP_SIZE_INDEX_DISTANCE / 8 * 25 + 11
        );
    }

    #[test]
    fn test_get_cone_entries_and_versions() {
        let path = Builder::new()
            .prefix("_test_get_cone_entries_and_versions")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut causet_opts = PrimaryCausetNetworkOptions::new();
        causet_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        causet_opts.add_Block_properties_collector_factory("edb.tail_pointer-properties-collector", f);
        let causets_opts = LARGE_CausetS
            .iter()
            .map(|causet| CausetOptions::new(causet, causet_opts.clone()))
            .collect();
        let db = Arc::new(crate::raw_util::new_engine_opt(path_str, db_opts, causets_opts).unwrap());

        let cases = ["a", "b", "c"];
        for &key in &cases {
            let k1 = tuplespaceInstanton::data_key(
                Key::from_raw(key.as_bytes())
                    .applightlike_ts(2.into())
                    .as_encoded(),
            );
            let write_causet = db.causet_handle(Causet_WRITE).unwrap();
            db.put_causet(write_causet, &k1, b"v1").unwrap();
            db.delete_causet(write_causet, &k1).unwrap();
            let key = tuplespaceInstanton::data_key(
                Key::from_raw(key.as_bytes())
                    .applightlike_ts(3.into())
                    .as_encoded(),
            );
            db.put_causet(write_causet, &key, b"v2").unwrap();
            db.flush_causet(write_causet, true).unwrap();
        }

        let spacelike_tuplespaceInstanton = tuplespaceInstanton::data_key(&[]);
        let lightlike_tuplespaceInstanton = tuplespaceInstanton::data_lightlike_key(&[]);
        let causet = db.c().causet_handle(Causet_WRITE).unwrap();
        let (entries, versions) =
            get_cone_entries_and_versions(db.c(), causet, &spacelike_tuplespaceInstanton, &lightlike_tuplespaceInstanton).unwrap();
        assert_eq!(entries, (cases.len() * 2) as u64);
        assert_eq!(versions, cases.len() as u64);
    }

    #[test]
    fn test_tail_pointer_properties() {
        let cases = [
            ("ab", 2, WriteType::Put, DBEntryType::Put),
            ("ab", 1, WriteType::Delete, DBEntryType::Put),
            ("ab", 1, WriteType::Delete, DBEntryType::Delete),
            ("cd", 5, WriteType::Delete, DBEntryType::Put),
            ("cd", 4, WriteType::Put, DBEntryType::Put),
            ("cd", 3, WriteType::Put, DBEntryType::Put),
            ("ef", 6, WriteType::Put, DBEntryType::Put),
            ("ef", 6, WriteType::Put, DBEntryType::Delete),
            ("gh", 7, WriteType::Delete, DBEntryType::Put),
        ];
        let mut collector = MvccPropertiesCollector::new();
        for &(key, ts, write_type, entry_type) in &cases {
            let ts = ts.into();
            let k = Key::from_raw(key.as_bytes()).applightlike_ts(ts);
            let k = tuplespaceInstanton::data_key(k.as_encoded());
            let v = Write::new(write_type, ts, None).as_ref().to_bytes();
            collector.add(&k, &v, entry_type, 0, 0);
        }
        let result = UserProperties(collector.finish());

        let props = MvccProperties::decode(&result).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 7.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 7);
        assert_eq!(props.max_row_versions, 3);
    }

    #[bench]
    fn bench_tail_pointer_properties(b: &mut Bencher) {
        let ts = 1.into();
        let num_entries = 100;
        let mut entries = Vec::new();
        for i in 0..num_entries {
            let s = format!("{:032}", i);
            let k = Key::from_raw(s.as_bytes()).applightlike_ts(ts);
            let k = tuplespaceInstanton::data_key(k.as_encoded());
            let w = Write::new(WriteType::Put, ts, Some(s.as_bytes().to_owned()));
            entries.push((k, w.as_ref().to_bytes()));
        }

        let mut collector = MvccPropertiesCollector::new();
        b.iter(|| {
            for &(ref k, ref v) in &entries {
                collector.add(k, v, DBEntryType::Put, 0, 0);
            }
        });
    }
}
