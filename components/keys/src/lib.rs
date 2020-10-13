// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

//! EinsteinDB key building

#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate failure;
#[allow(unused_extern_crates)]
extern crate einsteindb_alloc;

use byteorder::{BigEndian, ByteOrder};

use ekvproto::metapb::Brane;
use std::mem;

pub mod rewrite;

pub const MIN_KEY: &[u8] = &[];
pub const MAX_KEY: &[u8] = &[0xFF];

pub const EMPTY_KEY: &[u8] = &[];

// local is in (0x01, 0x02);
pub const LOCAL_PREFIX: u8 = 0x01;
pub const LOCAL_MIN_KEY: &[u8] = &[LOCAL_PREFIX];
pub const LOCAL_MAX_KEY: &[u8] = &[LOCAL_PREFIX + 1];

pub const DATA_PREFIX: u8 = b'z';
pub const DATA_PREFIX_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MIN_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MAX_KEY: &[u8] = &[DATA_PREFIX + 1];

// Following tuplespaceInstanton are all local tuplespaceInstanton, so the first byte must be 0x01.
pub const STORE_IDENT_KEY: &[u8] = &[LOCAL_PREFIX, 0x01];
pub const PREPARE_BOOTSTRAP_KEY: &[u8] = &[LOCAL_PREFIX, 0x02];
// We save two types brane data in DB, for violetabft and other meta data.
// When the store spacelikes, we should iterate all brane meta data to
// construct peer, no need to travel large violetabft data, so we separate them
// with different prefixes.
pub const REGION_VIOLETABFT_PREFIX: u8 = 0x02;
pub const REGION_VIOLETABFT_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_VIOLETABFT_PREFIX];
pub const REGION_META_PREFIX: u8 = 0x03;
pub const REGION_META_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX];
pub const REGION_META_MIN_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX];
pub const REGION_META_MAX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX + 1];

// Following are the suffix after the local prefix.
// For brane id
pub const VIOLETABFT_LOG_SUFFIX: u8 = 0x01;
pub const VIOLETABFT_STATE_SUFFIX: u8 = 0x02;
pub const APPLY_STATE_SUFFIX: u8 = 0x03;
pub const SNAPSHOT_VIOLETABFT_STATE_SUFFIX: u8 = 0x04;

// For brane meta
pub const REGION_STATE_SUFFIX: u8 = 0x01;

#[inline]
fn make_brane_prefix(brane_id: u64, suffix: u8) -> [u8; 11] {
    let mut key = [0; 11];
    key[..2].copy_from_slice(REGION_VIOLETABFT_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], brane_id);
    key[10] = suffix;
    key
}

#[inline]
fn make_brane_key(brane_id: u64, suffix: u8, sub_id: u64) -> [u8; 19] {
    let mut key = [0; 19];
    key[..2].copy_from_slice(REGION_VIOLETABFT_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], brane_id);
    key[10] = suffix;
    BigEndian::write_u64(&mut key[11..19], sub_id);
    key
}

pub fn brane_violetabft_prefix(brane_id: u64) -> [u8; 10] {
    let mut key = [0; 10];
    key[0..2].copy_from_slice(REGION_VIOLETABFT_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], brane_id);
    key
}

pub fn brane_violetabft_prefix_len() -> usize {
    // REGION_VIOLETABFT_PREFIX_KEY + brane_id + suffix
    REGION_VIOLETABFT_PREFIX_KEY.len() + mem::size_of::<u64>() + 1
}

pub fn violetabft_log_key(brane_id: u64, log_index: u64) -> [u8; 19] {
    make_brane_key(brane_id, VIOLETABFT_LOG_SUFFIX, log_index)
}

pub fn violetabft_state_key(brane_id: u64) -> [u8; 11] {
    make_brane_prefix(brane_id, VIOLETABFT_STATE_SUFFIX)
}

pub fn snapshot_violetabft_state_key(brane_id: u64) -> [u8; 11] {
    make_brane_prefix(brane_id, SNAPSHOT_VIOLETABFT_STATE_SUFFIX)
}

pub fn apply_state_key(brane_id: u64) -> [u8; 11] {
    make_brane_prefix(brane_id, APPLY_STATE_SUFFIX)
}

/// Get the log index from violetabft log key generated by `violetabft_log_key`.
pub fn violetabft_log_index(key: &[u8]) -> Result<u64> {
    let expect_key_len = REGION_VIOLETABFT_PREFIX_KEY.len()
        + mem::size_of::<u64>()
        + mem::size_of::<u8>()
        + mem::size_of::<u64>();
    if key.len() != expect_key_len {
        return Err(Error::InvalidVioletaBftLogKey(key.to_owned()));
    }
    Ok(BigEndian::read_u64(
        &key[expect_key_len - mem::size_of::<u64>()..],
    ))
}

/// Get the brane id and index from violetabft log key generated by `violetabft_log_key`.
pub fn decode_violetabft_log_key(key: &[u8]) -> Result<(u64, u64)> {
    let suffix_idx = REGION_VIOLETABFT_PREFIX_KEY.len() + mem::size_of::<u64>();
    let expect_key_len = suffix_idx + mem::size_of::<u8>() + mem::size_of::<u64>();
    if key.len() != expect_key_len
        || !key.spacelikes_with(REGION_VIOLETABFT_PREFIX_KEY)
        || key[suffix_idx] != VIOLETABFT_LOG_SUFFIX
    {
        return Err(Error::InvalidVioletaBftLogKey(key.to_owned()));
    }
    let brane_id = BigEndian::read_u64(&key[REGION_VIOLETABFT_PREFIX_KEY.len()..suffix_idx]);
    let index = BigEndian::read_u64(&key[suffix_idx + mem::size_of::<u8>()..]);
    Ok((brane_id, index))
}

pub fn violetabft_log_prefix(brane_id: u64) -> [u8; 11] {
    make_brane_prefix(brane_id, VIOLETABFT_LOG_SUFFIX)
}

/// Decode brane violetabft key, return the brane id and violetabft suffix type.
pub fn decode_brane_violetabft_key(key: &[u8]) -> Result<(u64, u8)> {
    decode_brane_key(REGION_VIOLETABFT_PREFIX_KEY, key, "violetabft")
}

#[inline]
fn make_brane_meta_key(brane_id: u64, suffix: u8) -> [u8; 11] {
    let mut key = [0; 11];
    key[0..2].copy_from_slice(REGION_META_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], brane_id);
    key[10] = suffix;
    key
}

/// Decode brane key, return the brane id and meta suffix type.
fn decode_brane_key(prefix: &[u8], key: &[u8], category: &str) -> Result<(u64, u8)> {
    if prefix.len() + mem::size_of::<u64>() + mem::size_of::<u8>() != key.len() {
        return Err(Error::InvalidBraneKeyLength(
            category.to_owned(),
            key.to_owned(),
        ));
    }

    if !key.spacelikes_with(prefix) {
        return Err(Error::InvalidBranePrefix(
            category.to_owned(),
            key.to_owned(),
        ));
    }

    let brane_id = BigEndian::read_u64(&key[prefix.len()..prefix.len() + mem::size_of::<u64>()]);

    Ok((brane_id, key[key.len() - 1]))
}

/// Decode brane meta key, return the brane id and meta suffix type.
pub fn decode_brane_meta_key(key: &[u8]) -> Result<(u64, u8)> {
    decode_brane_key(REGION_META_PREFIX_KEY, key, "meta")
}

pub fn brane_meta_prefix(brane_id: u64) -> [u8; 10] {
    let mut key = [0; 10];
    key[0..2].copy_from_slice(REGION_META_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], brane_id);
    key
}

pub fn brane_state_key(brane_id: u64) -> [u8; 11] {
    make_brane_meta_key(brane_id, REGION_STATE_SUFFIX)
}

pub fn validate_data_key(key: &[u8]) -> bool {
    key.spacelikes_with(DATA_PREFIX_KEY)
}

pub fn data_key(key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(DATA_PREFIX_KEY.len() + key.len());
    v.extlightlike_from_slice(DATA_PREFIX_KEY);
    v.extlightlike_from_slice(key);
    v
}

pub fn origin_key(key: &[u8]) -> &[u8] {
    assert!(
        validate_data_key(key),
        "invalid data key {}",
        hex::encode_upper(key)
    );
    &key[DATA_PREFIX_KEY.len()..]
}

/// Get the `spacelike_key` of current brane in encoded form.
pub fn enc_spacelike_key(brane: &Brane) -> Vec<u8> {
    // only initialized brane's spacelike_key can be encoded, otherwise there must be bugs
    // somewhere.
    assert!(!brane.get_peers().is_empty());
    data_key(brane.get_spacelike_key())
}

/// Get the `lightlike_key` of current brane in encoded form.
pub fn enc_lightlike_key(brane: &Brane) -> Vec<u8> {
    // only initialized brane's lightlike_key can be encoded, otherwise there must be bugs
    // somewhere.
    assert!(!brane.get_peers().is_empty());
    data_lightlike_key(brane.get_lightlike_key())
}

#[inline]
pub fn data_lightlike_key(brane_lightlike_key: &[u8]) -> Vec<u8> {
    if brane_lightlike_key.is_empty() {
        DATA_MAX_KEY.to_vec()
    } else {
        data_key(brane_lightlike_key)
    }
}

pub fn origin_lightlike_key(key: &[u8]) -> &[u8] {
    if key == DATA_MAX_KEY {
        b""
    } else {
        origin_key(key)
    }
}

pub(crate) fn next_key_no_alloc(key: &[u8]) -> Option<(&[u8], u8)> {
    let pos = key.iter().rposition(|b| *b != 0xff)?;
    Some((&key[..pos], key[pos] + 1))
}

/// Computes the next key of the given key.
///
/// If the key has no successor key (e.g. the input is "\xff\xff"), the result
/// would be an empty vector.
///
/// # Examples
///
/// ```
/// use tuplespaceInstanton::next_key;
/// assert_eq!(next_key(b"123"), b"124");
/// assert_eq!(next_key(b"12\xff"), b"13");
/// assert_eq!(next_key(b"\xff\xff"), b"");
/// assert_eq!(next_key(b"\xff\xfe"), b"\xff\xff");
/// assert_eq!(next_key(b"T"), b"U");
/// assert_eq!(next_key(b""), b"");
/// ```
pub fn next_key(key: &[u8]) -> Vec<u8> {
    if let Some((s, e)) = next_key_no_alloc(key) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extlightlike_from_slice(s);
        res.push(e);
        res
    } else {
        Vec::new()
    }
}

#[derive(Debug, Display, Fail)]
pub enum Error {
    #[display(fmt = "{} is not a valid violetabft log key", "hex::encode_upper(_0)")]
    InvalidVioletaBftLogKey(Vec<u8>),
    #[display(
        fmt = "invalid brane {} key length for key {}",
        "_0",
        "hex::encode_upper(_1)"
    )]
    InvalidBraneKeyLength(String, Vec<u8>),
    #[display(
        fmt = "invalid brane {} prefix for key {}",
        "_0",
        "hex::encode_upper(_1)"
    )]
    InvalidBranePrefix(String, Vec<u8>),
}

pub type Result<T> = std::result::Result<T, Error>;

#[causetg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, WriteBytesExt};
    use ekvproto::metapb::{Peer, Brane};
    use std::cmp::Ordering;

    #[test]
    fn test_brane_id_key() {
        let brane_ids = vec![0, 1, 1024, std::u64::MAX];
        for brane_id in brane_ids {
            let prefix = brane_violetabft_prefix(brane_id);

            assert!(violetabft_log_prefix(brane_id).spacelikes_with(&prefix));
            assert!(violetabft_log_key(brane_id, 1).spacelikes_with(&prefix));
            assert!(violetabft_state_key(brane_id).spacelikes_with(&prefix));
            assert!(apply_state_key(brane_id).spacelikes_with(&prefix));
        }

        // test sort.
        let tbls = vec![
            (1, 0, Ordering::Greater),
            (1, 1, Ordering::Equal),
            (1, 2, Ordering::Less),
        ];
        for (lid, rid, order) in tbls {
            let lhs = brane_violetabft_prefix(lid);
            let rhs = brane_violetabft_prefix(rid);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));

            let lhs = violetabft_state_key(lid);
            let rhs = violetabft_state_key(rid);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));

            let lhs = apply_state_key(lid);
            let rhs = apply_state_key(rid);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));
        }
    }

    #[test]
    fn test_violetabft_log_sort() {
        let tbls = vec![
            (1, 1, 1, 2, Ordering::Less),
            (2, 1, 1, 2, Ordering::Greater),
            (1, 1, 1, 1, Ordering::Equal),
        ];

        for (lid, l_log_id, rid, r_log_id, order) in tbls {
            let lhs = violetabft_log_key(lid, l_log_id);
            let rhs = violetabft_log_key(rid, r_log_id);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));
        }
    }

    #[test]
    fn test_brane_key() {
        let ids = vec![1, 1024, u64::max_value()];
        for id in ids {
            // brane meta key
            let meta_prefix = brane_meta_prefix(id);
            let meta_info_key = brane_state_key(id);
            assert!(meta_info_key.spacelikes_with(&meta_prefix));

            assert_eq!(
                decode_brane_meta_key(&meta_info_key).unwrap(),
                (id, REGION_STATE_SUFFIX)
            );

            // brane violetabft key
            let violetabft_prefix = brane_violetabft_prefix(id);
            let violetabft_apply_key = apply_state_key(id);
            assert!(violetabft_apply_key.spacelikes_with(&violetabft_prefix));

            assert_eq!(
                decode_brane_violetabft_key(&violetabft_apply_key).unwrap(),
                (id, APPLY_STATE_SUFFIX)
            );
        }

        // test brane key sort.
        let tbls: Vec<(u64, u64, Ordering)> = vec![
            (1, 2, Ordering::Less),
            (1, 1, Ordering::Equal),
            (2, 1, Ordering::Greater),
        ];

        for (lkey, rkey, order) in tbls {
            // brane meta key.
            let meta_lhs = brane_state_key(lkey);
            let meta_rhs = brane_state_key(rkey);
            assert_eq!(meta_lhs.partial_cmp(&meta_rhs), Some(order));

            // brane meta key.
            let violetabft_lhs = apply_state_key(lkey);
            let violetabft_rhs = apply_state_key(rkey);
            assert_eq!(violetabft_lhs.partial_cmp(&violetabft_rhs), Some(order));
        }
    }

    #[test]
    fn test_violetabft_log_key() {
        for brane_id in 1..10 {
            for idx_id in 1..10 {
                let key = violetabft_log_key(brane_id, idx_id);
                assert_eq!(idx_id, violetabft_log_index(&key).unwrap());
                assert_eq!((brane_id, idx_id), decode_violetabft_log_key(&key).unwrap());
            }
        }

        let state_key = violetabft_state_key(1);
        // invalid length
        assert!(decode_violetabft_log_key(&state_key).is_err());

        let mut state_key = state_key.to_vec();
        state_key.write_u64::<BigEndian>(2).unwrap();
        // invalid suffix
        assert!(decode_violetabft_log_key(&state_key).is_err());

        let mut brane_state_key = brane_state_key(1).to_vec();
        brane_state_key.write_u64::<BigEndian>(2).unwrap();
        // invalid prefix
        assert!(decode_violetabft_log_key(&brane_state_key).is_err());
    }

    #[test]
    fn test_data_key() {
        assert!(validate_data_key(&data_key(b"abc")));
        assert!(!validate_data_key(b"abc"));

        let mut brane = Brane::default();
        // uninitialised brane should not be passed in `enc_spacelike_key` and `enc_lightlike_key`.
        assert!(::panic_hook::recover_safe(|| enc_spacelike_key(&brane)).is_err());
        assert!(::panic_hook::recover_safe(|| enc_lightlike_key(&brane)).is_err());

        brane.mut_peers().push(Peer::default());
        assert_eq!(enc_spacelike_key(&brane), vec![DATA_PREFIX]);
        assert_eq!(enc_lightlike_key(&brane), vec![DATA_PREFIX + 1]);

        brane.set_spacelike_key(vec![1]);
        brane.set_lightlike_key(vec![2]);
        assert_eq!(enc_spacelike_key(&brane), vec![DATA_PREFIX, 1]);
        assert_eq!(enc_lightlike_key(&brane), vec![DATA_PREFIX, 2]);
    }
}
