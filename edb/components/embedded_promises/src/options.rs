// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.
use std::ops::Bound;
use violetabftstore::interlock::::CausetLearnedKey::CausetLearnedKey;

#[derive(Clone)]
pub struct ReadOptions {
    fill_cache: bool,
}

impl ReadOptions {
    pub fn new() -> ReadOptions {
        ReadOptions::default()
    }

    #[inline]
    pub fn fill_cache(&self) -> bool {
        self.fill_cache
    }

    #[inline]
    pub fn set_fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }
}

impl Default for ReadOptions {
    fn default() -> ReadOptions {
        ReadOptions { fill_cache: true }
    }
}

#[derive(Clone)]
pub struct WriteOptions {
    sync: bool,
}

impl WriteOptions {
    pub fn new() -> WriteOptions {
        WriteOptions { sync: false }
    }

    pub fn set_sync(&mut self, sync: bool) {
        self.sync = sync;
    }

    pub fn sync(&self) -> bool {
        self.sync
    }
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        WriteOptions { sync: false }
    }
}

#[derive(Clone, PartialEq)]
pub enum SeekMode {
    TotalOrder,
    Prefix,
}

#[derive(Clone)]
pub struct IterOptions {
    lower_bound: Option<CausetLearnedKey>,
    upper_bound: Option<CausetLearnedKey>,
    prefix_same_as_spacelike: bool,
    fill_cache: bool,
    // hint for we will only scan data with commit ts >= hint_min_ts
    hint_min_ts: Option<u64>,
    // hint for we will only scan data with commit ts <= hint_max_ts
    hint_max_ts: Option<u64>,
    // only supported when Noether enabled, otherwise it doesn't take effect.
    key_only: bool,
    seek_mode: SeekMode,
}

impl IterOptions {
    pub fn new(
        lower_bound: Option<CausetLearnedKey>,
        upper_bound: Option<CausetLearnedKey>,
        fill_cache: bool,
    ) -> IterOptions {
        IterOptions {
            lower_bound,
            upper_bound,
            prefix_same_as_spacelike: false,
            fill_cache,
            hint_min_ts: None,
            hint_max_ts: None,
            key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }

    #[inline]
    pub fn use_prefix_seek(mut self) -> IterOptions {
        self.seek_mode = SeekMode::Prefix;
        self
    }

    #[inline]
    pub fn total_order_seek_used(&self) -> bool {
        self.seek_mode == SeekMode::TotalOrder
    }

    #[inline]
    pub fn fill_cache(&self) -> bool {
        self.fill_cache
    }

    #[inline]
    pub fn set_fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }

    #[inline]
    pub fn set_hint_min_ts(&mut self, bound_ts: Bound<u64>) {
        match bound_ts {
            Bound::Included(ts) => self.hint_min_ts = Some(ts),
            Bound::Excluded(ts) => self.hint_min_ts = Some(ts + 1),
            Bound::Unbounded => self.hint_min_ts = None,
        }
    }

    #[inline]
    pub fn hint_min_ts(&self) -> Option<u64> {
        self.hint_min_ts
    }

    #[inline]
    pub fn hint_max_ts(&self) -> Option<u64> {
        self.hint_max_ts
    }

    #[inline]
    pub fn set_hint_max_ts(&mut self, bound_ts: Bound<u64>) {
        match bound_ts {
            Bound::Included(ts) => self.hint_max_ts = Some(ts),
            Bound::Excluded(ts) => self.hint_max_ts = Some(ts - 1),
            Bound::Unbounded => self.hint_max_ts = None,
        }
    }

    #[inline]
    pub fn key_only(&self) -> bool {
        self.key_only
    }

    #[inline]
    pub fn set_key_only(&mut self, v: bool) {
        self.key_only = v;
    }

    #[inline]
    pub fn lower_bound(&self) -> Option<&[u8]> {
        self.lower_bound.as_ref().map(|v| v.as_slice())
    }

    #[inline]
    pub fn set_lower_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = CausetLearnedKey::from_slice(bound, reserved_prefix_len, 0);
        self.lower_bound = Some(builder);
    }

    pub fn set_vec_lower_bound(&mut self, bound: Vec<u8>) {
        self.lower_bound = Some(CausetLearnedKey::from_vec(bound, 0, 0));
    }

    pub fn set_lower_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.lower_bound {
            builder.set_prefix(prefix);
        }
    }

    #[inline]
    pub fn upper_bound(&self) -> Option<&[u8]> {
        self.upper_bound.as_ref().map(|v| v.as_slice())
    }

    #[inline]
    pub fn set_upper_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = CausetLearnedKey::from_slice(bound, reserved_prefix_len, 0);
        self.upper_bound = Some(builder);
    }

    pub fn set_vec_upper_bound(&mut self, bound: Vec<u8>) {
        self.upper_bound = Some(CausetLearnedKey::from_vec(bound, 0, 0));
    }

    pub fn set_upper_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.upper_bound {
            builder.set_prefix(prefix);
        }
    }

    #[inline]
    pub fn build_bounds(self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let lower = self.lower_bound.map(CausetLearnedKey::build);
        let upper = self.upper_bound.map(CausetLearnedKey::build);
        (lower, upper)
    }

    #[inline]
    pub fn prefix_same_as_spacelike(&self) -> bool {
        self.prefix_same_as_spacelike
    }

    #[inline]
    pub fn set_prefix_same_as_spacelike(mut self, enable: bool) -> IterOptions {
        self.prefix_same_as_spacelike = enable;
        self
    }
}

impl Default for IterOptions {
    fn default() -> IterOptions {
        IterOptions {
            lower_bound: None,
            upper_bound: None,
            prefix_same_as_spacelike: false,
            fill_cache: true,
            hint_min_ts: None,
            hint_max_ts: None,
            key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    #[test]
    fn test_hint_ts() {
        let mut ops = IterOptions::default();
        assert_eq!(ops.hint_min_ts(), None);
        assert_eq!(ops.hint_max_ts(), None);

        ops.set_hint_min_ts(Bound::Included(1));
        ops.set_hint_max_ts(Bound::Included(10));
        assert_eq!(ops.hint_min_ts(), Some(1));
        assert_eq!(ops.hint_max_ts(), Some(10));

        ops.set_hint_min_ts(Bound::Excluded(1));
        ops.set_hint_max_ts(Bound::Excluded(10));
        assert_eq!(ops.hint_min_ts(), Some(2));
        assert_eq!(ops.hint_max_ts(), Some(9));
    }
}
