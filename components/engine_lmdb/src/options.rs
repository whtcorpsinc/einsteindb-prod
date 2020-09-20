// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use rocksdb::{
    ReadOptions as RawReadOptions, TableFilter, TableProperties, WriteOptions as RawWriteOptions,
};
use einsteindb_util::codec::number;

pub struct LmdbReadOptions(RawReadOptions);

impl LmdbReadOptions {
    pub fn into_raw(self) -> RawReadOptions {
        self.0
    }
}

impl From<engine_promises::ReadOptions> for LmdbReadOptions {
    fn from(opts: engine_promises::ReadOptions) -> Self {
        let mut r = RawReadOptions::default();
        r.fill_cache(opts.fill_cache());
        LmdbReadOptions(r)
    }
}

impl From<&engine_promises::ReadOptions> for LmdbReadOptions {
    fn from(opts: &engine_promises::ReadOptions) -> Self {
        opts.clone().into()
    }
}

pub struct LmdbWriteOptions(RawWriteOptions);

impl LmdbWriteOptions {
    pub fn into_raw(self) -> RawWriteOptions {
        self.0
    }
}

impl From<engine_promises::WriteOptions> for LmdbWriteOptions {
    fn from(opts: engine_promises::WriteOptions) -> Self {
        let mut r = RawWriteOptions::default();
        r.set_sync(opts.sync());
        LmdbWriteOptions(r)
    }
}

impl From<&engine_promises::WriteOptions> for LmdbWriteOptions {
    fn from(opts: &engine_promises::WriteOptions) -> Self {
        opts.clone().into()
    }
}

impl From<engine_promises::IterOptions> for LmdbReadOptions {
    fn from(opts: engine_promises::IterOptions) -> Self {
        let r = build_read_opts(opts);
        LmdbReadOptions(r)
    }
}

fn build_read_opts(iter_opts: engine_promises::IterOptions) -> RawReadOptions {
    let mut opts = RawReadOptions::new();
    opts.fill_cache(iter_opts.fill_cache());
    if iter_opts.key_only() {
        opts.set_titan_key_only(true);
    }
    if iter_opts.total_order_seek_used() {
        opts.set_total_order_seek(true);
    } else if iter_opts.prefix_same_as_spacelike() {
        opts.set_prefix_same_as_spacelike(true);
    }

    if iter_opts.hint_min_ts().is_some() || iter_opts.hint_max_ts().is_some() {
        let ts_filter = TsFilter::new(iter_opts.hint_min_ts(), iter_opts.hint_max_ts());
        opts.set_table_filter(Box::new(ts_filter))
    }

    let (lower, upper) = iter_opts.build_bounds();
    if let Some(lower) = lower {
        opts.set_iterate_lower_bound(lower);
    }
    if let Some(upper) = upper {
        opts.set_iterate_upper_bound(upper);
    }

    opts
}

struct TsFilter {
    hint_min_ts: Option<u64>,
    hint_max_ts: Option<u64>,
}

impl TsFilter {
    fn new(hint_min_ts: Option<u64>, hint_max_ts: Option<u64>) -> TsFilter {
        TsFilter {
            hint_min_ts,
            hint_max_ts,
        }
    }
}

impl TableFilter for TsFilter {
    fn table_filter(&self, props: &TableProperties) -> bool {
        if self.hint_max_ts.is_none() && self.hint_min_ts.is_none() {
            return true;
        }

        let user_props = props.user_collected_properties();

        if let Some(hint_min_ts) = self.hint_min_ts {
            // TODO avoid hard code after refactor MvccProperties from
            // einsteindb/src/violetabftstore/interlock/ into some component about engine.
            if let Some(mut p) = user_props.get("einsteindb.max_ts") {
                if let Ok(get_max) = number::decode_u64(&mut p) {
                    if get_max < hint_min_ts {
                        return false;
                    }
                }
            }
        }

        if let Some(hint_max_ts) = self.hint_max_ts {
            // TODO avoid hard code after refactor MvccProperties from
            // einsteindb/src/violetabftstore/interlock/ into some component about engine.
            if let Some(mut p) = user_props.get("einsteindb.min_ts") {
                if let Ok(get_min) = number::decode_u64(&mut p) {
                    if get_min > hint_max_ts {
                        return false;
                    }
                }
            }
        }

        true
    }
}
