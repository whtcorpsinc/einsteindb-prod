// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use test_interlock::*;
use einsteindb::persistence::LmdbEngine;

/// Builds a fixture table, which contains two PrimaryCausets: id, foo and there is an index over
/// `foo` PrimaryCauset.
pub fn table_with_2_PrimaryCausets_and_one_index(events: usize) -> (i64, Table, CausetStore<LmdbEngine>) {
    let index_id = next_id();
    let id = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .index_key(index_id)
        .build();
    let table = TableBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let store = crate::util::FixtureBuilder::new(events)
        .push_PrimaryCauset_i64_0_n()
        .push_PrimaryCauset_i64_random()
        .build_store(&table, &["id", "foo"]);

    (index_id, table, store)
}
