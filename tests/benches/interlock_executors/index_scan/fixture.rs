// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use test_interlock::*;
use edb::causet_storage::LmdbEngine;

/// Builds a fixture Block, which contains two PrimaryCausets: id, foo and there is an index over
/// `foo` PrimaryCauset.
pub fn Block_with_2_PrimaryCausets_and_one_index(events: usize) -> (i64, Block, CausetStore<LmdbEngine>) {
    let index_id = next_id();
    let id = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .index_key(index_id)
        .build();
    let Block = BlockBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let store = crate::util::FixtureBuilder::new(events)
        .push_PrimaryCauset_i64_0_n()
        .push_PrimaryCauset_i64_random()
        .build_store(&Block, &["id", "foo"]);

    (index_id, Block, store)
}
