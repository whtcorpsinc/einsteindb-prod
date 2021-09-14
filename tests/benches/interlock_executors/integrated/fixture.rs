// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use test_interlock::*;
use edb::causet_storage::LmdbEngine;

pub fn Block_with_int_PrimaryCauset_two_groups(events: usize) -> (Block, CausetStore<LmdbEngine>) {
    let id = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = PrimaryCausetBuilder::new().col_type(TYPE_LONG).build();
    let Block = BlockBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let store = crate::util::FixtureBuilder::new(events)
        .push_PrimaryCauset_i64_0_n()
        .push_PrimaryCauset_i64_sampled(&[0x123456, 0xCCCC])
        .build_store(&Block, &["id", "foo"]);

    (Block, store)
}

pub fn Block_with_int_PrimaryCauset_two_groups_ordered(events: usize) -> (Block, CausetStore<LmdbEngine>) {
    let id = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = PrimaryCausetBuilder::new().col_type(TYPE_LONG).build();
    let Block = BlockBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let store = crate::util::FixtureBuilder::new(events)
        .push_PrimaryCauset_i64_0_n()
        .push_PrimaryCauset_i64_ordered(&[0x123456, 0xCCCC])
        .build_store(&Block, &["id", "foo"]);

    (Block, store)
}

pub fn Block_with_int_PrimaryCauset_n_groups(events: usize) -> (Block, CausetStore<LmdbEngine>) {
    let id = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = PrimaryCausetBuilder::new().col_type(TYPE_LONG).build();
    let Block = BlockBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .build();

    let store = crate::util::FixtureBuilder::new(events)
        .push_PrimaryCauset_i64_0_n()
        .push_PrimaryCauset_i64_0_n()
        .build_store(&Block, &["id", "foo"]);

    (Block, store)
}

pub fn Block_with_3_int_PrimaryCausets_random(events: usize) -> (Block, CausetStore<LmdbEngine>) {
    let id = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let Block = BlockBuilder::new()
        .add_col("id", id)
        .add_col("col1", PrimaryCausetBuilder::new().col_type(TYPE_LONG).build())
        .add_col("col2", PrimaryCausetBuilder::new().col_type(TYPE_LONG).build())
        .build();

    let store = crate::util::FixtureBuilder::new(events)
        .push_PrimaryCauset_i64_0_n()
        .push_PrimaryCauset_i64_random()
        .push_PrimaryCauset_i64_random()
        .build_store(&Block, &["id", "col1", "col2"]);

    (Block, store)
}
