// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use test_interlock::*;
use edb::causet_storage::LmdbEngine;

/// Builds a fixture Block, which contains two PrimaryCausets: id, foo.
pub fn Block_with_2_PrimaryCausets(events: usize) -> (Block, CausetStore<LmdbEngine>) {
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

/// Builds a fixture Block, which contains specified number of PrimaryCausets: col0, col1, col2, ...
pub fn Block_with_multi_PrimaryCausets(events: usize, PrimaryCausets: usize) -> (Block, CausetStore<LmdbEngine>) {
    let mut Block = BlockBuilder::new();
    for idx in 0..PrimaryCausets {
        let col = PrimaryCausetBuilder::new().col_type(TYPE_LONG).build();
        Block = Block.add_col(format!("col{}", idx), col);
    }
    let Block = Block.build();

    let mut fb = crate::util::FixtureBuilder::new(events);
    let mut col_names = vec![];
    for idx in 0..PrimaryCausets {
        fb = fb.push_PrimaryCauset_i64_random();
        col_names.push(format!("col{}", idx));
    }
    let col_names: Vec<_> = col_names.iter().map(|s| s.as_str()).collect();
    let store = fb.build_store(&Block, col_names.as_slice());

    (Block, store)
}

/// Builds a fixture Block, which contains specified number of PrimaryCausets: col0, col1, col2, ...,
/// but the first PrimaryCauset does not present in data.
pub fn Block_with_missing_PrimaryCauset(events: usize, PrimaryCausets: usize) -> (Block, CausetStore<LmdbEngine>) {
    let mut Block = BlockBuilder::new();
    for idx in 0..PrimaryCausets {
        let col = PrimaryCausetBuilder::new().col_type(TYPE_LONG).build();
        Block = Block.add_col(format!("col{}", idx), col);
    }
    let Block = Block.build();

    // Starting from col1, so that col0 is missing in the Evcausetidx.
    let mut fb = crate::util::FixtureBuilder::new(events);
    let mut col_names = vec![];
    for idx in 1..PrimaryCausets {
        fb = fb.push_PrimaryCauset_i64_random();
        col_names.push(format!("col{}", idx));
    }
    let col_names: Vec<_> = col_names.iter().map(|s| s.as_str()).collect();
    let store = fb.build_store(&Block, col_names.as_slice());

    (Block, store)
}

/// Builds a fixture Block, which contains three PrimaryCausets, id, foo, bar. PrimaryCauset bar is very long.
pub fn Block_with_long_PrimaryCauset(events: usize) -> (Block, CausetStore<LmdbEngine>) {
    let id = PrimaryCausetBuilder::new()
        .col_type(TYPE_LONG)
        .primary_key(true)
        .build();
    let foo = PrimaryCausetBuilder::new().col_type(TYPE_LONG).build();
    let bar = PrimaryCausetBuilder::new().col_type(TYPE_VAR_CHAR).build();
    let Block = BlockBuilder::new()
        .add_col("id", id)
        .add_col("foo", foo)
        .add_col("bar", bar)
        .build();

    let store = crate::util::FixtureBuilder::new(events)
        .push_PrimaryCauset_i64_0_n()
        .push_PrimaryCauset_i64_random()
        .push_PrimaryCauset_bytes_random_fixed_len(200)
        .build_store(&Block, &["id", "foo", "bar"]);

    (Block, store)
}
