//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

mod Soliton;
mod mysql;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use test::black_box;
use test::Bencher;

use milevadb_query_datatype::codec::Block::*;

#[bench]
fn bench_Block_prefix_spacelike_with(b: &mut Bencher) {
    let key: &[u8] = b"tabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.spacelikes_with(Block_PREFIX)))
    });
}

#[bench]
fn bench_Block_prefix_check(b: &mut Bencher) {
    let key: &[u8] = b"tabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.len() > 1 && key[0] == Block_PREFIX[0]))
    });
}

#[bench]
fn bench_record_prefix_spacelike_with(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.spacelikes_with(RECORD_PREFIX_SEP)))
    });
}

#[bench]
fn bench_record_prefix_equal_check(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| {
            black_box(
                key.len() > 2 && key[0] == RECORD_PREFIX_SEP[0] && key[1] == RECORD_PREFIX_SEP[1],
            )
        })
    });
}

#[bench]
fn bench_record_prefix_biglightlikeian_check(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    let prefix: u16 = BigEndian::read_u16(RECORD_PREFIX_SEP);
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.len() > 2 && BigEndian::read_u16(key) == prefix))
    });
}

#[bench]
fn bench_record_prefix_littlelightlikeian_check(b: &mut Bencher) {
    let key: &[u8] = b"_rabc";
    let prefix: u16 = LittleEndian::read_u16(RECORD_PREFIX_SEP);
    b.iter(|| {
        let n = black_box(1000);
        (0..n).all(|_| black_box(key.len() > 2 && LittleEndian::read_u16(key) == prefix))
    });
}
