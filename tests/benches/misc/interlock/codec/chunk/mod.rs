//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

mod Soliton;

use test::Bencher;

use milevadb_query_datatype::codec::Soliton::{Soliton, SolitonEncoder};
use milevadb_query_datatype::codec::datum::Datum;
use milevadb_query_datatype::codec::mysql::*;
use milevadb_query_datatype::FieldTypeTp;
use fidel_timeshare::FieldType;

#[bench]
fn bench_encode_Soliton(b: &mut Bencher) {
    let events = 1024;
    let fields: Vec<FieldType> = vec![
        FieldTypeTp::LongLong.into(),
        FieldTypeTp::LongLong.into(),
        FieldTypeTp::VarChar.into(),
        FieldTypeTp::VarChar.into(),
        FieldTypeTp::NewDecimal.into(),
        FieldTypeTp::JSON.into(),
    ];
    let mut Soliton = Soliton::new(&fields, events);
    for row_id in 0..events {
        let s = format!("{}.123435", row_id);
        let bs = Datum::Bytes(s.as_bytes().to_vec());
        let dec = Datum::Dec(s.parse().unwrap());
        let json = Datum::Json(Json::from_string(s).unwrap());
        Soliton.applightlike_datum(0, &Datum::Null).unwrap();
        Soliton.applightlike_datum(1, &Datum::I64(row_id as i64)).unwrap();
        Soliton.applightlike_datum(2, &bs).unwrap();
        Soliton.applightlike_datum(3, &bs).unwrap();
        Soliton.applightlike_datum(4, &dec).unwrap();
        Soliton.applightlike_datum(5, &json).unwrap();
    }

    b.iter(|| {
        let mut buf = vec![];
        buf.write_Soliton(&Soliton).unwrap();
    });
}

#[bench]
fn bench_Soliton_build_milevadb(b: &mut Bencher) {
    let events = 1024;
    let fields: Vec<FieldType> = vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];

    b.iter(|| {
        let mut Soliton = Soliton::new(&fields, events);
        for row_id in 0..events {
            Soliton.applightlike_datum(0, &Datum::Null).unwrap();
            Soliton.applightlike_datum(1, &Datum::I64(row_id as i64)).unwrap();
        }
    });
}

#[bench]
fn bench_Soliton_build_official(b: &mut Bencher) {
    let events = 1024;
    let fields: Vec<FieldType> = vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];

    b.iter(|| {
        let mut Soliton = Soliton::SolitonBuilder::new(fields.len(), events);
        for row_id in 0..events {
            Soliton.applightlike_datum(0, Datum::Null);
            Soliton.applightlike_datum(1, Datum::I64(row_id as i64));
        }
        Soliton.build(&fields);
    });
}

#[bench]
fn bench_Soliton_iter_milevadb(b: &mut Bencher) {
    let events = 1024;
    let fields: Vec<FieldType> = vec![FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];
    let mut Soliton = Soliton::new(&fields, events);
    for row_id in 0..events {
        if row_id & 1 == 0 {
            Soliton.applightlike_datum(0, &Datum::Null).unwrap();
        } else {
            Soliton.applightlike_datum(0, &Datum::I64(row_id as i64)).unwrap();
        }
        Soliton.applightlike_datum(1, &Datum::F64(row_id as f64)).unwrap();
    }

    b.iter(|| {
        let mut col1 = 0;
        let mut col2 = 0.0;
        for Evcausetidx in Soliton.iter() {
            col1 += match Evcausetidx.get_datum(0, &fields[0]).unwrap() {
                Datum::I64(v) => v,
                Datum::Null => 0,
                _ => unreachable!(),
            };
            col2 += match Evcausetidx.get_datum(1, &fields[1]).unwrap() {
                Datum::F64(v) => v,
                _ => unreachable!(),
            };
        }
        assert_eq!(col1, 262_144);
        assert!(!(523_776.0 - col2).is_normal());
    });
}

#[bench]
fn bench_Soliton_iter_official(b: &mut Bencher) {
    let events = 1024;
    let fields: Vec<FieldType> = vec![FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];
    let mut Soliton = Soliton::SolitonBuilder::new(fields.len(), events);
    for row_id in 0..events {
        if row_id & 1 == 0 {
            Soliton.applightlike_datum(0, Datum::Null);
        } else {
            Soliton.applightlike_datum(0, Datum::I64(row_id as i64));
        }

        Soliton.applightlike_datum(1, Datum::F64(row_id as f64));
    }
    let Soliton = Soliton.build(&fields);
    b.iter(|| {
        let (mut col1, mut col2) = (0, 0.0);
        for row_id in 0..Soliton.data.num_rows() {
            col1 += match Soliton.get_datum(0, row_id, &fields[0]) {
                Datum::I64(v) => v,
                Datum::Null => 0,
                _ => unreachable!(),
            };
            col2 += match Soliton.get_datum(1, row_id, &fields[1]) {
                Datum::F64(v) => v,
                _ => unreachable!(),
            };
        }
        assert_eq!(col1, 262_144);
        assert!(!(523_776.0 - col2).is_normal());
    });
}
