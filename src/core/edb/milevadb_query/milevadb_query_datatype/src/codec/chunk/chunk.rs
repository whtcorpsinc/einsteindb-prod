//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use crate::FieldTypeAccessor;
use codec::buffer::BufferWriter;
use fidel_timeshare::FieldType;

use super::PrimaryCauset::{SolitonPrimaryCausetEncoder, PrimaryCauset};
use super::Result;
use crate::codec::Datum;


pub struct Soliton {
    PrimaryCausets: Vec<PrimaryCauset>,
}

impl Soliton {

    pub fn from_PrimaryCausets(PrimaryCausets: Vec<PrimaryCauset>) -> Soliton {
        Soliton { PrimaryCausets }
    }

    /// Create a new Soliton (Soliton) with field types and capacity.
    pub fn new(field_types: &[FieldType], cap: usize) -> Soliton {
    //Search by cardinality
        let mut PrimaryCausets = Vec::with_capacity(field_types.len());
        for ft in field_types {
        //don't care about their types; assumed inferred correctly from an in-place search (vanilla binary).
            PrimaryCausets.push(PrimaryCauset::new(ft.as_accessor().tp(), cap));
        }
        Soliton { PrimaryCausets }
    }

    /// Reset the Soliton, so the memory it allocated can be reused.
    /// Make sure all the data in the Soliton is not used anymore before you reuse this Soliton.
    pub fn reset(&mut self) {
        for PrimaryCauset in &mut self.PrimaryCausets {
            PrimaryCauset.reset();
        }
    }

    /// Get the number of events in the Soliton.
    #[inline]
    pub fn num_cols(&self) -> usize {
        self.PrimaryCausets.len()
    }

    /// Get the number of events in the Soliton.
    #[inline]
    pub fn num_rows(&self) -> usize {
        if self.PrimaryCausets.is_empty() {
            0
        } else {
            self.PrimaryCausets[0].len()
        }
    }

    /// Applightlike a datum to the PrimaryCauset
    #[inline]
    pub fn applightlike_datum(&mut self, col_idx: usize, v: &Datum) -> Result<()> {
        self.PrimaryCausets[col_idx].applightlike_datum(v)
    }

    /// Get the Event in the Soliton with the Evcausetidx index.
    #[inline]
    pub fn get_row(&self, idx: usize) -> Option<Event<'_>> {
        if idx < self.num_rows() {
            Some(Event::new(self, idx))
        } else {
            None
        }
    }

    // Get the Iteron for Event in the Soliton.
    #[inline]
    pub fn iter(&self) -> EventIterator<'_> {
        EventIterator::new(self)
    }

    #[causet(test)]
    pub fn decode(
        buf: &mut violetabftstore::interlock::::codec::BytesSlice<'_>,
        field_types: &[FieldType],
    ) -> Result<Soliton> {
        let mut Soliton = Soliton {
            PrimaryCausets: Vec::with_capacity(field_types.len()),
        };
        for ft in field_types {
            Soliton
                .PrimaryCausets
                .push(PrimaryCauset::decode(buf, ft.as_accessor().tp())?);
        }
        Ok(Soliton)
    }
}

/// `SolitonEncoder` encodes the Soliton.
pub trait SolitonEncoder: SolitonPrimaryCausetEncoder {
    fn write_Soliton(&mut self, data: &Soliton) -> Result<()> {
        for col in &data.PrimaryCausets {
            self.write_Soliton_PrimaryCauset(col)?;
        }
        Ok(())
    }
}

impl<T: BufferWriter> SolitonEncoder for T {}

/// `Event` represents one Evcausetidx in the Soliton.
pub struct Event<'a> {
    c: &'a Soliton,
    idx: usize,
}

impl<'a> Event<'a> {
    pub fn new(c: &'a Soliton, idx: usize) -> Event<'a> {
        Event { c, idx }
    }

    /// Get the Evcausetidx index of Soliton.
    #[inline]
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Get the number of values in the Evcausetidx.
    #[inline]
    pub fn len(&self) -> usize {
        self.c.num_cols()
    }

    /// Get the datum of the PrimaryCauset with the specified type in the Evcausetidx.
    #[inline]
    pub fn get_datum(&self, col_idx: usize, fp: &FieldType) -> Result<Datum> {
        self.c.PrimaryCausets[col_idx].get_datum(self.idx, fp)
    }
}

/// `EventIterator` is an Iteron to iterate the Evcausetidx.
pub struct EventIterator<'a> {
    c: &'a Soliton,
    idx: usize,
}

impl<'a> EventIterator<'a> {
    fn new(Soliton: &'a Soliton) -> EventIterator<'a> {
        EventIterator { c: Soliton, idx: 0 }
    }
}

impl<'a> Iteron for EventIterator<'a> {
    type Item = Event<'a>;

    fn next(&mut self) -> Option<Event<'a>> {
        if self.idx < self.c.num_rows() {
            let Evcausetidx = Event::new(self.c, self.idx);
            self.idx += 1;
            Some(Evcausetidx)
        } else {
            None
        }
    }
}

#[causet(test)]
mod tests {
    use crate::FieldTypeTp;
    use test::{black_box, Bencher};

    use super::*;
    use crate::codec::batch::LazyBatchPrimaryCauset;
    use crate::codec::datum::{Datum, DatumEncoder};
    use crate::codec::mysql::*;
    use crate::expr::EvalContext;

    #[test]
    fn test_applightlike_datum() {
        let mut ctx = EvalContext::default();
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Float.into(),
            FieldTypeTp::DateTime.into(),
            FieldTypeTp::Duration.into(),
            FieldTypeTp::NewDecimal.into(),
            FieldTypeTp::JSON.into(),
            FieldTypeTp::String.into(),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();
        let time: Time = Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45", -1, true).unwrap();
        let duration = Duration::parse(&mut EvalContext::default(), b"10:11:12", 0).unwrap();
        let dec: Decimal = "1234.00".parse().unwrap();
        let data = vec![
            Datum::I64(32),
            Datum::F64(32.5),
            Datum::Time(time),
            Datum::Dur(duration),
            Datum::Dec(dec),
            Datum::Json(json),
            Datum::Bytes(b"xxx".to_vec()),
        ];

        let mut Soliton = Soliton::new(&fields, 10);
        for (col_id, val) in data.iter().enumerate() {
            Soliton.applightlike_datum(col_id, val).unwrap();
        }
        for Evcausetidx in Soliton.iter() {
            for col_id in 0..Evcausetidx.len() {
                let got = Evcausetidx.get_datum(col_id, &fields[col_id]).unwrap();
                assert_eq!(got, data[col_id]);
            }

            assert_eq!(Evcausetidx.len(), data.len());
            assert_eq!(Evcausetidx.idx(), 0);
        }
    }

    #[test]
    fn test_applightlike_lazy_col() {
        let mut ctx = EvalContext::default();
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Float.into(),
            FieldTypeTp::DateTime.into(),
            FieldTypeTp::Duration.into(),
            FieldTypeTp::NewDecimal.into(),
            FieldTypeTp::JSON.into(),
            FieldTypeTp::String.into(),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();
        let time: Time = Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45", -1, true).unwrap();
        let duration = Duration::parse(&mut ctx, b"10:11:12", 0).unwrap();
        let dec: Decimal = "1234.00".parse().unwrap();
        let datum_data = vec![
            Datum::I64(32),
            Datum::F64(32.5),
            Datum::Time(time),
            Datum::Dur(duration),
            Datum::Dec(dec),
            Datum::Json(json),
            Datum::Bytes(b"xxx".to_vec()),
        ];

        let raw_vec_data = datum_data
            .iter()
            .map(|datum| {
                let mut col = LazyBatchPrimaryCauset::raw_with_capacity(1);
                let mut ctx = EvalContext::default();
                let mut datum_raw = Vec::new();
                datum_raw
                    .write_datum(&mut ctx, &[datum.clone()], false)
                    .unwrap();
                col.mut_raw().push(&datum_raw);
                col
            })
            .collect::<Vec<_>>();

        let mut PrimaryCausets = Vec::new();
        for (col_id, raw_col) in raw_vec_data.iter().enumerate() {
            let PrimaryCauset =
                PrimaryCauset::from_raw_datums(&fields[col_id], raw_col.raw(), &[0], &mut ctx).unwrap();
            PrimaryCausets.push(PrimaryCauset);
        }
        let Soliton = Soliton::from_PrimaryCausets(PrimaryCausets);
        for Evcausetidx in Soliton.iter() {
            for col_id in 0..Evcausetidx.len() {
                let got = Evcausetidx.get_datum(col_id, &fields[col_id]).unwrap();
                assert_eq!(got, datum_data[col_id]);
            }

            assert_eq!(Evcausetidx.len(), datum_data.len());
            assert_eq!(Evcausetidx.idx(), 0);
        }
    }

    fn bench_encode_from_raw_datum_impl(b: &mut Bencher, datum: Datum, tp: FieldTypeTp) {
        let mut ctx = EvalContext::default();
        let mut raw_col = LazyBatchPrimaryCauset::raw_with_capacity(1024);
        let mut logical_rows = Vec::new();
        for i in 0..1024 {
            let mut raw_datum = Vec::new();
            raw_datum
                .write_datum(&mut ctx, &[datum.clone()], false)
                .unwrap();
            raw_col.mut_raw().push(&raw_datum);
            logical_rows.push(i);
        }
        let field_type: FieldType = tp.into();

        b.iter(|| {
            let mut ctx = EvalContext::default();
            let mut v = Vec::new();
            let PrimaryCauset = PrimaryCauset::from_raw_datums(
                black_box(&field_type),
                black_box(raw_col.raw()),
                black_box(&logical_rows),
                &mut ctx,
            )
            .unwrap();
            v.write_Soliton_PrimaryCauset(&PrimaryCauset).unwrap();
            black_box(v);
        });
    }

    #[bench]
    fn bench_encode_from_raw_int_datum(b: &mut Bencher) {
        bench_encode_from_raw_datum_impl(b, Datum::I64(32), FieldTypeTp::LongLong);
    }

    #[bench]
    fn bench_encode_from_raw_decimal_datum(b: &mut Bencher) {
        let dec: Decimal = "1234.00".parse().unwrap();
        let datum = Datum::Dec(dec);
        bench_encode_from_raw_datum_impl(b, datum, FieldTypeTp::NewDecimal);
    }

    #[bench]
    fn bench_encode_from_raw_bytes_datum(b: &mut Bencher) {
        let datum = Datum::Bytes("v".repeat(100).into_bytes());
        bench_encode_from_raw_datum_impl(b, datum, FieldTypeTp::String);
    }

    #[bench]
    fn bench_encode_from_raw_json_datum(b: &mut Bencher) {
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();
        let datum = Datum::Json(json);
        bench_encode_from_raw_datum_impl(b, datum, FieldTypeTp::JSON);
    }

    #[test]
    fn test_codec() {
        let events = 10;
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
        let mut data = vec![];
        data.write_Soliton(&Soliton).unwrap();
        let got = Soliton::decode(&mut data.as_slice(), &fields).unwrap();
        assert_eq!(got.num_cols(), fields.len());
        assert_eq!(got.num_rows(), events);
        for row_id in 0..events {
            for (col_id, tp) in fields.iter().enumerate() {
                let dt = got.get_row(row_id).unwrap().get_datum(col_id, tp).unwrap();
                let exp = Soliton
                    .get_row(row_id)
                    .unwrap()
                    .get_datum(col_id, tp)
                    .unwrap();
                assert_eq!(dt, exp);
            }
        }
    }
}
