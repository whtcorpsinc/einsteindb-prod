/! This module contains helper code for building `Entry` from line protocol and the
//! `DatabaseRules` configuration.

use bytes::Bytes;
use std::{collections::BTreeMap, convert::TryFrom, fmt::Formatter};
use std::cmp::Ordering;
use std::mem;
use chrono::{DateTime, TimeZone, Utc};
use flatbuffers::{FlatBufferBuilder, Follow, ForwardsUOffset, Vector, VectorIter, WIPOffset};
use ouroboros::self_referencing;
use snafu::{OptionExt, ResultExt, Snafu};

use *::database_rules::{Error as DataError, Partitioner, ShardId, Sharder};
use *::sequence::Sequence;
use *::write_summary::TimestampSummary;
use generated_types::influxdata::pbdata::v1 as pb;
//context missing
use time::Time;
use trace::ctx::SpanContext;

use crate::entry_fb;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error generating partition key {}", source))]
    GeneratingPartitionKey { source: DataError },

    #[snafu(display("Error getting shard id {}", source))]
    GeneratingShardId { source: DataError },

    #[snafu(display(
        "table {} has column {} {} with new data on line {}",
        table,
        column,
        source,
        line_number
    ))]
    TableColumnTypeMismatch {
        table: String,
        column: String,
        line_number: usize,
        source: ColumnError,
    },

    #[snafu(display("invalid flatbuffers: field {} is required", field))]
    FlatbufferFieldMissing { field: String },

    #[snafu(display("'time' column is required"))]
    TimeColumnMissing,

    #[snafu(display("time value missing from batch or no rows in batch"))]
    TimeValueMissing,

    #[snafu(display("'time' column must be i64 type"))]
    TimeColumnWrongType,

    #[snafu(display("proto column {} semantic type {} invalid", column_name, semantic_type))]
    PBColumnSemanticTypeInvalid {
        column_name: String,
        semantic_type: i32,
    },

    #[snafu(display("proto column {} contains only null values", column_name))]
    PBColumnContainsOnlyNullValues { column_name: String },

    #[snafu(display("table column type conflict {}", message))]
    PBSemanticTypeConflict { message: String },
}
