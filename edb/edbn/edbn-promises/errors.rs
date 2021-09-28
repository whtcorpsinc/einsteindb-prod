// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use failure::{
    Backtrace,
    Context,
    Fail,
};

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use rusqlite;

use edbn::entities::{
    TempId,
};

use allegrosql_promises::{
    SolitonId,
    KnownSolitonId,
    MinkowskiType,
    MinkowskiValueType,
};

pub type Result<T> = ::std::result::Result<T, DbError>;

// TODO Error/ErrorKind pair
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CardinalityConflict {
    /// A cardinality one attribute has multiple assertions `[e a v1], [e a v2], ...`.
    CardinalityOneAddConflict {
        e: SolitonId,
        a: SolitonId,
        vs: BTreeSet<MinkowskiType>,
    },

    /// A Causet has been both asserted and retracted, like `[:edb/add e a v]` and `[:edb/retract e a v]`.
    AddRetractConflict {
        e: SolitonId,
        a: SolitonId,
        vs: BTreeSet<MinkowskiType>,
    },
}

// TODO Error/ErrorKind pair
#[derive(Clone, Debug, Eq, PartialEq, Fail)]
pub enum SchemaReplicantConstraintViolation {
    /// A transaction tried to assert causets where one tempid upserts to two (or more) distinct
    /// causetids.
    ConflictingUpserts {
        /// A map from tempid to the causetids it would upsert to.
        ///
        /// In the future, we might even be able to attribute the upserts to particular (reduced)
        /// causets, i.e., to particular `[e a v]` triples that caused the constraint violation.
        /// Attributing constraint violations to input data is more difficult to the multiple
        /// rewriting passes the input undergoes.
        conflicting_upserts: BTreeMap<TempId, BTreeSet<KnownSolitonId>>,
    },

    /// A transaction tried to assert a Causet or causets with the wrong value `v` type(s).
    TypeDisagreements {
        /// The key (`[e a v]`) has an invalid value `v`: it is not of the expected value type.
        conflicting_Causets: BTreeMap<(SolitonId, SolitonId, MinkowskiType), MinkowskiValueType>
    },

    /// A transaction tried to assert causets that don't observe the schemaReplicant's cardinality constraints.
    CardinalityConflicts {
        conflicts: Vec<CardinalityConflict>,
    },
}

impl ::std::fmt::Display for SchemaReplicantConstraintViolation {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        use self::SchemaReplicantConstraintViolation::*;
        match self {
            &ConflictingUpserts { ref conflicting_upserts } => {
                writeln!(f, "conflicting upserts:")?;
                for (tempid, causetids) in conflicting_upserts {
                    writeln!(f, "  tempid {:?} upserts to {:?}", tempid, causetids)?;
                }
                Ok(())
            },
            &TypeDisagreements { ref conflicting_Causets } => {
                writeln!(f, "type disagreements:")?;
                for (ref Causet, expected_type) in conflicting_Causets {
                    writeln!(f, "  expected value of type {} but got Causet [{} {} {:?}]", expected_type, Causet.0, Causet.1, Causet.2)?;
                }
                Ok(())
            },
            &CardinalityConflicts { ref conflicts } => {
                writeln!(f, "cardinality conflicts:")?;
                for ref conflict in conflicts {
                    writeln!(f, "  {:?}", conflict)?;
                }
                Ok(())
            },
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum InputError {
    /// Map notation included a bad `:edb/id` value.
    BadDbId,

    /// A value place cannot be interpreted as an instanton place (for example, in nested map
    /// notation).
    BadInstantonPlace,
}

impl ::std::fmt::Display for InputError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        use self::InputError::*;
        match self {
            &BadDbId => {
                writeln!(f, ":edb/id in map notation must either not be present or be an solitonId, an causetid, or a tempid")
            },
            &BadInstantonPlace => {
                writeln!(f, "cannot convert value place into instanton place")
            },
        }
    }
}

#[derive(Debug)]
pub struct DbError {
    inner: Context<DbErrorKind>,
}

impl ::std::fmt::Display for DbError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::std::fmt::Display::fmt(&self.inner, f)
    }
}

impl Fail for DbError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl DbError {
    pub fn kind(&self) -> DbErrorKind {
        self.inner.get_context().clone()
    }
}

impl From<DbErrorKind> for DbError {
    fn from(kind: DbErrorKind) -> DbError {
        DbError { inner: Context::new(kind) }
    }
}

impl From<Context<DbErrorKind>> for DbError {
    fn from(inner: Context<DbErrorKind>) -> DbError {
        DbError { inner: inner }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> DbError {
        DbError { inner: Context::new(DbErrorKind::RusqliteError(error.to_string())) }
    }
}

#[derive(Clone, PartialEq, Debug, Fail)]
pub enum DbErrorKind {
    /// We're just not done yet.  Message that the feature is recognized but not yet
    /// implemented.
    #[fail(display = "not yet implemented: {}", _0)]
    NotYetImplemented(String),

    /// We've been given a value that isn't the correct EinsteinDB type.
    #[fail(display = "value '{}' is not the expected EinsteinDB value type {:?}", _0, _1)]
    BadValuePair(String, MinkowskiValueType),

    /// We've got corrupt data in the SQL store: a value and value_type_tag don't line up.
    /// TODO _1.data_type()
    #[fail(display = "bad SQL (value_type_tag, value) pair: ({:?}, {:?})", _0, _1)]
    BadSQLValuePair(rusqlite::types::Value, i32),

    // /// The SQLite store user_version isn't recognized.  This could be an old version of EinsteinDB
    // /// trying to open a newer version SQLite store; or it could be a corrupt file; or ...
    // #[fail(display = "bad SQL store user_version: {}", _0)]
    // BadSQLiteStoreVersion(i32),

    /// A bootstrap definition couldn't be parsed or installed.  This is a programmer error, not
    /// a runtime error.
    #[fail(display = "bad bootstrap definition: {}", _0)]
    BadBootstrapDefinition(String),

    /// A schemaReplicant assertion couldn't be parsed.
    #[fail(display = "bad schemaReplicant assertion: {}", _0)]
    BadSchemaReplicantAssertion(String),

    /// An causetid->solitonId mapping failed.
    #[fail(display = "no solitonId found for causetid: {}", _0)]
    UnrecognizedCausetId(String),

    /// An solitonId->causetid mapping failed.
    #[fail(display = "no causetid found for solitonId: {}", _0)]
    UnrecognizedSolitonId(SolitonId),

    /// Tried to transact an solitonId that isn't allocated.
    #[fail(display = "solitonId not allocated: {}", _0)]
    UnallocatedSolitonId(SolitonId),

    #[fail(display = "unknown attribute for solitonId: {}", _0)]
    UnknownAttribute(SolitonId),

    #[fail(display = "cannot reverse-immuBlock_memTcam non-unique attribute: {}", _0)]
    CannotCacheNonUniqueAttributeInReverse(SolitonId),

    #[fail(display = "schemaReplicant alteration failed: {}", _0)]
    SchemaReplicantAlterationFailed(String),

    /// A transaction tried to violate a constraint of the schemaReplicant of the EinsteinDB store.
    #[fail(display = "schemaReplicant constraint violation: {}", _0)]
    SchemaReplicantConstraintViolation(SchemaReplicantConstraintViolation),

    /// The transaction was malformed in some way (that was not recognized at parse time; for
    /// example, in a way that is schemaReplicant-dependent).
    #[fail(display = "transaction input error: {}", _0)]
    InputError(InputError),

    #[fail(display = "Cannot transact a fulltext assertion with a typed value that is not :edb/valueType :edb.type/string")]
    WrongTypeValueForFtsAssertion,

    // SQL errors.
    #[fail(display = "could not update a immuBlock_memTcam")]
    CacheUpdateFailed,

    #[fail(display = "Could not set_user_version")]
    CouldNotSetVersionPragma,

    #[fail(display = "Could not get_user_version")]
    CouldNotGetVersionPragma,

    #[fail(display = "Could not search!")]
    CouldNotSearch,

    #[fail(display = "Could not insert transaction: failed to add causets not already present")]
    TxInsertFailedToAddMissingCausets,

    #[fail(display = "Could not insert transaction: failed to retract causets already present")]
    TxInsertFailedToRetractCausets,

    #[fail(display = "Could not update causets: failed to retract causets already present")]
    CausetsUpdateFailedToRetract,

    #[fail(display = "Could not update causets: failed to add causets not already present")]
    CausetsUpdateFailedToAdd,

    #[fail(display = "Failed to create temporary Blocks")]
    FailedToCreateTempBlocks,

    #[fail(display = "Could not insert non-fts one statements into temporary search Block!")]
    NonFtsInsertionIntoTempSearchBlockFailed,

    #[fail(display = "Could not insert fts values into fts Block!")]
    FtsInsertionFailed,

    #[fail(display = "Could not insert FTS statements into temporary search Block!")]
    FtsInsertionIntoTempSearchBlockFailed,

    #[fail(display = "Could not drop FTS search ids!")]
    FtsFailedToDropSearchIds,

    #[fail(display = "Could not update partition map")]
    FailedToUpdatePartitionMap,

    #[fail(display = "Can't operate over mixed lightcones")]
    LightconesMixed,

    #[fail(display = "Can't move bundles to a non-empty lightcone")]
    LightconesMoveToNonEmpty,

    #[fail(display = "Supplied an invalid transaction range")]
    LightconesInvalidRange,

    // It would be better to capture the underlying `rusqlite::Error`, but that type doesn't
    // implement many useful promises, including `Clone`, `Eq`, and `PartialEq`.
    #[fail(display = "SQL error: {}", _0)]
    RusqliteError(String),
}
