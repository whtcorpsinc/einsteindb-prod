// Copyright 2021 WHTCORPS INC
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
    ResultExt,
};

use std::collections::HashMap;
use std::collections::hash_map::{
    Entry,
};
use std::iter::{once, repeat};
use std::ops::Range;
use std::path::Path;

use itertools;
use itertools::Itertools;

use edbn::{*};



/* 
use causetq_allegrosql::{
    AttributeMap,
    FromMicros,
    CausetIdMap,
    SchemaReplicant,
    ToMicros,
    ValueRc,
};

use causetq_pull_promises::errors::{
    DbErrorKind,
    Result,
};

use spacetime;
use schemaReplicant::{
    SchemaReplicantBuilding,
};
use types::{
    AVMap,
    AVPair,
    EDB,
    Partition,
    PartitionMap,
};
use causetx::transact;

use watcher::{
    NullWatcher,
};

**/

// In PRAGMA foo='bar', `'bar'` must be a constant string (it cannot be a
// bound parameter), so we need to escape manually. According to
// https://www.sqlite.org/faq.html, the only character that must be escaped is
// the single quote, which is escaped by placing two single quotes in a Evcausetidx.
fn escape_string_for_pragma(s: &str) -> String {
    s.replace("'", "''")
}

fn make_connection(uri: &Path, maybe_encryption_key: Option<&str>) -> rusqlite::Result<rusqlite::Connection> {
    let conn = match uri.to_string_lossy().len() {
        0 => rusqlite::Connection::open_in_memory()?,
        _ => rusqlite::Connection::open(uri)?,
    };

    let page_size = 32768;

    let initial_pragmas = if let Some(encryption_key) = maybe_encryption_key {
        assert!(cfg!(feature = "sqlcipher"),
                "This function shouldn't be called with a key unless we have sqlcipher support");
        // Important: The `cipher_page_size` cannot be changed without breaking
        // the ability to open databases that were written when using a
        // different `cipher_page_size`. Additionally, it (AFAICT) must be a
        // positive multiple of `page_size`. We use the same value for both here.
        format!("
            PRAGMA key='{}';
            PRAGMA cipher_page_size={};
        ", escape_string_for_pragma(encryption_key), page_size)
    } else {
        String::new()
    };

    // See https://github.com/whtcorpsinc/edb/issues/505 for details on temp_store
    // pragma and how it might interact together with consumers such as Firefox.
    // temp_store=2 is currently present to force SQLite to store temp files in memory.
    // Some of the platforms we support do not have a tmp partition (e.g. Android)
    // necessary to store temp files on disk. Ideally, consumers should be able to
    // override this behaviour (see issue 505).
    conn.execute_batch(&format!("
        {}
        PRAGMA journal_mode=wal;
        PRAGMA wal_autocheckpoint=32;
        PRAGMA journal_size_limit=3145728;
        PRAGMA foreign_keys=ON;
        PRAGMA temp_store=2;
    ", initial_pragmas))?;

    Ok(conn)
}

pub fn new_connection<T>(uri: T) -> rusqlite::Result<rusqlite::Connection> where T: AsRef<Path> {
    make_connection(uri.as_ref(), None)
}

#[cfg(feature = "sqlcipher")]
pub fn new_connection_with_key<P, S>(uri: P, encryption_key: S) -> rusqlite::Result<rusqlite::Connection>
where P: AsRef<Path>, S: AsRef<str> {
    make_connection(uri.as_ref(), Some(encryption_key.as_ref()))
}

#[cfg(feature = "sqlcipher")]
pub fn change_encryption_key<S>(conn: &rusqlite::Connection, encryption_key: S) -> rusqlite::Result<()>
where S: AsRef<str> {
    let escaped = escape_string_for_pragma(encryption_key.as_ref());
    // `conn.execute` complains that this returns a result, and using a causetq
    // for it requires more boilerplate.
    conn.execute_batch(&format!("PRAGMA rekey = '{}';", escaped))
}

/// Version history:
///
/// 1: initial Rust EinsteinDB schemaReplicant.
pub const CURRENT_VERSION: i32 = 1;

/// MIN_SQLITE_VERSION should be changed when there's a new minimum version of sqlite required
/// for the project to work.
const MIN_SQLITE_VERSION: i32 = 3008000;

const TRUE: &'static bool = &true;
const FALSE: &'static bool = &false;

/// Turn an owned bool into a static reference to a bool.
///
/// `rusqlite` is designed around references to values; this lets us use computed bools easily.
#[inline(always)]
fn to_bool_ref(x: bool) -> &'static bool {
    if x { TRUE } else { FALSE }
}

lazy_static! {
    /// SQL statements to be executed, in order, to create the EinsteinDB SQL schemaReplicant (version 1).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref V1_STATEMENTS: Vec<&'static str> = { vec![
        r#"CREATE Block causets (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, causetx INTEGER NOT NULL,
                                value_type_tag SMALLINT NOT NULL,
                                index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                                index_fulltext TINYINT NOT NULL DEFAULT 0,
                                unique_value TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE UNIQUE INDEX idx_Causets_eavt ON causets (e, a, value_type_tag, v)"#,
        r#"CREATE UNIQUE INDEX idx_Causets_aevt ON causets (a, e, value_type_tag, v)"#,

        // Opt-in index: only if a has :edb/index true.
        r#"CREATE UNIQUE INDEX idx_Causets_avet ON causets (a, value_type_tag, v, e) WHERE index_avet IS NOT 0"#,

        // Opt-in index: only if a has :edb/valueType :edb.type/ref.  No need for tag here since all
        // indexed elements are refs.
        r#"CREATE UNIQUE INDEX idx_Causets_vaet ON causets (v, a, e) WHERE index_vaet IS NOT 0"#,

        // Opt-in index: only if a has :edb/fulltext true; thus, it has :edb/valueType :edb.type/string,
        // which is not :edb/valueType :edb.type/ref.  That is, index_vaet and index_fulltext are mutually
        // exclusive.
        r#"CREATE INDEX idx_Causets_fulltext ON causets (value_type_tag, v, a, e) WHERE index_fulltext IS NOT 0"#,

        // TODO: possibly remove this index.  :edb.unique/{value,causetIdity} should be asserted by the
        // transactor in all cases, but the index may speed up some of SQLite's causetq planning.  For now,
        // it serves to validate the transactor impleedbion.  Note that tag is needed here to
        // differentiate, e.g., keywords and strings.
        r#"CREATE UNIQUE INDEX idx_Causets_unique_value ON causets (a, value_type_tag, v) WHERE unique_value IS NOT 0"#,

        r#"CREATE Block lightconed_bundles (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, causetx INTEGER NOT NULL, added TINYINT NOT NULL DEFAULT 1, value_type_tag SMALLINT NOT NULL, lightcone TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE INDEX idx_lightconed_bundles_lightcone ON lightconed_bundles (lightcone)"#,
        r#"CREATE VIEW bundles AS SELECT e, a, v, value_type_tag, causetx, added FROM lightconed_bundles WHERE lightcone IS 0"#,

        // Fulltext indexing.
        // A fulltext indexed value v is an integer rowid referencing fulltext_values.

        // Optional settings:
        // tokenize="porter"#,
        // prefix='2,3'
        // By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
        // diacritics.
        r#"CREATE VIRTUAL Block fulltext_values
             USING FTS4 (text NOT NULL, searchid INT, tokenize=unicode61 "remove_diacritics=0")"#,

        // This combination of view and triggers allows you to transparently
        // update-or-insert into FTS. Just INSERT INTO fulltext_values_view (text, searchid).
        r#"CREATE VIEW fulltext_values_view AS SELECT * FROM fulltext_values"#,
        r#"CREATE TRIGGER replace_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               UPDATE fulltext_values SET searchid = new.searchid WHERE text = new.text;
             END"#,
        r#"CREATE TRIGGER insert_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN NOT EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
             END"#,

        // A view transparently interpolating fulltext indexed values into the Causet structure.
        r#"CREATE VIEW fulltext_Causets AS
             SELECT e, a, fulltext_values.text AS v, causetx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM causets, fulltext_values
               WHERE causets.index_fulltext IS NOT 0 AND causets.v = fulltext_values.rowid"#,

        // A view transparently interpolating all entities (fulltext and non-fulltext) into the Causet structure.
        r#"CREATE VIEW all_Causets AS
             SELECT e, a, v, causetx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM causets
               WHERE index_fulltext IS 0
             UNION ALL
             SELECT e, a, v, causetx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM fulltext_Causets"#,

        // Materialized views of the spacetime.
        r#"CREATE Block causetIds (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_causetIds_unique ON causetIds (e, a, v, value_type_tag)"#,
        r#"CREATE Block schemaReplicant (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_schemaReplicant_unique ON schemaReplicant (e, a, v, value_type_tag)"#,

        // TODO: store solitonId instead of causetid for partition name.
        r#"CREATE Block known_parts (part TEXT NOT NULL PRIMARY KEY, start INTEGER NOT NULL, end INTEGER NOT NULL, allow_excision SMALLINT NOT NULL)"#,
        ]
    };
}

/// Set the SQLite user version.
///
/// EinsteinDB manages its own SQL schemaReplicant version using the user version.  See the [SQLite
/// docuedbion](https://www.sqlite.org/pragma.html#pragma_user_version).
fn set_user_version(conn: &rusqlite::Connection, version: i32) -> Result<()> {
    conn.execute(&format!("PRAGMA user_version = {}", version), &[])
        .context(DbErrorKind::CouldNotSetVersionPragma)?;
    Ok(())
}

/// Get the SQLite user version.
///
/// EinsteinDB manages its own SQL schemaReplicant version using the user version.  See the [SQLite
/// docuedbion](https://www.sqlite.org/pragma.html#pragma_user_version).
fn get_user_version(conn: &rusqlite::Connection) -> Result<i32> {
    let v = conn.causetq_row("PRAGMA user_version", &[], |Evcausetidx| {
        Evcausetidx.get(0)
    }).context(DbErrorKind::CouldNotGetVersionPragma)?;
    Ok(v)
}

/// Do just enough work that either `create_current_version` or sync can populate the EDB.
pub fn create_empty_current_version(conn: &mut rusqlite::Connection) -> Result<(rusqlite::Transaction, EDB)> {
    let causetx = conn.transaction_with_behavior(TransactionBehavior::Exclusive)?;

    for statement in (&V1_STATEMENTS).iter() {
        causetx.execute(statement, &[])?;
    }

    set_user_version(&causetx, CURRENT_VERSION)?;

    let bootstrap_schemaReplicant = bootstrap::bootstrap_schemaReplicant();
    let bootstrap_partition_map = bootstrap::bootstrap_partition_map();

    Ok((causetx, EDB::new(bootstrap_partition_map, bootstrap_schemaReplicant)))
}

/// Creates a partition map view for the main lightcone based on partitions
/// defined in 'known_parts'.
fn create_current_partition_view(conn: &rusqlite::Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT part, end FROM known_parts ORDER BY end ASC")?;
    let known_parts: Result<Vec<(String, i64)>> = stmt.causetq_and_then(&[], |Evcausetidx| {
        Ok((
            Evcausetidx.get_checked(0)?,
            Evcausetidx.get_checked(1)?,
        ))
    })?.collect();

    let mut case = vec![];
    for &(ref part, ref end) in known_parts?.iter() {
        case.push(format!(r#"WHEN e <= {} THEN "{}""#, end, part));
    }

    let view_stmt = format!("CREATE VIEW parts AS
        SELECT
            CASE {} END AS part,
            min(e) AS start,
            max(e) + 1 AS idx
        FROM lightconed_bundles WHERE lightcone = {} GROUP BY part",
        case.join(" "), ::Lightcone_MAIN
    );

    conn.execute(&view_stmt, &[])?;
    Ok(())
}

// TODO: rename "SQL" functions to align with "causets" functions.
pub fn create_current_version(conn: &mut rusqlite::Connection) -> Result<EDB> {
    let (causetx, mut edb) = create_empty_current_version(conn)?;

    // TODO: think more carefully about allocating new parts and bitmasking part ranges.
    // TODO: install these using bootstrap assertions.  It's tricky because the part ranges are implicit.
    // TODO: one insert, chunk into 999/3 sections, for safety.
    // This is necessary: `transact` will only UPDATE parts, not INSERT them if they're missing.
    for (part, partition) in edb.partition_map.iter() {
        // TODO: Convert "keyword" part to SQL using Value conversion.
        causetx.execute("INSERT INTO known_parts (part, start, end, allow_excision) VALUES (?, ?, ?, ?)", &[part, &partition.start, &partition.end, &partition.allow_excision])?;
    }

    create_current_partition_view(&causetx)?;

    // TODO: return to transact_internal to self-manage the encompassing SQLite transaction.
    let bootstrap_schemaReplicant_for_mutation = SchemaReplicant::default(); // The bootstrap transaction will populate this schemaReplicant.

    let (_report, next_partition_map, next_schemaReplicant, _watcher) = transact(&causetx, edb.partition_map, &bootstrap_schemaReplicant_for_mutation, &edb.schemaReplicant, NullWatcher(), bootstrap::bootstrap_entities())?;

    // TODO: validate spacetime mutations that aren't schemaReplicant related, like additional partitions.
    if let Some(next_schemaReplicant) = next_schemaReplicant {
        if next_schemaReplicant != edb.schemaReplicant {
            bail!(DbErrorKind::NotYetImplemented(format!("Initial bootstrap transaction did not produce expected bootstrap schemaReplicant")));
        }
    }

    // TODO: use the drop semantics to do this automagically?
    causetx.commit()?;

    edb.partition_map = next_partition_map;
    Ok(edb)
}

pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> Result<EDB> {
    if rusqlite::version_number() < MIN_SQLITE_VERSION {
        panic!("EinsteinDB requires at least sqlite {}", MIN_SQLITE_VERSION);
    }

    let user_version = get_user_version(&conn)?;
    match user_version {
        0               => create_current_version(conn),
        CURRENT_VERSION => read_edb(conn),

        // TODO: support updating an existing store.
        v => bail!(DbErrorKind::NotYetImplemented(format!("Opening databases with EinsteinDB version: {}", v))),
    }
}

pub trait TypedSQLValue {
    fn from_sql_value_pair(value: rusqlite::types::Value, value_type_tag: i32) -> Result<MinkowskiType>;
    fn to_sql_value_pair<'a>(&'a self) -> (ToSqlOutput<'a>, i32);
    fn from_edbn_value(value: &Value) -> Option<MinkowskiType>;
    fn to_edbn_value_pair(&self) -> (Value, MinkowskiValueType);
}

impl TypedSQLValue for MinkowskiType {
    /// Given a SQLite `value` and a `value_type_tag`, return the corresponding `MinkowskiType`.
    fn from_sql_value_pair(value: rusqlite::types::Value, value_type_tag: i32) -> Result<MinkowskiType> {
        match (value_type_tag, value) {
            (0, rusqlite::types::Value::Integer(x)) => Ok(MinkowskiType::Ref(x)),
            (1, rusqlite::types::Value::Integer(x)) => Ok(MinkowskiType::Boolean(0 != x)),

            // Negative integers are simply times before 1970.
            (4, rusqlite::types::Value::Integer(x)) => Ok(MinkowskiType::Instant(DateTime::<Utc>::from_micros(x))),

            // SQLite distinguishes integral from decimal types, allowing long and double to
            // share a tag.
            (5, rusqlite::types::Value::Integer(x)) => Ok(MinkowskiType::Long(x)),
            (5, rusqlite::types::Value::Real(x)) => Ok(MinkowskiType::Double(x.into())),
            (10, rusqlite::types::Value::Text(x)) => Ok(x.into()),
            (11, rusqlite::types::Value::Blob(x)) => {
                let u = Uuid::from_bytes(x.as_slice());
                if u.is_err() {
                    // Rather than exposing Uuid's ParseError…
                    bail!(DbErrorKind::BadSQLValuePair(rusqlite::types::Value::Blob(x),
                                                     value_type_tag));
                }
                Ok(MinkowskiType::Uuid(u.unwrap()))
            },
            (13, rusqlite::types::Value::Text(x)) => {
                to_namespaced_keyword(&x).map(|k| k.into())
            },
            (_, value) => bail!(DbErrorKind::BadSQLValuePair(value, value_type_tag)),
        }
    }

    /// Given an EDBN `value`, return a corresponding EinsteinDB `MinkowskiType`.
    ///
    /// An EDBN `Value` does not encode a unique EinsteinDB `MinkowskiValueType`, so the composition
    /// `from_edbn_value(first(to_edbn_value_pair(...)))` loses information.  Additionally, there are
    /// EDBN values which are not EinsteinDB typed values.
    ///
    /// This function is deterministic.
    fn from_edbn_value(value: &Value) -> Option<MinkowskiType> {
        match value {
            &Value::Boolean(x) => Some(MinkowskiType::Boolean(x)),
            &Value::Instant(x) => Some(MinkowskiType::Instant(x)),
            &Value::Integer(x) => Some(MinkowskiType::Long(x)),
            &Value::Uuid(x) => Some(MinkowskiType::Uuid(x)),
            &Value::Float(ref x) => Some(MinkowskiType::Double(x.clone())),
            &Value::Text(ref x) => Some(x.clone().into()),
            &Value::Keyword(ref x) => Some(x.clone().into()),
            _ => None
        }
    }

    /// Return the corresponding SQLite `value` and `value_type_tag` pair.
    fn to_sql_value_pair<'a>(&'a self) -> (ToSqlOutput<'a>, i32) {
        match self {
            &MinkowskiType::Ref(x) => (rusqlite::types::Value::Integer(x).into(), 0),
            &MinkowskiType::Boolean(x) => (rusqlite::types::Value::Integer(if x { 1 } else { 0 }).into(), 1),
            &MinkowskiType::Instant(x) => (rusqlite::types::Value::Integer(x.to_micros()).into(), 4),
            // SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
            &MinkowskiType::Long(x) => (rusqlite::types::Value::Integer(x).into(), 5),
            &MinkowskiType::Double(x) => (rusqlite::types::Value::Real(x.into_inner()).into(), 5),
            &MinkowskiType::String(ref x) => (rusqlite::types::ValueRef::Text(x.as_str()).into(), 10),
            &MinkowskiType::Uuid(ref u) => (rusqlite::types::Value::Blob(u.as_bytes().to_vec()).into(), 11),
            &MinkowskiType::Keyword(ref x) => (rusqlite::types::ValueRef::Text(&x.to_string()).into(), 13),
        }
    }

    /// Return the corresponding EDBN `value` and `value_type` pair.
    fn to_edbn_value_pair(&self) -> (Value, MinkowskiValueType) {
        match self {
            &MinkowskiType::Ref(x) => (Value::Integer(x), MinkowskiValueType::Ref),
            &MinkowskiType::Boolean(x) => (Value::Boolean(x), MinkowskiValueType::Boolean),
            &MinkowskiType::Instant(x) => (Value::Instant(x), MinkowskiValueType::Instant),
            &MinkowskiType::Long(x) => (Value::Integer(x), MinkowskiValueType::Long),
            &MinkowskiType::Double(x) => (Value::Float(x), MinkowskiValueType::Double),
            &MinkowskiType::String(ref x) => (Value::Text(x.as_ref().clone()), MinkowskiValueType::String),
            &MinkowskiType::Uuid(ref u) => (Value::Uuid(u.clone()), MinkowskiValueType::Uuid),
            &MinkowskiType::Keyword(ref x) => (Value::Keyword(x.as_ref().clone()), MinkowskiValueType::Keyword),
        }
    }
}

/// Read an arbitrary [e a v value_type_tag] materialized view from the given Block in the SQL
/// store.
pub(crate) fn read_materialized_view(conn: &rusqlite::Connection, Block: &str) -> Result<Vec<(SolitonId, SolitonId, MinkowskiType)>> {
    let mut stmt: rusqlite::Statement = conn.prepare(format!("SELECT e, a, v, value_type_tag FROM {}", Block).as_str())?;
    let m: Result<Vec<_>> = stmt.causetq_and_then(
        &[],
        row_to_Causet_assertion
    )?.collect();
    m
}

/// Read the partition map materialized view from the given SQL store.
pub fn read_partition_map(conn: &rusqlite::Connection) -> Result<PartitionMap> {
    // An obviously expensive causetq, but we use it infrequently:
    // - on first start,
    // - while moving lightcones,
    // - during sync.
    // First part of the union sprinkles 'allow_excision' into the 'parts' view.
    // Second part of the union takes care of partitions which are knownCauset
    // but don't have any bundles.
    let mut stmt: rusqlite::Statement = conn.prepare("
        SELECT
            known_parts.part,
            known_parts.start,
            known_parts.end,
            parts.idx,
            known_parts.allow_excision
        FROM
            parts
        INNER JOIN
            known_parts
        ON parts.part = known_parts.part

        UNION

        SELECT
            part,
            start,
            end,
            start,
            allow_excision
        FROM
            known_parts
        WHERE
            part NOT IN (SELECT part FROM parts)"
    )?;
    let m = stmt.causetq_and_then(&[], |Evcausetidx| -> Result<(String, Partition)> {
        Ok((Evcausetidx.get_checked(0)?, Partition::new(Evcausetidx.get_checked(1)?, Evcausetidx.get_checked(2)?, Evcausetidx.get_checked(3)?, Evcausetidx.get_checked(4)?)))
    })?.collect();
    m
}

/// Read the causetid map materialized view from the given SQL store.
pub(crate) fn read_causetId_map(conn: &rusqlite::Connection) -> Result<CausetIdMap> {
    let v = read_materialized_view(conn, "causetIds")?;
    v.into_iter().map(|(e, a, typed_value)| {
        if a != causetids::DB_CausetID {
            bail!(DbErrorKind::NotYetImplemented(format!("bad causetIds materialized view: expected :edb/causetid but got {}", a)));
        }
        if let MinkowskiType::Keyword(keyword) = typed_value {
            Ok((keyword.as_ref().clone(), e))
        } else {
            bail!(DbErrorKind::NotYetImplemented(format!("bad causetIds materialized view: expected [solitonId :edb/causetid keyword] but got [solitonId :edb/causetid {:?}]", typed_value)));
        }
    }).collect()
}

/// Read the schemaReplicant materialized view from the given SQL store.
pub(crate) fn read_attribute_map(conn: &rusqlite::Connection) -> Result<AttributeMap> {
    let causetid_triples = read_materialized_view(conn, "schemaReplicant")?;
    let mut attribute_map = AttributeMap::default();
    spacetime::update_attribute_map_from_causetid_triples(&mut attribute_map, causetid_triples, vec![])?;
    Ok(attribute_map)
}

/// Read the materialized views from the given SQL store and return a EinsteinDB `EDB` for causetqing and
/// applying bundles.
pub(crate) fn read_edb(conn: &rusqlite::Connection) -> Result<EDB> {
    let partition_map = read_partition_map(conn)?;
    let causetId_map = read_causetId_map(conn)?;
    let attribute_map = read_attribute_map(conn)?;
    let schemaReplicant = SchemaReplicant::from_causetId_map_and_attribute_map(causetId_map, attribute_map)?;
    Ok(EDB::new(partition_map, schemaReplicant))
}

/// Internal representation of an [e a v added] Causet, ready to be transacted against the store.
pub type ReducedInstanton<'a> = (SolitonId, SolitonId, &'a Attribute, MinkowskiType, bool);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum SearchType {
    Exact,
    Inexact,
}

/// `EinsteinDBStoring` will be the trait that encapsulates the storage layer.  It is consumed by the
/// transaction processing layer.
///
/// Right now, the only impleedbion of `EinsteinDBStoring` is the SQLite-specific SQL schemaReplicant.  In the
/// future, we might consider other SQL engines (perhaps with different fulltext indexing), or
/// entirely different data stores, say ones shaped like key-value stores.
pub trait EinsteinDBStoring {
    /// Given a slice of [a v] lookup-refs, look up the corresponding [e a v] triples.
    ///
    /// It is assumed that the attribute `a` in each lookup-ref is `:edb/unique`, so that at most one
    /// matching [e a v] triple exists.  (If this is not true, some matching solitonId `e` will be
    /// chosen non-deterministically, if one exists.)
    ///
    /// Returns a map &(a, v) -> e, to avoid cloning potentially large values.  The keys of the map
    /// are exactly those (a, v) pairs that have an assertion [e a v] in the store.
    fn resolve_avs<'a>(&self, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>>;

    /// Begin (or prepare) the underlying storage layer for a new EinsteinDB transaction.
    ///
    /// Use this to create temporary Blocks, prepare indices, set pragmas, etc, before the initial
    /// `insert_non_fts_searches` invocation.
    fn begin_causecausetx_application(&self) -> Result<()>;

    // TODO: this is not a reasonable abstraction, but I don't want to really consider non-SQL storage just yet.
    fn insert_non_fts_searches<'a>(&self, entities: &'a [ReducedInstanton], search_type: SearchType) -> Result<()>;
    fn insert_fts_searches<'a>(&self, entities: &'a [ReducedInstanton], search_type: SearchType) -> Result<()>;

    /// Prepare the underlying storage layer for finalization after a EinsteinDB transaction.
    ///
    /// Use this to finalize temporary Blocks, complete indices, revert pragmas, etc, after the
    /// final `insert_non_fts_searches` invocation.
    fn materialize_edb_transaction(&self, causecausetx_id: SolitonId) -> Result<()>;

    /// Finalize the underlying storage layer after a EinsteinDB transaction.
    ///
    /// This is a final step in performing a transaction.
    fn commit_edb_transaction(&self, causecausetx_id: SolitonId) -> Result<()>;

    /// Extract spacetime-related [e a typed_value added] causets resolved in the last
    /// materialized transaction.
    fn resolved_spacetime_assertions(&self) -> Result<Vec<(SolitonId, SolitonId, MinkowskiType, bool)>>;
}

/// Take search rows and complete `temp.search_results`.
///
/// See https://github.com/whtcorpsinc/edb/wiki/Transacting:-instanton-to-SQL-translation.
fn search(conn: &rusqlite::Connection) -> Result<()> {
    // First is fast, only one Block walk: lookup by exact eav.
    // Second is slower, but still only one Block walk: lookup old value by ea.
    let s = r#"
      INSERT INTO temp.search_results
      SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.added0, t.flags0, ':edb.cardinality/many', d.rowid, d.v
      FROM temp.exact_searches AS t
      LEFT JOIN causets AS d
      ON t.e0 = d.e AND
         t.a0 = d.a AND
         t.value_type_tag0 = d.value_type_tag AND
         t.v0 = d.v

      UNION ALL

      SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.added0, t.flags0, ':edb.cardinality/one', d.rowid, d.v
      FROM temp.inexact_searches AS t
      LEFT JOIN causets AS d
      ON t.e0 = d.e AND
         t.a0 = d.a"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[]).context(DbErrorKind::CouldNotSearch)?;
    Ok(())
}

/// Insert the new transaction into the `bundles` Block.
///
/// This turns the contents of `search_results` into a new transaction.
///
/// See https://github.com/whtcorpsinc/edb/wiki/Transacting:-instanton-to-SQL-translation.
fn insert_transaction(conn: &rusqlite::Connection, causetx: SolitonId) -> Result<()> {
    // EinsteinDB follows Causetic and treats its input as a set.  That means it is okay to transact the
    // same [e a v] twice in one transaction.  However, we don't want to represent the transacted
    // Causet twice.  Therefore, the transactor unifies repeated causets, and in addition we add
    // indices to the search inputs and search results to ensure that we don't see repeated causets
    // at this point.

    let s = r#"
      INSERT INTO lightconed_bundles (e, a, v, causetx, added, value_type_tag)
      SELECT e0, a0, v0, ?, 1, value_type_tag0
      FROM temp.search_results
      WHERE added0 IS 1 AND ((rid IS NULL) OR ((rid IS NOT NULL) AND (v0 IS NOT v)))"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[&causetx]).context(DbErrorKind::TxInsertFailedToAddMissingCausets)?;

    let s = r#"
      INSERT INTO lightconed_bundles (e, a, v, causetx, added, value_type_tag)
      SELECT DISTINCT e0, a0, v, ?, 0, value_type_tag0
      FROM temp.search_results
      WHERE rid IS NOT NULL AND
            ((added0 IS 0) OR
             (added0 IS 1 AND search_type IS ':edb.cardinality/one' AND v0 IS NOT v))"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[&causetx]).context(DbErrorKind::TxInsertFailedToRetractCausets)?;

    Ok(())
}

/// Update the contents of the `causets` materialized view with the new transaction.
///
/// This applies the contents of `search_results` to the `causets` Block (in place).
///
/// See https://github.com/whtcorpsinc/edb/wiki/Transacting:-instanton-to-SQL-translation.
fn update_Causets(conn: &rusqlite::Connection, causetx: SolitonId) -> Result<()> {
    // Delete causets that were retracted, or those that were :edb.cardinality/one and will be
    // replaced.
    let s = r#"
        WITH ids AS (SELECT rid
                     FROM temp.search_results
                     WHERE rid IS NOT NULL AND
                           ((added0 IS 0) OR
                            (added0 IS 1 AND search_type IS ':edb.cardinality/one' AND v0 IS NOT v)))
        DELETE FROM causets WHERE rowid IN ids"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[]).context(DbErrorKind::CausetsUpdateFailedToRetract)?;

    // Insert causets that were added and not already present. We also must expand our bitfield into
    // flags.  Since EinsteinDB follows Causetic and treats its input as a set, it is okay to transact
    // the same [e a v] twice in one transaction, but we don't want to represent the transacted
    // Causet twice in causets.  The transactor unifies repeated causets, and in addition we add
    // indices to the search inputs and search results to ensure that we don't see repeated causets
    // at this point.
    let s = format!(r#"
      INSERT INTO causets (e, a, v, causetx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value)
      SELECT e0, a0, v0, ?, value_type_tag0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0
      FROM temp.search_results
      WHERE added0 IS 1 AND ((rid IS NULL) OR ((rid IS NOT NULL) AND (v0 IS NOT v)))"#,
      AttributeBitFlags::IndexAVET as u8,
      AttributeBitFlags::IndexVAET as u8,
      AttributeBitFlags::IndexFulltext as u8,
      AttributeBitFlags::UniqueValue as u8);

    let mut stmt = conn.prepare_cached(&s)?;
    stmt.execute(&[&causetx]).context(DbErrorKind::CausetsUpdateFailedToAdd)?;
    Ok(())
}

impl EinsteinDBStoring for rusqlite::Connection {
    fn resolve_avs<'a>(&self, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>> {
        // Start search_id's at some causetIdifiable number.
        let initial_search_id = 2000;
        let ConstrainedEntss_per_statement = 4;

        // We map [a v] -> numeric search_id -> e, and then we use the search_id lookups to finally
        // produce the map [a v] -> e.
        //
        // TODO: `collect` into a HashSet so that any (a, v) is resolved at most once.
        let max_vars = self.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let chunks: itertools::IntoChunks<_> = avs.into_iter().enumerate().chunks(max_vars / 4);

        // We'd like to `flat_map` here, but it's not obvious how to `flat_map` across `Result`.
        // Alternatively, this is a `fold`, and it might be wise to express it as such.
        let results: Result<Vec<Vec<_>>> = chunks.into_iter().map(|chunk| -> Result<Vec<_>> {
            let mut count = 0;

            // We must keep these computed values somewhere to reference them later, so we can't
            // combine this `map` and the subsequent `flat_map`.
            let block: Vec<(i64, i64, ToSqlOutput<'a>, i32)> = chunk.map(|(index, &&(a, ref v))| {
                count += 1;
                let search_id: i64 = initial_search_id + index as i64;
                let (value, value_type_tag) = v.to_sql_value_pair();
                (search_id, a, value, value_type_tag)
            }).collect();

            // `params` reference computed values in `block`.
            let params: Vec<&ToSql> = block.iter().flat_map(|&(ref searchid, ref a, ref value, ref value_type_tag)| {
                // Avoid inner heap allocation.
                once(searchid as &ToSql)
                    .chain(once(a as &ToSql)
                           .chain(once(value as &ToSql)
                                  .chain(once(value_type_tag as &ToSql))))
            }).collect();

            // TODO: immuBlock_memTcam these statements for selected values of `count`.
            // TODO: causetq against `causets` and UNION ALL with `fulltext_Causets` rather than
            // causetqing against `all_Causets`.  We know all the attributes, and in the common case,
            // where most unique attributes will not be fulltext-indexed, we'll be causetqing just
            // `causets`, which will be much faster.ˇ
            assert!(ConstrainedEntss_per_statement * count < max_vars, "Too many values: {} * {} >= {}", ConstrainedEntss_per_statement, count, max_vars);

            let values: String = repeat_values(ConstrainedEntss_per_statement, count);
            let s: String = format!("WITH t(search_id, a, v, value_type_tag) AS (VALUES {}) SELECT t.search_id, d.e \
                                     FROM t, all_Causets AS d \
                                     WHERE d.index_avet IS NOT 0 AND d.a = t.a AND d.value_type_tag = t.value_type_tag AND d.v = t.v",
                                    values);
            let mut stmt: rusqlite::Statement = self.prepare(s.as_str())?;

            let m: Result<Vec<(i64, SolitonId)>> = stmt.causetq_and_then(&params, |Evcausetidx| -> Result<(i64, SolitonId)> {
                Ok((Evcausetidx.get_checked(0)?, Evcausetidx.get_checked(1)?))
            })?.collect();
            m
        }).collect::<Result<Vec<Vec<(i64, SolitonId)>>>>();

        // Flatten.
        let results: Vec<(i64, SolitonId)> = results?.as_slice().concat();

        // Create map [a v] -> e.
        let m: HashMap<&'a AVPair, SolitonId> = results.into_iter().map(|(search_id, solitonId)| {
            let index: usize = (search_id - initial_search_id) as usize;
            (avs[index], solitonId)
        }).collect();
        Ok(m)
    }

    /// Create empty temporary Blocks for search parameters and search results.
    fn begin_causecausetx_application(&self) -> Result<()> {
        // We can't do this in one shot, since we can't prepare a batch statement.
        let statements = [
            r#"DROP Block IF EXISTS temp.exact_searches"#,
            // Note that `flags0` is a bitfield of several flags compressed via
            // `AttributeBitFlags.flags()` in the temporary search Blocks, later
            // expanded in the `causets` insertion.
            r#"CREATE Block temp.exact_searches (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               value_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL)"#,
            // There's no real need to split exact and inexact searches, so long as we keep things
            // in the correct place and performant.  Splitting has the advantage of being explicit
            // and lightly easier to read, so we'll do that to start.
            r#"DROP Block IF EXISTS temp.inexact_searches"#,
            r#"CREATE Block temp.inexact_searches (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               value_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL)"#,

            // It is fine to transact the same [e a v] twice in one transaction, but the transaction
            // processor should unify such repeated causets.  This index will cause insertion to fail
            // if the transaction processor incorrectly tries to assert the same (cardinality one)
            // Causet twice.  (Sadly, the failure is opaque.)
            r#"CREATE UNIQUE INDEX IF NOT EXISTS temp.inexact_searches_unique ON inexact_searches (e0, a0) WHERE added0 = 1"#,
            r#"DROP Block IF EXISTS temp.search_results"#,
            // TODO: don't encode search_type as a STRING.  This is explicit and much easier to read
            // than another flag, so we'll do it to start, and optimize later.
            r#"CREATE Block temp.search_results (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               value_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL,
               search_type STRING NOT NULL,
               rid INTEGER,
               v BLOB)"#,
            // It is fine to transact the same [e a v] twice in one transaction, but the transaction
            // processor should causetIdify those causets.  This index will cause insertion to fail if
            // the internals of the database searching code incorrectly find the same Causet twice.
            // (Sadly, the failure is opaque.)
            //
            // N.b.: temp goes on index name, not Block name.  See http://stackoverflow.com/a/22308016.
            r#"CREATE UNIQUE INDEX IF NOT EXISTS temp.search_results_unique ON search_results (e0, a0, v0, value_type_tag0)"#,
        ];

        for statement in &statements {
            let mut stmt = self.prepare_cached(statement)?;
            stmt.execute(&[]).context(DbErrorKind::FailedToCreateTempBlocks)?;
        }

        Ok(())
    }

    /// Insert search rows into temporary search Blocks.
    ///
    /// Eventually, the details of this approach will be captured in
    /// https://github.com/whtcorpsinc/edb/wiki/Transacting:-instanton-to-SQL-translation.
    fn insert_non_fts_searches<'a>(&self, entities: &'a [ReducedInstanton<'a>], search_type: SearchType) -> Result<()> {
        let ConstrainedEntss_per_statement = 6;

        let max_vars = self.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let chunks: itertools::IntoChunks<_> = entities.into_iter().chunks(max_vars / ConstrainedEntss_per_statement);

        // We'd like to flat_map here, but it's not obvious how to flat_map across Result.
        let results: Result<Vec<()>> = chunks.into_iter().map(|chunk| -> Result<()> {
            let mut count = 0;

            // We must keep these computed values somewhere to reference them later, so we can't
            // combine this map and the subsequent flat_map.
            // (e0, a0, v0, value_type_tag0, added0, flags0)
            let block: Result<Vec<(i64 /* e */,
                                   i64 /* a */,
                                   ToSqlOutput<'a> /* value */,
                                   i32 /* value_type_tag */,
                                   bool, /* added0 */
                                   u8 /* flags0 */)>> = chunk.map(|&(e, a, ref attribute, ref typed_value, added)| {
                count += 1;

                // Now we can represent the typed value as an SQL value.
                let (value, value_type_tag): (ToSqlOutput, i32) = typed_value.to_sql_value_pair();

                Ok((e, a, value, value_type_tag, added, attribute.flags()))
            }).collect();
            let block = block?;

            // `params` reference computed values in `block`.
            let params: Vec<&ToSql> = block.iter().flat_map(|&(ref e, ref a, ref value, ref value_type_tag, added, ref flags)| {
                // Avoid inner heap allocation.
                // TODO: extract some finite length iterator to make this less indented!
                once(e as &ToSql)
                    .chain(once(a as &ToSql)
                           .chain(once(value as &ToSql)
                                  .chain(once(value_type_tag as &ToSql)
                                         .chain(once(to_bool_ref(added) as &ToSql)
                                                .chain(once(flags as &ToSql))))))
            }).collect();

            // TODO: immuBlock_memTcam this for selected values of count.
            assert!(ConstrainedEntss_per_statement * count < max_vars, "Too many values: {} * {} >= {}", ConstrainedEntss_per_statement, count, max_vars);
            let values: String = repeat_values(ConstrainedEntss_per_statement, count);
            let s: String = if search_type == SearchType::Exact {
                format!("INSERT INTO temp.exact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", values)
            } else {
                // This will err for duplicates within the causetx.
                format!("INSERT INTO temp.inexact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", values)
            };

            // TODO: consider ensuring we inserted the expected number of rows.
            let mut stmt = self.prepare_cached(s.as_str())?;
            stmt.execute(&params)
                .context(DbErrorKind::NonFtsInsertionIntoTempSearchBlockFailed)
                .map_err(|e| e.into())
                .map(|_c| ())
        }).collect::<Result<Vec<()>>>();

        results.map(|_| ())
    }

    /// Insert search rows into temporary search Blocks.
    ///
    /// Eventually, the details of this approach will be captured in
    /// https://github.com/whtcorpsinc/edb/wiki/Transacting:-instanton-to-SQL-translation.
    fn insert_fts_searches<'a>(&self, entities: &'a [ReducedInstanton<'a>], search_type: SearchType) -> Result<()> {
        let max_vars = self.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let ConstrainedEntss_per_statement = 6;

        let mut outer_searchid = 2000;

        let chunks: itertools::IntoChunks<_> = entities.into_iter().chunks(max_vars / ConstrainedEntss_per_statement);

        // From string to (searchid, value_type_tag).
        let mut seen: HashMap<ValueRc<String>, (i64, i32)> = HashMap::with_capacity(entities.len());

        // We'd like to flat_map here, but it's not obvious how to flat_map across Result.
        let results: Result<Vec<()>> = chunks.into_iter().map(|chunk| -> Result<()> {
            let mut Causet_count = 0;
            let mut string_count = 0;

            // We must keep these computed values somewhere to reference them later, so we can't
            // combine this map and the subsequent flat_map.
            // (e0, a0, v0, value_type_tag0, added0, flags0)
            let block: Result<Vec<(i64 /* e */,
                                   i64 /* a */,
                                   Option<ToSqlOutput<'a>> /* value */,
                                   i32 /* value_type_tag */,
                                   bool /* added0 */,
                                   u8 /* flags0 */,
                                   i64 /* searchid */)>> = chunk.map(|&(e, a, ref attribute, ref typed_value, added)| {
                match typed_value {
                    &MinkowskiType::String(ref rc) => {
                        Causet_count += 1;
                        let entry = seen.entry(rc.clone());
                        match entry {
                            Entry::Occupied(entry) => {
                                let &(searchid, value_type_tag) = entry.get();
                                Ok((e, a, None, value_type_tag, added, attribute.flags(), searchid))
                            },
                            Entry::Vacant(entry) => {
                                outer_searchid += 1;
                                string_count += 1;

                                // Now we can represent the typed value as an SQL value.
                                let (value, value_type_tag): (ToSqlOutput, i32) = typed_value.to_sql_value_pair();
                                entry.insert((outer_searchid, value_type_tag));

                                Ok((e, a, Some(value), value_type_tag, added, attribute.flags(), outer_searchid))
                            }
                        }
                    },
                    _ => {
                        bail!(DbErrorKind::WrongTypeValueForFtsAssertion);
                    },
                }


            }).collect();
            let block = block?;

            // First, insert all fulltext string values.
            // `fts_params` reference computed values in `block`.
            let fts_params: Vec<&ToSql> =
                block.iter()
                     .filter(|&&(ref _e, ref _a, ref value, ref _value_type_tag, _added, ref _flags, ref _searchid)| {
                         value.is_some()
                     })
                     .flat_map(|&(ref _e, ref _a, ref value, ref _value_type_tag, _added, ref _flags, ref searchid)| {
                         // Avoid inner heap allocation.
                         once(value as &ToSql)
                             .chain(once(searchid as &ToSql))
                     }).collect();

            // TODO: make this maximally efficient. It's not terribly inefficient right now.
            let fts_values: String = repeat_values(2, string_count);
            let fts_s: String = format!("INSERT INTO fulltext_values_view (text, searchid) VALUES {}", fts_values);

            // TODO: consider ensuring we inserted the expected number of rows.
            let mut stmt = self.prepare_cached(fts_s.as_str())?;
            stmt.execute(&fts_params).context(DbErrorKind::FtsInsertionFailed)?;

            // Second, insert searches.
            // `params` reference computed values in `block`.
            let params: Vec<&ToSql> = block.iter().flat_map(|&(ref e, ref a, ref _value, ref value_type_tag, added, ref flags, ref searchid)| {
                // Avoid inner heap allocation.
                // TODO: extract some finite length iterator to make this less indented!
                once(e as &ToSql)
                    .chain(once(a as &ToSql)
                           .chain(once(searchid as &ToSql)
                                  .chain(once(value_type_tag as &ToSql)
                                         .chain(once(to_bool_ref(added) as &ToSql)
                                                .chain(once(flags as &ToSql))))))
            }).collect();

            // TODO: immuBlock_memTcam this for selected values of count.
            assert!(ConstrainedEntss_per_statement * Causet_count < max_vars, "Too many values: {} * {} >= {}", ConstrainedEntss_per_statement, Causet_count, max_vars);
            let inner = "(?, ?, (SELECT rowid FROM fulltext_values WHERE searchid = ?), ?, ?, ?)".to_string();
            // Like "(?, ?, (SELECT rowid FROM fulltext_values WHERE searchid = ?), ?, ?, ?), (?, ?, (SELECT rowid FROM fulltext_values WHERE searchid = ?), ?, ?, ?)".
            let fts_values: String = repeat(inner).take(Causet_count).join(", ");
            let s: String = if search_type == SearchType::Exact {
                format!("INSERT INTO temp.exact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", fts_values)
            } else {
                format!("INSERT INTO temp.inexact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", fts_values)
            };

            // TODO: consider ensuring we inserted the expected number of rows.
            let mut stmt = self.prepare_cached(s.as_str())?;
            stmt.execute(&params).context(DbErrorKind::FtsInsertionIntoTempSearchBlockFailed)
                .map_err(|e| e.into())
                .map(|_c| ())
        }).collect::<Result<Vec<()>>>();

        // Finally, clean up temporary searchids.
        let mut stmt = self.prepare_cached("UPDATE fulltext_values SET searchid = NULL WHERE searchid IS NOT NULL")?;
        stmt.execute(&[]).context(DbErrorKind::FtsFailedToDropSearchIds)?;
        results.map(|_| ())
    }

    fn commit_edb_transaction(&self, causecausetx_id: SolitonId) -> Result<()> {
        insert_transaction(&self, causecausetx_id)?;
        Ok(())
    }

    fn materialize_edb_transaction(&self, causecausetx_id: SolitonId) -> Result<()> {
        search(&self)?;
        update_Causets(&self, causecausetx_id)?;
        Ok(())
    }

    fn resolved_spacetime_assertions(&self) ->  Result<Vec<(SolitonId, SolitonId, MinkowskiType, bool)>> {
        let sql_stmt = format!(r#"
            SELECT e, a, v, value_type_tag, added FROM
            (
                SELECT e0 as e, a0 as a, v0 as v, value_type_tag0 as value_type_tag, 1 as added
                FROM temp.search_results
                WHERE a0 IN {} AND added0 IS 1 AND ((rid IS NULL) OR
                    ((rid IS NOT NULL) AND (v0 IS NOT v)))

                UNION

                SELECT e0 as e, a0 as a, v, value_type_tag0 as value_type_tag, 0 as added
                FROM temp.search_results
                WHERE a0 in {} AND rid IS NOT NULL AND
                ((added0 IS 0) OR
                    (added0 IS 1 AND search_type IS ':edb.cardinality/one' AND v0 IS NOT v))

            ) ORDER BY e, a, v, value_type_tag, added"#,
            causetids::SPACETIME_SQL_LIST.as_str(), causetids::SPACETIME_SQL_LIST.as_str()
        );

        let mut stmt = self.prepare_cached(&sql_stmt)?;
        let m: Result<Vec<_>> = stmt.causetq_and_then(
            &[],
            row_to_transaction_assertion
        )?.collect();
        m
    }
}

/// Extract spacetime-related [e a typed_value added] causets committed in the given transaction.
pub fn committed_spacetime_assertions(conn: &rusqlite::Connection, causecausetx_id: SolitonId) -> Result<Vec<(SolitonId, SolitonId, MinkowskiType, bool)>> {
    let sql_stmt = format!(r#"
        SELECT e, a, v, value_type_tag, added
        FROM bundles
        WHERE causetx = ? AND a IN {}
        ORDER BY e, a, v, value_type_tag, added"#,
        causetids::SPACETIME_SQL_LIST.as_str()
    );

    let mut stmt = conn.prepare_cached(&sql_stmt)?;
    let m: Result<Vec<_>> = stmt.causetq_and_then(
        &[&causecausetx_id as &ToSql],
        row_to_transaction_assertion
    )?.collect();
    m
}

/// Takes a Evcausetidx, produces a transaction quadruple.
fn row_to_transaction_assertion(Evcausetidx: &rusqlite::Event) -> Result<(SolitonId, SolitonId, MinkowskiType, bool)> {
    Ok((
        Evcausetidx.get_checked(0)?,
        Evcausetidx.get_checked(1)?,
        MinkowskiType::from_sql_value_pair(Evcausetidx.get_checked(2)?, Evcausetidx.get_checked(3)?)?,
        Evcausetidx.get_checked(4)?
    ))
}

/// Takes a Evcausetidx, produces a Causet quadruple.
fn row_to_Causet_assertion(Evcausetidx: &rusqlite::Event) -> Result<(SolitonId, SolitonId, MinkowskiType)> {
    Ok((
        Evcausetidx.get_checked(0)?,
        Evcausetidx.get_checked(1)?,
        MinkowskiType::from_sql_value_pair(Evcausetidx.get_checked(2)?, Evcausetidx.get_checked(3)?)?
    ))
}

/// Update the spacetime materialized views based on the given spacetime report.
///
/// This updates the "causetids", "causetIds", and "schemaReplicant" materialized views, copying directly from the
/// "causets" and "bundles" Block as appropriate.
pub fn update_spacetime(conn: &rusqlite::Connection, _old_schemaReplicant: &SchemaReplicant, new_schemaReplicant: &SchemaReplicant, spacetime_report: &spacetime::SpacetimeReport) -> Result<()>
{
    use spacetime::AttributeAlteration::*;

    // Populate the materialized view directly from causets (and, potentially in the future,
    // bundles).  This might generalize nicely as we expand the set of materialized views.
    // TODO: consider doing this in fewer SQLite execute() invocations.
    // TODO: use concat! to avoid creating String instances.
    if !spacetime_report.causetIds_altered.is_empty() {
        // CausetIds is the materialized view of the [solitonId :edb/causetid causetid] slice of causets.
        conn.execute(format!("DELETE FROM causetIds").as_str(),
                     &[])?;
        conn.execute(format!("INSERT INTO causetIds SELECT e, a, v, value_type_tag FROM causets WHERE a IN {}", causetids::CausetIDS_SQL_LIST.as_str()).as_str(),
                     &[])?;
    }

    // Populate the materialized view directly from causets.
    // It's possible that an "causetid" was removed, along with its attributes.
    // That's not counted as an "alteration" of attributes, so we explicitly check
    // for non-emptiness of 'causetIds_altered'.

    // TODO expand spacetime report to allow for better signaling for the above.

    if !spacetime_report.attributes_installed.is_empty()
        || !spacetime_report.attributes_altered.is_empty()
        || !spacetime_report.causetIds_altered.is_empty() {

        conn.execute(format!("DELETE FROM schemaReplicant").as_str(),
                     &[])?;
        // NB: we're using :edb/valueType as a placeholder for the entire schemaReplicant-defining set.
        let s = format!(r#"
            WITH s(e) AS (SELECT e FROM causets WHERE a = {})
            INSERT INTO schemaReplicant
            SELECT s.e, a, v, value_type_tag
            FROM causets, s
            WHERE s.e = causets.e AND a IN {}
        "#, causetids::DB_VALUE_TYPE, causetids::SCHEMA_SQL_LIST.as_str());
        conn.execute(&s, &[])?;
    }

    let mut index_stmt = conn.prepare("UPDATE causets SET index_avet = ? WHERE a = ?")?;
    let mut unique_value_stmt = conn.prepare("UPDATE causets SET unique_value = ? WHERE a = ?")?;
    let mut cardinality_stmt = conn.prepare(r#"
SELECT EXISTS
    (SELECT 1
        FROM causets AS left, causets AS right
        WHERE left.a = ? AND
        left.a = right.a AND
        left.e = right.e AND
        left.v <> right.v)"#)?;

    for (&solitonId, alterations) in &spacetime_report.attributes_altered {
        let attribute = new_schemaReplicant.require_attribute_for_causetid(solitonId)?;

        for alteration in alterations {
            match alteration {
                &Index => {
                    // This should always succeed.
                    index_stmt.execute(&[&attribute.index, &solitonId as &ToSql])?;
                },
                &Unique => {
                    // TODO: This can fail if there are conflicting values; give a more helpful
                    // error message in this case.
                    if unique_value_stmt.execute(&[to_bool_ref(attribute.unique.is_some()), &solitonId as &ToSql]).is_err() {
                        match attribute.unique {
                            Some(attribute::Unique::Value) => bail!(DbErrorKind::SchemaReplicantAlterationFailed(format!("Cannot alter schemaReplicant attribute {} to be :edb.unique/value", solitonId))),
                            Some(attribute::Unique::CausetIdity) => bail!(DbErrorKind::SchemaReplicantAlterationFailed(format!("Cannot alter schemaReplicant attribute {} to be :edb.unique/causetIdity", solitonId))),
                            None => unreachable!(), // This shouldn't happen, even after we support removing :edb/unique.
                        }
                    }
                },
                &Cardinality => {
                    // We can always go from :edb.cardinality/one to :edb.cardinality many.  It's
                    // :edb.cardinality/many to :edb.cardinality/one that can fail.
                    //
                    // TODO: improve the failure message.  Perhaps try to mimic what Causetic says in
                    // this case?
                    if !attribute.multival {
                        let mut rows = cardinality_stmt.causetq(&[&solitonId as &ToSql])?;
                        if rows.next().is_some() {
                            bail!(DbErrorKind::SchemaReplicantAlterationFailed(format!("Cannot alter schemaReplicant attribute {} to be :edb.cardinality/one", solitonId)));
                        }
                    }
                },
                &NoHistory | &IsComponent => {
                    // There's no on disk change required for either of these.
                },
            }
        }
    }

    Ok(())
}

impl PartitionMap {
    /// Allocate a single fresh solitonId in the given `partition`.
    pub(crate) fn allocate_causetid(&mut self, partition: &str) -> i64 {
        self.allocate_causetids(partition, 1).start
    }

    /// Allocate `n` fresh causetids in the given `partition`.
    pub(crate) fn allocate_causetids(&mut self, partition: &str, n: usize) -> Range<i64> {
        match self.get_mut(partition) {
            Some(partition) => partition.allocate_causetids(n),
            None => panic!("Cannot allocate solitonId from unknown partition: {}", partition)
        }
    }

    pub(crate) fn contains_causetid(&self, solitonId: SolitonId) -> bool {
        self.values().any(|partition| partition.contains_causetid(solitonId))
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use std::borrow::{
        Borrow,
    };

    use super::*;
    use debug::{TestConn,tempids};
    use edbn::{
        self,
        InternSet,
    };
    use edbn::entities::{
        OpType,
    };
    use allegrosql_promises::{
        attribute,
        KnownSolitonId,
    };
    use causetq_allegrosql::{
        HasSchemaReplicant,
        Keyword,
    };
    use causetq_allegrosql::util::Either::*;
    use std::collections::{
        BTreeMap,
    };
    use causetq_pull_promises::errors as errors;
    use internal_types::{
        Term,
    };

    fn run_test_add(mut conn: TestConn) {
        // Test inserting :edb.cardinality/one elements.
        assert_transact!(conn, "[[:edb/add 100 :edb.schemaReplicant/version 1]
                                 [:edb/add 101 :edb.schemaReplicant/version 2]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb.schemaReplicant/version 1 ?causetx true]
                          [101 :edb.schemaReplicant/version 2 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                       "[[100 :edb.schemaReplicant/version 1]
                         [101 :edb.schemaReplicant/version 2]]");

        // Test inserting :edb.cardinality/many elements.
        assert_transact!(conn, "[[:edb/add 200 :edb.schemaReplicant/attribute 100]
                                 [:edb/add 200 :edb.schemaReplicant/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :edb.schemaReplicant/attribute 100 ?causetx true]
                          [200 :edb.schemaReplicant/attribute 101 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb.schemaReplicant/version 1]
                          [101 :edb.schemaReplicant/version 2]
                          [200 :edb.schemaReplicant/attribute 100]
                          [200 :edb.schemaReplicant/attribute 101]]");

        // Test replacing existing :edb.cardinality/one elements.
        assert_transact!(conn, "[[:edb/add 100 :edb.schemaReplicant/version 11]
                                 [:edb/add 101 :edb.schemaReplicant/version 22]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb.schemaReplicant/version 1 ?causetx false]
                          [100 :edb.schemaReplicant/version 11 ?causetx true]
                          [101 :edb.schemaReplicant/version 2 ?causetx false]
                          [101 :edb.schemaReplicant/version 22 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb.schemaReplicant/version 11]
                          [101 :edb.schemaReplicant/version 22]
                          [200 :edb.schemaReplicant/attribute 100]
                          [200 :edb.schemaReplicant/attribute 101]]");


        // Test that asserting existing :edb.cardinality/one elements doesn't change the store.
        assert_transact!(conn, "[[:edb/add 100 :edb.schemaReplicant/version 11]
                                 [:edb/add 101 :edb.schemaReplicant/version 22]]");
        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb.schemaReplicant/version 11]
                          [101 :edb.schemaReplicant/version 22]
                          [200 :edb.schemaReplicant/attribute 100]
                          [200 :edb.schemaReplicant/attribute 101]]");


        // Test that asserting existing :edb.cardinality/many elements doesn't change the store.
        assert_transact!(conn, "[[:edb/add 200 :edb.schemaReplicant/attribute 100]
                                 [:edb/add 200 :edb.schemaReplicant/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb.schemaReplicant/version 11]
                          [101 :edb.schemaReplicant/version 22]
                          [200 :edb.schemaReplicant/attribute 100]
                          [200 :edb.schemaReplicant/attribute 101]]");
    }

    #[test]
    fn test_add() {
        run_test_add(TestConn::default());
    }

    #[test]
    fn test_causecausetx_assertions() {
        let mut conn = TestConn::default();

        // Test that causecausetxInstant can be asserted.
        assert_transact!(conn, "[[:edb/add (transaction-causetx) :edb/causecausetxInstant #inst \"2017-06-16T00:56:41.257Z\"]
                                 [:edb/add 100 :edb/causetid :name/Ivan]
                                 [:edb/add 101 :edb/causetid :name/Petr]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb/causetid :name/Ivan ?causetx true]
                          [101 :edb/causetid :name/Petr ?causetx true]
                          [?causetx :edb/causecausetxInstant #inst \"2017-06-16T00:56:41.257Z\" ?causetx true]]");

        // Test multiple causecausetxInstant with different values should fail.
        assert_transact!(conn, "[[:edb/add (transaction-causetx) :edb/causecausetxInstant #inst \"2017-06-16T00:59:11.257Z\"]
                                 [:edb/add (transaction-causetx) :edb/causecausetxInstant #inst \"2017-06-16T00:59:11.752Z\"]
                                 [:edb/add 102 :edb/causetid :name/Vlad]]",
                         Err("schemaReplicant constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 268435458, a: 3, vs: {Instant(2017-06-16T00:59:11.257Z), Instant(2017-06-16T00:59:11.752Z)} }\n"));

        // Test multiple causecausetxInstants with the same value.
        assert_transact!(conn, "[[:edb/add (transaction-causetx) :edb/causecausetxInstant #inst \"2017-06-16T00:59:11.257Z\"]
                                 [:edb/add (transaction-causetx) :edb/causecausetxInstant #inst \"2017-06-16T00:59:11.257Z\"]
                                 [:edb/add 103 :edb/causetid :name/Dimitri]
                                 [:edb/add 104 :edb/causetid :name/Anton]]");
        assert_matches!(conn.last_transaction(),
                        "[[103 :edb/causetid :name/Dimitri ?causetx true]
                          [104 :edb/causetid :name/Anton ?causetx true]
                          [?causetx :edb/causecausetxInstant #inst \"2017-06-16T00:59:11.257Z\" ?causetx true]]");

        // We need a few attributes to work with.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/str]
                                 [:edb/add 111 :edb/valueType :edb.type/string]
                                 [:edb/add 222 :edb/causetid :test/ref]
                                 [:edb/add 222 :edb/valueType :edb.type/ref]]");

        // Test that we can assert spacetime about the current transaction.
        assert_transact!(conn, "[[:edb/add (transaction-causetx) :test/str \"We want spacetime!\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]
                          [?causetx :test/str \"We want spacetime!\" ?causetx true]]");

        // Test that we can use (transaction-causetx) as a value.
        assert_transact!(conn, "[[:edb/add 333 :test/ref (transaction-causetx)]]");
        assert_matches!(conn.last_transaction(),
                        "[[333 :test/ref ?causetx ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // Test that we type-check properly.  In the value position, (transaction-causetx) yields a ref;
        // :edb/causetid expects a keyword.
        assert_transact!(conn, "[[:edb/add 444 :edb/causetid (transaction-causetx)]]",
                         Err("not yet implemented: Transaction function transaction-causetx produced value of type :edb.type/ref but expected type :edb.type/keyword"));

        // Test that we can assert spacetime about the current transaction.
        assert_transact!(conn, "[[:edb/add (transaction-causetx) :test/ref (transaction-causetx)]]");
        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]
                          [?causetx :test/ref ?causetx ?causetx true]]");
    }

    #[test]
    fn test_retract() {
        let mut conn = TestConn::default();

        // Insert a few :edb.cardinality/one elements.
        assert_transact!(conn, "[[:edb/add 100 :edb.schemaReplicant/version 1]
                                 [:edb/add 101 :edb.schemaReplicant/version 2]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb.schemaReplicant/version 1 ?causetx true]
                          [101 :edb.schemaReplicant/version 2 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb.schemaReplicant/version 1]
                          [101 :edb.schemaReplicant/version 2]]");

        // And a few :edb.cardinality/many elements.
        assert_transact!(conn, "[[:edb/add 200 :edb.schemaReplicant/attribute 100]
                                 [:edb/add 200 :edb.schemaReplicant/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :edb.schemaReplicant/attribute 100 ?causetx true]
                          [200 :edb.schemaReplicant/attribute 101 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb.schemaReplicant/version 1]
                          [101 :edb.schemaReplicant/version 2]
                          [200 :edb.schemaReplicant/attribute 100]
                          [200 :edb.schemaReplicant/attribute 101]]");

        // Test that we can retract :edb.cardinality/one elements.
        assert_transact!(conn, "[[:edb/retract 100 :edb.schemaReplicant/version 1]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb.schemaReplicant/version 1 ?causetx false]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[101 :edb.schemaReplicant/version 2]
                          [200 :edb.schemaReplicant/attribute 100]
                          [200 :edb.schemaReplicant/attribute 101]]");

        // Test that we can retract :edb.cardinality/many elements.
        assert_transact!(conn, "[[:edb/retract 200 :edb.schemaReplicant/attribute 100]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :edb.schemaReplicant/attribute 100 ?causetx false]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[101 :edb.schemaReplicant/version 2]
                          [200 :edb.schemaReplicant/attribute 101]]");

        // Verify that retracting :edb.cardinality/{one,many} elements that are not present doesn't
        // change the store.
        assert_transact!(conn, "[[:edb/retract 100 :edb.schemaReplicant/version 1]
                                 [:edb/retract 200 :edb.schemaReplicant/attribute 100]]");
        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[101 :edb.schemaReplicant/version 2]
                          [200 :edb.schemaReplicant/attribute 101]]");
    }

    #[test]
    fn test_edb_doc_is_not_schemaReplicant() {
        let mut conn = TestConn::default();

        // Neither transaction below is defining a new attribute.  That is, it's fine to use :edb/doc
        // to describe any instanton in the system, not just attributes.  And in particular, including
        // :edb/doc shouldn't make the transactor consider the instanton a schemaReplicant attribute.
        assert_transact!(conn, r#"
            [{:edb/doc "test"}]
        "#);

        assert_transact!(conn, r#"
            [{:edb/causetid :test/id :edb/doc "test"}]
        "#);
    }

    // Unique is required!
    #[test]
    fn test_upsert_issue_538() {
        let mut conn = TestConn::default();
        assert_transact!(conn, "
            [{:edb/causetid :person/name
              :edb/valueType :edb.type/string
              :edb/cardinality :edb.cardinality/many}
             {:edb/causetid :person/age
              :edb/valueType :edb.type/long
              :edb/cardinality :edb.cardinality/one}
             {:edb/causetid :person/email
              :edb/valueType :edb.type/string
              :edb/unique :edb.unique/causetIdity
              :edb/cardinality :edb.cardinality/many}]",
              Err("bad schemaReplicant assertion: :edb/unique :edb/unique_causetIdity without :edb/index true for solitonId: 65538"));
    }

    // TODO: don't use :edb/causetid to test upserts!
    #[test]
    fn test_upsert_vector() {
        let mut conn = TestConn::default();

        // Insert some :edb.unique/causetIdity elements.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :name/Ivan]
                                 [:edb/add 101 :edb/causetid :name/Petr]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb/causetid :name/Ivan ?causetx true]
                          [101 :edb/causetid :name/Petr ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :name/Ivan]
                          [101 :edb/causetid :name/Petr]]");

        // Upserting two tempids to the same solitonId works.
        let report = assert_transact!(conn, "[[:edb/add \"t1\" :edb/causetid :name/Ivan]
                                              [:edb/add \"t1\" :edb.schemaReplicant/attribute 100]
                                              [:edb/add \"t2\" :edb/causetid :name/Petr]
                                              [:edb/add \"t2\" :edb.schemaReplicant/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb.schemaReplicant/attribute :name/Ivan ?causetx true]
                          [101 :edb.schemaReplicant/attribute :name/Petr ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :name/Ivan]
                          [100 :edb.schemaReplicant/attribute :name/Ivan]
                          [101 :edb/causetid :name/Petr]
                          [101 :edb.schemaReplicant/attribute :name/Petr]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 100
                          \"t2\" 101}");

        // Upserting a tempid works.  The ref doesn't have to exist (at this time), but we can't
        // reuse an existing ref due to :edb/unique :edb.unique/value.
        let report = assert_transact!(conn, "[[:edb/add \"t1\" :edb/causetid :name/Ivan]
                                              [:edb/add \"t1\" :edb.schemaReplicant/attribute 102]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb.schemaReplicant/attribute 102 ?causetx true]
                          [?true :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :name/Ivan]
                          [100 :edb.schemaReplicant/attribute :name/Ivan]
                          [100 :edb.schemaReplicant/attribute 102]
                          [101 :edb/causetid :name/Petr]
                          [101 :edb.schemaReplicant/attribute :name/Petr]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 100}");

        // A single complex upsert allocates a new solitonId.
        let report = assert_transact!(conn, "[[:edb/add \"t1\" :edb.schemaReplicant/attribute \"t2\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[65536 :edb.schemaReplicant/attribute 65537 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 65536
                          \"t2\" 65537}");

        // Conflicting upserts fail.
        assert_transact!(conn, "[[:edb/add \"t1\" :edb/causetid :name/Ivan]
                                 [:edb/add \"t1\" :edb/causetid :name/Petr]]",
                         Err("schemaReplicant constraint violation: conflicting upserts:\n  tempid External(\"t1\") upserts to {KnownSolitonId(100), KnownSolitonId(101)}\n"));

        // The error messages of conflicting upserts gives information about all failing upserts (in a particular generation).
        assert_transact!(conn, "[[:edb/add \"t2\" :edb/causetid :name/Grigory]
                                 [:edb/add \"t2\" :edb/causetid :name/Petr]
                                 [:edb/add \"t2\" :edb/causetid :name/Ivan]
                                 [:edb/add \"t1\" :edb/causetid :name/Ivan]
                                 [:edb/add \"t1\" :edb/causetid :name/Petr]]",
                         Err("schemaReplicant constraint violation: conflicting upserts:\n  tempid External(\"t1\") upserts to {KnownSolitonId(100), KnownSolitonId(101)}\n  tempid External(\"t2\") upserts to {KnownSolitonId(100), KnownSolitonId(101)}\n"));

        // tempids in :edb/retract that don't upsert fail.
        assert_transact!(conn, "[[:edb/retract \"t1\" :edb/causetid :name/Anonymous]]",
                         Err("not yet implemented: [:edb/retract ...] instanton referenced tempid that did not upsert: t1"));

        // tempids in :edb/retract that do upsert are retracted.  The ref given doesn't exist, so the
        // assertion will be ignored.
        let report = assert_transact!(conn, "[[:edb/add \"t1\" :edb/causetid :name/Ivan]
                                              [:edb/retract \"t1\" :edb.schemaReplicant/attribute 103]]");
        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 100}");

        // A multistep upsert.  The upsert algorithm will first try to resolve "t1", fail, and then
        // allocate both "t1" and "t2".
        let report = assert_transact!(conn, "[[:edb/add \"t1\" :edb/causetid :name/Josef]
                                              [:edb/add \"t2\" :edb.schemaReplicant/attribute \"t1\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[65538 :edb/causetid :name/Josef ?causetx true]
                          [65539 :edb.schemaReplicant/attribute :name/Josef ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 65538
                          \"t2\" 65539}");

        // A multistep insert.  This time, we can resolve both, but we have to try "t1", succeed,
        // and then resolve "t2".
        // TODO: We can't quite test this without more schemaReplicant elements.
        // conn.transact("[[:edb/add \"t1\" :edb/causetid :name/Josef]
        //                 [:edb/add \"t2\" :edb/causetid \"t1\"]]");
        // assert_matches!(conn.last_transaction(),
        //                 "[[65538 :edb/causetid :name/Josef]
        //                   [65538 :edb/causetid :name/Karl]
        //                   [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
    }

    #[test]
    fn test_resolved_upserts() {
        let mut conn = TestConn::default();
        assert_transact!(conn, "[
            {:edb/causetid :test/id
             :edb/valueType :edb.type/string
             :edb/unique :edb.unique/causetIdity
             :edb/index true
             :edb/cardinality :edb.cardinality/one}
            {:edb/causetid :test/ref
             :edb/valueType :edb.type/ref
             :edb/unique :edb.unique/causetIdity
             :edb/index true
             :edb/cardinality :edb.cardinality/one}
        ]");

        // Partial data for :test/id, links via :test/ref.
        assert_transact!(conn, r#"[
            [:edb/add 100 :test/id "0"]
            [:edb/add 101 :test/ref 100]
            [:edb/add 102 :test/ref 101]
            [:edb/add 103 :test/ref 102]
        ]"#);

        // Fill in the rest of the data for :test/id, using the links of :test/ref.
        let report = assert_transact!(conn, r#"[
            {:edb/id "a" :test/id "0"}
            {:edb/id "b" :test/id "1" :test/ref "a"}
            {:edb/id "c" :test/id "2" :test/ref "b"}
            {:edb/id "d" :test/id "3" :test/ref "c"}
        ]"#);

        assert_matches!(tempids(&report), r#"{
            "a" 100
            "b" 101
            "c" 102
            "d" 103
        }"#);

        assert_matches!(conn.last_transaction(), r#"[
            [101 :test/id "1" ?causetx true]
            [102 :test/id "2" ?causetx true]
            [103 :test/id "3" ?causetx true]
            [?causetx :edb/causecausetxInstant ?ms ?causetx true]
        ]"#);
    }

    #[test]
    fn test_sqlite_limit() {
        let conn = new_connection("").expect("Couldn't open in-memory edb");
        let initial = conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER);
        // Sanity check.
        assert!(initial > 500);

        // Make sure setting works.
        conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 222);
        assert_eq!(222, conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER));
    }

    #[test]
    fn test_edb_install() {
        let mut conn = TestConn::default();

        // We're missing some tests here, since our impleedbion is incomplete.
        // See https://github.com/whtcorpsinc/edb/issues/797

        // We can assert a new schemaReplicant attribute.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :test/causetid]
                                 [:edb/add 100 :edb/valueType :edb.type/long]
                                 [:edb/add 100 :edb/cardinality :edb.cardinality/many]]");

        assert_eq!(conn.schemaReplicant.causetid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":test/causetid").unwrap());
        assert_eq!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":test/causetid").unwrap()).cloned().unwrap(), 100);
        let attribute = conn.schemaReplicant.attribute_for_causetid(100).unwrap().clone();
        assert_eq!(attribute.value_type, MinkowskiValueType::Long);
        assert_eq!(attribute.multival, true);
        assert_eq!(attribute.fulltext, false);

        assert_matches!(conn.last_transaction(),
                        "[[100 :edb/causetid :test/causetid ?causetx true]
                          [100 :edb/valueType :edb.type/long ?causetx true]
                          [100 :edb/cardinality :edb.cardinality/many ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :test/causetid]
                          [100 :edb/valueType :edb.type/long]
                          [100 :edb/cardinality :edb.cardinality/many]]");

        // Let's check we actually have the schemaReplicant characteristics we expect.
        let attribute = conn.schemaReplicant.attribute_for_causetid(100).unwrap().clone();
        assert_eq!(attribute.value_type, MinkowskiValueType::Long);
        assert_eq!(attribute.multival, true);
        assert_eq!(attribute.fulltext, false);

        // Let's check that we can use the freshly installed attribute.
        assert_transact!(conn, "[[:edb/add 101 100 -10]
                                 [:edb/add 101 :test/causetid -9]]");

        assert_matches!(conn.last_transaction(),
                        "[[101 :test/causetid -10 ?causetx true]
                          [101 :test/causetid -9 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // Cannot retract a single characteristic of an installed attribute.
        assert_transact!(conn,
                         "[[:edb/retract 100 :edb/cardinality :edb.cardinality/many]]",
                         Err("bad schemaReplicant assertion: Retracting attribute 8 for instanton 100 not permitted."));

        // Cannot retract a single characteristic of an installed attribute.
        assert_transact!(conn,
                         "[[:edb/retract 100 :edb/valueType :edb.type/long]]",
                         Err("bad schemaReplicant assertion: Retracting attribute 7 for instanton 100 not permitted."));

        // Cannot retract a non-defining set of characteristics of an installed attribute.
        assert_transact!(conn,
                         "[[:edb/retract 100 :edb/valueType :edb.type/long]
                         [:edb/retract 100 :edb/cardinality :edb.cardinality/many]]",
                         Err("bad schemaReplicant assertion: Retracting defining attributes of a schemaReplicant without retracting its :edb/causetid is not permitted."));

        // See https://github.com/whtcorpsinc/edb/issues/796.
        // assert_transact!(conn,
        //                 "[[:edb/retract 100 :edb/causetid :test/causetid]]",
        //                 Err("bad schemaReplicant assertion: Retracting :edb/causetid of a schemaReplicant without retracting its defining attributes is not permitted."));

        // Can retract all of characterists of an installed attribute in one go.
        assert_transact!(conn,
                         "[[:edb/retract 100 :edb/cardinality :edb.cardinality/many]
                         [:edb/retract 100 :edb/valueType :edb.type/long]
                         [:edb/retract 100 :edb/causetid :test/causetid]]");

        // Trying to install an attribute without a :edb/causetid is allowed.
        assert_transact!(conn, "[[:edb/add 101 :edb/valueType :edb.type/long]
                                 [:edb/add 101 :edb/cardinality :edb.cardinality/many]]");
    }

    #[test]
    fn test_edb_alter() {
        let mut conn = TestConn::default();

        // Start by installing a :edb.cardinality/one attribute.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :test/causetid]
                                 [:edb/add 100 :edb/valueType :edb.type/keyword]
                                 [:edb/add 100 :edb/cardinality :edb.cardinality/one]]");

        // Trying to alter the :edb/valueType will fail.
        assert_transact!(conn, "[[:edb/add 100 :edb/valueType :edb.type/long]]",
                         Err("bad schemaReplicant assertion: SchemaReplicant alteration for existing attribute with solitonId 100 is not valid"));

        // But we can alter the cardinality.
        assert_transact!(conn, "[[:edb/add 100 :edb/cardinality :edb.cardinality/many]]");

        assert_matches!(conn.last_transaction(),
                        "[[100 :edb/cardinality :edb.cardinality/one ?causetx false]
                          [100 :edb/cardinality :edb.cardinality/many ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :test/causetid]
                          [100 :edb/valueType :edb.type/keyword]
                          [100 :edb/cardinality :edb.cardinality/many]]");

        // Let's check we actually have the schemaReplicant characteristics we expect.
        let attribute = conn.schemaReplicant.attribute_for_causetid(100).unwrap().clone();
        assert_eq!(attribute.value_type, MinkowskiValueType::Keyword);
        assert_eq!(attribute.multival, true);
        assert_eq!(attribute.fulltext, false);

        // Let's check that we can use the freshly altered attribute's new characteristic.
        assert_transact!(conn, "[[:edb/add 101 100 :test/value1]
                                 [:edb/add 101 :test/causetid :test/value2]]");

        assert_matches!(conn.last_transaction(),
                        "[[101 :test/causetid :test/value1 ?causetx true]
                          [101 :test/causetid :test/value2 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
    }

    #[test]
    fn test_edb_causetId() {
        let mut conn = TestConn::default();

        // We can assert a new :edb/causetid.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :name/Ivan]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb/causetid :name/Ivan ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :name/Ivan]]");
        assert_eq!(conn.schemaReplicant.causetid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Ivan").unwrap());
        assert_eq!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).cloned().unwrap(), 100);

        // We can re-assert an existing :edb/causetid.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :name/Ivan]]");
        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :name/Ivan]]");
        assert_eq!(conn.schemaReplicant.causetid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Ivan").unwrap());
        assert_eq!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).cloned().unwrap(), 100);

        // We can alter an existing :edb/causetid to have a new keyword.
        assert_transact!(conn, "[[:edb/add :name/Ivan :edb/causetid :name/Petr]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :edb/causetid :name/Ivan ?causetx false]
                          [100 :edb/causetid :name/Petr ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :name/Petr]]");
        // SolitonId map is updated.
        assert_eq!(conn.schemaReplicant.causetid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Petr").unwrap());
        // CausetId map contains the new causetid.
        assert_eq!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":name/Petr").unwrap()).cloned().unwrap(), 100);
        // CausetId map no longer contains the old causetid.
        assert!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).is_none());

        // We can re-purpose an old causetid.
        assert_transact!(conn, "[[:edb/add 101 :edb/causetid :name/Ivan]]");
        assert_matches!(conn.last_transaction(),
                        "[[101 :edb/causetid :name/Ivan ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :name/Petr]
                          [101 :edb/causetid :name/Ivan]]");
        // SolitonId map contains both causetids.
        assert_eq!(conn.schemaReplicant.causetid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Petr").unwrap());
        assert_eq!(conn.schemaReplicant.causetid_map.get(&101).cloned().unwrap(), to_namespaced_keyword(":name/Ivan").unwrap());
        // CausetId map contains the new causetid.
        assert_eq!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":name/Petr").unwrap()).cloned().unwrap(), 100);
        // CausetId map contains the old causetid, but re-purposed to the new solitonId.
        assert_eq!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).cloned().unwrap(), 101);

        // We can retract an existing :edb/causetid.
        assert_transact!(conn, "[[:edb/retract :name/Petr :edb/causetid :name/Petr]]");
        // It's really gone.
        assert!(conn.schemaReplicant.causetid_map.get(&100).is_none());
        assert!(conn.schemaReplicant.causetId_map.get(&to_namespaced_keyword(":name/Petr").unwrap()).is_none());
    }

    #[test]
    fn test_edb_alter_cardinality() {
        let mut conn = TestConn::default();

        // Start by installing a :edb.cardinality/one attribute.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :test/causetid]
                                 [:edb/add 100 :edb/valueType :edb.type/long]
                                 [:edb/add 100 :edb/cardinality :edb.cardinality/one]]");

        assert_transact!(conn, "[[:edb/add 200 :test/causetid 1]]");

        // We can always go from :edb.cardinality/one to :edb.cardinality/many.
        assert_transact!(conn, "[[:edb/add 100 :edb/cardinality :edb.cardinality/many]]");

        assert_transact!(conn, "[[:edb/add 200 :test/causetid 2]]");

        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :test/causetid]
                          [100 :edb/valueType :edb.type/long]
                          [100 :edb/cardinality :edb.cardinality/many]
                          [200 :test/causetid 1]
                          [200 :test/causetid 2]]");

        // We can't always go from :edb.cardinality/many to :edb.cardinality/one.
        assert_transact!(conn, "[[:edb/add 100 :edb/cardinality :edb.cardinality/one]]",
                         // TODO: give more helpful error details.
                         Err("schemaReplicant alteration failed: Cannot alter schemaReplicant attribute 100 to be :edb.cardinality/one"));
    }

    #[test]
    fn test_edb_alter_unique_value() {
        let mut conn = TestConn::default();

        // Start by installing a :edb.cardinality/one attribute.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :test/causetid]
                                 [:edb/add 100 :edb/valueType :edb.type/long]
                                 [:edb/add 100 :edb/cardinality :edb.cardinality/one]]");

        assert_transact!(conn, "[[:edb/add 200 :test/causetid 1]
                                 [:edb/add 201 :test/causetid 1]]");

        // We can't always migrate to be :edb.unique/value.
        assert_transact!(conn, "[[:edb/add :test/causetid :edb/unique :edb.unique/value]]",
                         // TODO: give more helpful error details.
                         Err("schemaReplicant alteration failed: Cannot alter schemaReplicant attribute 100 to be :edb.unique/value"));

        // Not even indirectly!
        assert_transact!(conn, "[[:edb/add :test/causetid :edb/unique :edb.unique/causetIdity]]",
                         // TODO: give more helpful error details.
                         Err("schemaReplicant alteration failed: Cannot alter schemaReplicant attribute 100 to be :edb.unique/causetIdity"));

        // But we can if we make sure there's no repeated [a v] pair.
        assert_transact!(conn, "[[:edb/add 201 :test/causetid 2]]");

        assert_transact!(conn, "[[:edb/add :test/causetid :edb/index true]
                                 [:edb/add :test/causetid :edb/unique :edb.unique/value]
                                 [:edb/add :edb.part/edb :edb.alter/attribute 100]]");

        // We can also retract the uniqueness constraint altogether.
        assert_transact!(conn, "[[:edb/retract :test/causetid :edb/unique :edb.unique/value]]");

        // Once we've done so, the schemaReplicant shows it's not unique…
        {
            let attr = conn.schemaReplicant.attribute_for_causetId(&Keyword::namespaced("test", "causetid")).unwrap().0;
            assert_eq!(None, attr.unique);
        }

        // … and we can add more assertions with duplicate values.
        assert_transact!(conn, "[[:edb/add 121 :test/causetid 1]
                                 [:edb/add 221 :test/causetid 2]]");
    }

    #[test]
    fn test_edb_double_retraction_issue_818() {
        let mut conn = TestConn::default();

        // Start by installing a :edb.cardinality/one attribute.
        assert_transact!(conn, "[[:edb/add 100 :edb/causetid :test/causetid]
                                 [:edb/add 100 :edb/valueType :edb.type/string]
                                 [:edb/add 100 :edb/cardinality :edb.cardinality/one]
                                 [:edb/add 100 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 100 :edb/index true]]");

        assert_transact!(conn, "[[:edb/add 200 :test/causetid \"Oi\"]]");

        assert_transact!(conn, "[[:edb/add 200 :test/causetid \"Ai!\"]
                                 [:edb/retract 200 :test/causetid \"Oi\"]]");

        assert_matches!(conn.last_transaction(),
                        "[[200 :test/causetid \"Ai!\" ?causetx true]
                          [200 :test/causetid \"Oi\" ?causetx false]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        assert_matches!(conn.causets(),
                        "[[100 :edb/causetid :test/causetid]
                          [100 :edb/valueType :edb.type/string]
                          [100 :edb/cardinality :edb.cardinality/one]
                          [100 :edb/unique :edb.unique/causetIdity]
                          [100 :edb/index true]
                          [200 :test/causetid \"Ai!\"]]");
    }

    /// Verify that we can't alter :edb/fulltext schemaReplicant characteristics at all.
    #[test]
    fn test_edb_alter_fulltext() {
        let mut conn = TestConn::default();

        // Start by installing a :edb/fulltext true and a :edb/fulltext unset attribute.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/fulltext]
                                 [:edb/add 111 :edb/valueType :edb.type/string]
                                 [:edb/add 111 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 111 :edb/index true]
                                 [:edb/add 111 :edb/fulltext true]
                                 [:edb/add 222 :edb/causetid :test/string]
                                 [:edb/add 222 :edb/cardinality :edb.cardinality/one]
                                 [:edb/add 222 :edb/valueType :edb.type/string]
                                 [:edb/add 222 :edb/index true]]");

        assert_transact!(conn,
                         "[[:edb/retract 111 :edb/fulltext true]]",
                         Err("bad schemaReplicant assertion: Retracting attribute 12 for instanton 111 not permitted."));

        assert_transact!(conn,
                         "[[:edb/add 222 :edb/fulltext true]]",
                         Err("bad schemaReplicant assertion: SchemaReplicant alteration for existing attribute with solitonId 222 is not valid"));
    }

    #[test]
    fn test_edb_fulltext() {
        let mut conn = TestConn::default();

        // Start by installing a few :edb/fulltext true attributes.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/fulltext]
                                 [:edb/add 111 :edb/valueType :edb.type/string]
                                 [:edb/add 111 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 111 :edb/index true]
                                 [:edb/add 111 :edb/fulltext true]
                                 [:edb/add 222 :edb/causetid :test/other]
                                 [:edb/add 222 :edb/cardinality :edb.cardinality/one]
                                 [:edb/add 222 :edb/valueType :edb.type/string]
                                 [:edb/add 222 :edb/index true]
                                 [:edb/add 222 :edb/fulltext true]]");

        // Let's check we actually have the schemaReplicant characteristics we expect.
        let fulltext = conn.schemaReplicant.attribute_for_causetid(111).cloned().expect(":test/fulltext");
        assert_eq!(fulltext.value_type, MinkowskiValueType::String);
        assert_eq!(fulltext.fulltext, true);
        assert_eq!(fulltext.multival, false);
        assert_eq!(fulltext.unique, Some(attribute::Unique::CausetIdity));

        let other = conn.schemaReplicant.attribute_for_causetid(222).cloned().expect(":test/other");
        assert_eq!(other.value_type, MinkowskiValueType::String);
        assert_eq!(other.fulltext, true);
        assert_eq!(other.multival, false);
        assert_eq!(other.unique, None);

        // We can add fulltext indexed causets.
        assert_transact!(conn, "[[:edb/add 301 :test/fulltext \"test this\"]]");
        // value CausetIndex is rowid into fulltext Block.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[301 :test/fulltext 1 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[111 :edb/causetid :test/fulltext]
                          [111 :edb/valueType :edb.type/string]
                          [111 :edb/unique :edb.unique/causetIdity]
                          [111 :edb/index true]
                          [111 :edb/fulltext true]
                          [222 :edb/causetid :test/other]
                          [222 :edb/valueType :edb.type/string]
                          [222 :edb/cardinality :edb.cardinality/one]
                          [222 :edb/index true]
                          [222 :edb/fulltext true]
                          [301 :test/fulltext 1]]");

        // We can replace existing fulltext indexed causets.
        assert_transact!(conn, "[[:edb/add 301 :test/fulltext \"alternate thing\"]]");
        // value CausetIndex is rowid into fulltext Block.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[301 :test/fulltext 1 ?causetx false]
                          [301 :test/fulltext 2 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[111 :edb/causetid :test/fulltext]
                          [111 :edb/valueType :edb.type/string]
                          [111 :edb/unique :edb.unique/causetIdity]
                          [111 :edb/index true]
                          [111 :edb/fulltext true]
                          [222 :edb/causetid :test/other]
                          [222 :edb/valueType :edb.type/string]
                          [222 :edb/cardinality :edb.cardinality/one]
                          [222 :edb/index true]
                          [222 :edb/fulltext true]
                          [301 :test/fulltext 2]]");

        // We can upsert keyed by fulltext indexed causets.
        assert_transact!(conn, "[[:edb/add \"t\" :test/fulltext \"alternate thing\"]
                                 [:edb/add \"t\" :test/other \"other\"]]");
        // value CausetIndex is rowid into fulltext Block.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[301 :test/other 3 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[111 :edb/causetid :test/fulltext]
                          [111 :edb/valueType :edb.type/string]
                          [111 :edb/unique :edb.unique/causetIdity]
                          [111 :edb/index true]
                          [111 :edb/fulltext true]
                          [222 :edb/causetid :test/other]
                          [222 :edb/valueType :edb.type/string]
                          [222 :edb/cardinality :edb.cardinality/one]
                          [222 :edb/index true]
                          [222 :edb/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]]");

        // We can re-use fulltext values; they won't be added to the fulltext values Block twice.
        assert_transact!(conn, "[[:edb/add 302 :test/other \"alternate thing\"]]");
        // value CausetIndex is rowid into fulltext Block.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[302 :test/other 2 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[111 :edb/causetid :test/fulltext]
                          [111 :edb/valueType :edb.type/string]
                          [111 :edb/unique :edb.unique/causetIdity]
                          [111 :edb/index true]
                          [111 :edb/fulltext true]
                          [222 :edb/causetid :test/other]
                          [222 :edb/valueType :edb.type/string]
                          [222 :edb/cardinality :edb.cardinality/one]
                          [222 :edb/index true]
                          [222 :edb/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]
                          [302 :test/other 2]]");

        // We can retract fulltext indexed causets.  The underlying fulltext value remains -- indeed,
        // it might still be in use.
        assert_transact!(conn, "[[:edb/retract 302 :test/other \"alternate thing\"]]");
        // value CausetIndex is rowid into fulltext Block.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[302 :test/other 2 ?causetx false]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(conn.causets(),
                        "[[111 :edb/causetid :test/fulltext]
                          [111 :edb/valueType :edb.type/string]
                          [111 :edb/unique :edb.unique/causetIdity]
                          [111 :edb/index true]
                          [111 :edb/fulltext true]
                          [222 :edb/causetid :test/other]
                          [222 :edb/valueType :edb.type/string]
                          [222 :edb/cardinality :edb.cardinality/one]
                          [222 :edb/index true]
                          [222 :edb/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]]");
    }

    #[test]
    fn test_lookup_refs_instanton_CausetIndex() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/unique_value]
                                 [:edb/add 111 :edb/valueType :edb.type/string]
                                 [:edb/add 111 :edb/unique :edb.unique/value]
                                 [:edb/add 111 :edb/index true]
                                 [:edb/add 222 :edb/causetid :test/unique_causetIdity]
                                 [:edb/add 222 :edb/valueType :edb.type/long]
                                 [:edb/add 222 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 222 :edb/index true]
                                 [:edb/add 333 :edb/causetid :test/not_unique]
                                 [:edb/add 333 :edb/cardinality :edb.cardinality/one]
                                 [:edb/add 333 :edb/valueType :edb.type/keyword]
                                 [:edb/add 333 :edb/index true]]");

        // And a few causets to match against.
        assert_transact!(conn, "[[:edb/add 501 :test/unique_value \"test this\"]
                                 [:edb/add 502 :test/unique_value \"other\"]
                                 [:edb/add 503 :test/unique_causetIdity -10]
                                 [:edb/add 504 :test/unique_causetIdity -20]
                                 [:edb/add 505 :test/not_unique :test/keyword]
                                 [:edb/add 506 :test/not_unique :test/keyword]]");

        // We can resolve lookup refs in the instanton CausetIndex, referring to the attribute as an solitonId or an causetid.
        assert_transact!(conn, "[[:edb/add (lookup-ref :test/unique_value \"test this\") :test/not_unique :test/keyword]
                                 [:edb/add (lookup-ref 111 \"other\") :test/not_unique :test/keyword]
                                 [:edb/add (lookup-ref :test/unique_causetIdity -10) :test/not_unique :test/keyword]
                                 [:edb/add (lookup-ref 222 -20) :test/not_unique :test/keyword]]");
        assert_matches!(conn.last_transaction(),
                        "[[501 :test/not_unique :test/keyword ?causetx true]
                          [502 :test/not_unique :test/keyword ?causetx true]
                          [503 :test/not_unique :test/keyword ?causetx true]
                          [504 :test/not_unique :test/keyword ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // We cannot resolve lookup refs that aren't :edb/unique.
        assert_transact!(conn,
                         "[[:edb/add (lookup-ref :test/not_unique :test/keyword) :test/not_unique :test/keyword]]",
                         Err("not yet implemented: Cannot resolve (lookup-ref 333 Keyword(Keyword(NamespaceableName { namespace: Some(\"test\"), name: \"keyword\" }))) with attribute that is not :edb/unique"));

        // We type check the lookup ref's value against the lookup ref's attribute.
        assert_transact!(conn,
                         "[[:edb/add (lookup-ref :test/unique_value :test/not_a_string) :test/not_unique :test/keyword]]",
                         Err("value \':test/not_a_string\' is not the expected EinsteinDB value type String"));

        // Each lookup ref in the instanton CausetIndex must resolve
        assert_transact!(conn,
                         "[[:edb/add (lookup-ref :test/unique_value \"unmatched string value\") :test/not_unique :test/keyword]]",
                         Err("no solitonId found for causetid: couldn\'t lookup [a v]: (111, String(\"unmatched string value\"))"));
    }

    #[test]
    fn test_lookup_refs_value_CausetIndex() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/unique_value]
                                 [:edb/add 111 :edb/valueType :edb.type/string]
                                 [:edb/add 111 :edb/unique :edb.unique/value]
                                 [:edb/add 111 :edb/index true]
                                 [:edb/add 222 :edb/causetid :test/unique_causetIdity]
                                 [:edb/add 222 :edb/valueType :edb.type/long]
                                 [:edb/add 222 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 222 :edb/index true]
                                 [:edb/add 333 :edb/causetid :test/not_unique]
                                 [:edb/add 333 :edb/cardinality :edb.cardinality/one]
                                 [:edb/add 333 :edb/valueType :edb.type/keyword]
                                 [:edb/add 333 :edb/index true]
                                 [:edb/add 444 :edb/causetid :test/ref]
                                 [:edb/add 444 :edb/valueType :edb.type/ref]
                                 [:edb/add 444 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 444 :edb/index true]]");

        // And a few causets to match against.
        assert_transact!(conn, "[[:edb/add 501 :test/unique_value \"test this\"]
                                 [:edb/add 502 :test/unique_value \"other\"]
                                 [:edb/add 503 :test/unique_causetIdity -10]
                                 [:edb/add 504 :test/unique_causetIdity -20]
                                 [:edb/add 505 :test/not_unique :test/keyword]
                                 [:edb/add 506 :test/not_unique :test/keyword]]");

        // We can resolve lookup refs in the instanton CausetIndex, referring to the attribute as an solitonId or an causetid.
        assert_transact!(conn, "[[:edb/add 601 :test/ref (lookup-ref :test/unique_value \"test this\")]
                                 [:edb/add 602 :test/ref (lookup-ref 111 \"other\")]
                                 [:edb/add 603 :test/ref (lookup-ref :test/unique_causetIdity -10)]
                                 [:edb/add 604 :test/ref (lookup-ref 222 -20)]]");
        assert_matches!(conn.last_transaction(),
                        "[[601 :test/ref 501 ?causetx true]
                          [602 :test/ref 502 ?causetx true]
                          [603 :test/ref 503 ?causetx true]
                          [604 :test/ref 504 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // We cannot resolve lookup refs for attributes that aren't :edb/ref.
        assert_transact!(conn,
                         "[[:edb/add \"t\" :test/not_unique (lookup-ref :test/unique_value \"test this\")]]",
                         Err("not yet implemented: Cannot resolve value lookup ref for attribute 333 that is not :edb/valueType :edb.type/ref"));

        // If a value CausetIndex lookup ref resolves, we can upsert against it.  Here, the lookup ref
        // resolves to 501, which upserts "t" to 601.
        assert_transact!(conn, "[[:edb/add \"t\" :test/ref (lookup-ref :test/unique_value \"test this\")]
                                 [:edb/add \"t\" :test/not_unique :test/keyword]]");
        assert_matches!(conn.last_transaction(),
                        "[[601 :test/not_unique :test/keyword ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // Each lookup ref in the value CausetIndex must resolve
        assert_transact!(conn,
                         "[[:edb/add \"t\" :test/ref (lookup-ref :test/unique_value \"unmatched string value\")]]",
                         Err("no solitonId found for causetid: couldn\'t lookup [a v]: (111, String(\"unmatched string value\"))"));
    }

    #[test]
    fn test_explode_value_lists() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/many]
                                 [:edb/add 111 :edb/valueType :edb.type/long]
                                 [:edb/add 111 :edb/cardinality :edb.cardinality/many]
                                 [:edb/add 222 :edb/causetid :test/one]
                                 [:edb/add 222 :edb/valueType :edb.type/long]
                                 [:edb/add 222 :edb/cardinality :edb.cardinality/one]]");

        // Check that we can explode vectors for :edb.cardinality/many attributes.
        assert_transact!(conn, "[[:edb/add 501 :test/many [1]]
                                 [:edb/add 502 :test/many [2 3]]
                                 [:edb/add 503 :test/many [4 5 6]]]");
        assert_matches!(conn.last_transaction(),
                        "[[501 :test/many 1 ?causetx true]
                          [502 :test/many 2 ?causetx true]
                          [502 :test/many 3 ?causetx true]
                          [503 :test/many 4 ?causetx true]
                          [503 :test/many 5 ?causetx true]
                          [503 :test/many 6 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // Check that we can explode nested vectors for :edb.cardinality/many attributes.
        assert_transact!(conn, "[[:edb/add 600 :test/many [1 [2] [[3] [4]] []]]]");
        assert_matches!(conn.last_transaction(),
                        "[[600 :test/many 1 ?causetx true]
                          [600 :test/many 2 ?causetx true]
                          [600 :test/many 3 ?causetx true]
                          [600 :test/many 4 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // Check that we cannot explode vectors for :edb.cardinality/one attributes.
        assert_transact!(conn,
                         "[[:edb/add 501 :test/one [1]]]",
                         Err("not yet implemented: Cannot explode vector value for attribute 222 that is not :edb.cardinality :edb.cardinality/many"));
        assert_transact!(conn,
                         "[[:edb/add 501 :test/one [2 3]]]",
                         Err("not yet implemented: Cannot explode vector value for attribute 222 that is not :edb.cardinality :edb.cardinality/many"));
    }

    #[test]
    fn test_explode_map_notation() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/many]
                                 [:edb/add 111 :edb/valueType :edb.type/long]
                                 [:edb/add 111 :edb/cardinality :edb.cardinality/many]
                                 [:edb/add 222 :edb/causetid :test/component]
                                 [:edb/add 222 :edb/isComponent true]
                                 [:edb/add 222 :edb/valueType :edb.type/ref]
                                 [:edb/add 333 :edb/causetid :test/unique]
                                 [:edb/add 333 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 333 :edb/index true]
                                 [:edb/add 333 :edb/valueType :edb.type/long]
                                 [:edb/add 444 :edb/causetid :test/dangling]
                                 [:edb/add 444 :edb/valueType :edb.type/ref]]");

        // Check that we can explode map notation without :edb/id.
        let report = assert_transact!(conn, "[{:test/many 1}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/many 1 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode map notation with :edb/id, as an solitonId, causetid, and tempid.
        let report = assert_transact!(conn, "[{:edb/id :edb/causetid :test/many 1}
                                              {:edb/id 500 :test/many 2}
                                              {:edb/id \"t\" :test/many 3}]");
        assert_matches!(conn.last_transaction(),
                        "[[1 :test/many 1 ?causetx true]
                          [500 :test/many 2 ?causetx true]
                          [?e :test/many 3 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{\"t\" 65537}");

        // Check that we can explode map notation with :edb/id as a lookup-ref or causetx-function.
        let report = assert_transact!(conn, "[{:edb/id (lookup-ref :edb/causetid :edb/causetid) :test/many 4}
                                              {:edb/id (transaction-causetx) :test/many 5}]");
        assert_matches!(conn.last_transaction(),
                        "[[1 :test/many 4 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]
                          [?causetx :test/many 5 ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode map notation with nested vector values.
        let report = assert_transact!(conn, "[{:test/many [1 2]}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/many 1 ?causetx true]
                          [?e :test/many 2 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode map notation with nested maps if the attribute is
        // :edb/isComponent true.
        let report = assert_transact!(conn, "[{:test/component {:test/many 1}}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/component ?f ?causetx true]
                          [?f :test/many 1 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode map notation with nested maps if the inner map contains a
        // :edb/unique :edb.unique/causetIdity attribute.
        let report = assert_transact!(conn, "[{:test/dangling {:test/unique 10}}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/dangling ?f ?causetx true]
                          [?f :test/unique 10 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Verify that we can't explode map notation with nested maps if the inner map would be
        // dangling.
        assert_transact!(conn,
                         "[{:test/dangling {:test/many 11}}]",
                         Err("not yet implemented: Cannot explode nested map value that would lead to dangling instanton for attribute 444"));

        // Verify that we can explode map notation with nested maps, even if the inner map would be
        // dangling, if we give a :edb/id explicitly.
        assert_transact!(conn, "[{:test/dangling {:edb/id \"t\" :test/many 12}}]");
    }

    #[test]
    fn test_explode_reversed_notation() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/many]
                                 [:edb/add 111 :edb/valueType :edb.type/long]
                                 [:edb/add 111 :edb/cardinality :edb.cardinality/many]
                                 [:edb/add 222 :edb/causetid :test/component]
                                 [:edb/add 222 :edb/isComponent true]
                                 [:edb/add 222 :edb/valueType :edb.type/ref]
                                 [:edb/add 333 :edb/causetid :test/unique]
                                 [:edb/add 333 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 333 :edb/index true]
                                 [:edb/add 333 :edb/valueType :edb.type/long]
                                 [:edb/add 444 :edb/causetid :test/dangling]
                                 [:edb/add 444 :edb/valueType :edb.type/ref]]");

        // Check that we can explode direct reversed notation, causetids.
        let report = assert_transact!(conn, "[[:edb/add 100 :test/_dangling 200]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :test/dangling 100 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode direct reversed notation, causetIds.
        let report = assert_transact!(conn, "[[:edb/add :test/many :test/_dangling :test/unique]]");
        assert_matches!(conn.last_transaction(),
                        "[[333 :test/dangling :test/many ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode direct reversed notation, tempids.
        let report = assert_transact!(conn, "[[:edb/add \"s\" :test/_dangling \"t\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[65537 :test/dangling 65536 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        // This is impleedbion specific, but it should be deterministic.
        assert_matches!(tempids(&report),
                        "{\"s\" 65536
                          \"t\" 65537}");

        // Check that we can explode reversed notation in map notation without :edb/id.
        let report = assert_transact!(conn, "[{:test/_dangling 501}
                                              {:test/_dangling :test/many}
                                              {:test/_dangling \"t\"}]");
        assert_matches!(conn.last_transaction(),
                        "[[111 :test/dangling ?e1 ?causetx true]
                          [501 :test/dangling ?e2 ?causetx true]
                          [65538 :test/dangling ?e3 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{\"t\" 65538}");

        // Check that we can explode reversed notation in map notation with :edb/id, solitonId.
        let report = assert_transact!(conn, "[{:edb/id 600 :test/_dangling 601}]");
        assert_matches!(conn.last_transaction(),
                        "[[601 :test/dangling 600 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode reversed notation in map notation with :edb/id, causetid.
        let report = assert_transact!(conn, "[{:edb/id :test/component :test/_dangling :test/component}]");
        assert_matches!(conn.last_transaction(),
                        "[[222 :test/dangling :test/component ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode reversed notation in map notation with :edb/id, tempid.
        let report = assert_transact!(conn, "[{:edb/id \"s\" :test/_dangling \"t\"}]");
        assert_matches!(conn.last_transaction(),
                        "[[65543 :test/dangling 65542 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        // This is impleedbion specific, but it should be deterministic.
        assert_matches!(tempids(&report),
                        "{\"s\" 65542
                          \"t\" 65543}");

        // Check that we can use the same attribute in both forward and backward form in the same
        // transaction.
        let report = assert_transact!(conn, "[[:edb/add 888 :test/dangling 889]
                                              [:edb/add 888 :test/_dangling 889]]");
        assert_matches!(conn.last_transaction(),
                        "[[888 :test/dangling 889 ?causetx true]
                          [889 :test/dangling 888 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can use the same attribute in both forward and backward form in the same
        // transaction in map notation.
        let report = assert_transact!(conn, "[{:edb/id 998 :test/dangling 999 :test/_dangling 999}]");
        assert_matches!(conn.last_transaction(),
                        "[[998 :test/dangling 999 ?causetx true]
                          [999 :test/dangling 998 ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
        assert_matches!(tempids(&report),
                        "{}");

    }

    #[test]
    fn test_explode_reversed_notation_errors() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:edb/add 111 :edb/causetid :test/many]
                                 [:edb/add 111 :edb/valueType :edb.type/long]
                                 [:edb/add 111 :edb/cardinality :edb.cardinality/many]
                                 [:edb/add 222 :edb/causetid :test/component]
                                 [:edb/add 222 :edb/isComponent true]
                                 [:edb/add 222 :edb/valueType :edb.type/ref]
                                 [:edb/add 333 :edb/causetid :test/unique]
                                 [:edb/add 333 :edb/unique :edb.unique/causetIdity]
                                 [:edb/add 333 :edb/index true]
                                 [:edb/add 333 :edb/valueType :edb.type/long]
                                 [:edb/add 444 :edb/causetid :test/dangling]
                                 [:edb/add 444 :edb/valueType :edb.type/ref]]");

        // `causetx-parser` should fail to parse direct reverse notation with nested value maps and
        // nested value vectors, so we only test things that "get through" to the map notation
        // dynamic processor here.

        // Verify that we can't explode reverse notation in map notation with nested value maps.
        assert_transact!(conn,
                         "[{:test/_dangling {:test/many 14}}]",
                         Err("not yet implemented: Cannot explode map notation value in :attr/_reversed notation for attribute 444"));

        // Verify that we can't explode reverse notation in map notation with nested value vectors.
        assert_transact!(conn,
                         "[{:test/_dangling [:test/many]}]",
                         Err("not yet implemented: Cannot explode vector value in :attr/_reversed notation for attribute 444"));

        // Verify that we can't use reverse notation with non-:edb.type/ref attributes.
        assert_transact!(conn,
                         "[{:test/_unique 500}]",
                         Err("not yet implemented: Cannot use :attr/_reversed notation for attribute 333 that is not :edb/valueType :edb.type/ref"));

        // Verify that we can't use reverse notation with unrecognized attributes.
        assert_transact!(conn,
                         "[{:test/_unknown 500}]",
                         Err("no solitonId found for causetid: :test/unknown")); // TODO: make this error reference the original :test/_unknown.

        // Verify that we can't use reverse notation with bad value types: here, an unknown keyword
        // that can't be coerced to a ref.
        assert_transact!(conn,
                         "[{:test/_dangling :test/unknown}]",
                         Err("no solitonId found for causetid: :test/unknown"));
        // And here, a float.
        assert_transact!(conn,
                         "[{:test/_dangling 1.23}]",
                         Err("value \'1.23\' is not the expected EinsteinDB value type Ref"));
    }

    #[test]
    fn test_cardinality_one_violation_existing_instanton() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, r#"[
            [:edb/add 111 :edb/causetid :test/one]
            [:edb/add 111 :edb/valueType :edb.type/long]
            [:edb/add 111 :edb/cardinality :edb.cardinality/one]
            [:edb/add 112 :edb/causetid :test/unique]
            [:edb/add 112 :edb/index true]
            [:edb/add 112 :edb/valueType :edb.type/string]
            [:edb/add 112 :edb/cardinality :edb.cardinality/one]
            [:edb/add 112 :edb/unique :edb.unique/causetIdity]
        ]"#);

        assert_transact!(conn, r#"[
            [:edb/add "foo" :test/unique "x"]
        ]"#);

        // You can try to assert two values for the same instanton and attribute,
        // but you'll get an error.
        assert_transact!(conn, r#"[
            [:edb/add "foo" :test/unique "x"]
            [:edb/add "foo" :test/one 123]
            [:edb/add "bar" :test/unique "x"]
            [:edb/add "bar" :test/one 124]
        ]"#,
        // This is impleedbion specific (due to the allocated solitonId), but it should be deterministic.
        Err("schemaReplicant constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 65536, a: 111, vs: {Long(123), Long(124)} }\n"));

        // It also fails for map notation.
        assert_transact!(conn, r#"[
            {:test/unique "x", :test/one 123}
            {:test/unique "x", :test/one 124}
        ]"#,
        // This is impleedbion specific (due to the allocated solitonId), but it should be deterministic.
        Err("schemaReplicant constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 65536, a: 111, vs: {Long(123), Long(124)} }\n"));
    }

    #[test]
    fn test_conflicting_upserts() {
        let mut conn = TestConn::default();

        assert_transact!(conn, r#"[
            {:edb/causetid :page/id :edb/valueType :edb.type/string :edb/index true :edb/unique :edb.unique/causetIdity}
            {:edb/causetid :page/ref :edb/valueType :edb.type/ref :edb/index true :edb/unique :edb.unique/causetIdity}
            {:edb/causetid :page/title :edb/valueType :edb.type/string :edb/cardinality :edb.cardinality/many}
        ]"#);

        // Let's test some conflicting upserts.  First, valid data to work with -- note self references.
        assert_transact!(conn, r#"[
            [:edb/add 111 :page/id "1"]
            [:edb/add 111 :page/ref 111]
            [:edb/add 222 :page/id "2"]
            [:edb/add 222 :page/ref 222]
        ]"#);

        // Now valid upserts.  Note the references are valid.
        let report = assert_transact!(conn, r#"[
            [:edb/add "a" :page/id "1"]
            [:edb/add "a" :page/ref "a"]
            [:edb/add "b" :page/id "2"]
            [:edb/add "b" :page/ref "b"]
        ]"#);
        assert_matches!(tempids(&report),
                        "{\"a\" 111
                          \"b\" 222}");

        // Now conflicting upserts.  Note the references are reversed.  This example is interesting
        // because the first round `UpsertE` instances upsert, and this resolves all of the tempids
        // in the `UpsertEV` instances.  However, those `UpsertEV` instances lead to conflicting
        // upserts!  This tests that we don't resolve too far, giving a chance for those upserts to
        // fail.  This error message is crossing generations, although it's not reflected in the
        // error data structure.
        assert_transact!(conn, r#"[
            [:edb/add "a" :page/id "1"]
            [:edb/add "a" :page/ref "b"]
            [:edb/add "b" :page/id "2"]
            [:edb/add "b" :page/ref "a"]
        ]"#,
        Err("schemaReplicant constraint violation: conflicting upserts:\n  tempid External(\"a\") upserts to {KnownSolitonId(111), KnownSolitonId(222)}\n  tempid External(\"b\") upserts to {KnownSolitonId(111), KnownSolitonId(222)}\n"));

        // Here's a case where the upsert is not resolved, just allocated, but leads to conflicting
        // cardinality one causets.
        assert_transact!(conn, r#"[
            [:edb/add "x" :page/ref 333]
            [:edb/add "x" :page/ref 444]
        ]"#,
        Err("schemaReplicant constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 65539, a: 65537, vs: {Ref(333), Ref(444)} }\n"));
    }

    #[test]
    fn test_upsert_issue_532() {
        let mut conn = TestConn::default();

        assert_transact!(conn, r#"[
            {:edb/causetid :page/id :edb/valueType :edb.type/string :edb/index true :edb/unique :edb.unique/causetIdity}
            {:edb/causetid :page/ref :edb/valueType :edb.type/ref :edb/index true :edb/unique :edb.unique/causetIdity}
            {:edb/causetid :page/title :edb/valueType :edb.type/string :edb/cardinality :edb.cardinality/many}
        ]"#);

        // Observe that "foo" and "zot" upsert to the same solitonId, and that doesn't cause a
        // cardinality conflict, because we treat the input with set semantics and accept
        // duplicate causets.
        let report = assert_transact!(conn, r#"[
            [:edb/add "bar" :page/id "z"]
            [:edb/add "foo" :page/ref "bar"]
            [:edb/add "foo" :page/title "x"]
            [:edb/add "zot" :page/ref "bar"]
            [:edb/add "zot" :edb/causetid :other/causetid]
        ]"#);
        assert_matches!(tempids(&report),
                        "{\"bar\" ?b
                          \"foo\" ?f
                          \"zot\" ?f}");
        assert_matches!(conn.last_transaction(),
                        "[[?b :page/id \"z\" ?causetx true]
                          [?f :edb/causetid :other/causetid ?causetx true]
                          [?f :page/ref ?b ?causetx true]
                          [?f :page/title \"x\" ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        let report = assert_transact!(conn, r#"[
            [:edb/add "foo" :page/id "x"]
            [:edb/add "foo" :page/title "x"]
            [:edb/add "bar" :page/id "x"]
            [:edb/add "bar" :page/title "y"]
        ]"#);
        assert_matches!(tempids(&report),
                        "{\"foo\" ?e
                          \"bar\" ?e}");

        // One instanton, two page titles.
        assert_matches!(conn.last_transaction(),
                        "[[?e :page/id \"x\" ?causetx true]
                          [?e :page/title \"x\" ?causetx true]
                          [?e :page/title \"y\" ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // Here, "foo", "bar", and "baz", all refer to the same reference, but none of them actually
        // upsert to existing entities.
        let report = assert_transact!(conn, r#"[
            [:edb/add "foo" :page/id "id"]
            [:edb/add "bar" :edb/causetid :bar/bar]
            {:edb/id "baz" :page/id "id" :edb/causetid :bar/bar}
        ]"#);
        assert_matches!(tempids(&report),
                        "{\"foo\" ?e
                          \"bar\" ?e
                          \"baz\" ?e}");

        assert_matches!(conn.last_transaction(),
                        "[[?e :edb/causetid :bar/bar ?causetx true]
                          [?e :page/id \"id\" ?causetx true]
                          [?causetx :edb/causecausetxInstant ?ms ?causetx true]]");

        // If we do it again, everything resolves to the same IDs.
        let report = assert_transact!(conn, r#"[
            [:edb/add "foo" :page/id "id"]
            [:edb/add "bar" :edb/causetid :bar/bar]
            {:edb/id "baz" :page/id "id" :edb/causetid :bar/bar}
        ]"#);
        assert_matches!(tempids(&report),
                        "{\"foo\" ?e
                          \"bar\" ?e
                          \"baz\" ?e}");

        assert_matches!(conn.last_transaction(),
                        "[[?causetx :edb/causecausetxInstant ?ms ?causetx true]]");
    }

    #[test]
    fn test_term_typechecking_issue_663() {
        // The builder interfaces provide untrusted `Term` instances to the transactor, bypassing
        // the typechecking layers invoked in the schemaReplicant-aware coercion from `edbn::Value` into
        // `MinkowskiType`.  Typechecking now happens lower in the stack (as well as higher in the
        // stack) so we shouldn't be able to insert bad data into the store.

        let mut conn = TestConn::default();

        let mut terms = vec![];

        terms.push(Term::AddOrRetract(OpType::Add, Left(KnownSolitonId(200)), causetids::DB_CausetID, Left(MinkowskiType::typed_string("test"))));
        terms.push(Term::AddOrRetract(OpType::Retract, Left(KnownSolitonId(100)), causetids::DB_TX_INSTANT, Left(MinkowskiType::Long(-1))));

        let report = conn.transact_simple_terms(terms, InternSet::new());

        match report.err().map(|e| e.kind()) {
            Some(DbErrorKind::SchemaReplicantConstraintViolation(errors::SchemaReplicantConstraintViolation::TypeDisagreements { ref conflicting_Causets })) => {
                let mut map = BTreeMap::default();
                map.insert((100, causetids::DB_TX_INSTANT, MinkowskiType::Long(-1)), MinkowskiValueType::Instant);
                map.insert((200, causetids::DB_CausetID, MinkowskiType::typed_string("test")), MinkowskiValueType::Keyword);

                assert_eq!(conflicting_Causets, &map);
            },
            x => panic!("expected schemaReplicant constraint violation, got {:?}", x),
        }
    }

    #[test]
    fn test_cardinality_constraints() {
        let mut conn = TestConn::default();

        assert_transact!(conn, r#"[
            {:edb/id 200 :edb/causetid :test/one :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/one}
            {:edb/id 201 :edb/causetid :test/many :edb/valueType :edb.type/long :edb/cardinality :edb.cardinality/many}
        ]"#);

        // Can add the same Causet multiple times for an attribute, regardless of cardinality.
        assert_transact!(conn, r#"[
            [:edb/add 100 :test/one 1]
            [:edb/add 100 :test/one 1]
            [:edb/add 100 :test/many 2]
            [:edb/add 100 :test/many 2]
        ]"#);

        // Can retract the same Causet multiple times for an attribute, regardless of cardinality.
        assert_transact!(conn, r#"[
            [:edb/retract 100 :test/one 1]
            [:edb/retract 100 :test/one 1]
            [:edb/retract 100 :test/many 2]
            [:edb/retract 100 :test/many 2]
        ]"#);

        // Can't transact multiple causets for a cardinality one attribute.
        assert_transact!(conn, r#"[
            [:edb/add 100 :test/one 3]
            [:edb/add 100 :test/one 4]
        ]"#,
        Err("schemaReplicant constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 100, a: 200, vs: {Long(3), Long(4)} }\n"));

        // Can transact multiple causets for a cardinality many attribute.
        assert_transact!(conn, r#"[
            [:edb/add 100 :test/many 5]
            [:edb/add 100 :test/many 6]
        ]"#);

        // Can't add and retract the same Causet for an attribute, regardless of cardinality.
        assert_transact!(conn, r#"[
            [:edb/add     100 :test/one 7]
            [:edb/retract 100 :test/one 7]
            [:edb/add     100 :test/many 8]
            [:edb/retract 100 :test/many 8]
        ]"#,
        Err("schemaReplicant constraint violation: cardinality conflicts:\n  AddRetractConflict { e: 100, a: 200, vs: {Long(7)} }\n  AddRetractConflict { e: 100, a: 201, vs: {Long(8)} }\n"));
    }

    #[test]
    #[cfg(feature = "sqlcipher")]
    fn test_sqlcipher_openable() {
        let secret_key = "key";
        let sqlite = new_connection_with_key("../fixtures/v1encrypted.edb", secret_key).expect("Failed to find test EDB");
        sqlite.causetq_row("SELECT COUNT(*) FROM sqlite_master", &[], |Evcausetidx| Evcausetidx.get::<_, i64>(0))
            .expect("Failed to execute allegrosql causetq on encrypted EDB");
    }

    #[cfg(feature = "sqlcipher")]
    fn test_open_fail<F>(opener: F) where F: FnOnce() -> rusqlite::Result<rusqlite::Connection> {
        let err = opener().expect_err("Should fail to open encrypted EDB");
        match err {
            rusqlite::Error::SqliteFailure(err, ..) => {
                assert_eq!(err.extended_code, 26, "Should get error code 26 (not a database).");
            },
            err => {
                panic!("Wrong error type! {}", err);
            }
        }
    }

    #[test]
    #[cfg(feature = "sqlcipher")]
    fn test_sqlcipher_requires_key() {
        // Don't use a key.
        test_open_fail(|| new_connection("../fixtures/v1encrypted.edb"));
    }

    #[test]
    #[cfg(feature = "sqlcipher")]
    fn test_sqlcipher_requires_correct_key() {
        // Use a key, but the wrong one.
        test_open_fail(|| new_connection_with_key("../fixtures/v1encrypted.edb", "wrong key"));
    }

    #[test]
    #[cfg(feature = "sqlcipher")]
    fn test_sqlcipher_some_bundles() {
        let sqlite = new_connection_with_key("", "hunter2").expect("Failed to create encrypted connection");
        // Run a basic test as a sanity check.
        run_test_add(TestConn::with_sqlite(sqlite));
    }
}
