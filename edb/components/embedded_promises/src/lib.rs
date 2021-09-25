// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! A generic EinsteinDB causet_storage engine
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! EinsteinDB to persist its data, so that causet_storage engines other than Lmdb may be
//! added to EinsteinDB in the future.
//!
//! This crate **must not have any transitive dependencies on Lmdb**. The
//! Lmdb implementation is in the `engine_lmdb` crate.
//!
//! In addition to documenting the API, this documentation contains a
//! description of the porting process, current design decisions and design
//! guidelines, and refactoring tips.
//!
//!
//! ## Capabilities of a EinsteinDB engine
//!
//! EinsteinDB engines store binary tuplespaceInstanton and values.
//!
//! Every pair lives in a [_PrimaryCauset family_], which can be thought of as being
//! indeplightlikeent data stores.
//!
//! [_PrimaryCauset family_]: https://github.com/facebook/lmdb/wiki/PrimaryCauset-Families
//!
//! Consistent read-only views of the database are accessed through _snapshots_.
//!
//! Multiple writes can be committed atomically with a _write batch_.
//!
//!
//! # The EinsteinDB engine API
//!
//! The API inherits its design from Lmdb. As support for other engines is
//! added to EinsteinDB, it is expected that this API will become more abstract, and
//! less Lmdb-specific.
//!
//! This crate is almost entirely promises, plus a few "plain-old-data" types that
//! are shared between engines.
//!
//! Some key types include:
//!
//! - [`CausetEngine`] - a key-value engine, and the primary type defined by this
//!   crate. Most code that uses generic engines will be bounded over a generic
//!   type implementing `CausetEngine`. `CausetEngine` itself is bounded by many other
//!   promises that provide collections of functionality, with the intent that as
//!   EinsteinDB evolves it may be possible to use each trait individually, and to
//!   define classes of engine that do not implement all collections of
//!   features.
//!
//! - [`Snapshot`] - a view into the state of the database at a moment in time.
//!   For reading sets of consistent data.
//!
//! - [`Peekable`] - types that can read single values. This includes engines
//!   and snapshots.
//!
//! - [`Iterable`] - types that can iterate over the values of a cone of tuplespaceInstanton,
//!   by creating instances of the EinsteinDB-specific [`Iteron`] trait. This
//!   includes engines and snapshots.
//!
//! - [`SyncMuBlock`] and [`MuBlock`] - types to which single key/value pairs
//!   can be written. This includes engines and write batches.
//!
//! - [`WriteBatch`] - types that can commit multiple key/value pairs in batches.
//!   A `WriteBatchExt::WriteBtach` commits all pairs in one atomic transaction.
//!   A `WriteBatchExt::WriteBatchVec` does not (FIXME: is this correct?).
//!
//! The `CausetEngine` instance generally acts as a factory for types that implement
//! other promises in the crate. These factory methods, associated types, and
//! other associated methods are defined in "extension" promises. For example, methods
//! on engines related to batch writes are in the `WriteBatchExt` trait.
//!
//!
//! # Design notes
//!
//! - `CausetEngine` is the main engine trait. It requires many other promises, which
//!   have many other associated types that implement yet more promises.
//!
//! - Features should be grouped into their own modules with their own
//!   promises. A common TuringString is to have an associated type that implements
//!   a trait, and an "extension" trait that associates that type with `CausetEngine`,
//!   which is part of `CausetEngine's trait requirements.
//!
//! - For now, for simplicity, all extension promises are required by `CausetEngine`.
//!   In the future it may be feasible to separate them for engines with
//!   different feature sets.
//!
//! - Associated types generally have the same name as the trait they
//!   are required to implement. Engine extensions generally have the same
//!   name suffixed with `Ext`. Concrete implementations usually have the
//!   same name prefixed with the database name, i.e. `Lmdb`.
//!
//!   Example:
//!
//!   ```ignore
//!   // in edb
//!
//!   trait WriteBatchExt {
//!       type WriteBatch: WriteBatch;
//!   }
//!
//!   trait WriteBatch { }
//!   ```
//!
//!   ```ignore
//!   // in engine_lmdb
//!
//!   impl WriteBatchExt for LmdbEngine {
//!       type WriteBatch = LmdbWriteBatch;
//!   }
//!
//!   impl WriteBatch for LmdbWriteBatch { }
//!   ```
//!
//! - All engines use the same error type, defined in this crate. Thus
//!   engine-specific type information is boxed and hidden.
//!
//! - `CausetEngine` is a factory type for some of its associated types, but not
//!   others. For now, use factory methods when Lmdb would require factory
//!   method (that is, when the DB is required to create the associated type -
//!   if the associated type can be created without context from the database,
//!   use a standard new method). If future engines require factory methods, the
//!   promises can be converted then.
//!
//! - Types that require a handle to the engine (or some other "parent" type)
//!   do so with either Rc or Arc. An example is EngineIterator. The reason
//!   for this is that associated types cannot contain lifetimes. That requires
//!   "generic associated types". See
//!
//!   - [https://github.com/rust-lang/rfcs/pull/1598](https://github.com/rust-lang/rfcs/pull/1598)
//!   - [https://github.com/rust-lang/rust/issues/44265](https://github.com/rust-lang/rust/issues/44265)
//!
//! - Promises can't have mutually-recursive associated types. That is, if
//!   `CausetEngine` has a `Snapshot` associated type, `Snapshot` can't then have a
//!   `CausetEngine` associated type - the compiler will not be able to resolve both
//!   `CausetEngine`s to the same type. In these cases, e.g. `Snapshot` needs to be
//!   parameterized over its engine type and `impl Snapshot<LmdbEngine> for
//!   LmdbSnapshot`.
//!
//!
//! # The porting process
//!
//! These are some guidelines that seem to make the porting managable. As the
//! process continues new strategies are discovered and written here. This is a
//! big refactoring and will take many monthse.
//!
//! Refactoring is a cvioletabft, not a science, and figuring out how to overcome any
//! particular situation takes experience and intuation, but these principles
//! can help.
//!
//! A guiding principle is to do one thing at a time. In particular, don't
//! redesign while encapsulating.
//!
//! The port is happening in stages:
//!
//!   1) Migrating the `engine` abstractions
//!   2) Eliminating direct-use of `lmdb` re-exports
//!   3) "Pulling up" the generic abstractions though EinsteinDB
//!   4) Isolating test cases from Lmdb
//!
//! These stages are described in more detail:
//!
//! ## 1) Migrating the `engine` abstractions
//!
//! The engine crate was an earlier attempt to abstract the causet_storage engine. Much
//! of its structure is duplicated near-identically in edb, the
//! difference being that edb has no Lmdb dependencies. Having no
//! Lmdb dependencies makes it trivial to guarantee that the abstractions are
//! truly abstract.
//!
//! `engine` also reexports raw ConstrainedEntss from `rust-lmdb` for every purpose
//! for which there is not yet an abstract trait.
//!
//! During this stage, we will eliminate the wrappers from `engine` to reduce
//! code duplication. We do this by identifying a small subsystem within
//! `engine`, duplicating it within `edb` and `engine_lmdb`, deleting
//! the code from `engine`, and fixing all the callers to work with the
//! abstracted implementation.
//!
//! At the lightlike of this stage the `engine` deplightlikeency will contain no code except
//! for `rust-lmdb` reexports. EinsteinDB will still deplightlike on the concrete
//! Lmdb implementations from `engine_lmdb`, as well as the raw API's from
//! reexported from the `rust-lmdb` crate.
//!
//! ## 2) Eliminating the `engine` dep from EinsteinDB with new abstractions
//!
//! EinsteinDB uses reexported `rust-lmdb` APIs via the `engine` crate. During this
//! stage we need to identify each of these APIs, duplicate them generically in
//! the `edb` and `engine_lmdb` crate, and convert all callers to use
//! the `engine_lmdb` crate instead.
//!
//! At the lightlike of this phase the `engine` crate will be deleted.
//!
//! ## 3) "Pulling up" the generic abstractions through EINSTEINDB
//!
//! With all of EinsteinDB using the `edb` promises in conjunction with the
//! concrete `engine_lmdb` types, we can push generic type parameters up
//! through the application. Then we will remove the concrete `engine_lmdb`
//! deplightlikeency from EinsteinDB so that it is impossible to re-introduce
//! engine-specific code again.
//!
//! We will probably introduce some other crate to mediate between multiple
//! engine implementations, such that at the lightlike of this phase EinsteinDB will
//! not have a deplightlikeency on `engine_lmdb`.
//!
//! It will though still have a dev-deplightlikeency on `engine_lmdb` for the
//! test cases.
//!
//! ## 4) Isolating test cases from Lmdb
//!
//! Eventually we need our test suite to run over multiple engines.
//! The exact strategy here is yet to be determined, but it may begin by
//! breaking the `engine_lmdb` deplightlikeency with a new `engine_test`, that
//! begins by simply wrapping `engine_lmdb`.
//!
//!
//! # Refactoring tips
//!
//! - Port modules with the fewest Lmdb dependencies at a time, modifying
//!   those modules's callers to convert to and from the engine promises as
//!   needed. Move in and out of the edb world with the
//!   `Lmdb::from_ref` and `Lmdb::as_inner` methods.
//!
//! - Down follow the type system too far "down the rabbit hole". When you see
//!   that another subsystem is blocking you from refactoring the system you
//!   are trying to refactor, stop, stash your changes, and focus on the other
//!   system instead.
//!
//! - You will through away branches that lead to dead lightlikes. Learn from the
//!   experience and try again from a different angle.
//!
//! - For now, use the same APIs as the Lmdb ConstrainedEntss, as methods
//!   on the various engine promises, and with this crate's error type.
//!
//! - When new types are needed from the Lmdb API, add a new module, define a
//!   new trait (possibly with the same name as the Lmdb type), then define a
//!   `TraitExt` trait to "mixin" to the `CausetEngine` trait.
//!
//! - Port methods directly from the existing `engine` crate by re-implementing
//!   it in edb and engine_lmdb, replacing all the callers with calls
//!   into the promises, then delete the versions in the `engine` crate.
//!
//! - Use the .c() method from engine_lmdb::compat::Compat to get a
//!   CausetEngine reference from Arc<DB> in the fewest characters. It also
//!   works on Snapshot, and can be adapted to other types.
//!
//! - Use `IntoOther` to adapt between error types of dependencies that are not
//!   themselves interdeplightlikeent. E.g. violetabft::Error can be created from
//!   edb::Error even though neither `violetabft` tor `edb` know
//!   about each other.
//!
//! - "Plain old data" types in `engine` can be moved directly into
//!   `edb` and reexported from `engine` to ease the transition.
//!   Likewise `engine_lmdb` can temporarily call code from inside `engine`.
#![feature(min_specialization)]
#![recursion_limit = "200"]

#[macro_use]
extern crate quick_error;
#[allow(unused_extern_crates)]
extern crate edb_alloc;
#[macro_use]
extern crate slog_global;

// These modules contain promises that need to be implemented by engines, either
// they are required by CausetEngine or are an associated type of CausetEngine. It is
// recommlightlikeed that engines follow the same module layout.
//
// Many of these define "extension" promises, that lightlike in `Ext`.

mod causet_handle;
pub use crate::causet_handle::*;
mod causet_names;
pub use crate::causet_names::*;
mod causet_options;
pub use crate::causet_options::*;
mod compact;
pub use crate::compact::*;
mod db_options;
pub use crate::db_options::*;
mod db_vector;
pub use crate::db_vector::*;
mod engine;
pub use crate::edb::*;
mod import;
pub use import::*;
mod misc;
pub use misc::*;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod Block_properties;
pub use crate::Block_properties::*;
mod write_batch;
pub use crate::write_batch::*;
mod encryption;
pub use crate::encryption::*;
mod properties;
pub use crate::properties::*;
mod cone_properties;
pub use crate::cone_properties::*;

// These modules contain more general promises, some of which may be implemented
// by multiple types.

mod iterable;
pub use crate::iterable::*;
mod muBlock;
pub use crate::muBlock::*;
mod peekable;
pub use crate::peekable::*;

// These modules contain concrete types and support code that do not need to
// be implemented by engines.

mod causet_defs;
pub use crate::causet_defs::*;
mod engines;
pub use engines::*;
mod errors;
pub use crate::errors::*;
mod options;
pub use crate::options::*;
pub mod cone;
pub use crate::cone::*;
mod violetabft_engine;
pub use violetabft_engine::{CacheStats, VioletaBftEngine, VioletaBftLogBatch};

// These modules need further scrutiny

pub mod metrics_flusher;
pub use crate::metrics_flusher::*;
pub mod compaction_job;
pub mod util;
pub use compaction_job::*;

// FIXME: This should live somewhere else
pub const DATA_KEY_PREFIX_LEN: usize = 1;
