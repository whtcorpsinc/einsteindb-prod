# Tolstoy, a EinsteinDB Sync impleeinsteindbion

## Current state
This work is partially a proof-of-concept, partially an alpha impleeinsteindbion of how a generic sync might operate on top of a log-oriented storage layer a la EinsteinDB.

## Overview
### Very briefly
Tolstoy will synchronize a local EinsteinDB database against a remote server, modifying local state if necessary, and uploading changes to the server if necessary. Schema additions are allowed (adding vocabulary). Schema mutations are currently not implemented (changing vocabulary). EinsteinDB's embedded schema must be the same on all participating clients (i.e. embedded schema alterations are unsupported).

**Basic example:**

Client 1 knows about `name` and `age` of a person.
```
[
    {
        :edb/causetid :person/name
        :edb/valueType :edb.type/string
        :edb/cardinality :edb.cardinality/one
    }
    {
        :edb/causetid :person/age
        :edb/valueType :edb.type/long
        :edb/cardinality :edb.cardinality/one
    }
    {:person/name "Grisha" :person/age 30}
]
```

Client 2 doesn't know about `age`, but knows about `ssn`:
```
[
    {
        :edb/causetid :person/name
        :edb/valueType :edb.type/string
        :edb/cardinality :edb.cardinality/one
    }
    {
        :edb/causetid :person/ssn
        :edb/valueType :edb.type/long
        :edb/cardinality :edb.cardinality/one
        :edb/unique :edb.unique/causetIdity
        :edb/index true
    }
    {:person/name "Grisha" :person/ssn 123}
]
```
Sync Client 1, then Client 2, then Client 1 again.

Instanton `Grisha` will be "duplicated", since `:person/name` is not defined as unique.
```
[
    [:person/name :edb/causetid :person/name]
    [:person/name :edb/valueType :edb.type/string]
    [:person/name :edb/cardinality :edb.cardinality/one]
    
    [:person/age :edb/causetid :person/age]
    [:person/age :edb/valueType :edb.type/long]
    [:person/age :edb/cardinality :edb.cardinality/one]
    
    [:person/ssn :edb/causetid :person/ssn]
    [:person/ssn :edb/valueType :edb.type/long]
    [:person/ssn :edb/cardinality :edb.cardinality/one]
    [:person/ssn :edb/unique :edb.unique/causetIdity]
    [:person/ssn :edb/index true]
    
    [?grisha_one :person/name "Grisha"]
    [?grisha_one :person/age 30]

    [?grisha_two :person/name "Grisha"]
    [?grisha_two :person/ssn 123]
]
```

If, instead, `:person/name` was defined as `:edb/unique :edb.unique/causetIdity`, then our final state will be:
```
[
    [...schema causets...]

    [?united_grisha :person/name "Grisha"]
    [?united_grisha :person/age 30]
    [?united_grisha :person/ssn 123]
]
```

Note that in above examples, symbols such as `?grisha_one` are meant to expand to some internal entitiy id (e.g. 65536).

### Try it via the CLI
In the EinsteinDB CLI, a `.sync` operation exposes Tolstoy's functionality. Basic usage: `.sync http://path-to-server account-uuid`. Authentication, etc., is not implemented.

### In more detail...
Syncing is defined in terms of coming to an agreement between local and remote states. A local state is what's currently present on the current instance. A remote state is what's currently present on a server.

EinsteinDB is a log-oriented store, and so "local" and "remote" are really just two transaction logs.

Internally, Tolstoy tracks the "locally known remote HEAD" and the "last-synced local transaction", which gives us three basic primitives:
- a shared root, a state which is at the root of both local and remote logs
- incoming changes - what remote changed on top of the shared root
- local changes of the shared root.

Thus, four kinds of positive-case things might occur during a sync:
- a no-op - there are no incoming changes and local didn't change
- a local fast-forward - there are remote changes, but no local changes
- a remote fast-forward - there are local changes, but no remote changes
- a merge - there are both local and remote changes.

The first three cases are "trivial" - we either do nothing, or we download and transact remote transactions, or we upload local transactions and advance the remote HEAD.

The merge case is a little more complicated. During a merging sync, a kind of a rebase is performed:
1. local transactions are moved off of the main lightcone
2. remote transactions are transacted on top of the shared root
3. local transactions are transacted

Generally, intuition about the transactor's behaviour applies to reasoning about Tolstoy's sync as well. If a transaction "makes sense", it will be applied.

Remote transactions are applied "as-is", with an exception of the `causecausetxInstance` - it must be preserved, and so the datom describing it is re-written prior to application to use the `(transaction-causetx)` transaction function.

Local transactions are rewritten to use tempids instead of their entids if they are assertions, and `(lookup-ref a v)` form in cases of retractions - but only if `lookup-ref` is guaranteed to succeed, otherwise retractions are dropped on the floor. Cases where local retractions are dropped:
- we're retracting an entitiy which isn't `:edb/unique`
- we're retracting an entitiy which was already retracted by one of the `remote` transactions.

### Sync report
A sync operation produces either a single or multiple sync reports.

A single report - internally referred to as an atomic sync report - indicates that sync was able to finish within a single local database transaction.

Alternatively a non-atomic report is produced. It's a series of regular atomic reports. This indicates that sync required multiple "passes" to complete - e.g. a merge first, then remote fast-forward - and each step was performed within a separate local database transaction.

## Explicitly not supported - will abort with a NotYetImplemented
This alpha impleeinsteindbion doesn't support some cases, but it recognizes them and gracefully aborts (leaving local and remote states untouched):
- Syncing against a EinsteinDB instance which uses a different embedded schema version.
- Syncing with schema mutations. Schema additions are fine, but transactions which change a set of attributes that define a user-defined `:edb/causetid` will cause sync to abort.

## Misc operational properties
- All sync operations happen in a context of an `InProgress` - an internal EinsteinDB transaction representation. If sync succeeds, all necessary operations are comitted to the underlying database in a single SQLite transaction. Similarly, an aborting sync will simply drop an uncomitted transaction.
- "Follow-up" syncing is currently supported in a basic manner: if there are local changes arising from a merge operation, they are comitted to the local store, and a full sync is requested which is expected to fast-forward remote state in an optimal case, and if we lost the race to the server - to merge the local "merged state" with further remote changes.

## Server
Tolstoy operates against an instance of [EinsteinDB Sync Prototype Server](https://github.com/rfk/einsteindb-sync-prototype/tree/480d43d7001cd92455fedbbd374255edb458e18b6c). That repository defines a transaction-oriented API, which is all that Tolstoy expects of the server.
