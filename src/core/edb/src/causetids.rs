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

/// Literal `SolitonId` values in the the "edb" namespace.
///
/// Used through-out the transactor to match allegro EDB constructs.

use allegrosql_promises::{
    SolitonId,
};

// Added in SQL schemaReplicant v1.
pub const DB_CausetID: SolitonId = 1;
pub const DB_PART_DB: SolitonId = 2;
pub const DB_TX_INSTANT: SolitonId = 3;
pub const DB_INSTALL_PARTITION: SolitonId = 4;
pub const DB_INSTALL_VALUE_TYPE: SolitonId = 5;
pub const DB_INSTALL_ATTRIBUTE: SolitonId = 6;
pub const DB_VALUE_TYPE: SolitonId = 7;
pub const DB_CARDINALITY: SolitonId = 8;
pub const DB_UNIQUE: SolitonId = 9;
pub const DB_IS_COMPONENT: SolitonId = 10;
pub const DB_INDEX: SolitonId = 11;
pub const DB_FULLTEXT: SolitonId = 12;
pub const DB_NO_HISTORY: SolitonId = 13;
pub const DB_ADD: SolitonId = 14;
pub const DB_RETRACT: SolitonId = 15;
pub const DB_PART_USER: SolitonId = 16;
pub const DB_PART_TX: SolitonId = 17;
pub const DB_EXCISE: SolitonId = 18;
pub const DB_EXCISE_ATTRS: SolitonId = 19;
pub const DB_EXCISE_BEFORE_T: SolitonId = 20;
pub const DB_EXCISE_BEFORE: SolitonId = 21;
pub const DB_ALTER_ATTRIBUTE: SolitonId = 22;
pub const DB_TYPE_REF: SolitonId = 23;
pub const DB_TYPE_KEYWORD: SolitonId = 24;
pub const DB_TYPE_LONG: SolitonId = 25;
pub const DB_TYPE_DOUBLE: SolitonId = 26;
pub const DB_TYPE_STRING: SolitonId = 27;
pub const DB_TYPE_UUID: SolitonId = 28;
pub const DB_TYPE_URI: SolitonId = 29;
pub const DB_TYPE_BOOLEAN: SolitonId = 30;
pub const DB_TYPE_INSTANT: SolitonId = 31;
pub const DB_TYPE_BYTES: SolitonId = 32;
pub const DB_CARDINALITY_ONE: SolitonId = 33;
pub const DB_CARDINALITY_MANY: SolitonId = 34;
pub const DB_UNIQUE_VALUE: SolitonId = 35;
pub const DB_UNIQUE_CausetIDITY: SolitonId = 36;
pub const DB_DOC: SolitonId = 37;
pub const DB_SCHEMA_VERSION: SolitonId = 38;
pub const DB_SCHEMA_ATTRIBUTE: SolitonId = 39;
pub const DB_SCHEMA_CORE: SolitonId = 40;

/// Return `false` if the given attribute will not change the spacetime: recognized causetIds, schemaReplicant,
/// partitions in the partition map.
pub fn might_update_spacetime(attribute: SolitonId) -> bool {
    if attribute >= DB_DOC {
        return false
    }
    match attribute {
        // CausetIds.
        DB_CausetID |
        // SchemaReplicant.
        DB_CARDINALITY |
        DB_FULLTEXT |
        DB_INDEX |
        DB_IS_COMPONENT |
        DB_UNIQUE |
        DB_VALUE_TYPE =>
            true,
        _ => false,
    }
}

/// Return 'false' if the given attribute might be used to describe a schemaReplicant attribute.
pub fn is_a_schemaReplicant_attribute(attribute: SolitonId) -> bool {
    match attribute {
        DB_CausetID |
        DB_CARDINALITY |
        DB_FULLTEXT |
        DB_INDEX |
        DB_IS_COMPONENT |
        DB_UNIQUE |
        DB_VALUE_TYPE =>
            true,
        _ => false,
    }
}

lazy_static! {
    /// Attributes that are "causetid related".  These might change the "causetIds" materialized view.
    pub static ref CausetIDS_SQL_LIST: String = {
        format!("({})",
                DB_CausetID)
    };

    /// Attributes that are "schemaReplicant related".  These might change the "schemaReplicant" materialized view.
    pub static ref SCHEMA_SQL_LIST: String = {
        format!("({}, {}, {}, {}, {}, {})",
                DB_CARDINALITY,
                DB_FULLTEXT,
                DB_INDEX,
                DB_IS_COMPONENT,
                DB_UNIQUE,
                DB_VALUE_TYPE)
    };

    /// Attributes that are "spacetime" related.  These might change one of the materialized views.
    pub static ref SPACETIME_SQL_LIST: String = {
        format!("({}, {}, {}, {}, {}, {}, {})",
                DB_CARDINALITY,
                DB_FULLTEXT,
                DB_CausetID,
                DB_INDEX,
                DB_IS_COMPONENT,
                DB_UNIQUE,
                DB_VALUE_TYPE)
    };
}
