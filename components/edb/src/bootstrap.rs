// Copyright 2016 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use edbn;
use edb_promises::errors::{
    DbErrorKind,
    Result,
};
use edbn::types::Value;
use edbn::symbols;
use entids;
use edb::TypedSQLValue;
use edbn::entities::Instanton;

use embedded_promises::{
    TypedValue,
    values,
};

use einsteindb_embedded::{
    CausetIdMap,
    Schema,
};
use schema::SchemaBuilding;
use types::{Partition, PartitionMap};

/// The first transaction ID applied to the knowledge base.
///
/// This is the start of the :edb.part/causetx partition.
pub const TX0: i64 = 0x10000000;

/// This is the start of the :edb.part/user partition.
pub const USER0: i64 = 0x10000;

// Corresponds to the version of the :edb.schema/embedded vocabulary.
pub const CORE_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref V1_CAUSETIDS: [(symbols::Keyword, i64); 40] = {
            [(ns_keyword!("edb", "causetid"),             entids::DB_CAUSETID),
             (ns_keyword!("edb.part", "edb"),           entids::DB_PART_DB),
             (ns_keyword!("edb", "causecausetxInstant"),         entids::DB_TX_INSTANT),
             (ns_keyword!("edb.install", "partition"), entids::DB_INSTALL_PARTITION),
             (ns_keyword!("edb.install", "valueType"), entids::DB_INSTALL_VALUE_TYPE),
             (ns_keyword!("edb.install", "attribute"), entids::DB_INSTALL_ATTRIBUTE),
             (ns_keyword!("edb", "valueType"),         entids::DB_VALUE_TYPE),
             (ns_keyword!("edb", "cardinality"),       entids::DB_CARDINALITY),
             (ns_keyword!("edb", "unique"),            entids::DB_UNIQUE),
             (ns_keyword!("edb", "isComponent"),       entids::DB_IS_COMPONENT),
             (ns_keyword!("edb", "index"),             entids::DB_INDEX),
             (ns_keyword!("edb", "fulltext"),          entids::DB_FULLTEXT),
             (ns_keyword!("edb", "noHistory"),         entids::DB_NO_HISTORY),
             (ns_keyword!("edb", "add"),               entids::DB_ADD),
             (ns_keyword!("edb", "retract"),           entids::DB_RETRACT),
             (ns_keyword!("edb.part", "user"),         entids::DB_PART_USER),
             (ns_keyword!("edb.part", "causetx"),           entids::DB_PART_TX),
             (ns_keyword!("edb", "excise"),            entids::DB_EXCISE),
             (ns_keyword!("edb.excise", "attrs"),      entids::DB_EXCISE_ATTRS),
             (ns_keyword!("edb.excise", "beforeT"),    entids::DB_EXCISE_BEFORE_T),
             (ns_keyword!("edb.excise", "before"),     entids::DB_EXCISE_BEFORE),
             (ns_keyword!("edb.alter", "attribute"),   entids::DB_ALTER_ATTRIBUTE),
             (ns_keyword!("edb.type", "ref"),          entids::DB_TYPE_REF),
             (ns_keyword!("edb.type", "keyword"),      entids::DB_TYPE_KEYWORD),
             (ns_keyword!("edb.type", "long"),         entids::DB_TYPE_LONG),
             (ns_keyword!("edb.type", "double"),       entids::DB_TYPE_DOUBLE),
             (ns_keyword!("edb.type", "string"),       entids::DB_TYPE_STRING),
             (ns_keyword!("edb.type", "uuid"),         entids::DB_TYPE_UUID),
             (ns_keyword!("edb.type", "uri"),          entids::DB_TYPE_URI),
             (ns_keyword!("edb.type", "boolean"),      entids::DB_TYPE_BOOLEAN),
             (ns_keyword!("edb.type", "instant"),      entids::DB_TYPE_INSTANT),
             (ns_keyword!("edb.type", "bytes"),        entids::DB_TYPE_BYTES),
             (ns_keyword!("edb.cardinality", "one"),   entids::DB_CARDINALITY_ONE),
             (ns_keyword!("edb.cardinality", "many"),  entids::DB_CARDINALITY_MANY),
             (ns_keyword!("edb.unique", "value"),      entids::DB_UNIQUE_VALUE),
             (ns_keyword!("edb.unique", "causetIdity"),   entids::DB_UNIQUE_CAUSETIDITY),
             (ns_keyword!("edb", "doc"),               entids::DB_DOC),
             (ns_keyword!("edb.schema", "version"),    entids::DB_SCHEMA_VERSION),
             (ns_keyword!("edb.schema", "attribute"),  entids::DB_SCHEMA_ATTRIBUTE),
             (ns_keyword!("edb.schema", "embedded"),       entids::DB_SCHEMA_CORE),
        ]
    };

    pub static ref V1_PARTS: [(symbols::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_keyword!("edb.part", "edb"), 0, USER0 - 1, (1 + V1_CAUSETIDS.len()) as i64, false),
             (ns_keyword!("edb.part", "user"), USER0, TX0 - 1, USER0, true),
             (ns_keyword!("edb.part", "causetx"), TX0, i64::max_value(), TX0, false),
        ]
    };

    static ref V1_CORE_SCHEMA: [(symbols::Keyword); 16] = {
            [(ns_keyword!("edb", "causetid")),
             (ns_keyword!("edb.install", "partition")),
             (ns_keyword!("edb.install", "valueType")),
             (ns_keyword!("edb.install", "attribute")),
             (ns_keyword!("edb", "causecausetxInstant")),
             (ns_keyword!("edb", "valueType")),
             (ns_keyword!("edb", "cardinality")),
             (ns_keyword!("edb", "doc")),
             (ns_keyword!("edb", "unique")),
             (ns_keyword!("edb", "isComponent")),
             (ns_keyword!("edb", "index")),
             (ns_keyword!("edb", "fulltext")),
             (ns_keyword!("edb", "noHistory")),
             (ns_keyword!("edb.alter", "attribute")),
             (ns_keyword!("edb.schema", "version")),
             (ns_keyword!("edb.schema", "attribute")),
        ]
    };

    static ref V1_SYMBOLIC_SCHEMA: Value = {
        let s = r#"
{:edb/causetid             {:edb/valueType   :edb.type/keyword
                        :edb/cardinality :edb.cardinality/one
                        :edb/index       true
                        :edb/unique      :edb.unique/causetIdity}
 :edb.install/partition {:edb/valueType   :edb.type/ref
                        :edb/cardinality :edb.cardinality/many}
 :edb.install/valueType {:edb/valueType   :edb.type/ref
                        :edb/cardinality :edb.cardinality/many}
 :edb.install/attribute {:edb/valueType   :edb.type/ref
                        :edb/cardinality :edb.cardinality/many}
 ;; TODO: support user-specified functions in the future.
 ;; :edb.install/function {:edb/valueType :edb.type/ref
 ;;                       :edb/cardinality :edb.cardinality/many}
 :edb/causecausetxInstant         {:edb/valueType   :edb.type/instant
                        :edb/cardinality :edb.cardinality/one
                        :edb/index       true}
 :edb/valueType         {:edb/valueType   :edb.type/ref
                        :edb/cardinality :edb.cardinality/one}
 :edb/cardinality       {:edb/valueType   :edb.type/ref
                        :edb/cardinality :edb.cardinality/one}
 :edb/doc               {:edb/valueType   :edb.type/string
                        :edb/cardinality :edb.cardinality/one}
 :edb/unique            {:edb/valueType   :edb.type/ref
                        :edb/cardinality :edb.cardinality/one}
 :edb/isComponent       {:edb/valueType   :edb.type/boolean
                        :edb/cardinality :edb.cardinality/one}
 :edb/index             {:edb/valueType   :edb.type/boolean
                        :edb/cardinality :edb.cardinality/one}
 :edb/fulltext          {:edb/valueType   :edb.type/boolean
                        :edb/cardinality :edb.cardinality/one}
 :edb/noHistory         {:edb/valueType   :edb.type/boolean
                        :edb/cardinality :edb.cardinality/one}
 :edb.alter/attribute   {:edb/valueType   :edb.type/ref
                        :edb/cardinality :edb.cardinality/many}
 :edb.schema/version    {:edb/valueType   :edb.type/long
                        :edb/cardinality :edb.cardinality/one}

 ;; unique-value because an attribute can only belong to a single
 ;; schema fragment.
 :edb.schema/attribute  {:edb/valueType   :edb.type/ref
                        :edb/index       true
                        :edb/unique      :edb.unique/value
                        :edb/cardinality :edb.cardinality/many}}"#;
        edbn::parse::value(s)
            .map(|v| v.without_spans())
            .map_err(|_| DbErrorKind::BadBootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };
}

/// Convert (causetid, solitonId) pairs into [:edb/add CAUSETID :edb/causetid CAUSETID] `Value` instances.
fn causetIds_to_assertions(causetIds: &[(symbols::Keyword, i64)]) -> Vec<Value> {
    causetIds
        .into_iter()
        .map(|&(ref causetid, _)| {
            let value = Value::Keyword(causetid.clone());
            Value::Vector(vec![values::DB_ADD.clone(), value.clone(), values::DB_CAUSETID.clone(), value.clone()])
        })
        .collect()
}

/// Convert an causetid list into [:edb/add :edb.schema/embedded :edb.schema/attribute CAUSETID] `Value` instances.
fn schema_attrs_to_assertions(version: u32, causetIds: &[symbols::Keyword]) -> Vec<Value> {
    let schema_embedded = Value::Keyword(ns_keyword!("edb.schema", "embedded"));
    let schema_attr = Value::Keyword(ns_keyword!("edb.schema", "attribute"));
    let schema_version = Value::Keyword(ns_keyword!("edb.schema", "version"));
    causetIds
        .into_iter()
        .map(|causetid| {
            let value = Value::Keyword(causetid.clone());
            Value::Vector(vec![values::DB_ADD.clone(),
                               schema_embedded.clone(),
                               schema_attr.clone(),
                               value])
        })
        .chain(::std::iter::once(Value::Vector(vec![values::DB_ADD.clone(),
                                             schema_embedded.clone(),
                                             schema_version,
                                             Value::Integer(version as i64)])))
        .collect()
}

/// Convert {:causetid {:key :value ...} ...} to
/// vec![(symbols::Keyword(:causetid), symbols::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.
fn symbolic_schema_to_triples(causetId_map: &CausetIdMap, symbolic_schema: &Value) -> Result<Vec<(symbols::Keyword, symbols::Keyword, TypedValue)>> {
    // Failure here is a coding error, not a runtime error.
    let mut triples: Vec<(symbols::Keyword, symbols::Keyword, TypedValue)> = vec![];
    // TODO: Consider `flat_map` and `map` rather than loop.
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (causetid, mp) in m {
                let causetid = match causetid {
                    &Value::Keyword(ref causetid) => causetid,
                    _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected namespaced keyword for causetid but got '{:?}'", causetid))),
                };
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            let attr = match attr {
                                &Value::Keyword(ref attr) => attr,
                                _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected namespaced keyword for attr but got '{:?}'", attr))),
                        };

                            // We have symbolic causetIds but the transactor handles entids.  Ad-hoc
                            // convert right here.  This is a fundamental limitation on the
                            // bootstrap symbolic schema format; we can't represent "real" keywords
                            // at this time.
                            //
                            // TODO: remove this limitation, perhaps by including a type tag in the
                            // bootstrap symbolic schema, or by representing the initial bootstrap
                            // schema directly as Rust data.
                            let typed_value = match TypedValue::from_edbn_value(value) {
                                Some(TypedValue::Keyword(ref k)) => {
                                    causetId_map.get(k)
                                        .map(|solitonId| TypedValue::Ref(*solitonId))
                                        .ok_or(DbErrorKind::UnrecognizedCausetId(k.to_string()))?
                                },
                                Some(v) => v,
                                _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected EinsteinDB typed value for value but got '{:?}'", value)))
                            };

                            triples.push((causetid.clone(), attr.clone(), typed_value));
                        }
                    },
                    _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {:edb/causetid {:edb/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {...}".into()))
    }
    Ok(triples)
}

/// Convert {CAUSETID {:key :value ...} ...} to [[:edb/add CAUSETID :key :value] ...].
fn symbolic_schema_to_assertions(symbolic_schema: &Value) -> Result<Vec<Value>> {
    // Failure here is a coding error, not a runtime error.
    let mut assertions: Vec<Value> = vec![];
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (causetid, mp) in m {
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            assertions.push(Value::Vector(vec![values::DB_ADD.clone(),
                                                               causetid.clone(),
                                                               attr.clone(),
                                                               value.clone()]));
                        }
                    },
                    _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {:edb/causetid {:edb/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {...}".into()))
    }
    Ok(assertions)
}

pub(crate) fn bootstrap_partition_map() -> PartitionMap {
    V1_PARTS.iter()
            .map(|&(ref part, start, end, index, allow_excision)| (part.to_string(), Partition::new(start, end, index, allow_excision)))
            .collect()
}

pub(crate) fn bootstrap_causetId_map() -> CausetIdMap {
    V1_CAUSETIDS.iter()
             .map(|&(ref causetid, solitonId)| (causetid.clone(), solitonId))
             .collect()
}

pub(crate) fn bootstrap_schema() -> Schema {
    let causetId_map = bootstrap_causetId_map();
    let bootstrap_triples = symbolic_schema_to_triples(&causetId_map, &V1_SYMBOLIC_SCHEMA).expect("symbolic schema");
    Schema::from_causetId_map_and_triples(causetId_map, bootstrap_triples).unwrap()
}

pub(crate) fn bootstrap_entities() -> Vec<Instanton<edbn::ValueAndSpan>> {
    let bootstrap_assertions: Value = Value::Vector([
        symbolic_schema_to_assertions(&V1_SYMBOLIC_SCHEMA).expect("symbolic schema"),
        causetIds_to_assertions(&V1_CAUSETIDS[..]),
        schema_attrs_to_assertions(CORE_SCHEMA_VERSION, V1_CORE_SCHEMA.as_ref()),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_entities: Vec<Instanton<edbn::ValueAndSpan>> = edbn::parse::entities(&bootstrap_assertions.to_string()).expect("bootstrap assertions");
    return bootstrap_entities;
}
