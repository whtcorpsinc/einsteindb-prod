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

use edb;

/// The first transaction ID applied to the knowledge base.
///
/// This is the start of the :edb.part/causetx partition.
pub const TX0: i64 = 0x10000000;

/// This is the start of the :edb.part/user partition.
pub const USER0: i64 = 0x10000;

// Corresponds to the version of the :edb.schemaReplicant/allegro vocabulary.
pub const CORE_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref V1_CausetIDS: [(symbols::Keyword, i64); 40] = {
            [(ns_keyword!("edb", "causetid"),             causetids::DB_CausetID),
             (ns_keyword!("edb.part", "edb"),           causetids::DB_PART_DB),
             (ns_keyword!("edb", "causecausetxInstant"),         causetids::DB_TX_INSTANT),
             (ns_keyword!("edb.install", "partition"), causetids::DB_INSTALL_PARTITION),
             (ns_keyword!("edb.install", "valueType"), causetids::DB_INSTALL_VALUE_TYPE),
             (ns_keyword!("edb.install", "attribute"), causetids::DB_INSTALL_ATTRIBUTE),
             (ns_keyword!("edb", "valueType"),         causetids::DB_VALUE_TYPE),
             (ns_keyword!("edb", "cardinality"),       causetids::DB_CARDINALITY),
             (ns_keyword!("edb", "unique"),            causetids::DB_UNIQUE),
             (ns_keyword!("edb", "isComponent"),       causetids::DB_IS_COMPONENT),
             (ns_keyword!("edb", "index"),             causetids::DB_INDEX),
             (ns_keyword!("edb", "fulltext"),          causetids::DB_FULLTEXT),
             (ns_keyword!("edb", "noHistory"),         causetids::DB_NO_HISTORY),
             (ns_keyword!("edb", "add"),               causetids::DB_ADD),
             (ns_keyword!("edb", "retract"),           causetids::DB_RETRACT),
             (ns_keyword!("edb.part", "user"),         causetids::DB_PART_USER),
             (ns_keyword!("edb.part", "causetx"),           causetids::DB_PART_TX),
             (ns_keyword!("edb", "excise"),            causetids::DB_EXCISE),
             (ns_keyword!("edb.excise", "attrs"),      causetids::DB_EXCISE_ATTRS),
             (ns_keyword!("edb.excise", "beforeT"),    causetids::DB_EXCISE_BEFORE_T),
             (ns_keyword!("edb.excise", "before"),     causetids::DB_EXCISE_BEFORE),
             (ns_keyword!("edb.alter", "attribute"),   causetids::DB_ALTER_ATTRIBUTE),
             (ns_keyword!("edb.type", "ref"),          causetids::DB_TYPE_REF),
             (ns_keyword!("edb.type", "keyword"),      causetids::DB_TYPE_KEYWORD),
             (ns_keyword!("edb.type", "long"),         causetids::DB_TYPE_LONG),
             (ns_keyword!("edb.type", "double"),       causetids::DB_TYPE_DOUBLE),
             (ns_keyword!("edb.type", "string"),       causetids::DB_TYPE_STRING),
             (ns_keyword!("edb.type", "uuid"),         causetids::DB_TYPE_UUID),
             (ns_keyword!("edb.type", "uri"),          causetids::DB_TYPE_URI),
             (ns_keyword!("edb.type", "boolean"),      causetids::DB_TYPE_BOOLEAN),
             (ns_keyword!("edb.type", "instant"),      causetids::DB_TYPE_INSTANT),
             (ns_keyword!("edb.type", "bytes"),        causetids::DB_TYPE_BYTES),
             (ns_keyword!("edb.cardinality", "one"),   causetids::DB_CARDINALITY_ONE),
             (ns_keyword!("edb.cardinality", "many"),  causetids::DB_CARDINALITY_MANY),
             (ns_keyword!("edb.unique", "value"),      causetids::DB_UNIQUE_VALUE),
             (ns_keyword!("edb.unique", "causetIdity"),   causetids::DB_UNIQUE_CausetIDITY),
             (ns_keyword!("edb", "doc"),               causetids::DB_DOC),
             (ns_keyword!("edb.schemaReplicant", "version"),    causetids::DB_SCHEMA_VERSION),
             (ns_keyword!("edb.schemaReplicant", "attribute"),  causetids::DB_SCHEMA_ATTRIBUTE),
             (ns_keyword!("edb.schemaReplicant", "allegro"),       causetids::DB_SCHEMA_CORE),
        ]
    };

    pub static ref V1_PARTS: [(symbols::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_keyword!("edb.part", "edb"), 0, USER0 - 1, (1 + V1_CausetIDS.len()) as i64, false),
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
             (ns_keyword!("edb.schemaReplicant", "version")),
             (ns_keyword!("edb.schemaReplicant", "attribute")),
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
 :edb.schemaReplicant/version    {:edb/valueType   :edb.type/long
                        :edb/cardinality :edb.cardinality/one}

 ;; unique-value because an attribute can only belong to a single
 ;; schemaReplicant fragment.
 :edb.schemaReplicant/attribute  {:edb/valueType   :edb.type/ref
                        :edb/index       true
                        :edb/unique      :edb.unique/value
                        :edb/cardinality :edb.cardinality/many}}"#;
        edbn::parse::value(s)
            .map(|v| v.without_spans())
            .map_err(|_| DbErrorKind::BadBootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };
}

/// Convert (causetid, solitonId) pairs into [:edb/add CausetID :edb/causetid CausetID] `Value` instances.
fn causetIds_to_assertions(causetIds: &[(symbols::Keyword, i64)]) -> Vec<Value> {
    causetIds
        .into_iter()
        .map(|&(ref causetid, _)| {
            let value = Value::Keyword(causetid.clone());
            Value::Vector(vec![values::DB_ADD.clone(), value.clone(), values::DB_CausetID.clone(), value.clone()])
        })
        .collect()
}

/// Convert an causetid list into [:edb/add :edb.schemaReplicant/allegro :edb.schemaReplicant/attribute CausetID] `Value` instances.
fn schemaReplicant_attrs_to_assertions(version: u32, causetIds: &[symbols::Keyword]) -> Vec<Value> {
    let schemaReplicant_allegro = Value::Keyword(ns_keyword!("edb.schemaReplicant", "allegro"));
    let schemaReplicant_attr = Value::Keyword(ns_keyword!("edb.schemaReplicant", "attribute"));
    let schemaReplicant_version = Value::Keyword(ns_keyword!("edb.schemaReplicant", "version"));
    causetIds
        .into_iter()
        .map(|causetid| {
            let value = Value::Keyword(causetid.clone());
            Value::Vector(vec![values::DB_ADD.clone(),
                               schemaReplicant_allegro.clone(),
                               schemaReplicant_attr.clone(),
                               value])
        })
        .chain(::std::iter::once(Value::Vector(vec![values::DB_ADD.clone(),
                                             schemaReplicant_allegro.clone(),
                                             schemaReplicant_version,
                                             Value::Integer(version as i64)])))
        .collect()
}

/// Convert {:causetid {:key :value ...} ...} to
/// vec![(symbols::Keyword(:causetid), symbols::Keyword(:key), MinkowskiType(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.
fn symbolic_schemaReplicant_to_triples(causetId_map: &CausetIdMap, symbolic_schemaReplicant: &Value) -> Result<Vec<(symbols::Keyword, symbols::Keyword, MinkowskiType)>> {
    // Failure here is a coding error, not a runtime error.
    let mut triples: Vec<(symbols::Keyword, symbols::Keyword, MinkowskiType)> = vec![];
    // TODO: Consider `flat_map` and `map` rather than loop.
    match *symbolic_schemaReplicant {
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

                            // We have symbolic causetIds but the transactor handles causetids.  Ad-hoc
                            // convert right here.  This is a fundamental limitation on the
                            // bootstrap symbolic schemaReplicant format; we can't represent "real" keywords
                            // at this time.
                            //
                            // TODO: remove this limitation, perhaps by including a type tag in the
                            // bootstrap symbolic schemaReplicant, or by representing the initial bootstrap
                            // schemaReplicant directly as Rust data.
                            let typed_value = match MinkowskiType::from_edbn_value(value) {
                                Some(MinkowskiType::Keyword(ref k)) => {
                                    causetId_map.get(k)
                                        .map(|solitonId| MinkowskiType::Ref(*solitonId))
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

/// Convert {CausetID {:key :value ...} ...} to [[:edb/add CausetID :key :value] ...].
fn symbolic_schemaReplicant_to_assertions(symbolic_schemaReplicant: &Value) -> Result<Vec<Value>> {
    // Failure here is a coding error, not a runtime error.
    let mut assertions: Vec<Value> = vec![];
    match *symbolic_schemaReplicant {
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
    V1_CausetIDS.iter()
             .map(|&(ref causetid, solitonId)| (causetid.clone(), solitonId))
             .collect()
}

pub(crate) fn bootstrap_schemaReplicant() -> SchemaReplicant {
    let causetId_map = bootstrap_causetId_map();
    let bootstrap_triples = symbolic_schemaReplicant_to_triples(&causetId_map, &V1_SYMBOLIC_SCHEMA).expect("symbolic schemaReplicant");
    SchemaReplicant::from_causetId_map_and_triples(causetId_map, bootstrap_triples).unwrap()
}

pub(crate) fn bootstrap_entities() -> Vec<Instanton<edbn::ValueAndSpan>> {
    let bootstrap_assertions: Value = Value::Vector([
        symbolic_schemaReplicant_to_assertions(&V1_SYMBOLIC_SCHEMA).expect("symbolic schemaReplicant"),
        causetIds_to_assertions(&V1_CausetIDS[..]),
        schemaReplicant_attrs_to_assertions(CORE_SCHEMA_VERSION, V1_CORE_SCHEMA.as_ref()),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_entities: Vec<Instanton<edbn::ValueAndSpan>> = edbn::parse::entities(&bootstrap_assertions.to_string()).expect("bootstrap assertions");
    return bootstrap_entities;
}
