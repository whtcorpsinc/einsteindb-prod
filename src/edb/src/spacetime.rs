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

//! Most bundles can mutate the EinsteinDB spacetime by transacting assertions:
//!
//! - they can add (and, eventually, retract and alter) recognized causetIds using the `:edb/causetid`
//!   attribute;
//!
//! - they can add (and, eventually, retract and alter) schemaReplicant attributes using various `:edb/*`
//!   attributes;
//!
//! - eventually, they will be able to add (and possibly retract) solitonId partitions using a EinsteinDB
//!   equivalent (perhaps :edb/partition or :edb.partition/start) to Causetic's `:edb.install/partition`
//!   attribute.
//!
//! This module recognizes, validates, applies, and reports on these mutations.

use failure::ResultExt;

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;

use add_retract_alter_set::{
    AddRetractAlterSet,
};
use edbn::symbols;
use causetids;
use causetq_pull_promises::errors::{
    DbErrorKind,
    Result,
};

use allegrosql_promises::{
    attribute,
    SolitonId,
    MinkowskiType,
    MinkowskiValueType,
};

use causetq_allegrosql::{
    SchemaReplicant,
    AttributeMap,
};

use schemaReplicant::{
    AttributeBuilder,
    AttributeValidation,
};

use types::{
    EAV,
};

/// An alteration to an attribute.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributeAlteration {
    /// From http://blog.Causetic.com/2014/01/schemaReplicant-alteration.html:
    /// - rename attributes
    /// - rename your own programmatic causetIdities (uses of :edb/causetid)
    /// - add or remove indexes
    Index,
    /// - add or remove uniqueness constraints
    Unique,
    /// - change attribute cardinality
    Cardinality,
    /// - change whether history is retained for an attribute
    NoHistory,
    /// - change whether an attribute is treated as a component
    IsComponent,
}

/// An alteration to an causetid.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum CausetIdAlteration {
    CausetId(symbols::Keyword),
}

/// Summarizes changes to spacetime such as a a `SchemaReplicant` and (in the future) a `PartitionMap`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct SpacetimeReport {
    // SolitonIds that were not present in the original `AttributeMap` that was mutated.
    pub attributes_installed: BTreeSet<SolitonId>,

    // SolitonIds that were present in the original `AttributeMap` that was mutated, together with a
    // representation of the mutations that were applied.
    pub attributes_altered: BTreeMap<SolitonId, Vec<AttributeAlteration>>,

    // CausetIds that were installed into the `AttributeMap`.
    pub causetIds_altered: BTreeMap<SolitonId, CausetIdAlteration>,
}

impl SpacetimeReport {
    pub fn attributes_did_change(&self) -> bool {
        !(self.attributes_installed.is_empty() &&
          self.attributes_altered.is_empty())
    }
}

/// Update an 'AttributeMap' in place given two sets of causetid and attribute retractions, which
/// together contain enough information to reason about a "schemaReplicant retraction".
///
/// SchemaReplicant may only be retracted if all of its necessary attributes are being retracted:
/// - :edb/causetid, :edb/valueType, :edb/cardinality.
///
/// Note that this is currently incomplete/flawed:
/// - we're allowing optional attributes to not be retracted and dangle afterwards
///
/// Returns a set of attribute retractions which do not involve schemaReplicant-defining attributes.
fn update_attribute_map_from_schemaReplicant_retractions(attribute_map: &mut AttributeMap, retractions: Vec<EAV>, causetId_retractions: &BTreeMap<SolitonId, symbols::Keyword>) -> Result<Vec<EAV>> {
    // Process retractions of schemaReplicant attributes first. It's allowed to retract a schemaReplicant attribute
    // if all of the schemaReplicant-defining schemaReplicant attributes are being retracted.
    // A defining set of attributes is :edb/causetid, :edb/valueType, :edb/cardinality.
    let mut filtered_retractions = vec![];
    let mut suspect_retractions = vec![];

    // Filter out sets of schemaReplicant altering retractions.
    let mut eas = BTreeMap::new();
    for (e, a, v) in retractions.into_iter() {
        if causetids::is_a_schemaReplicant_attribute(a) {
            eas.entry(e).or_insert(vec![]).push(a);
            suspect_retractions.push((e, a, v));
        } else {
            filtered_retractions.push((e, a, v));
        }
    }

    // TODO (see https://github.com/whtcorpsinc/edb/issues/796).
    // Retraction of causetIds is allowed, but if an causetid names a schemaReplicant attribute, then we should enforce
    // retraction of all of the associated schemaReplicant attributes.
    // Unfortunately, our current in-memory schemaReplicant representation (namely, how we define an Attribute) is not currently
    // rich enough: it lacks distinction between presence and absence, and instead assumes default values.

    // Currently, in order to do this enforcement correctly, we'd need to inspect 'causets'.

    // Here is an incorrect way to enforce this. It's incorrect because it prevents us from retracting non-"schemaReplicant naming" causetIds.
    // for retracted_e in causetId_retractions.keys() {
    //     if !eas.contains_key(retracted_e) {
    //         bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Retracting :edb/causetid of a schemaReplicant without retracting its defining attributes is not permitted.")));
    //     }
    // }

    for (e, a, v) in suspect_retractions.into_iter() {
        let attributes = eas.get(&e).unwrap();

        // Found a set of retractions which negate a schemaReplicant.
        if attributes.contains(&causetids::DB_CARDINALITY) && attributes.contains(&causetids::DB_VALUE_TYPE) {
            // Ensure that corresponding :edb/causetid is also being retracted at the same time.
            if causetId_retractions.contains_key(&e) {
                // Remove attributes corresponding to retracted attribute.
                attribute_map.remove(&e);
            } else {
                bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Retracting defining attributes of a schemaReplicant without retracting its :edb/causetid is not permitted.")));
            }
        } else {
            filtered_retractions.push((e, a, v));
        }
    }

    Ok(filtered_retractions)
}

/// Update a `AttributeMap` in place from the given `[e a typed_value]` triples.
///
/// This is suiBlock for producing a `AttributeMap` from the `schemaReplicant` materialized view, which does not
/// contain install and alter markers.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_attribute_map_from_causetid_triples(attribute_map: &mut AttributeMap, assertions: Vec<EAV>, retractions: Vec<EAV>) -> Result<SpacetimeReport> {
    fn attribute_builder_to_modify(attribute_id: SolitonId, existing: &AttributeMap) -> AttributeBuilder {
        existing.get(&attribute_id)
                .map(AttributeBuilder::to_modify_attribute)
                .unwrap_or_else(AttributeBuilder::default)
    }

    // Group mutations by impacted solitonId.
    let mut builders: BTreeMap<SolitonId, AttributeBuilder> = BTreeMap::new();

    // For retractions, we start with an attribute builder that's pre-populated with the existing
    // attribute values. That allows us to check existing values and unset them.
    for (solitonId, attr, ref value) in retractions {
        let builder = builders.entry(solitonId).or_insert_with(|| attribute_builder_to_modify(solitonId, attribute_map));
        match attr {
            // You can only retract :edb/unique, :edb/isComponent; all others must be altered instead
            // of retracted, or are not allowed to change.
            causetids::DB_IS_COMPONENT => {
                match value {
                    &MinkowskiType::Boolean(v) if builder.component == Some(v) => {
                        builder.component(false);
                    },
                    v => {
                        bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Attempted to retract :edb/isComponent with the wrong value {:?}.", v)));
                    },
                }
            },

            causetids::DB_UNIQUE => {
                match *value {
                    MinkowskiType::Ref(u) => {
                        match u {
                            causetids::DB_UNIQUE_VALUE if builder.unique == Some(Some(attribute::Unique::Value)) => {
                                builder.non_unique();
                            },
                            causetids::DB_UNIQUE_CausetIDITY if builder.unique == Some(Some(attribute::Unique::CausetIdity)) => {
                                builder.non_unique();
                            },
                            v => {
                                bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Attempted to retract :edb/unique with the wrong value {}.", v)));
                            },
                        }
                    },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [:edb/retract _ :edb/unique :edb.unique/_] but got [:edb/retract {} :edb/unique {:?}]", solitonId, value)))
                }
            },

            causetids::DB_VALUE_TYPE |
            causetids::DB_CARDINALITY |
            causetids::DB_INDEX |
            causetids::DB_FULLTEXT |
            causetids::DB_NO_HISTORY => {
                bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Retracting attribute {} for instanton {} not permitted.", attr, solitonId)));
            },

            _ => {
                bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Do not recognize attribute {} for solitonId {}", attr, solitonId)))
            }
        }
    }

    for (solitonId, attr, ref value) in assertions.into_iter() {
        // For assertions, we can start with an empty attribute builder.
        let builder = builders.entry(solitonId).or_insert_with(Default::default);

        // TODO: improve error messages throughout.
        match attr {
            causetids::DB_VALUE_TYPE => {
                match *value {
                    MinkowskiType::Ref(causetids::DB_TYPE_BOOLEAN) => { builder.value_type(MinkowskiValueType::Boolean); },
                    MinkowskiType::Ref(causetids::DB_TYPE_DOUBLE)  => { builder.value_type(MinkowskiValueType::Double); },
                    MinkowskiType::Ref(causetids::DB_TYPE_INSTANT) => { builder.value_type(MinkowskiValueType::Instant); },
                    MinkowskiType::Ref(causetids::DB_TYPE_KEYWORD) => { builder.value_type(MinkowskiValueType::Keyword); },
                    MinkowskiType::Ref(causetids::DB_TYPE_LONG)    => { builder.value_type(MinkowskiValueType::Long); },
                    MinkowskiType::Ref(causetids::DB_TYPE_REF)     => { builder.value_type(MinkowskiValueType::Ref); },
                    MinkowskiType::Ref(causetids::DB_TYPE_STRING)  => { builder.value_type(MinkowskiValueType::String); },
                    MinkowskiType::Ref(causetids::DB_TYPE_UUID)    => { builder.value_type(MinkowskiValueType::Uuid); },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [... :edb/valueType :edb.type/*] but got [... :edb/valueType {:?}] for solitonId {} and attribute {}", value, solitonId, attr)))
                }
            },

            causetids::DB_CARDINALITY => {
                match *value {
                    MinkowskiType::Ref(causetids::DB_CARDINALITY_MANY) => { builder.multival(true); },
                    MinkowskiType::Ref(causetids::DB_CARDINALITY_ONE) => { builder.multival(false); },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [... :edb/cardinality :edb.cardinality/many|:edb.cardinality/one] but got [... :edb/cardinality {:?}]", value)))
                }
            },

            causetids::DB_UNIQUE => {
                match *value {
                    MinkowskiType::Ref(causetids::DB_UNIQUE_VALUE) => { builder.unique(attribute::Unique::Value); },
                    MinkowskiType::Ref(causetids::DB_UNIQUE_CausetIDITY) => { builder.unique(attribute::Unique::CausetIdity); },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [... :edb/unique :edb.unique/value|:edb.unique/causetIdity] but got [... :edb/unique {:?}]", value)))
                }
            },

            causetids::DB_INDEX => {
                match *value {
                    MinkowskiType::Boolean(x) => { builder.index(x); },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [... :edb/index true|false] but got [... :edb/index {:?}]", value)))
                }
            },

            causetids::DB_FULLTEXT => {
                match *value {
                    MinkowskiType::Boolean(x) => { builder.fulltext(x); },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [... :edb/fulltext true|false] but got [... :edb/fulltext {:?}]", value)))
                }
            },

            causetids::DB_IS_COMPONENT => {
                match *value {
                    MinkowskiType::Boolean(x) => { builder.component(x); },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [... :edb/isComponent true|false] but got [... :edb/isComponent {:?}]", value)))
                }
            },

            causetids::DB_NO_HISTORY => {
                match *value {
                    MinkowskiType::Boolean(x) => { builder.no_history(x); },
                    _ => bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Expected [... :edb/noHistory true|false] but got [... :edb/noHistory {:?}]", value)))
                }
            },

            _ => {
                bail!(DbErrorKind::BadSchemaReplicantAssertion(format!("Do not recognize attribute {} for solitonId {}", attr, solitonId)))
            }
        }
    };

    let mut attributes_installed: BTreeSet<SolitonId> = BTreeSet::default();
    let mut attributes_altered: BTreeMap<SolitonId, Vec<AttributeAlteration>> = BTreeMap::default();

    for (solitonId, builder) in builders.into_iter() {
        match attribute_map.entry(solitonId) {
            Entry::Vacant(entry) => {
                // Validate once…
                builder.validate_install_attribute().context(DbErrorKind::BadSchemaReplicantAssertion(format!("SchemaReplicant alteration for new attribute with solitonId {} is not valid", solitonId)))?;

                // … and twice, now we have the Attribute.
                let a = builder.build();
                a.validate(|| solitonId.to_string())?;
                entry.insert(a);
                attributes_installed.insert(solitonId);
            },

            Entry::Occupied(mut entry) => {
                builder.validate_alter_attribute().context(DbErrorKind::BadSchemaReplicantAssertion(format!("SchemaReplicant alteration for existing attribute with solitonId {} is not valid", solitonId)))?;
                let mutations = builder.mutate(entry.get_mut());
                attributes_altered.insert(solitonId, mutations);
            },
        }
    }

    Ok(SpacetimeReport {
        attributes_installed: attributes_installed,
        attributes_altered: attributes_altered,
        causetIds_altered: BTreeMap::default(),
    })
}

/// Update a `SchemaReplicant` in place from the given `[e a typed_value added]` quadruples.
///
/// This layer enforces that causetid assertions of the form [solitonId :edb/causetid ...] (as distinct from
/// attribute assertions) are present and correct.
///
/// This is suiBlock for mutating a `SchemaReplicant` from an applied transaction.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_schemaReplicant_from_causetid_quadruples<U>(schemaReplicant: &mut SchemaReplicant, assertions: U) -> Result<SpacetimeReport>
    where U: IntoIterator<Item=(SolitonId, SolitonId, MinkowskiType, bool)> {

    // Group attribute assertions into asserted, retracted, and updated.  We assume all our
    // attribute assertions are :edb/cardinality :edb.cardinality/one (so they'll only be added or
    // retracted at most once), which means all attribute alterations are simple changes from an old
    // value to a new value.
    let mut attribute_set: AddRetractAlterSet<(SolitonId, SolitonId), MinkowskiType> = AddRetractAlterSet::default();
    let mut causetId_set: AddRetractAlterSet<SolitonId, symbols::Keyword> = AddRetractAlterSet::default();

    for (e, a, typed_value, added) in assertions.into_iter() {
        // Here we handle :edb/causetid assertions.
        if a == causetids::DB_CausetID {
            if let MinkowskiType::Keyword(ref keyword) = typed_value {
                causetId_set.witness(e, keyword.as_ref().clone(), added);
                continue
            } else {
                // Something is terribly wrong: the schemaReplicant ensures we have a keyword.
                unreachable!();
            }
        }

        attribute_set.witness((e, a), typed_value, added);
    }

    // Collect triples.
    let retracted_triples = attribute_set.retracted.into_iter().map(|((e, a), typed_value)| (e, a, typed_value));
    let asserted_triples = attribute_set.asserted.into_iter().map(|((e, a), typed_value)| (e, a, typed_value));
    let altered_triples = attribute_set.altered.into_iter().map(|((e, a), (_old_value, new_value))| (e, a, new_value));

    // First we process retractions which remove schemaReplicant.
    // This operation consumes our current list of attribute retractions, producing a filtered one.
    let non_schemaReplicant_retractions = update_attribute_map_from_schemaReplicant_retractions(&mut schemaReplicant.attribute_map,
                                                                              retracted_triples.collect(),
                                                                              &causetId_set.retracted)?;

    // Now we process all other retractions.
    let report = update_attribute_map_from_causetid_triples(&mut schemaReplicant.attribute_map,
                                                         asserted_triples.chain(altered_triples).collect(),
                                                         non_schemaReplicant_retractions)?;

    let mut causetIds_altered: BTreeMap<SolitonId, CausetIdAlteration> = BTreeMap::new();

    // Asserted, altered, or retracted :edb/causetIds update the relevant causetids.
    for (solitonId, causetid) in causetId_set.asserted {
        schemaReplicant.causetid_map.insert(solitonId, causetid.clone());
        schemaReplicant.causetId_map.insert(causetid.clone(), solitonId);
        causetIds_altered.insert(solitonId, CausetIdAlteration::CausetId(causetid.clone()));
    }

    for (solitonId, (old_causetId, new_causetId)) in causetId_set.altered {
        schemaReplicant.causetid_map.insert(solitonId, new_causetId.clone()); // Overwrite existing.
        schemaReplicant.causetId_map.remove(&old_causetId); // Remove old.
        schemaReplicant.causetId_map.insert(new_causetId.clone(), solitonId); // Insert new.
        causetIds_altered.insert(solitonId, CausetIdAlteration::CausetId(new_causetId.clone()));
    }

    for (solitonId, causetid) in &causetId_set.retracted {
        schemaReplicant.causetid_map.remove(solitonId);
        schemaReplicant.causetId_map.remove(causetid);
        causetIds_altered.insert(*solitonId, CausetIdAlteration::CausetId(causetid.clone()));
    }

    // Component attributes need to change if either:
    // - a component attribute changed
    // - a schemaReplicant attribute that was a component was retracted
    // These two checks are a rather heavy-handed way of keeping schemaReplicant's
    // component_attributes up-to-date: most of the time we'll rebuild it
    // even though it's not necessary (e.g. a schemaReplicant attribute that's _not_
    // a component was removed, or a non-component related attribute changed).
    if report.attributes_did_change() || causetId_set.retracted.len() > 0 {
        schemaReplicant.update_component_attributes();
    }

    Ok(SpacetimeReport {
        causetIds_altered: causetIds_altered,
        .. report
    })
}
