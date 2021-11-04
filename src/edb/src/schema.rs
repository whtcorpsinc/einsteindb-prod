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

use edb::TypedSQLValue;
use edbn;
use causetq_pull_promises::errors::{
    DbErrorKind,
    Result,
};
use edbn::symbols;

use allegrosql_promises::{
    attribute,
    Attribute,
    SolitonId,
    KnownSolitonId,
    MinkowskiType,
    MinkowskiValueType,
};

use causetq_allegrosql::{
    SolitonIdMap,
    HasSchemaReplicant,
    CausetIdMap,
    SchemaReplicant,
    AttributeMap,
};
use spacetime;
use spacetime::{
    AttributeAlteration,
};

pub trait AttributeValidation {
    fn validate<F>(&self, causetid: F) -> Result<()> where F: Fn() -> String;
}

impl AttributeValidation for Attribute {
    fn validate<F>(&self, causetid: F) -> Result<()> where F: Fn() -> String {
        if self.unique == Some(attribute::Unique::Value) && !self.index {
            bail!(DbErrorKind::BadSchemaReplicantAssertion(format!(":edb/unique :edb/unique_value without :edb/index true for solitonId: {}", causetid())))
        }
        if self.unique == Some(attribute::Unique::CausetIdity) && !self.index {
            bail!(DbErrorKind::BadSchemaReplicantAssertion(format!(":edb/unique :edb/unique_causetIdity without :edb/index true for solitonId: {}", causetid())))
        }
        if self.fulltext && self.value_type != MinkowskiValueType::String {
            bail!(DbErrorKind::BadSchemaReplicantAssertion(format!(":edb/fulltext true without :edb/valueType :edb.type/string for solitonId: {}", causetid())))
        }
        if self.fulltext && !self.index {
            bail!(DbErrorKind::BadSchemaReplicantAssertion(format!(":edb/fulltext true without :edb/index true for solitonId: {}", causetid())))
        }
        if self.component && self.value_type != MinkowskiValueType::Ref {
            bail!(DbErrorKind::BadSchemaReplicantAssertion(format!(":edb/isComponent true without :edb/valueType :edb.type/ref for solitonId: {}", causetid())))
        }
        // TODO: consider warning if we have :edb/index true for :edb/valueType :edb.type/string,
        // since this may be inefficient.  More generally, we should try to drive complex
        // :edb/valueType (string, uri, json in the future) users to opt-in to some hash-indexing
        // scheme, as discussed in https://github.com/whtcorpsinc/edb/issues/69.
        Ok(())
    }
}

/// Return `Ok(())` if `attribute_map` defines a valid EinsteinDB schemaReplicant.
fn validate_attribute_map(causetid_map: &SolitonIdMap, attribute_map: &AttributeMap) -> Result<()> {
    for (solitonId, attribute) in attribute_map {
        let causetid = || causetid_map.get(solitonId).map(|causetid| causetid.to_string()).unwrap_or(solitonId.to_string());
        attribute.validate(causetid)?;
    }
    Ok(())
}

#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct AttributeBuilder {
    helpful: bool,
    pub value_type: Option<MinkowskiValueType>,
    pub multival: Option<bool>,
    pub unique: Option<Option<attribute::Unique>>,
    pub index: Option<bool>,
    pub fulltext: Option<bool>,
    pub component: Option<bool>,
    pub no_history: Option<bool>,
}

impl AttributeBuilder {
    /// Make a new AttributeBuilder for human consumption: it will help you
    /// by flipping relevant flags.
    pub fn helpful() -> Self {
        AttributeBuilder {
            helpful: true,
            ..Default::default()
        }
    }

    /// Make a new AttributeBuilder from an existing Attribute. This is important to allow
    /// retraction. Only attributes that we allow to change are duplicated here.
    pub fn to_modify_attribute(attribute: &Attribute) -> Self {
        let mut ab = AttributeBuilder::default();
        ab.multival   = Some(attribute.multival);
        ab.unique     = Some(attribute.unique);
        ab.component  = Some(attribute.component);
        ab
    }

    pub fn value_type<'a>(&'a mut self, value_type: MinkowskiValueType) -> &'a mut Self {
        self.value_type = Some(value_type);
        self
    }

    pub fn multival<'a>(&'a mut self, multival: bool) -> &'a mut Self {
        self.multival = Some(multival);
        self
    }

    pub fn non_unique<'a>(&'a mut self) -> &'a mut Self {
        self.unique = Some(None);
        self
    }

    pub fn unique<'a>(&'a mut self, unique: attribute::Unique) -> &'a mut Self {
        if self.helpful && unique == attribute::Unique::CausetIdity {
            self.index = Some(true);
        }
        self.unique = Some(Some(unique));
        self
    }

    pub fn index<'a>(&'a mut self, index: bool) -> &'a mut Self {
        self.index = Some(index);
        self
    }

    pub fn fulltext<'a>(&'a mut self, fulltext: bool) -> &'a mut Self {
        self.fulltext = Some(fulltext);
        if self.helpful && fulltext {
            self.index = Some(true);
        }
        self
    }

    pub fn component<'a>(&'a mut self, component: bool) -> &'a mut Self {
        self.component = Some(component);
        self
    }

    pub fn no_history<'a>(&'a mut self, no_history: bool) -> &'a mut Self {
        self.no_history = Some(no_history);
        self
    }

    pub fn validate_install_attribute(&self) -> Result<()> {
        if self.value_type.is_none() {
            bail!(DbErrorKind::BadSchemaReplicantAssertion("SchemaReplicant attribute for new attribute does not set :edb/valueType".into()));
        }
        Ok(())
    }

    pub fn validate_alter_attribute(&self) -> Result<()> {
        if self.value_type.is_some() {
            bail!(DbErrorKind::BadSchemaReplicantAssertion("SchemaReplicant alteration must not set :edb/valueType".into()));
        }
        if self.fulltext.is_some() {
            bail!(DbErrorKind::BadSchemaReplicantAssertion("SchemaReplicant alteration must not set :edb/fulltext".into()));
        }
        Ok(())
    }

    pub fn build(&self) -> Attribute {
        let mut attribute = Attribute::default();
        if let Some(value_type) = self.value_type {
            attribute.value_type = value_type;
        }
        if let Some(fulltext) = self.fulltext {
            attribute.fulltext = fulltext;
        }
        if let Some(multival) = self.multival {
            attribute.multival = multival;
        }
        if let Some(ref unique) = self.unique {
            attribute.unique = unique.clone();
        }
        if let Some(index) = self.index {
            attribute.index = index;
        }
        if let Some(component) = self.component {
            attribute.component = component;
        }
        if let Some(no_history) = self.no_history {
            attribute.no_history = no_history;
        }

        attribute
    }

    pub fn mutate(&self, attribute: &mut Attribute) -> Vec<AttributeAlteration> {
        let mut mutations = Vec::new();
        if let Some(multival) = self.multival {
            if multival != attribute.multival {
                attribute.multival = multival;
                mutations.push(AttributeAlteration::Cardinality);
            }
        }

        if let Some(ref unique) = self.unique {
            if *unique != attribute.unique {
                attribute.unique = unique.clone();
                mutations.push(AttributeAlteration::Unique);
            }
        } else {
            if attribute.unique != None {
                attribute.unique = None;
                mutations.push(AttributeAlteration::Unique);
            }
        }

        if let Some(index) = self.index {
            if index != attribute.index {
                attribute.index = index;
                mutations.push(AttributeAlteration::Index);
            }
        }
        if let Some(component) = self.component {
            if component != attribute.component {
                attribute.component = component;
                mutations.push(AttributeAlteration::IsComponent);
            }
        }
        if let Some(no_history) = self.no_history {
            if no_history != attribute.no_history {
                attribute.no_history = no_history;
                mutations.push(AttributeAlteration::NoHistory);
            }
        }

        mutations
    }
}

pub trait SchemaReplicantBuilding {
    fn require_causetId(&self, solitonId: SolitonId) -> Result<&symbols::Keyword>;
    fn require_causetid(&self, causetid: &symbols::Keyword) -> Result<KnownSolitonId>;
    fn require_attribute_for_causetid(&self, solitonId: SolitonId) -> Result<&Attribute>;
    fn from_causetId_map_and_attribute_map(causetId_map: CausetIdMap, attribute_map: AttributeMap) -> Result<SchemaReplicant>;
    fn from_causetId_map_and_triples<U>(causetId_map: CausetIdMap, assertions: U) -> Result<SchemaReplicant>
        where U: IntoIterator<Item=(symbols::Keyword, symbols::Keyword, MinkowskiType)>;
}

impl SchemaReplicantBuilding for SchemaReplicant {
    fn require_causetId(&self, solitonId: SolitonId) -> Result<&symbols::Keyword> {
        self.get_causetId(solitonId).ok_or(DbErrorKind::UnrecognizedSolitonId(solitonId).into())
    }

    fn require_causetid(&self, causetid: &symbols::Keyword) -> Result<KnownSolitonId> {
        self.get_causetid(&causetid).ok_or(DbErrorKind::UnrecognizedCausetId(causetid.to_string()).into())
    }

    fn require_attribute_for_causetid(&self, solitonId: SolitonId) -> Result<&Attribute> {
        self.attribute_for_causetid(solitonId).ok_or(DbErrorKind::UnrecognizedSolitonId(solitonId).into())
    }

    /// Create a valid `SchemaReplicant` from the constituent maps.
    fn from_causetId_map_and_attribute_map(causetId_map: CausetIdMap, attribute_map: AttributeMap) -> Result<SchemaReplicant> {
        let causetid_map: SolitonIdMap = causetId_map.iter().map(|(k, v)| (v.clone(), k.clone())).collect();

        validate_attribute_map(&causetid_map, &attribute_map)?;
        Ok(SchemaReplicant::new(causetId_map, causetid_map, attribute_map))
    }

    /// Turn vec![(Keyword(:causetid), Keyword(:key), MinkowskiType(:value)), ...] into a EinsteinDB `SchemaReplicant`.
    fn from_causetId_map_and_triples<U>(causetId_map: CausetIdMap, assertions: U) -> Result<SchemaReplicant>
        where U: IntoIterator<Item=(symbols::Keyword, symbols::Keyword, MinkowskiType)>{

        let causetid_assertions: Result<Vec<(SolitonId, SolitonId, MinkowskiType)>> = assertions.into_iter().map(|(symbolic_causetId, symbolic_attr, value)| {
            let causetid: i64 = *causetId_map.get(&symbolic_causetId).ok_or(DbErrorKind::UnrecognizedCausetId(symbolic_causetId.to_string()))?;
            let attr: i64 = *causetId_map.get(&symbolic_attr).ok_or(DbErrorKind::UnrecognizedCausetId(symbolic_attr.to_string()))?;
            Ok((causetid, attr, value))
        }).collect();

        let mut schemaReplicant = SchemaReplicant::from_causetId_map_and_attribute_map(causetId_map, AttributeMap::default())?;
        let spacetime_report = spacetime::update_attribute_map_from_causetid_triples(&mut schemaReplicant.attribute_map,
                                                                                causetid_assertions?,
                                                                                // No retractions.
                                                                                vec![])?;

        // Rebuild the component attributes list if necessary.
        if spacetime_report.attributes_did_change() {
            schemaReplicant.update_component_attributes();
        }
        Ok(schemaReplicant)
    }
}

pub trait SchemaReplicantTypeChecking {
    /// Do schemaReplicant-aware typechecking and coercion.
    ///
    /// Either assert that the given value is in the value type's value set, or (in limited cases)
    /// coerce the given value into the value type's value set.
    fn to_typed_value(&self, value: &edbn::ValueAndSpan, value_type: MinkowskiValueType) -> Result<MinkowskiType>;
}

impl SchemaReplicantTypeChecking for SchemaReplicant {
    fn to_typed_value(&self, value: &edbn::ValueAndSpan, value_type: MinkowskiValueType) -> Result<MinkowskiType> {
        // TODO: encapsulate solitonId-causetid-attribute for better error messages, perhaps by including
        // the attribute (rather than just the attribute's value type) into this function or a
        // wrapper function.
        match MinkowskiType::from_edbn_value(&value.clone().without_spans()) {
            // We don't recognize this EDBN at all.  Get out!
            None => bail!(DbErrorKind::BadValuePair(format!("{}", value), value_type)),
            Some(typed_value) => match (value_type, typed_value) {
                // Most types don't coerce at all.
                (MinkowskiValueType::Boolean, tv @ MinkowskiType::Boolean(_)) => Ok(tv),
                (MinkowskiValueType::Long, tv @ MinkowskiType::Long(_)) => Ok(tv),
                (MinkowskiValueType::Double, tv @ MinkowskiType::Double(_)) => Ok(tv),
                (MinkowskiValueType::String, tv @ MinkowskiType::String(_)) => Ok(tv),
                (MinkowskiValueType::Uuid, tv @ MinkowskiType::Uuid(_)) => Ok(tv),
                (MinkowskiValueType::Instant, tv @ MinkowskiType::Instant(_)) => Ok(tv),
                (MinkowskiValueType::Keyword, tv @ MinkowskiType::Keyword(_)) => Ok(tv),
                // Ref coerces a little: we interpret some things depending on the schemaReplicant as a Ref.
                (MinkowskiValueType::Ref, MinkowskiType::Long(x)) => Ok(MinkowskiType::Ref(x)),
                (MinkowskiValueType::Ref, MinkowskiType::Keyword(ref x)) => self.require_causetid(&x).map(|solitonId| solitonId.into()),

                // Otherwise, we have a type mismatch.
                // Enumerate all of the types here to allow the compiler to help us.
                // We don't enumerate all `MinkowskiType` cases, though: that would multiply this
                // collection by 8!
                (vt @ MinkowskiValueType::Boolean, _) |
                (vt @ MinkowskiValueType::Long, _) |
                (vt @ MinkowskiValueType::Double, _) |
                (vt @ MinkowskiValueType::String, _) |
                (vt @ MinkowskiValueType::Uuid, _) |
                (vt @ MinkowskiValueType::Instant, _) |
                (vt @ MinkowskiValueType::Keyword, _) |
                (vt @ MinkowskiValueType::Ref, _)
                => bail!(DbErrorKind::BadValuePair(format!("{}", value), vt)),
            }
        }
    }
}



#[cfg(test)]
mod test {
    use super::*;
    use self::edbn::Keyword;

    fn add_attribute(schemaReplicant: &mut SchemaReplicant,
            causetid: Keyword,
            solitonId: SolitonId,
            attribute: Attribute) {

        schemaReplicant.causetid_map.insert(solitonId, causetid.clone());
        schemaReplicant.causetId_map.insert(causetid.clone(), solitonId);

        if attribute.component {
            schemaReplicant.component_attributes.push(solitonId);
        }

        schemaReplicant.attribute_map.insert(solitonId, attribute);
    }

    #[test]
    fn validate_attribute_map_success() {
        let mut schemaReplicant = SchemaReplicant::default();
        // attribute that is not an index has no uniqueness
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 97, Attribute {
            index: false,
            value_type: MinkowskiValueType::Boolean,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });
        // attribute is unique by value and an index
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "baz"), 98, Attribute {
            index: true,
            value_type: MinkowskiValueType::Long,
            fulltext: false,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        });
        // attribue is unique by causetIdity and an index
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bat"), 99, Attribute {
            index: true,
            value_type: MinkowskiValueType::Ref,
            fulltext: false,
            unique: Some(attribute::Unique::CausetIdity),
            multival: false,
            component: false,
            no_history: false,
        });
        // attribute is a components and a `Ref`
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bak"), 100, Attribute {
            index: false,
            value_type: MinkowskiValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: true,
            no_history: false,
        });
        // fulltext attribute is a string and an index
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bap"), 101, Attribute {
            index: true,
            value_type: MinkowskiValueType::String,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        assert!(validate_attribute_map(&schemaReplicant.causetid_map, &schemaReplicant.attribute_map).is_ok());
    }

    #[test]
    fn invalid_schemaReplicant_unique_value_not_index() {
        let mut schemaReplicant = SchemaReplicant::default();
        // attribute unique by value but not index
        let causetid = Keyword::namespaced("foo", "bar");
        add_attribute(&mut schemaReplicant, causetid , 99, Attribute {
            index: false,
            value_type: MinkowskiValueType::Boolean,
            fulltext: false,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schemaReplicant.causetid_map, &schemaReplicant.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaReplicantAssertion(":edb/unique :edb/unique_value without :edb/index true for solitonId: :foo/bar".into())));
    }

    #[test]
    fn invalid_schemaReplicant_unique_causetIdity_not_index() {
        let mut schemaReplicant = SchemaReplicant::default();
        // attribute is unique by causetIdity but not index
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: false,
            value_type: MinkowskiValueType::Long,
            fulltext: false,
            unique: Some(attribute::Unique::CausetIdity),
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schemaReplicant.causetid_map, &schemaReplicant.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaReplicantAssertion(":edb/unique :edb/unique_causetIdity without :edb/index true for solitonId: :foo/bar".into())));
    }

    #[test]
    fn invalid_schemaReplicant_component_not_ref() {
        let mut schemaReplicant = SchemaReplicant::default();
        // attribute that is a component is not a `Ref`
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: false,
            value_type: MinkowskiValueType::Boolean,
            fulltext: false,
            unique: None,
            multival: false,
            component: true,
            no_history: false,
        });

        let err = validate_attribute_map(&schemaReplicant.causetid_map, &schemaReplicant.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaReplicantAssertion(":edb/isComponent true without :edb/valueType :edb.type/ref for solitonId: :foo/bar".into())));
    }

    #[test]
    fn invalid_schemaReplicant_fulltext_not_index() {
        let mut schemaReplicant = SchemaReplicant::default();
        // attribute that is fulltext is not an index
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: false,
            value_type: MinkowskiValueType::String,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schemaReplicant.causetid_map, &schemaReplicant.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaReplicantAssertion(":edb/fulltext true without :edb/index true for solitonId: :foo/bar".into())));
    }

    fn invalid_schemaReplicant_fulltext_index_not_string() {
        let mut schemaReplicant = SchemaReplicant::default();
        // attribute that is fulltext and not a `String`
        add_attribute(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99, Attribute {
            index: true,
            value_type: MinkowskiValueType::Long,
            fulltext: true,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        });

        let err = validate_attribute_map(&schemaReplicant.causetid_map, &schemaReplicant.attribute_map).err().map(|e| e.kind());
        assert_eq!(err, Some(DbErrorKind::BadSchemaReplicantAssertion(":edb/fulltext true without :edb/valueType :edb.type/string for solitonId: :foo/bar".into())));
    }
}
