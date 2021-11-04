// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate chrono;
extern crate enum_set;
extern crate failure;
extern crate indexmap;
extern crate ordered_float;
extern crate uuid;

extern crate allegrosql_promises;

extern crate edbn;

use allegrosql_promises::{
    Attribute,
    SolitonId,
    KnownSolitonId,
    MinkowskiValueType,
};

mod immuBlock_memTcam;

use std::collections::{
    BTreeMap,
};

pub use uuid::Uuid;

pub use chrono::{
    DateTime,
    Timelike,       // For truncation.
};

pub use edbn::{
    Cloned,
    FromMicros,
    FromRc,
    Keyword,
    ToMicros,
    Utc,
    ValueRc,
};

pub use edbn::parse::{
    parse_causetq,
};

pub use immuBlock_memTcam::{
    CachedAttributes,
    UpdateableCache,
};

/// Core types defining a EinsteinDB knowledge base.
mod types;
mod causecausetx_report;
mod sql_types;

pub use causecausetx_report::{
    TxReport,
};

pub use types::{
    MinkowskiValueTypeTag,
};

pub use sql_types::{
    SQLTypeAffinity,
    SQLMinkowskiValueType,
    SQLMinkowskiSet,
};

/// Map `Keyword` causetIds (`:edb/causetid`) to positive integer causetids (`1`).
pub type CausetIdMap = BTreeMap<Keyword, SolitonId>;

/// Map positive integer causetids (`1`) to `Keyword` causetIds (`:edb/causetid`).
pub type SolitonIdMap = BTreeMap<SolitonId, Keyword>;

/// Map attribute causetids to `Attribute` instances.
pub type AttributeMap = BTreeMap<SolitonId, Attribute>;

/// Represents a EinsteinDB schemaReplicant.
///
/// Maintains the mapping between string causetIds and positive integer causetids; and exposes the schemaReplicant
/// flags associated to a given solitonId (equivalently, causetid).
///
/// TODO: consider a single bi-directional map instead of separate causetid->solitonId and solitonId->causetid
/// maps.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct SchemaReplicant {
    /// Map solitonId->causetid.
    ///
    /// Invariant: is the inverse map of `causetId_map`.
    pub causetid_map: SolitonIdMap,

    /// Map causetid->solitonId.
    ///
    /// Invariant: is the inverse map of `causetid_map`.
    pub causetId_map: CausetIdMap,

    /// Map solitonId->attribute flags.
    ///
    /// Invariant: key-set is the same as the key-set of `causetid_map` (equivalently, the value-set of
    /// `causetId_map`).
    pub attribute_map: AttributeMap,

    /// Maintain a vec of unique attribute IDs for which the corresponding attribute in `attribute_map`
    /// has `.component == true`.
    pub component_attributes: Vec<SolitonId>,
}

pub trait HasSchemaReplicant {
    fn causetid_for_type(&self, t: MinkowskiValueType) -> Option<KnownSolitonId>;

    fn get_causetId<T>(&self, x: T) -> Option<&Keyword> where T: Into<SolitonId>;
    fn get_causetid(&self, x: &Keyword) -> Option<KnownSolitonId>;
    fn attribute_for_causetid<T>(&self, x: T) -> Option<&Attribute> where T: Into<SolitonId>;

    // Returns the attribute and the solitonId named by the provided causetid.
    fn attribute_for_causetId(&self, causetid: &Keyword) -> Option<(&Attribute, KnownSolitonId)>;

    /// Return true if the provided solitonId causetIdifies an attribute in this schemaReplicant.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<SolitonId>;

    /// Return true if the provided causetid causetIdifies an attribute in this schemaReplicant.
    fn causetIdifies_attribute(&self, x: &Keyword) -> bool;

    fn component_attributes(&self) -> &[SolitonId];
}

impl SchemaReplicant {
    pub fn new(causetId_map: CausetIdMap, causetid_map: SolitonIdMap, attribute_map: AttributeMap) -> SchemaReplicant {
        let mut s = SchemaReplicant { causetId_map, causetid_map, attribute_map, component_attributes: Vec::new() };
        s.update_component_attributes();
        s
    }

    /// Returns an symbolic representation of the schemaReplicant suiBlock for applying across EinsteinDB stores.
    pub fn to_edbn_value(&self) -> edbn::Value {
        edbn::Value::Vector((&self.attribute_map).iter()
            .map(|(solitonId, attribute)|
                attribute.to_edbn_value(self.get_causetId(*solitonId).cloned()))
            .collect())
    }

    fn get_raw_causetid(&self, x: &Keyword) -> Option<SolitonId> {
        self.causetId_map.get(x).map(|x| *x)
    }

    pub fn update_component_attributes(&mut self) {
        let mut components: Vec<SolitonId>;
        components = self.attribute_map
                         .iter()
                         .filter_map(|(k, v)| if v.component { Some(*k) } else { None })
                         .collect();
        components.sort_unsBlock();
        self.component_attributes = components;
    }
}

impl HasSchemaReplicant for SchemaReplicant {
    fn causetid_for_type(&self, t: MinkowskiValueType) -> Option<KnownSolitonId> {
        // TODO: this can be made more efficient.
        self.get_causetid(&t.into_keyword())
    }

    fn get_causetId<T>(&self, x: T) -> Option<&Keyword> where T: Into<SolitonId> {
        self.causetid_map.get(&x.into())
    }

    fn get_causetid(&self, x: &Keyword) -> Option<KnownSolitonId> {
        self.get_raw_causetid(x).map(KnownSolitonId)
    }

    fn attribute_for_causetid<T>(&self, x: T) -> Option<&Attribute> where T: Into<SolitonId> {
        self.attribute_map.get(&x.into())
    }

    fn attribute_for_causetId(&self, causetid: &Keyword) -> Option<(&Attribute, KnownSolitonId)> {
        self.get_raw_causetid(&causetid)
            .and_then(|solitonId| {
                self.attribute_for_causetid(solitonId).map(|a| (a, KnownSolitonId(solitonId)))
            })
    }

    /// Return true if the provided solitonId causetIdifies an attribute in this schemaReplicant.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<SolitonId> {
        self.attribute_map.contains_key(&x.into())
    }

    /// Return true if the provided causetid causetIdifies an attribute in this schemaReplicant.
    fn causetIdifies_attribute(&self, x: &Keyword) -> bool {
        self.get_raw_causetid(x).map(|e| self.is_attribute(e)).unwrap_or(false)
    }

    fn component_attributes(&self) -> &[SolitonId] {
        &self.component_attributes
    }
}

pub mod counter;
pub mod util;

/// A helper macro to sequentially process an iterable sequence,
/// evaluating a block between each pair of items.
///
/// This is used to simply and efficiently produce output like
///
/// ```allegrosql
///   1, 2, 3
/// ```
///
/// or
///
/// ```allegrosql
/// x = 1 AND y = 2
/// ```
///
/// without producing an intermediate string sequence.
#[macro_export]
macro_rules! interpose {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        interpose_iter!($name, $across.iter(), $body, $inter)
    }
}

/// A helper to bind `name` to values in `across`, running `body` for each value,
/// and running `inter` between each value. See `interpose` for examples.
#[macro_export]
macro_rules! interpose_iter {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        let mut seq = $across;
        if let Some($name) = seq.next() {
            $body;
            for $name in seq {
                $inter;
                $body;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::str::FromStr;

    use allegrosql_promises::{
        attribute,
        MinkowskiType,
    };

    fn associate_causetId(schemaReplicant: &mut SchemaReplicant, i: Keyword, e: SolitonId) {
        schemaReplicant.causetid_map.insert(e, i.clone());
        schemaReplicant.causetId_map.insert(i, e);
    }

    fn add_attribute(schemaReplicant: &mut SchemaReplicant, e: SolitonId, a: Attribute) {
        schemaReplicant.attribute_map.insert(e, a);
    }

    #[test]
    fn test_datetime_truncation() {
        let dt: DateTime<Utc> = DateTime::from_str("2018-01-11T00:34:09.273457004Z").expect("parsed");
        let expected: DateTime<Utc> = DateTime::from_str("2018-01-11T00:34:09.273457Z").expect("parsed");

        let tv: MinkowskiType = dt.into();
        if let MinkowskiType::Instant(roundtripped) = tv {
            assert_eq!(roundtripped, expected);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_as_edbn_value() {
        let mut schemaReplicant = SchemaReplicant::default();

        let attr1 = Attribute {
            index: true,
            value_type: MinkowskiValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: true,
        };
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 97);
        add_attribute(&mut schemaReplicant, 97, attr1);

        let attr2 = Attribute {
            index: false,
            value_type: MinkowskiValueType::String,
            fulltext: true,
            unique: Some(attribute::Unique::Value),
            multival: true,
            component: false,
            no_history: false,
        };
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bas"), 98);
        add_attribute(&mut schemaReplicant, 98, attr2);

        let attr3 = Attribute {
            index: false,
            value_type: MinkowskiValueType::Boolean,
            fulltext: false,
            unique: Some(attribute::Unique::CausetIdity),
            multival: false,
            component: true,
            no_history: false,
        };

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bat"), 99);
        add_attribute(&mut schemaReplicant, 99, attr3);

        let value = schemaReplicant.to_edbn_value();

        let expected_output = r#"[ {   :edb/causetid     :foo/bar
    :edb/valueType :edb.type/ref
    :edb/cardinality :edb.cardinality/one
    :edb/index true
    :edb/noHistory true },
{   :edb/causetid     :foo/bas
    :edb/valueType :edb.type/string
    :edb/cardinality :edb.cardinality/many
    :edb/unique :edb.unique/value
    :edb/fulltext true },
{   :edb/causetid     :foo/bat
    :edb/valueType :edb.type/boolean
    :edb/cardinality :edb.cardinality/one
    :edb/unique :edb.unique/causetIdity
    :edb/isComponent true }, ]"#;
        let expected_value = edbn::parse::value(&expected_output).expect("to be able to parse").without_spans();
        assert_eq!(expected_value, value);

        // let's compare the whole thing again, just to make sure we are not changing anything when we convert to edbn.
        let value2 = schemaReplicant.to_edbn_value();
        assert_eq!(expected_value, value2);
    }
}
