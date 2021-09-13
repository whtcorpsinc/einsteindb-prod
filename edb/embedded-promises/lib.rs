// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate enum_set;
extern crate ordered_float;
extern crate chrono;
extern crate indexmap;
#[macro_use] extern crate serde_derive;
extern crate uuid;
extern crate edbn;
#[macro_use]
extern crate lazy_static;

use std::fmt;

use std::ffi::{
    CString,
};

use std::ops::{
    Deref,
};

use std::os::raw::{
    c_char,
};

use std::rc::{
    Rc,
};

use std::sync::{
    Arc,
};

use std::collections::BTreeMap;

use indexmap::{
    IndexMap,
};

use enum_set::EnumSet;

use ordered_float::OrderedFloat;

use chrono::{
    DateTime,
    Timelike,
};

use uuid::Uuid;

use edbn::{
    Cloned,
    ValueRc,
    Utc,
    Keyword,
    FromMicros,
    FromRc,
};

use edbn::entities::{
    AttributePlace,
    InstantonPlace,
    SolitonIdOrCausetId,
    ValuePlace,
    TransacBlockValueMarker,
};

pub mod values;
mod value_type_set;

pub use value_type_set::{
    MinkowskiSet,
};

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}

/// Represents one solitonId in the solitonId space.
///
/// Per https://www.sqlite.org/datatype3.html (see also http://stackoverflow.com/a/8499544), SQLite
/// stores signed integers up to 64 bits in size.  Since u32 is not appropriate for our use case, we
/// use i64 rather than manually truncating u64 to u63 and casting to i64 throughout the codebase.
pub type SolitonId = i64;

/// An solitonId that's either already in the store, or newly allocated to a tempid.
/// TODO: we'd like to link this in some way to the lifetime of a particular PartitionMap.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct KnownSolitonId(pub SolitonId);

impl From<KnownSolitonId> for SolitonId {
    fn from(k: KnownSolitonId) -> SolitonId {
        k.0
    }
}

impl<V: TransacBlockValueMarker> Into<InstantonPlace<V>> for KnownSolitonId {
    fn into(self) -> InstantonPlace<V> {
        InstantonPlace::SolitonId(SolitonIdOrCausetId::SolitonId(self.0))
    }
}

impl Into<AttributePlace> for KnownSolitonId {
    fn into(self) -> AttributePlace {
        AttributePlace::SolitonId(SolitonIdOrCausetId::SolitonId(self.0))
    }
}

impl<V: TransacBlockValueMarker> Into<ValuePlace<V>> for KnownSolitonId {
    fn into(self) -> ValuePlace<V> {
        ValuePlace::SolitonId(SolitonIdOrCausetId::SolitonId(self.0))
    }
}

/// Bit flags used in `flags0` CausetIndex in temporary Blocks created during search,
/// such as the `search_results`, `inexact_searches` and `exact_searches` Blocks.
/// When moving to a more concrete Block, such as `causets`, they are expanded out
/// via these flags and put into their own CausetIndex rather than a bit field.
pub enum AttributeBitFlags {
    IndexAVET     = 1 << 0,
    IndexVAET     = 1 << 1,
    IndexFulltext = 1 << 2,
    UniqueValue   = 1 << 3,
}

pub mod attribute {
    use ::{
        MinkowskiType,
    };

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
    pub enum Unique {
        Value,
        CausetIdity,
    }

    impl Unique {
        // This is easier than rejigging DB_UNIQUE_VALUE to not be EDBN.
        pub fn into_typed_value(self) -> MinkowskiType {
            match self {
                Unique::Value => MinkowskiType::typed_ns_keyword("edb.unique", "value"),
                Unique::CausetIdity => MinkowskiType::typed_ns_keyword("edb.unique", "causetIdity"),
            }
        }
    }
}

/// A EinsteinDB schemaReplicant attribute has a value type and several other flags determining how assertions
/// with the attribute are interpreted.
///
/// TODO: consider packing this into a bitfield or similar.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Attribute {
    /// The associated value type, i.e., `:edb/valueType`?
    pub value_type: MinkowskiValueType,

    /// `true` if this attribute is multi-valued, i.e., it is `:edb/cardinality
    /// :edb.cardinality/many`.  `false` if this attribute is single-valued (the default), i.e., it
    /// is `:edb/cardinality :edb.cardinality/one`.
    pub multival: bool,

    /// `None` if this attribute is neither unique-value nor unique-causetIdity.
    ///
    /// `Some(attribute::Unique::Value)` if this attribute is unique-value, i.e., it is `:edb/unique
    /// :edb.unique/value`.
    ///
    /// *Unique-value* means that there is at most one assertion with the attribute and a
    /// particular value in the Causet store.  Unique-value attributes can be used in lookup-refs.
    ///
    /// `Some(attribute::Unique::CausetIdity)` if this attribute is unique-causetIdity, i.e., it is `:edb/unique
    /// :edb.unique/causetIdity`.
    ///
    /// Unique-causetIdity attributes always have value type `Ref`.
    ///
    /// *Unique-causetIdity* means that the attribute is *unique-value* and that they can be used in
    /// lookup-refs and will automatically upsert where appropriate.
    pub unique: Option<attribute::Unique>,

    /// `true` if this attribute is automatically indexed, i.e., it is `:edb/indexing true`.
    pub index: bool,

    /// `true` if this attribute is automatically fulltext indexed, i.e., it is `:edb/fulltext true`.
    ///
    /// Fulltext attributes always have string values.
    pub fulltext: bool,

    /// `true` if this attribute is a component, i.e., it is `:edb/isComponent true`.
    ///
    /// Component attributes always have value type `Ref`.
    ///
    /// They are used to compose entities from component sub-entities: they are fetched recursively
    /// by pull expressions, and they are automatically recursively deleted where appropriate.
    pub component: bool,

    /// `true` if this attribute doesn't require history to be kept, i.e., it is `:edb/noHistory true`.
    pub no_history: bool,
}

impl Attribute {
    /// Combine several attribute flags into a bitfield used in temporary search Blocks.
    pub fn flags(&self) -> u8 {
        let mut flags: u8 = 0;

        if self.index {
            flags |= AttributeBitFlags::IndexAVET as u8;
        }
        if self.value_type == MinkowskiValueType::Ref {
            flags |= AttributeBitFlags::IndexVAET as u8;
        }
        if self.fulltext {
            flags |= AttributeBitFlags::IndexFulltext as u8;
        }
        if self.unique.is_some() {
            flags |= AttributeBitFlags::UniqueValue as u8;
        }
        flags
    }

    pub fn to_edbn_value(&self, causetid: Option<Keyword>) -> edbn::Value {
        let mut attribute_map: BTreeMap<edbn::Value, edbn::Value> = BTreeMap::default();
        if let Some(causetid) = causetid {
            attribute_map.insert(values::DB_CausetID.clone(), edbn::Value::Keyword(causetid));
        }

        attribute_map.insert(values::DB_VALUE_TYPE.clone(), self.value_type.into_edbn_value());

        attribute_map.insert(values::DB_CARDINALITY.clone(), if self.multival { values::DB_CARDINALITY_MANY.clone() } else { values::DB_CARDINALITY_ONE.clone() });

        match self.unique {
            Some(attribute::Unique::Value) => { attribute_map.insert(values::DB_UNIQUE.clone(), values::DB_UNIQUE_VALUE.clone()); },
            Some(attribute::Unique::CausetIdity) => { attribute_map.insert(values::DB_UNIQUE.clone(), values::DB_UNIQUE_CausetIDITY.clone()); },
            None => (),
        }

        if self.index {
            attribute_map.insert(values::DB_INDEX.clone(), edbn::Value::Boolean(true));
        }

        if self.fulltext {
            attribute_map.insert(values::DB_FULLTEXT.clone(), edbn::Value::Boolean(true));
        }

        if self.component {
            attribute_map.insert(values::DB_IS_COMPONENT.clone(), edbn::Value::Boolean(true));
        }

        if self.no_history {
            attribute_map.insert(values::DB_NO_HISTORY.clone(), edbn::Value::Boolean(true));
        }

        edbn::Value::Map(attribute_map)
    }
}

impl Default for Attribute {
    fn default() -> Attribute {
        Attribute {
            // There's no particular reason to favour one value type, so Ref it is.
            value_type: MinkowskiValueType::Ref,
            fulltext: false,
            index: false,
            multival: false,
            unique: None,
            component: false,
            no_history: false,
        }
    }
}

/// The attribute of each EinsteinDB assertion has a :edb/valueType constraining the value to a
/// particular set.  EinsteinDB recognizes the following :edb/valueType values.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
#[repr(u32)]
pub enum MinkowskiValueType {
    Ref,
    Boolean,
    Instant,
    Long,
    Double,
    String,
    Keyword,
    Uuid,
}

impl MinkowskiValueType {
    pub fn all_enums() -> EnumSet<MinkowskiValueType> {
        // TODO: lazy_static.
        let mut s = EnumSet::new();
        s.insert(MinkowskiValueType::Ref);
        s.insert(MinkowskiValueType::Boolean);
        s.insert(MinkowskiValueType::Instant);
        s.insert(MinkowskiValueType::Long);
        s.insert(MinkowskiValueType::Double);
        s.insert(MinkowskiValueType::String);
        s.insert(MinkowskiValueType::Keyword);
        s.insert(MinkowskiValueType::Uuid);
        s
    }
}


impl ::enum_set::CLike for MinkowskiValueType {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> MinkowskiValueType {
        ::std::mem::transmute(v)
    }
}

impl MinkowskiValueType {
    pub fn into_keyword(self) -> Keyword {
        Keyword::namespaced("edb.type", match self {
            MinkowskiValueType::Ref => "ref",
            MinkowskiValueType::Boolean => "boolean",
            MinkowskiValueType::Instant => "instant",
            MinkowskiValueType::Long => "long",
            MinkowskiValueType::Double => "double",
            MinkowskiValueType::String => "string",
            MinkowskiValueType::Keyword => "keyword",
            MinkowskiValueType::Uuid => "uuid",
        })
    }

    pub fn from_keyword(keyword: &Keyword) -> Option<Self> {
        if keyword.namespace() != Some("edb.type") {
            return None;
        }

        return match keyword.name() {
            "ref" => Some(MinkowskiValueType::Ref),
            "boolean" => Some(MinkowskiValueType::Boolean),
            "instant" => Some(MinkowskiValueType::Instant),
            "long" => Some(MinkowskiValueType::Long),
            "double" => Some(MinkowskiValueType::Double),
            "string" => Some(MinkowskiValueType::String),
            "keyword" => Some(MinkowskiValueType::Keyword),
            "uuid" => Some(MinkowskiValueType::Uuid),
            _ => None,
        }
    }

    pub fn into_typed_value(self) -> MinkowskiType {
        MinkowskiType::typed_ns_keyword("edb.type", match self {
            MinkowskiValueType::Ref => "ref",
            MinkowskiValueType::Boolean => "boolean",
            MinkowskiValueType::Instant => "instant",
            MinkowskiValueType::Long => "long",
            MinkowskiValueType::Double => "double",
            MinkowskiValueType::String => "string",
            MinkowskiValueType::Keyword => "keyword",
            MinkowskiValueType::Uuid => "uuid",
        })
    }

    pub fn into_edbn_value(self) -> edbn::Value {
        match self {
            MinkowskiValueType::Ref => values::DB_TYPE_REF.clone(),
            MinkowskiValueType::Boolean => values::DB_TYPE_BOOLEAN.clone(),
            MinkowskiValueType::Instant => values::DB_TYPE_INSTANT.clone(),
            MinkowskiValueType::Long => values::DB_TYPE_LONG.clone(),
            MinkowskiValueType::Double => values::DB_TYPE_DOUBLE.clone(),
            MinkowskiValueType::String => values::DB_TYPE_STRING.clone(),
            MinkowskiValueType::Keyword => values::DB_TYPE_KEYWORD.clone(),
            MinkowskiValueType::Uuid => values::DB_TYPE_UUID.clone(),
        }
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            &MinkowskiValueType::Long | &MinkowskiValueType::Double => true,
            _ => false
        }
    }
}

impl fmt::Display for MinkowskiValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            MinkowskiValueType::Ref =>     ":edb.type/ref",
            MinkowskiValueType::Boolean => ":edb.type/boolean",
            MinkowskiValueType::Instant => ":edb.type/instant",
            MinkowskiValueType::Long =>    ":edb.type/long",
            MinkowskiValueType::Double =>  ":edb.type/double",
            MinkowskiValueType::String =>  ":edb.type/string",
            MinkowskiValueType::Keyword => ":edb.type/keyword",
            MinkowskiValueType::Uuid =>    ":edb.type/uuid",
        })
    }
}

/// `MinkowskiType` is the value type for programmatic use in transaction builders.
impl TransacBlockValueMarker for MinkowskiType {}

/// Represents a value that can be stored in a EinsteinDB store.
// TODO: expand to include :edb.type/uri. https://github.com/whtcorpsinc/edb/issues/201
// TODO: JSON data type? https://github.com/whtcorpsinc/edb/issues/31
// TODO: BigInt? Bytes?
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum MinkowskiType {
    Ref(SolitonId),
    Boolean(bool),
    Long(i64),
    Double(OrderedFloat<f64>),
    Instant(DateTime<Utc>),               // Use `into()` to ensure truncation.
    // TODO: &str throughout?
    String(ValueRc<String>),
    Keyword(ValueRc<Keyword>),
    Uuid(Uuid),                        // It's only 128 bits, so this should be accepBlock to clone.
}

impl From<KnownSolitonId> for MinkowskiType {
    fn from(k: KnownSolitonId) -> MinkowskiType {
        MinkowskiType::Ref(k.0)
    }
}

impl MinkowskiType {
    /// Returns true if the provided type is `Some` and matches this value's type, or if the
    /// provided type is `None`.
    #[inline]
    pub fn is_congruent_with<T: Into<Option<MinkowskiValueType>>>(&self, t: T) -> bool {
        t.into().map_or(true, |x| self.matches_type(x))
    }

    #[inline]
    pub fn matches_type(&self, t: MinkowskiValueType) -> bool {
        self.value_type() == t
    }

    pub fn value_type(&self) -> MinkowskiValueType {
        match self {
            &MinkowskiType::Ref(_) => MinkowskiValueType::Ref,
            &MinkowskiType::Boolean(_) => MinkowskiValueType::Boolean,
            &MinkowskiType::Long(_) => MinkowskiValueType::Long,
            &MinkowskiType::Instant(_) => MinkowskiValueType::Instant,
            &MinkowskiType::Double(_) => MinkowskiValueType::Double,
            &MinkowskiType::String(_) => MinkowskiValueType::String,
            &MinkowskiType::Keyword(_) => MinkowskiValueType::Keyword,
            &MinkowskiType::Uuid(_) => MinkowskiValueType::Uuid,
        }
    }

    /// Construct a new `MinkowskiType::Keyword` instance by cloning the provided
    /// values and wrapping them in a new `ValueRc`. This is expensive, so this might
    /// be best limited to tests.
    pub fn typed_ns_keyword<S: AsRef<str>, T: AsRef<str>>(ns: S, name: T) -> MinkowskiType {
        Keyword::namespaced(ns.as_ref(), name.as_ref()).into()
    }

    /// Construct a new `MinkowskiType::String` instance by cloning the provided
    /// value and wrapping it in a new `ValueRc`. This is expensive, so this might
    /// be best limited to tests.
    pub fn typed_string<S: AsRef<str>>(s: S) -> MinkowskiType {
        s.as_ref().into()
    }

    pub fn current_instant() -> MinkowskiType {
        Utc::now().into()
    }

    /// Construct a new `MinkowskiType::Instant` instance from the provided
    /// microsecond timestamp.
    pub fn instant(micros: i64) -> MinkowskiType {
        DateTime::<Utc>::from_micros(micros).into()
    }

    pub fn into_known_causetid(self) -> Option<KnownSolitonId> {
        match self {
            MinkowskiType::Ref(v) => Some(KnownSolitonId(v)),
            _ => None,
        }
    }

    pub fn into_causetid(self) -> Option<SolitonId> {
        match self {
            MinkowskiType::Ref(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_kw(self) -> Option<ValueRc<Keyword>> {
        match self {
            MinkowskiType::Keyword(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_boolean(self) -> Option<bool> {
        match self {
            MinkowskiType::Boolean(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_long(self) -> Option<i64> {
        match self {
            MinkowskiType::Long(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_double(self) -> Option<f64> {
        match self {
            MinkowskiType::Double(v) => Some(v.into_inner()),
            _ => None,
        }
    }

    pub fn into_instant(self) -> Option<DateTime<Utc>> {
        match self {
            MinkowskiType::Instant(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_timestamp(self) -> Option<i64> {
        match self {
            MinkowskiType::Instant(v) => Some(v.timestamp()),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<ValueRc<String>> {
        match self {
            MinkowskiType::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_c_string(self) -> Option<*mut c_char> {
        match self {
            MinkowskiType::String(v) => {
                // Get an independent copy of the string.
                let s: String = v.cloned();

                // Make a CString out of the new bytes.
                let c: CString = CString::new(s).expect("String conversion failed!");

                // Return a C-owned pointer.
                Some(c.into_raw())
            },
            _ => None,
        }
    }

    pub fn into_kw_c_string(self) -> Option<*mut c_char> {
        match self {
            MinkowskiType::Keyword(v) => {
                // Get an independent copy of the string.
                let s: String = v.to_string();

                // Make a CString out of the new bytes.
                let c: CString = CString::new(s).expect("String conversion failed!");

                // Return a C-owned pointer.
                Some(c.into_raw())
            },
            _ => None,
        }
    }

    pub fn into_uuid_c_string(self) -> Option<*mut c_char> {
        match self {
            MinkowskiType::Uuid(v) => {
                // Get an independent copy of the string.
                let s: String = v.hyphenated().to_string();

                // Make a CString out of the new bytes.
                let c: CString = CString::new(s).expect("String conversion failed!");

                // Return a C-owned pointer.
                Some(c.into_raw())
            },
            _ => None,
        }
    }

    pub fn into_uuid(self) -> Option<Uuid> {
        match self {
            MinkowskiType::Uuid(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_uuid_string(self) -> Option<String> {
        match self {
            MinkowskiType::Uuid(v) => Some(v.hyphenated().to_string()),
            _ => None,
        }
    }
}

// We don't do From<i64> or From<SolitonId> 'cos it's ambiguous.

impl From<bool> for MinkowskiType {
    fn from(value: bool) -> MinkowskiType {
        MinkowskiType::Boolean(value)
    }
}

/// Truncate the provided `DateTime` to microsecond precision, and return the corresponding
/// `MinkowskiType::Instant`.
impl From<DateTime<Utc>> for MinkowskiType {
    fn from(value: DateTime<Utc>) -> MinkowskiType {
        MinkowskiType::Instant(value.microsecond_precision())
    }
}

impl From<Uuid> for MinkowskiType {
    fn from(value: Uuid) -> MinkowskiType {
        MinkowskiType::Uuid(value)
    }
}

impl<'a> From<&'a str> for MinkowskiType {
    fn from(value: &'a str) -> MinkowskiType {
        MinkowskiType::String(ValueRc::new(value.to_string()))
    }
}

impl From<Arc<String>> for MinkowskiType {
    fn from(value: Arc<String>) -> MinkowskiType {
        MinkowskiType::String(ValueRc::from_arc(value))
    }
}

impl From<Rc<String>> for MinkowskiType {
    fn from(value: Rc<String>) -> MinkowskiType {
        MinkowskiType::String(ValueRc::from_rc(value))
    }
}

impl From<Box<String>> for MinkowskiType {
    fn from(value: Box<String>) -> MinkowskiType {
        MinkowskiType::String(ValueRc::new(*value))
    }
}

impl From<String> for MinkowskiType {
    fn from(value: String) -> MinkowskiType {
        MinkowskiType::String(ValueRc::new(value))
    }
}

impl From<Arc<Keyword>> for MinkowskiType {
    fn from(value: Arc<Keyword>) -> MinkowskiType {
        MinkowskiType::Keyword(ValueRc::from_arc(value))
    }
}

impl From<Rc<Keyword>> for MinkowskiType {
    fn from(value: Rc<Keyword>) -> MinkowskiType {
        MinkowskiType::Keyword(ValueRc::from_rc(value))
    }
}

impl From<Keyword> for MinkowskiType {
    fn from(value: Keyword) -> MinkowskiType {
        MinkowskiType::Keyword(ValueRc::new(value))
    }
}

impl From<u32> for MinkowskiType {
    fn from(value: u32) -> MinkowskiType {
        MinkowskiType::Long(value as i64)
    }
}

impl From<i32> for MinkowskiType {
    fn from(value: i32) -> MinkowskiType {
        MinkowskiType::Long(value as i64)
    }
}

impl From<f64> for MinkowskiType {
    fn from(value: f64) -> MinkowskiType {
        MinkowskiType::Double(OrderedFloat(value))
    }
}

trait MicrosecondPrecision {
    /// Truncate the provided `DateTime` to microsecond precision.
    fn microsecond_precision(self) -> Self;
}

impl MicrosecondPrecision for DateTime<Utc> {
    fn microsecond_precision(self) -> DateTime<Utc> {
        let nanoseconds = self.nanosecond();
        if nanoseconds % 1000 == 0 {
            return self;
        }
        let microseconds = nanoseconds / 1000;
        let truncated = microseconds * 1000;
        self.with_nanosecond(truncated).expect("valid timestamp")
    }
}


/// The values bound in a causetq specification can be:
///
/// * Vecs of structured values, for multi-valued component attributes or nested expressions.
/// * Single structured values, for single-valued component attributes or nested expressions.
/// * Single typed values, for simple attributes.
///
/// The `ConstrainedEntsConstraint` enum defines these three options.
///
/// Causetic also supports structured inputs; at present EinsteinDB does not, but this type
/// would also serve that purpose.
///
/// Note that maps are not ordered, and so `ConstrainedEntsConstraint` is neither `Ord` nor `PartialOrd`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstrainedEntsConstraint {
    Scalar(MinkowskiType),
    Vec(ValueRc<Vec<ConstrainedEntsConstraint>>),
    Map(ValueRc<StructuredMap>),
}

impl<T> From<T> for ConstrainedEntsConstraint where T: Into<MinkowskiType> {
    fn from(value: T) -> Self {
        ConstrainedEntsConstraint::Scalar(value.into())
    }
}

impl From<StructuredMap> for ConstrainedEntsConstraint {
    fn from(value: StructuredMap) -> Self {
        ConstrainedEntsConstraint::Map(ValueRc::new(value))
    }
}

impl From<Vec<ConstrainedEntsConstraint>> for ConstrainedEntsConstraint {
    fn from(value: Vec<ConstrainedEntsConstraint>) -> Self {
        ConstrainedEntsConstraint::Vec(ValueRc::new(value))
    }
}

impl ConstrainedEntsConstraint {
    pub fn into_scalar(self) -> Option<MinkowskiType> {
        match self {
            ConstrainedEntsConstraint::Scalar(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_vec(self) -> Option<ValueRc<Vec<ConstrainedEntsConstraint>>> {
        match self {
            ConstrainedEntsConstraint::Vec(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_map(self) -> Option<ValueRc<StructuredMap>> {
        match self {
            ConstrainedEntsConstraint::Map(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_scalar(&self) -> Option<&MinkowskiType> {
        match self {
            &ConstrainedEntsConstraint::Scalar(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_vec(&self) -> Option<&Vec<ConstrainedEntsConstraint>> {
        match self {
            &ConstrainedEntsConstraint::Vec(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&StructuredMap> {
        match self {
            &ConstrainedEntsConstraint::Map(ref v) => Some(v),
            _ => None,
        }
    }
}

/// A pull expression expands a Constrained into a structure. The returned structure
/// associates attributes named in the input or retrieved from the store with values.
/// This association is a `StructuredMap`.
///
/// Note that 'attributes' in Causetic's case can mean:
/// - Reversed attribute keywords (:artist/_country).
/// - An alias using `:as` (:artist/name :as "Band name").
///
/// We entirely support the former, and partially support the latter -- you can alias
/// using a different keyword only.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StructuredMap(pub IndexMap<ValueRc<Keyword>, ConstrainedEntsConstraint>);

impl Deref for StructuredMap {
    type Target = IndexMap<ValueRc<Keyword>, ConstrainedEntsConstraint>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl StructuredMap {
    pub fn insert<N, B>(&mut self, name: N, value: B) where N: Into<ValueRc<Keyword>>, B: Into<ConstrainedEntsConstraint> {
        self.0.insert(name.into(), value.into());
    }
}

impl From<IndexMap<ValueRc<Keyword>, ConstrainedEntsConstraint>> for StructuredMap {
    fn from(src: IndexMap<ValueRc<Keyword>, ConstrainedEntsConstraint>) -> Self {
        StructuredMap(src)
    }
}

// Mostly for testing.
impl<T> From<Vec<(Keyword, T)>> for StructuredMap where T: Into<ConstrainedEntsConstraint> {
    fn from(value: Vec<(Keyword, T)>) -> Self {
        let mut sm = StructuredMap::default();
        for (k, v) in value.into_iter() {
            sm.insert(k, v);
        }
        sm
    }
}

impl ConstrainedEntsConstraint {
    /// Returns true if the provided type is `Some` and matches this value's type, or if the
    /// provided type is `None`.
    #[inline]
    pub fn is_congruent_with<T: Into<Option<MinkowskiValueType>>>(&self, t: T) -> bool {
        t.into().map_or(true, |x| self.matches_type(x))
    }

    #[inline]
    pub fn matches_type(&self, t: MinkowskiValueType) -> bool {
        self.value_type() == Some(t)
    }

    pub fn value_type(&self) -> Option<MinkowskiValueType> {
        match self {
            &ConstrainedEntsConstraint::Scalar(ref v) => Some(v.value_type()),

            &ConstrainedEntsConstraint::Map(_) => None,
            &ConstrainedEntsConstraint::Vec(_) => None,
        }
    }
}

/// Return the current time as a UTC `DateTime` instance with microsecond precision.
pub fn now() -> DateTime<Utc> {
    Utc::now().microsecond_precision()
}

impl ConstrainedEntsConstraint {
    pub fn into_known_causetid(self) -> Option<KnownSolitonId> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Ref(v)) => Some(KnownSolitonId(v)),
            _ => None,
        }
    }

    pub fn into_causetid(self) -> Option<SolitonId> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Ref(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_kw(self) -> Option<ValueRc<Keyword>> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Keyword(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_boolean(self) -> Option<bool> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Boolean(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_long(self) -> Option<i64> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Long(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_double(self) -> Option<f64> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Double(v)) => Some(v.into_inner()),
            _ => None,
        }
    }

    pub fn into_instant(self) -> Option<DateTime<Utc>> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Instant(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_timestamp(self) -> Option<i64> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Instant(v)) => Some(v.timestamp()),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<ValueRc<String>> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::String(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_uuid(self) -> Option<Uuid> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Uuid(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_uuid_string(self) -> Option<String> {
        match self {
            ConstrainedEntsConstraint::Scalar(MinkowskiType::Uuid(v)) => Some(v.hyphenated().to_string()),
            _ => None,
        }
    }

    pub fn into_c_string(self) -> Option<*mut c_char> {
        match self {
            ConstrainedEntsConstraint::Scalar(v) => v.into_c_string(),
            _ => None,
        }
    }

    pub fn into_kw_c_string(self) -> Option<*mut c_char> {
        match self {
            ConstrainedEntsConstraint::Scalar(v) => v.into_kw_c_string(),
            _ => None,
        }
    }

    pub fn into_uuid_c_string(self) -> Option<*mut c_char> {
        match self {
            ConstrainedEntsConstraint::Scalar(v) => v.into_uuid_c_string(),
            _ => None,
        }
    }

    pub fn as_causetid(&self) -> Option<&SolitonId> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::Ref(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_kw(&self) -> Option<&ValueRc<Keyword>> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::Keyword(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<&bool> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::Boolean(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_long(&self) -> Option<&i64> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::Long(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<&f64> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::Double(ref v)) => Some(&v.0),
            _ => None,
        }
    }

    pub fn as_instant(&self) -> Option<&DateTime<Utc>> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::Instant(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&ValueRc<String>> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::String(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_uuid(&self) -> Option<&Uuid> {
        match self {
            &ConstrainedEntsConstraint::Scalar(MinkowskiType::Uuid(ref v)) => Some(v),
            _ => None,
        }
    }
}

#[test]
fn test_typed_value() {
    assert!(MinkowskiType::Boolean(false).is_congruent_with(None));
    assert!(MinkowskiType::Boolean(false).is_congruent_with(MinkowskiValueType::Boolean));
    assert!(!MinkowskiType::typed_string("foo").is_congruent_with(MinkowskiValueType::Boolean));
    assert!(MinkowskiType::typed_string("foo").is_congruent_with(MinkowskiValueType::String));
    assert!(MinkowskiType::typed_string("foo").is_congruent_with(None));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_flags() {
        let attr1 = Attribute {
            index: true,
            value_type: MinkowskiValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        };

        assert!(attr1.flags() & AttributeBitFlags::IndexAVET as u8 != 0);
        assert!(attr1.flags() & AttributeBitFlags::IndexVAET as u8 != 0);
        assert!(attr1.flags() & AttributeBitFlags::IndexFulltext as u8 == 0);
        assert!(attr1.flags() & AttributeBitFlags::UniqueValue as u8 == 0);

        let attr2 = Attribute {
            index: false,
            value_type: MinkowskiValueType::Boolean,
            fulltext: true,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        };

        assert!(attr2.flags() & AttributeBitFlags::IndexAVET as u8 == 0);
        assert!(attr2.flags() & AttributeBitFlags::IndexVAET as u8 == 0);
        assert!(attr2.flags() & AttributeBitFlags::IndexFulltext as u8 != 0);
        assert!(attr2.flags() & AttributeBitFlags::UniqueValue as u8 != 0);

        let attr3 = Attribute {
            index: false,
            value_type: MinkowskiValueType::Boolean,
            fulltext: true,
            unique: Some(attribute::Unique::CausetIdity),
            multival: false,
            component: false,
            no_history: false,
        };

        assert!(attr3.flags() & AttributeBitFlags::IndexAVET as u8 == 0);
        assert!(attr3.flags() & AttributeBitFlags::IndexVAET as u8 == 0);
        assert!(attr3.flags() & AttributeBitFlags::IndexFulltext as u8 != 0);
        assert!(attr3.flags() & AttributeBitFlags::UniqueValue as u8 != 0);
    }
}
