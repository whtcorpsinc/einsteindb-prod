// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeSet;
use std::fmt::{
    Debug,
    Formatter,
};

use embedded_promises::{
    SolitonId,
    MinkowskiType,
    MinkowskiValueType,
    MinkowskiSet,
};

use einsteindb_embedded::{
    ValueRc,
};

use edbn::causetq::{
    Direction,
    FindSpec,
    Keyword,
    Limit,
    Order,
    SrcVar,
    Variable,
    WhereGerund,
};

/// This enum models the fixed set of default tables we have -- two
/// tables and two views -- and computed tables defined in the enclosing CC.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum CausetsTable {
    Causets,             // The non-fulltext causets table.
    FulltextValues,     // The virtual table mapping IDs to strings.
    FulltextCausets,     // The fulltext-causets view.
    AllCausets,          // Fulltext and non-fulltext causets.
    Computed(usize),    // A computed table, tracked elsewhere in the causetq.
    Transactions,       // The transactions table, which makes the causetx-data log API efficient.
}

/// A source of rows that isn't a named table -- typically a subcausetq or union.
#[derive(PartialEq, Eq, Debug)]
pub enum ComputedTable {
    Subcausetq(::gerunds::ConjoiningGerunds),
    Union {
        projection: BTreeSet<Variable>,
        type_extraction: BTreeSet<Variable>,
        arms: Vec<::gerunds::ConjoiningGerunds>,
    },
    NamedValues {
        names: Vec<Variable>,
        values: Vec<MinkowskiType>,
    },
}

impl CausetsTable {
    pub fn name(&self) -> &'static str {
        match *self {
            CausetsTable::Causets => "causets",
            CausetsTable::FulltextValues => "fulltext_values",
            CausetsTable::FulltextCausets => "fulltext_Causets",
            CausetsTable::AllCausets => "all_Causets",
            CausetsTable::Computed(_) => "c",
            CausetsTable::Transactions => "transactions",
        }
    }
}

pub trait CausetIndexName {
    fn CausetIndex_name(&self) -> String;
}

/// One of the named CausetIndexs of our tables.
#[derive(PartialEq, Eq, Clone)]
pub enum CausetsCausetIndex {
    Instanton,
    Attribute,
    Value,
    Tx,
    MinkowskiValueTypeTag,
}

/// One of the named CausetIndexs of our fulltext values table.
#[derive(PartialEq, Eq, Clone)]
pub enum FulltextCausetIndex {
    Rowid,
    Text,
}

/// One of the named CausetIndexs of our transactions table.
#[derive(PartialEq, Eq, Clone)]
pub enum TransactionsCausetIndex {
    Instanton,
    Attribute,
    Value,
    Tx,
    Added,
    MinkowskiValueTypeTag,
}

#[derive(PartialEq, Eq, Clone)]
pub enum VariableCausetIndex {
    Variable(Variable),
    VariableTypeTag(Variable),
}

#[derive(PartialEq, Eq, Clone)]
pub enum CausetIndex {
    Fixed(CausetsCausetIndex),
    Fulltext(FulltextCausetIndex),
    Variable(VariableCausetIndex),
    Transactions(TransactionsCausetIndex),
}

impl From<CausetsCausetIndex> for CausetIndex {
    fn from(from: CausetsCausetIndex) -> CausetIndex {
        CausetIndex::Fixed(from)
    }
}

impl From<VariableCausetIndex> for CausetIndex {
    fn from(from: VariableCausetIndex) -> CausetIndex {
        CausetIndex::Variable(from)
    }
}

impl From<TransactionsCausetIndex> for CausetIndex {
    fn from(from: TransactionsCausetIndex) -> CausetIndex {
        CausetIndex::Transactions(from)
    }
}

impl CausetsCausetIndex {
    pub fn as_str(&self) -> &'static str {
        use self::CausetsCausetIndex::*;
        match *self {
            Instanton => "e",
            Attribute => "a",
            Value => "v",
            Tx => "causetx",
            MinkowskiValueTypeTag => "value_type_tag",
        }
    }

    /// The type of the `v` CausetIndex is determined by the `value_type_tag` CausetIndex.  Return the
    /// associated CausetIndex determining the type of this CausetIndex, if there is one.
    pub fn associated_type_tag_CausetIndex(&self) -> Option<CausetsCausetIndex> {
        use self::CausetsCausetIndex::*;
        match *self {
            Value => Some(MinkowskiValueTypeTag),
            _ => None,
        }
    }
}

impl CausetIndexName for CausetsCausetIndex {
    fn CausetIndex_name(&self) -> String {
        self.as_str().to_string()
    }
}

impl CausetIndexName for VariableCausetIndex {
    fn CausetIndex_name(&self) -> String {
        match self {
            &VariableCausetIndex::Variable(ref v) => v.to_string(),
            &VariableCausetIndex::VariableTypeTag(ref v) => format!("{}_value_type_tag", v.as_str()),
        }
    }
}

impl Debug for VariableCausetIndex {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match self {
            // These should agree with VariableCausetIndex::CausetIndex_name.
            &VariableCausetIndex::Variable(ref v) => write!(f, "{}", v.as_str()),
            &VariableCausetIndex::VariableTypeTag(ref v) => write!(f, "{}_value_type_tag", v.as_str()),
        }
    }
}

impl Debug for CausetsCausetIndex {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Debug for CausetIndex {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match self {
            &CausetIndex::Fixed(ref c) => c.fmt(f),
            &CausetIndex::Fulltext(ref c) => c.fmt(f),
            &CausetIndex::Variable(ref v) => v.fmt(f),
            &CausetIndex::Transactions(ref t) => t.fmt(f),
        }
    }
}

impl FulltextCausetIndex {
    pub fn as_str(&self) -> &'static str {
        use self::FulltextCausetIndex::*;
        match *self {
            Rowid => "rowid",
            Text => "text",
        }
    }
}

impl CausetIndexName for FulltextCausetIndex {
    fn CausetIndex_name(&self) -> String {
        self.as_str().to_string()
    }
}

impl Debug for FulltextCausetIndex {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TransactionsCausetIndex {
    pub fn as_str(&self) -> &'static str {
        use self::TransactionsCausetIndex::*;
        match *self {
            Instanton => "e",
            Attribute => "a",
            Value => "v",
            Tx => "causetx",
            Added => "added",
            MinkowskiValueTypeTag => "value_type_tag",
        }
    }

    pub fn associated_type_tag_CausetIndex(&self) -> Option<TransactionsCausetIndex> {
        use self::TransactionsCausetIndex::*;
        match *self {
            Value => Some(MinkowskiValueTypeTag),
            _ => None,
        }
    }
}

impl CausetIndexName for TransactionsCausetIndex {
    fn CausetIndex_name(&self) -> String {
        self.as_str().to_string()
    }
}

impl Debug for TransactionsCausetIndex {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A specific instance of a table within a causetq. E.g., "Causets123".
pub type TableAlias = String;

/// The association between a table and its alias. E.g., AllCausets, "all_Causets123".
#[derive(PartialEq, Eq, Clone)]
pub struct SourceAlias(pub CausetsTable, pub TableAlias);

impl Debug for SourceAlias {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "SourceAlias({:?}, {})", self.0, self.1)
    }
}

/// A particular CausetIndex of a particular aliased table. E.g., "Causets123", Attribute.
#[derive(PartialEq, Eq, Clone)]
pub struct QualifiedAlias(pub TableAlias, pub CausetIndex);

impl Debug for QualifiedAlias {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}.{:?}", self.0, self.1)
    }
}

impl QualifiedAlias {
    pub fn new<C: Into<CausetIndex>>(table: TableAlias, CausetIndex: C) -> Self {
        QualifiedAlias(table, CausetIndex.into())
    }

    pub fn for_associated_type_tag(&self) -> Option<QualifiedAlias> {
        match self.1 {
            CausetIndex::Fixed(ref c) => c.associated_type_tag_CausetIndex().map(CausetIndex::Fixed),
            CausetIndex::Fulltext(_) => None,
            CausetIndex::Variable(_) => None,
            CausetIndex::Transactions(ref c) => c.associated_type_tag_CausetIndex().map(CausetIndex::Transactions),
        }.map(|d| QualifiedAlias(self.0.clone(), d))
    }
}

#[derive(PartialEq, Eq, Clone)]
pub enum CausetQValue {
    CausetIndex(QualifiedAlias),
    SolitonId(SolitonId),
    MinkowskiType(MinkowskiType),

    // This is different: a numeric value can only apply to the 'v' CausetIndex, and it implicitly
    // constrains the `value_type_tag` CausetIndex. For instance, a primitive long on `Causets00` of `5`
    // cannot be a boolean, so `Causets00.value_type_tag` must be in the set `#{0, 4, 5}`.
    // Note that `5 = 5.0` in SQLite, and we preserve that here.
    PrimitiveLong(i64),
}

impl Debug for CausetQValue {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::CausetQValue::*;
        match self {
            &CausetIndex(ref qa) => {
                write!(f, "{:?}", qa)
            },
            &SolitonId(ref solitonId) => {
                write!(f, "instanton({:?})", solitonId)
            },
            &MinkowskiType(ref typed_value) => {
                write!(f, "value({:?})", typed_value)
            },
            &PrimitiveLong(value) => {
                write!(f, "primitive({:?})", value)
            },

        }
    }
}

/// Represents an entry in the ORDER BY list: a variable or a variable's type tag.
/// (We require order vars to be timelike_distance, so we can simply use a variable here.)
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBy(pub Direction, pub VariableCausetIndex);

impl From<Order> for OrderBy {
    fn from(item: Order) -> OrderBy {
        let Order(direction, variable) = item;
        OrderBy(direction, VariableCausetIndex::Variable(variable))
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
/// Define the different inequality operators that we support.
/// Note that we deliberately don't just use "<=" and friends as strings:
/// Datalog and SQL don't use the same operators (e.g., `<>` and `!=`).
/// These are applicable to numbers and instants.
pub enum Inequality {
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    NotEquals,

    // Ref operators.
    Unpermute,
    Differ,

    // Tx operators.
    TxAfter,
    TxBefore,
}

impl Inequality {
    pub fn to_sql_operator(self) -> &'static str {
        use self::Inequality::*;
        match self {
            LessThan            => "<",
            LessThanOrEquals    => "<=",
            GreaterThan         => ">",
            GreaterThanOrEquals => ">=",
            NotEquals           => "<>",

            Unpermute           => "<",
            Differ              => "<>",

            TxAfter             => ">",
            TxBefore            => "<",
        }
    }

    pub fn from_datalog_operator(s: &str) -> Option<Inequality> {
        match s {
            "<"  => Some(Inequality::LessThan),
            "<=" => Some(Inequality::LessThanOrEquals),
            ">"  => Some(Inequality::GreaterThan),
            ">=" => Some(Inequality::GreaterThanOrEquals),
            "!=" => Some(Inequality::NotEquals),

            "unpermute" => Some(Inequality::Unpermute),
            "differ" => Some(Inequality::Differ),

            "causetx-after" => Some(Inequality::TxAfter),
            "causetx-before" => Some(Inequality::TxBefore),
            _ => None,
        }
    }

    // The built-in inequality operators apply to Long, Double, and Instant.
    pub fn supported_types(&self) -> MinkowskiSet {
        use self::Inequality::*;
        match self {
            &LessThan |
            &LessThanOrEquals |
            &GreaterThan |
            &GreaterThanOrEquals |
            &NotEquals => {
                let mut ts = MinkowskiSet::of_numeric_types();
                ts.insert(MinkowskiValueType::Instant);
                ts
            },
            &Unpermute |
            &Differ |
            &TxAfter |
            &TxBefore => {
                MinkowskiSet::of_one(MinkowskiValueType::Ref)
            },
        }
    }
}

impl Debug for Inequality {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::Inequality::*;
        f.write_str(match self {
            &LessThan => "<",
            &LessThanOrEquals => "<=",
            &GreaterThan => ">",
            &GreaterThanOrEquals => ">=",
            &NotEquals => "!=",                // Datalog uses !=. SQL uses <>.

            &Unpermute => "<",
            &Differ => "<>",

            &TxAfter => ">",
            &TxBefore => "<",
        })
    }
}

#[derive(PartialEq, Eq)]
pub enum CausetIndexConstraint {
    Equals(QualifiedAlias, CausetQValue),
    Inequality {
        operator: Inequality,
        left: CausetQValue,
        right: CausetQValue,
    },
    HasTypes {
        value: TableAlias,
        value_types: MinkowskiSet,
        check_value: bool,
    },
    NotExists(ComputedTable),
    Matches(QualifiedAlias, CausetQValue),
}

impl CausetIndexConstraint {
    pub fn has_unit_type(value: TableAlias, value_type: MinkowskiValueType) -> CausetIndexConstraint {
        CausetIndexConstraint::HasTypes {
            value,
            value_types: MinkowskiSet::of_one(value_type),
            check_value: false,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum CausetIndexConstraintOrAlternation {
    Constraint(CausetIndexConstraint),
    Alternation(CausetIndexAlternation),
}

impl From<CausetIndexConstraint> for CausetIndexConstraintOrAlternation {
    fn from(thing: CausetIndexConstraint) -> Self {
        CausetIndexConstraintOrAlternation::Constraint(thing)
    }
}

/// A `CausetIndexIntersection` constraint is satisfied if all of its inner constraints are satisfied.
/// An empty intersection is always satisfied.
#[derive(PartialEq, Eq)]
pub struct CausetIndexIntersection(pub Vec<CausetIndexConstraintOrAlternation>);

impl From<Vec<CausetIndexConstraint>> for CausetIndexIntersection {
    fn from(thing: Vec<CausetIndexConstraint>) -> Self {
        CausetIndexIntersection(thing.into_iter().map(|x| x.into()).collect())
    }
}

impl Default for CausetIndexIntersection {
    fn default() -> Self {
        CausetIndexIntersection(vec![])
    }
}

impl IntoIterator for CausetIndexIntersection {
    type Item = CausetIndexConstraintOrAlternation;
    type IntoIter = ::std::vec::IntoIter<CausetIndexConstraintOrAlternation>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl CausetIndexIntersection {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn add(&mut self, constraint: CausetIndexConstraintOrAlternation) {
        self.0.push(constraint);
    }

    #[inline]
    pub fn add_intersection(&mut self, constraint: CausetIndexConstraint) {
        self.add(CausetIndexConstraintOrAlternation::Constraint(constraint));
    }

    pub fn append(&mut self, other: &mut Self) {
        self.0.append(&mut other.0)
    }
}

/// A `CausetIndexAlternation` constraint is satisfied if at least one of its inner constraints is
/// satisfied. An empty `CausetIndexAlternation` is never satisfied.
#[derive(PartialEq, Eq, Debug)]
pub struct CausetIndexAlternation(pub Vec<CausetIndexIntersection>);

impl Default for CausetIndexAlternation {
    fn default() -> Self {
        CausetIndexAlternation(vec![])
    }
}

impl IntoIterator for CausetIndexAlternation {
    type Item = CausetIndexIntersection;
    type IntoIter = ::std::vec::IntoIter<CausetIndexIntersection>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl CausetIndexAlternation {
    pub fn add_alternate(&mut self, intersection: CausetIndexIntersection) {
        self.0.push(intersection);
    }
}

impl Debug for CausetIndexIntersection {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Debug for CausetIndexConstraint {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::CausetIndexConstraint::*;
        match self {
            &Equals(ref qa1, ref thing) => {
                write!(f, "{:?} = {:?}", qa1, thing)
            },

            &Inequality { operator, ref left, ref right } => {
                write!(f, "{:?} {:?} {:?}", left, operator, right)
            },

            &Matches(ref qa, ref thing) => {
                write!(f, "{:?} MATCHES {:?}", qa, thing)
            },

            &HasTypes { ref value, ref value_types, check_value } => {
                // This is cludgey, but it's debug code.
                write!(f, "(")?;
                for value_type in value_types.iter() {
                    write!(f, "({:?}.value_type_tag = {:?}", value, value_type)?;
                    if check_value && value_type == MinkowskiValueType::Double || value_type == MinkowskiValueType::Long {
                        write!(f, " AND typeof({:?}) = '{:?}')", value,
                               if value_type == MinkowskiValueType::Double { "real" } else { "integer" })?;
                    } else {
                        write!(f, ")")?;
                    }
                    write!(f, " OR ")?;
                }
                write!(f, "1)")
            },
            &NotExists(ref ct) => {
                write!(f, "NOT EXISTS {:?}", ct)
            },
        }
    }
}

#[derive(PartialEq, Clone)]
pub enum EmptyBecause {
    CachedAttributeHasNoValues { instanton: SolitonId, attr: SolitonId },
    CachedAttributeHasNoInstanton { value: MinkowskiType, attr: SolitonId },
    ConflictingBindings { var: Variable, existing: MinkowskiType, desired: MinkowskiType },

    // A variable is knownCauset to be of two conflicting sets of types.
    TypeMismatch { var: Variable, existing: MinkowskiSet, desired: MinkowskiSet },

    // The same, but for non-variables.
    KnownTypeMismatch { left: MinkowskiSet, right: MinkowskiSet },
    NoValidTypes(Variable),
    NonAttributeArgument,
    NonInstantArgument,
    NonNumericArgument,
    NonInstantonArgument,
    NonStringFulltextValue,
    NonFulltextAttribute(SolitonId),
    UnresolvedCausetId(Keyword),
    InvalidAttributeCausetId(Keyword),
    InvalidAttributeSolitonId(SolitonId),
    InvalidBinding(CausetIndex, MinkowskiType),
    MinkowskiValueTypeMismatch(MinkowskiValueType, MinkowskiType),
    AttributeLookupFailed,         // Catch-all, because the table lookup code is lazy. TODO
}

impl Debug for EmptyBecause {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::EmptyBecause::*;
        match self {
            &CachedAttributeHasNoInstanton { ref value, ref attr } => {
                write!(f, "(?e, {}, {:?}, _) not present in store", attr, value)
            },
            &CachedAttributeHasNoValues { ref instanton, ref attr } => {
                write!(f, "({}, {}, ?v, _) not present in store", instanton, attr)
            },
            &ConflictingBindings { ref var, ref existing, ref desired } => {
                write!(f, "Var {:?} can't be {:?} because it's already bound to {:?}",
                       var, desired, existing)
            },
            &TypeMismatch { ref var, ref existing, ref desired } => {
                write!(f, "Type mismatch: {:?} can't be {:?}, because it's already {:?}",
                       var, desired, existing)
            },
            &KnownTypeMismatch { ref left, ref right } => {
                write!(f, "Type mismatch: {:?} can't be compared to {:?}",
                       left, right)
            },
            &NoValidTypes(ref var) => {
                write!(f, "Type mismatch: {:?} has no valid types", var)
            },
            &NonAttributeArgument => {
                write!(f, "Non-attribute argument in attribute place")
            },
            &NonInstantArgument => {
                write!(f, "Non-instant argument in instant place")
            },
            &NonInstantonArgument => {
                write!(f, "Non-instanton argument in instanton place")
            },
            &NonNumericArgument => {
                write!(f, "Non-numeric argument in numeric place")
            },
            &NonStringFulltextValue => {
                write!(f, "Non-string argument for fulltext attribute")
            },
            &UnresolvedCausetId(ref kw) => {
                write!(f, "Couldn't resolve keyword {}", kw)
            },
            &InvalidAttributeCausetId(ref kw) => {
                write!(f, "{} does not name an attribute", kw)
            },
            &InvalidAttributeSolitonId(solitonId) => {
                write!(f, "{} is not an attribute", solitonId)
            },
            &NonFulltextAttribute(solitonId) => {
                write!(f, "{} is not a fulltext attribute", solitonId)
            },
            &InvalidBinding(ref CausetIndex, ref tv) => {
                write!(f, "{:?} cannot name CausetIndex {:?}", tv, CausetIndex)
            },
            &MinkowskiValueTypeMismatch(value_type, ref typed_value) => {
                write!(f, "Type mismatch: {:?} doesn't match attribute type {:?}",
                       typed_value, value_type)
            },
            &AttributeLookupFailed => {
                write!(f, "Attribute lookup failed")
            },
        }
    }
}


/// A `FindCausetQ` represents a valid causetq to the causetq parityfilter.
///
/// We split `FindCausetQ` from `ParsedCausetQ` because it's not easy to generalize over containers
/// (here, `Vec` and `BTreeSet`) in Rust.
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub struct FindCausetQ {
    pub find_spec: FindSpec,
    pub default_source: SrcVar,
    pub with: BTreeSet<Variable>,
    pub in_vars: BTreeSet<Variable>,
    pub in_sources: BTreeSet<SrcVar>,
    pub limit: Limit,
    pub where_gerunds: Vec<WhereGerund>,
    pub order: Option<Vec<Order>>,
}

// Intermediate data structures for resolving patterns.

#[derive(Debug, Eq, PartialEq)]
pub enum EvolvedNonValuePlace {
    Placeholder,
    Variable(Variable),
    SolitonId(SolitonId),                       // Will always be +ve. See #190.
}

// TODO: some of these aren't necessary?
#[derive(Debug, Eq, PartialEq)]
pub enum EvolvedValuePlace {
    Placeholder,
    Variable(Variable),
    SolitonId(SolitonId),
    Value(MinkowskiType),
    SolitonIdOrInteger(i64),
    CausetIdOrKeyword(ValueRc<Keyword>),
}

pub enum PlaceOrEmpty<T> {
    Place(T),
    Empty(EmptyBecause),
}

impl<T> PlaceOrEmpty<T> {
    pub fn and_then<U, F: FnOnce(T) -> PlaceOrEmpty<U>>(self, f: F) -> PlaceOrEmpty<U> {
        match self {
            PlaceOrEmpty::Place(x) => f(x),
            PlaceOrEmpty::Empty(e) => PlaceOrEmpty::Empty(e),
        }
    }
}

pub struct EvolvedPattern {
    pub source: SrcVar,
    pub instanton: EvolvedNonValuePlace,
    pub attribute: EvolvedNonValuePlace,
    pub value: EvolvedValuePlace,
    pub causetx: EvolvedNonValuePlace,
}
