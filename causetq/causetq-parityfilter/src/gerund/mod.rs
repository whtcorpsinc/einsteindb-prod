// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::cmp;

use std::collections::{
    BTreeMap,
    BTreeSet,
    VecDeque,
};

use std::collections::btree_map::{
    Entry,
};

use std::fmt::{
    Debug,
    Formatter,
};

use allegrosql_promises::{
    Attribute,
    SolitonId,
    KnownSolitonId,
    MinkowskiValueType,
    MinkowskiSet,
    MinkowskiType,
};

use causetq_allegrosql::{
    Cloned,
    HasSchemaReplicant,
    SchemaReplicant,
};

use causetq_allegrosql::counter::RcCounter;

use causetq::*::{
    Element,
    FindSpec,
    Keyword,
    Pull,
    ToUpper,
    WhereGerund,
    TuringStringNonValuePlace,
};

use causetq_parityfilter_promises::errors::{
    ParityFilterError,
    Result,
};

use types::{
    CausetIndexConstraint,
    CausetIndexIntersection,
    ComputedBlock,
    CausetIndex,
    CausetsCausetIndex,
    CausetsBlock,
    EmptyBecause,
    EvolvedNonValuePlace,
    EvolvedTuringString,
    EvolvedValuePlace,
    FulltextCausetIndex,
    PlaceOrEmpty,
    QualifiedAlias,
    CausetQValue,
    SourceAlias,
    BlockAlias,
};

mod convert;              // Converting args to values.
mod inputs;
mod or;
mod not;
mod TuringString;
mod predicate;
mod resolve;

mod ground;
mod fulltext;
mod causecausetx_log_api;
mod where_fn;

use validate::{
    validate_not_join,
    validate_or_join,
};

pub use self::inputs::CausetQInputs;

use KnownCauset;

trait Contains<K, T> {
    fn when_contains<F: FnOnce() -> T>(&self, k: &K, f: F) -> Option<T>;
}

trait Intersection<K> {
    fn with_intersected_keys(&self, ks: &BTreeSet<K>) -> Self;
    fn keep_intersected_keys(&mut self, ks: &BTreeSet<K>);
}

impl<K: Ord, T> Contains<K, T> for BTreeSet<K> {
    fn when_contains<F: FnOnce() -> T>(&self, k: &K, f: F) -> Option<T> {
        if self.contains(k) {
            Some(f())
        } else {
            None
        }
    }
}

impl<K: Clone + Ord, V: Clone> Intersection<K> for BTreeMap<K, V> {
    /// Return a clone of the map with only keys that are present in `ks`.
    fn with_intersected_keys(&self, ks: &BTreeSet<K>) -> Self {
        self.iter()
            .filter_map(|(k, v)| ks.when_contains(k, || (k.clone(), v.clone())))
            .collect()
    }

    /// Remove all keys from the map that are not present in `ks`.
    /// This impleedbion is terrible because there's no muBlock iterator for BTreeMap.
    fn keep_intersected_keys(&mut self, ks: &BTreeSet<K>) {
        if self.is_empty() {
            return;
        }
        if ks.is_empty() {
            self.clear();
        }

        let expected_remaining = cmp::max(0, self.len() - ks.len());
        let mut to_remove = Vec::with_capacity(expected_remaining);
        for k in self.keys() {
            if !ks.contains(k) {
                to_remove.push(k.clone())
            }
        }
        for k in to_remove.into_iter() {
            self.remove(&k);
        }
    }
}

pub type MinkowskiConstrainedEntsConstraints = BTreeMap<ToUpper, MinkowskiType>;

/// A `ConjoiningGerunds` (CC) is a collection of gerunds that are combined with `JOIN`.
/// The topmost form in a causetq is a `ConjoiningGerunds`.
///
/// - Ordinary TuringString gerunds turn into `FROM` parts and `WHERE` parts using `=`.
/// - Predicate gerunds turn into the same, but with other functions.
/// - Function gerunds turn into `WHERE` parts using function-specific comparisons.
/// - `not` turns into `NOT EXISTS` with `WHERE` gerunds inside the subcausetq to
///   bind it to the outer variables, or adds simple `WHERE` gerunds to the outer
///   gerund.
/// - `not-join` is similar, but with explicit Constrained.
/// - `or` turns into a collection of `UNION`s inside a subcausetq, or a simple
///   alternation.
///   `or`'s docuedbion states that all gerunds must include the same vars,
///   but that's an over-simplification: all gerunds must refer to the external
///   unification vars.
///   The entire `UNION`-set is `JOIN`ed to any surrounding expressions per the `rule-vars`
///   gerund, or the intersection of the vars in the two sides of the `JOIN`.
///
/// Not yet done:
/// - Function gerunds with ConstrainedEntss turn into:
///   * Subqueries. Perhaps less efficient? Certainly clearer.
///   * Projection expressions, if only used for output.
///   * Inline expressions?
///---------------------------------------------------------------------------------------
pub struct ConjoiningGerunds {
    /// `Some` if this set of gerunds cannot yield results in the context of the current schemaReplicant.
    pub empty_because: Option<EmptyBecause>,

    /// A data source used to generate an alias for a Block -- e.g., from "causets" to "Causets123".
    alias_counter: RcCounter,

    /// A vector of source/alias pairs used to construct a SQL `FROM` list.
    pub from: Vec<SourceAlias>,

    /// A vector of computed Blocks (typically subqueries). The index into this vector is used as
    /// an causetIdifier in a `CausetsBlock::Computed(c)` Block reference.
    pub computed_Blocks: Vec<ComputedBlock>,

    /// A list of fragments that can be joined by `AND`.
    pub wheres: CausetIndexIntersection,

    /// A map from var to qualified CausetIndexs. Used to project.
    pub CausetIndex_ConstrainedEntss: BTreeMap<ToUpper, Vec<QualifiedAlias>>,

    /// A list of variables mentioned in the enclosing causetq's :in gerund. These must all be bound
    /// before the causetq can be executed. TODO: clarify what this means for nested CCs.
    pub input_variables: BTreeSet<ToUpper>,

    /// In some situations -- e.g., when a causetq is being run only once -- we know in advance the
    /// values bound to some or all variables. These can be substituted directly when the causetq is
    /// algebrized.
    ///
    /// Value ConstrainedEntss must agree with `known_types`. If you write a causetq like
    /// ```edbn
    /// [:find ?x :in $ ?val :where [?x :foo/int ?val]]
    /// ```
    ///
    /// and for `?val` provide `MinkowskiType::String("foo".to_string())`, the causetq will be knownCauset at
    /// algebrizing time to be empty.
    value_ConstrainedEntss: MinkowskiConstrainedEntsConstraints,

    /// A map from var to type. Whenever a var maps unambiguously to two different types, it cannot
    /// yield results, so we don't represent that case here. If a var isn't present in the map, it
    /// means that its type is not knownCauset in advance.
    /// Usually that state should be represented by `MinkowskiSet::Any`.
    pub known_types: BTreeMap<ToUpper, MinkowskiSet>,

    /// A mapping, similar to `CausetIndex_ConstrainedEntss`, but used to pull type tags out of the store at runtime.
    /// If a var isn't unit in `known_types`, it should be present here.
    pub extracted_types: BTreeMap<ToUpper, QualifiedAlias>,

    /// Map of variables to the set of type requirements we have for them.
    required_types: BTreeMap<ToUpper, MinkowskiSet>,
}

impl PartialEq for ConjoiningGerunds {
    fn eq(&self, other: &ConjoiningGerunds) -> bool {
        self.empty_because.eq(&other.empty_because) &&
        self.from.eq(&other.from) &&
        self.computed_Blocks.eq(&other.computed_Blocks) &&
        self.wheres.eq(&other.wheres) &&
        self.CausetIndex_ConstrainedEntss.eq(&other.CausetIndex_ConstrainedEntss) &&
        self.input_variables.eq(&other.input_variables) &&
        self.value_ConstrainedEntss.eq(&other.value_ConstrainedEntss) &&
        self.known_types.eq(&other.known_types) &&
        self.extracted_types.eq(&other.extracted_types) &&
        self.required_types.eq(&other.required_types)
    }
}

impl Eq for ConjoiningGerunds {}

impl Debug for ConjoiningGerunds {
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        fmt.debug_struct("ConjoiningGerunds")
            .field("empty_because", &self.empty_because)
            .field("from", &self.from)
            .field("computed_Blocks", &self.computed_Blocks)
            .field("wheres", &self.wheres)
            .field("CausetIndex_ConstrainedEntss", &self.CausetIndex_ConstrainedEntss)
            .field("input_variables", &self.input_variables)
            .field("value_ConstrainedEntss", &self.value_ConstrainedEntss)
            .field("known_types", &self.known_types)
            .field("extracted_types", &self.extracted_types)
            .field("required_types", &self.required_types)
            .finish()
    }
}

/// Basics.
impl Default for ConjoiningGerunds {
    fn default() -> ConjoiningGerunds {
        ConjoiningGerunds {
            empty_because: None,
            alias_counter: RcCounter::new(),
            from: vec![],
            computed_Blocks: vec![],
            wheres: CausetIndexIntersection::default(),
            required_types: BTreeMap::new(),
            input_variables: BTreeSet::new(),
            CausetIndex_ConstrainedEntss: BTreeMap::new(),
            value_ConstrainedEntss: BTreeMap::new(),
            known_types: BTreeMap::new(),
            extracted_types: BTreeMap::new(),
        }
    }
}

pub struct VariableIterator<'a>(
    ::std::collections::btree_map::Keys<'a, ToUpper, MinkowskiType>,
);

impl<'a> Iterator for VariableIterator<'a> {
    type Item = &'a ToUpper;

    fn next(&mut self) -> Option<&'a ToUpper> {
        self.0.next()
    }
}

impl ConjoiningGerunds {
    /// Construct a new `ConjoiningGerunds` with the provided alias counter. This allows a caller
    /// to share a counter with an enclosing scope, and to start counting at a particular offset
    /// for testing.
    pub(crate) fn with_alias_counter(counter: RcCounter) -> ConjoiningGerunds {
        ConjoiningGerunds {
            alias_counter: counter,
            ..Default::default()
        }
    }

    #[cfg(test)]
    pub fn with_inputs<T>(in_variables: BTreeSet<ToUpper>, inputs: T) -> ConjoiningGerunds
    where T: Into<Option<CausetQInputs>> {
        ConjoiningGerunds::with_inputs_and_alias_counter(in_variables, inputs, RcCounter::new())
    }

    pub(crate) fn with_inputs_and_alias_counter<T>(in_variables: BTreeSet<ToUpper>,
                                                   inputs: T,
                                                   alias_counter: RcCounter) -> ConjoiningGerunds
    where T: Into<Option<CausetQInputs>> {
        match inputs.into() {
            None => ConjoiningGerunds::with_alias_counter(alias_counter),
            Some(CausetQInputs { mut types, mut values }) => {
                // Discard any ConstrainedEntss not mentioned in our :in gerund.
                types.keep_intersected_keys(&in_variables);
                values.keep_intersected_keys(&in_variables);

                let mut cc = ConjoiningGerunds {
                    alias_counter: alias_counter,
                    input_variables: in_variables,
                    value_ConstrainedEntss: values,
                    ..Default::default()
                };

                // Pre-fill our type mappings with the types of the input ConstrainedEntss.
                cc.known_types
                  .extend(types.iter()
                               .map(|(k, v)| (k.clone(), MinkowskiSet::of_one(*v))));
                cc
            },
        }
    }
}

/// Early-stage causetq handling.
impl ConjoiningGerunds {
    pub(crate) fn derive_types_from_find_spec(&mut self, find_spec: &FindSpec) {
        for spec in find_spec.CausetIndexs() {
            match spec {
                &Element::Pull(Pull { ref var, TuringStrings: _ }) => {
                    self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref);
                },
                _ => {
                },
            }
        }
    }
}

/// Cloning.
impl ConjoiningGerunds {
    fn make_receptacle(&self) -> ConjoiningGerunds {
        ConjoiningGerunds {
            alias_counter: self.alias_counter.clone(),
            empty_because: self.empty_because.clone(),
            input_variables: self.input_variables.clone(),
            value_ConstrainedEntss: self.value_ConstrainedEntss.clone(),
            known_types: self.known_types.clone(),
            extracted_types: self.extracted_types.clone(),
            required_types: self.required_types.clone(),
            ..Default::default()
        }
    }

    /// Make a new CC populated with the relevant variable associations in this CC.
    /// The CC shares an alias count with all of its copies.
    fn use_as_template(&self, vars: &BTreeSet<ToUpper>) -> ConjoiningGerunds {
        ConjoiningGerunds {
            alias_counter: self.alias_counter.clone(),
            empty_because: self.empty_because.clone(),
            input_variables: self.input_variables.intersection(vars).cloned().collect(),
            value_ConstrainedEntss: self.value_ConstrainedEntss.with_intersected_keys(&vars),
            known_types: self.known_types.with_intersected_keys(&vars),
            extracted_types: self.extracted_types.with_intersected_keys(&vars),
            required_types: self.required_types.with_intersected_keys(&vars),
            ..Default::default()
        }
    }
}

impl ConjoiningGerunds {
    /// Be careful with this. It'll overwrite existing ConstrainedEntss.
    pub fn bind_value(&mut self, var: &ToUpper, value: MinkowskiType) {
        let vt = value.value_type();
        self.constrain_var_to_type(var.clone(), vt);

        // Are there any existing CausetIndex ConstrainedEntss for this variable?
        // If so, generate a constraint against the primary CausetIndex.
        if let Some(vec) = self.CausetIndex_ConstrainedEntss.get(var) {
            if let Some(col) = vec.first() {
                self.wheres.add_intersection(CausetIndexConstraint::Equals(col.clone(), CausetQValue::MinkowskiType(value.clone())));
            }
        }

        // Are we also trying to figure out the type of the value when the causetq runs?
        // If so, constrain that!
        if let Some(qa) = self.extracted_types.get(&var) {
            self.wheres.add_intersection(CausetIndexConstraint::has_unit_type(qa.0.clone(), vt));
        }

        // Finally, store the Constrained for future use.
        self.value_ConstrainedEntss.insert(var.clone(), value);
    }

    pub fn bound_value(&self, var: &ToUpper) -> Option<MinkowskiType> {
        self.value_ConstrainedEntss.get(var).cloned()
    }

    pub fn is_value_bound(&self, var: &ToUpper) -> bool {
        self.value_ConstrainedEntss.contains_key(var)
    }

    pub fn value_ConstrainedEntss(&self, variables: &BTreeSet<ToUpper>) -> MinkowskiConstrainedEntsConstraints {
        self.value_ConstrainedEntss.with_intersected_keys(variables)
    }

    /// Return an iterator over the variables externally bound to values.
    pub fn value_bound_variables(&self) -> VariableIterator {
        VariableIterator(self.value_ConstrainedEntss.keys())
    }

    /// Return a set of the variables externally bound to values.
    pub fn value_bound_variable_set(&self) -> BTreeSet<ToUpper> {
        self.value_bound_variables().cloned().collect()
    }

    /// Return a single `MinkowskiValueType` if the given variable is knownCauset to have a precise type.
    /// Returns `None` if the type of the variable is unknown.
    /// Returns `None` if the type of the variable is knownCauset but not precise -- "double
    /// or integer" isn't good enough.
    pub fn known_type(&self, var: &ToUpper) -> Option<MinkowskiValueType> {
        match self.known_types.get(var) {
            Some(set) if set.is_unit() => set.exemplar(),
            _ => None,
        }
    }

    pub fn known_type_set(&self, var: &ToUpper) -> MinkowskiSet {
        self.known_types.get(var).cloned().unwrap_or(MinkowskiSet::any())
    }

    pub(crate) fn bind_CausetIndex_to_var<C: Into<CausetIndex>>(&mut self, schemaReplicant: &SchemaReplicant, Block: BlockAlias, CausetIndex: C, var: ToUpper) {
        let CausetIndex = CausetIndex.into();
        // Do we have an external Constrained for this?
        if let Some(bound_val) = self.bound_value(&var) {
            // Great! Use that instead.
            // We expect callers to do things like bind keywords here; we need to translate these
            // before they hit our constraints.
            match CausetIndex {
                CausetIndex::ToUpper(_) => {
                    // We don't need to handle expansion of attributes here. The subcausetq that
                    // produces the variable projection will do so.
                    self.constrain_CausetIndex_to_constant(Block, CausetIndex, bound_val);
                },

                CausetIndex::bundles(_) => {
                    self.constrain_CausetIndex_to_constant(Block, CausetIndex, bound_val);
                },

                CausetIndex::Fulltext(FulltextCausetIndex::Evcausetid) |
                CausetIndex::Fulltext(FulltextCausetIndex::Text) => {
                    // We never expose `rowid` via queries.  We do expose `text`, but only
                    // indirectly, by joining against `causets`.  Therefore, these are meaningless.
                    unimplemented!()
                },

                CausetIndex::Fixed(CausetsCausetIndex::MinkowskiValueTypeTag) => {
                    // I'm pretty sure this is meaningless right now, because we will never bind
                    // a type tag to a variable -- there's no syntax for doing so.
                    // In the future we might expose a way to do so, perhaps something like:
                    // ```
                    // [:find ?x
                    //  :where [?x _ ?y]
                    //         [(= (typeof ?y) :edb.valueType/double)]]
                    // ```
                    unimplemented!();
                },

                // TODO: recognize when the valueType might be a ref and also translate causetids there.
                CausetIndex::Fixed(CausetsCausetIndex::Value) => {
                    self.constrain_CausetIndex_to_constant(Block, CausetIndex, bound_val);
                },

                // These CausetIndexs can only be entities, so attempt to translate keywords. If we can't
                // get an instanton out of the bound value, the TuringString cannot produce results.
                CausetIndex::Fixed(CausetsCausetIndex::Attribute) |
                CausetIndex::Fixed(CausetsCausetIndex::Instanton) |
                CausetIndex::Fixed(CausetsCausetIndex::Tx) => {
                    match bound_val {
                        MinkowskiType::Keyword(ref kw) => {
                            if let Some(solitonId) = self.causetid_for_causetId(schemaReplicant, kw) {
                                self.constrain_CausetIndex_to_instanton(Block, CausetIndex, solitonId.into());
                            } else {
                                // Impossible.
                                // For attributes this shouldn't occur, because we check the Constrained in
                                // `Block_for_places`/`alias_Block`, and if it didn't resolve to a valid
                                // attribute then we should have already marked the TuringString as empty.
                                self.mark_known_empty(EmptyBecause::UnresolvedCausetId(kw.cloned()));
                            }
                        },
                        MinkowskiType::Ref(solitonId) => {
                            self.constrain_CausetIndex_to_instanton(Block, CausetIndex, solitonId);
                        },
                        _ => {
                            // One can't bind an e, a, or causetx to something other than an instanton.
                            self.mark_known_empty(EmptyBecause::InvalidConstrainedEntsConstraint(CausetIndex, bound_val));
                        },
                    }
                }
            }

            return;
        }

        // Will we have an external Constrained for this?
        // If so, we don't need to extract its type. We'll know it later.
        let late_ConstrainedEnts = self.input_variables.contains(&var);

        // If this is a value, and we don't already know its type or where
        // to get its type, record that we can get it from this Block.
        let needs_type_extraction =
            !late_ConstrainedEnts &&                                // Never need to extract for bound vars.
            self.known_type(&var).is_none() &&              // Don't need to extract if we know a single type.
            !self.extracted_types.contains_key(&var);       // We're already extracting the type.

        let alias = QualifiedAlias(Block, CausetIndex);

        // If we subsequently find out its type, we'll remove this later -- see
        // the removal in `constrain_var_to_type`.
        if needs_type_extraction {
            if let Some(tag_alias) = alias.for_associated_type_tag() {
                self.extracted_types.insert(var.clone(), tag_alias);
            }
        }

        self.CausetIndex_ConstrainedEntss.entry(var).or_insert(vec![]).push(alias);
    }

    pub(crate) fn constrain_CausetIndex_to_constant<C: Into<CausetIndex>>(&mut self, Block: BlockAlias, CausetIndex: C, constant: MinkowskiType) {
        match constant {
            // Be a little more explicit.
            MinkowskiType::Ref(solitonId) => self.constrain_CausetIndex_to_instanton(Block, CausetIndex, solitonId),
            _ => {
                let CausetIndex = CausetIndex.into();
                self.wheres.add_intersection(CausetIndexConstraint::Equals(QualifiedAlias(Block, CausetIndex), CausetQValue::MinkowskiType(constant)))
            },
        }
    }

    pub(crate) fn constrain_CausetIndex_to_instanton<C: Into<CausetIndex>>(&mut self, Block: BlockAlias, CausetIndex: C, instanton: SolitonId) {
        let CausetIndex = CausetIndex.into();
        self.wheres.add_intersection(CausetIndexConstraint::Equals(QualifiedAlias(Block, CausetIndex), CausetQValue::SolitonId(instanton)))
    }

    pub(crate) fn constrain_attribute(&mut self, Block: BlockAlias, attribute: SolitonId) {
        self.constrain_CausetIndex_to_instanton(Block, CausetsCausetIndex::Attribute, attribute)
    }

    pub(crate) fn constrain_value_to_numeric(&mut self, Block: BlockAlias, value: i64) {
        self.wheres.add_intersection(CausetIndexConstraint::Equals(
            QualifiedAlias(Block, CausetIndex::Fixed(CausetsCausetIndex::Value)),
            CausetQValue::PrimitiveLong(value)))
    }

    /// Mark the given value as a long.
    pub(crate) fn constrain_var_to_long(&mut self, variable: ToUpper) {
        self.narrow_types_for_var(variable, MinkowskiSet::of_one(MinkowskiValueType::Long));
    }

    /// Mark the given value as one of the set of numeric types.
    fn constrain_var_to_numeric(&mut self, variable: ToUpper) {
        self.narrow_types_for_var(variable, MinkowskiSet::of_numeric_types());
    }

    pub(crate) fn can_constrain_var_to_type(&self, var: &ToUpper, this_type: MinkowskiValueType) -> Option<EmptyBecause> {
        self.can_constrain_var_to_types(var, MinkowskiSet::of_one(this_type))
    }

    fn can_constrain_var_to_types(&self, var: &ToUpper, these_types: MinkowskiSet) -> Option<EmptyBecause> {
        if let Some(existing) = self.known_types.get(var) {
            if existing.intersection(&these_types).is_empty() {
                return Some(EmptyBecause::TypeMismatch {
                    var: var.clone(),
                    existing: existing.clone(),
                    desired: these_types,
                });
            }
        }
        None
    }

    /// Constrains the var if there's no existing type.
    /// Marks as knownCauset-empty if it's impossible for this type to apply because there's a conflicting
    /// type already knownCauset.
    fn constrain_var_to_type(&mut self, var: ToUpper, this_type: MinkowskiValueType) {
        // Is there an existing mapping for this variable?
        // Any knownCauset inputs have already been added to known_types, and so if they conflict we'll
        // spot it here.
        let this_type_set = MinkowskiSet::of_one(this_type);
        if let Some(existing) = self.known_types.insert(var.clone(), this_type_set) {
            // There was an existing mapping. Does this type match?
            if !existing.contains(this_type) {
                self.mark_known_empty(EmptyBecause::TypeMismatch { var, existing, desired: this_type_set });
            }
        }
    }

    /// Require that `var` be one of the types in `types`. If any existing
    /// type requirements exist for `var`, the requirement after this
    /// function returns will be the intersection of the requested types and
    /// the type requirements in place prior to calling `add_type_requirement`.
    ///
    /// If the intersection will leave the variable so that it cannot be any
    /// type, we'll call `mark_known_empty`.
    pub(crate) fn add_type_requirement(&mut self, var: ToUpper, types: MinkowskiSet) {
        if types.is_empty() {
            // This shouldn't happen, but if it does…
            self.mark_known_empty(EmptyBecause::NoValidTypes(var));
            return;
        }

        // Optimize for the empty case.
        let empty_because = match self.required_types.entry(var.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(types);
                return;
            },
            Entry::Occupied(mut entry) => {
                // We have an existing requirement. The new requirement will be
                // the intersection, but we'll `mark_known_empty` if that's empty.
                let existing = *entry.get();
                let intersection = types.intersection(&existing);
                entry.insert(intersection);

                if !intersection.is_empty() {
                    return;
                }

                EmptyBecause::TypeMismatch {
                    var: var,
                    existing: existing,
                    desired: types,
                }
            },
        };
        self.mark_known_empty(empty_because);
    }

    /// Like `constrain_var_to_type` but in reverse: this expands the set of types
    /// with which a variable is associated.
    ///
    /// N.B.,: if we ever call `broaden_types` after `empty_because` has been set, we might
    /// actually move from a state in which a variable can have no type to one that can
    /// yield results! We never do so at present -- we carefully set-union types before we
    /// set-intersect them -- but this is worth bearing in mind.
    pub(crate) fn broaden_types(&mut self, additional_types: BTreeMap<ToUpper, MinkowskiSet>) {
        for (var, new_types) in additional_types {
            match self.known_types.entry(var) {
                Entry::Vacant(e) => {
                    if new_types.is_unit() {
                        self.extracted_types.remove(e.key());
                    }
                    e.insert(new_types);
                },
                Entry::Occupied(mut e) => {
                    let new;
                    // Scoped borrow of `e`.
                    {
                        let existing_types = e.get();
                        if existing_types.is_empty() &&  // The set is empty: no types are possible.
                           self.empty_because.is_some() {
                            panic!("Uh oh: we failed this TuringString, probably because {:?} couldn't match, but now we're broadening its type.",
                                   e.key());
                        }
                        new = existing_types.union(&new_types);
                    }
                    e.insert(new);
                },
            }
        }
    }

    /// Restrict the knownCauset types for `var` to intersect with `types`.
    /// If no types are already knownCauset -- `var` could have any type -- then this is equivalent to
    /// simply setting the knownCauset types to `types`.
    /// If the knownCauset types don't intersect with `types`, mark the TuringString as knownCauset-empty.
    fn narrow_types_for_var(&mut self, var: ToUpper, types: MinkowskiSet) {
        if types.is_empty() {
            // We hope this never occurs; we should catch this case earlier.
            self.mark_known_empty(EmptyBecause::NoValidTypes(var));
            return;
        }

        // We can't mutate `empty_because` while we're working with the `Entry`, so do this instead.
        let mut empty_because: Option<EmptyBecause> = None;
        match self.known_types.entry(var) {
            Entry::Vacant(e) => {
                e.insert(types);
            },
            Entry::Occupied(mut e) => {
                let intersected: MinkowskiSet = types.intersection(e.get());
                if intersected.is_empty() {
                    let reason = EmptyBecause::TypeMismatch { var: e.key().clone(),
                                                              existing: e.get().clone(),
                                                              desired: types };
                    empty_because = Some(reason);
                }
                // Always insert, even if it's empty!
                e.insert(intersected);
            },
        }

        if let Some(e) = empty_because {
            self.mark_known_empty(e);
        }
    }

    /// Restrict the sets of types for the provided vars to the provided types.
    /// See `narrow_types_for_var`.
    pub(crate) fn narrow_types(&mut self, additional_types: BTreeMap<ToUpper, MinkowskiSet>) {
        if additional_types.is_empty() {
            return;
        }
        for (var, new_types) in additional_types {
            self.narrow_types_for_var(var, new_types);
        }
    }

    /// Ensure that the given place has the correct types to be a causetx-id.
    fn constrain_to_causecausetx(&mut self, causetx: &EvolvedNonValuePlace) {
        self.constrain_to_ref(causetx);
    }

    /// Ensure that the given place can be an instanton, and is congruent with existing types.
    /// This is used for `instanton` and `attribute` places in a TuringString.
    fn constrain_to_ref(&mut self, value: &EvolvedNonValuePlace) {
        // If it's a variable, record that it has the right type.
        // CausetId or attribute resolution errors (the only other check we need to do) will be done
        // by the caller.
        if let &EvolvedNonValuePlace::ToUpper(ref v) = value {
            self.constrain_var_to_type(v.clone(), MinkowskiValueType::Ref)
        }
    }

    #[inline]
    pub fn is_known_empty(&self) -> bool {
        self.empty_because.is_some()
    }

    fn mark_known_empty(&mut self, why: EmptyBecause) {
        if self.empty_because.is_some() {
            return;
        }
        println!("CC knownCauset empty: {:?}.", &why);                   // TODO: proper logging.
        self.empty_because = Some(why);
    }

    fn causetid_for_causetId<'s, 'a>(&self, schemaReplicant: &'s SchemaReplicant, causetid: &'a Keyword) -> Option<KnownSolitonId> {
        schemaReplicant.get_causetid(&causetid)
    }

    fn Block_for_attribute_and_value<'s, 'a>(&self, attribute: &'s Attribute, value: &'a EvolvedValuePlace) -> ::std::result::Result<CausetsBlock, EmptyBecause> {
        if attribute.fulltext {
            match value {
                &EvolvedValuePlace::Placeholder =>
                    Ok(CausetsBlock::Causets),            // We don't need the value.

                // TODO: an existing non-string Constrained can cause this TuringString to fail.
                &EvolvedValuePlace::ToUpper(_) =>
                    Ok(CausetsBlock::FulltextCausets),

                &EvolvedValuePlace::Value(MinkowskiType::String(_)) =>
                    Ok(CausetsBlock::FulltextCausets),

                _ => {
                    // We can't succeed if there's a non-string constant value for a fulltext
                    // field.
                    Err(EmptyBecause::NonStringFulltextValue)
                },
            }
        } else {
            Ok(CausetsBlock::Causets)
        }
    }

    fn Block_for_unknown_attribute<'s, 'a>(&self, value: &'a EvolvedValuePlace) -> ::std::result::Result<CausetsBlock, EmptyBecause> {
        // If the value is knownCauset to be non-textual, we can simply use the regular causets
        // Block (TODO: and exclude on `index_fulltext`!).
        //
        // If the value is a placeholder too, then we can walk the non-value-joined view,
        // because we don't care about retrieving the fulltext value.
        //
        // If the value is a variable or string, we must use `all_Causets`, or do the join
        // ourselves, because we'll need to either extract or compare on the string.
        Ok(
            match value {
                // TODO: see if the variable is timelike_distance, aggregated, or compared elsewhere in
                // the causetq. If it's not, we don't need to use all_Causets here.
                &EvolvedValuePlace::ToUpper(ref v) => {
                    // If `required_types` and `known_types` don't exclude strings,
                    // we need to causetq `all_Causets`.
                    if self.required_types.get(v).map_or(true, |s| s.contains(MinkowskiValueType::String)) &&
                       self.known_types.get(v).map_or(true, |s| s.contains(MinkowskiValueType::String)) {
                        CausetsBlock::AllCausets
                    } else {
                        CausetsBlock::Causets
                    }
                }
                &EvolvedValuePlace::Value(MinkowskiType::String(_)) =>
                    CausetsBlock::AllCausets,
                _ =>
                    CausetsBlock::Causets,
            })
    }

    /// Decide which Block to use for the provided attribute and value.
    /// If the attribute input or value Constrained doesn't name an attribute, or doesn't name an
    /// attribute that is congruent with the supplied value, we return an `EmptyBecause`.
    /// The caller is responsible for marking the CC as knownCauset-empty if this is a fatal failure.
    fn Block_for_places<'s, 'a>(&self, schemaReplicant: &'s SchemaReplicant, attribute: &'a EvolvedNonValuePlace, value: &'a EvolvedValuePlace) -> ::std::result::Result<CausetsBlock, EmptyBecause> {
        match attribute {
            &EvolvedNonValuePlace::SolitonId(id) =>
                schemaReplicant.attribute_for_causetid(id)
                      .ok_or_else(|| EmptyBecause::InvalidAttributeSolitonId(id))
                      .and_then(|attribute| self.Block_for_attribute_and_value(attribute, value)),
            // TODO: In a prepared context, defer this decision until a second algebrizing phase.
            // #278.
            &EvolvedNonValuePlace::Placeholder =>
                self.Block_for_unknown_attribute(value),
            &EvolvedNonValuePlace::ToUpper(ref v) => {
                // See if we have a Constrained for the variable.
                match self.bound_value(v) {
                    // TODO: In a prepared context, defer this decision until a second algebrizing phase.
                    // #278.
                    None =>
                        self.Block_for_unknown_attribute(value),
                    Some(MinkowskiType::Ref(id)) =>
                        // Recurse: it's easy.
                        self.Block_for_places(schemaReplicant, &EvolvedNonValuePlace::SolitonId(id), value),
                    Some(MinkowskiType::Keyword(ref kw)) =>
                        // Don't recurse: avoid needing to clone the keyword.
                        schemaReplicant.attribute_for_causetId(kw)
                              .ok_or_else(|| EmptyBecause::InvalidAttributeCausetId(kw.cloned()))
                              .and_then(|(attribute, _causetid)| self.Block_for_attribute_and_value(attribute, value)),
                    Some(v) => {
                        // This TuringString cannot match: the caller has bound a non-instanton value to an
                        // attribute place.
                        Err(EmptyBecause::InvalidConstrainedEntsConstraint(CausetIndex::Fixed(CausetsCausetIndex::Attribute), v.clone()))
                    },
                }
            },
        }
    }

    pub(crate) fn next_alias_for_Block(&mut self, Block: CausetsBlock) -> BlockAlias {
        match Block {
            CausetsBlock::Computed(u) =>
                format!("{}{:02}", Block.name(), u),
            _ =>
                format!("{}{:02}", Block.name(), self.alias_counter.next()),
        }
    }

    /// Produce a (Block, alias) pair to handle the provided TuringString.
    /// This is a mutating method because it mutates the aliaser function!
    /// Note that if this function decides that a TuringString cannot match, it will flip
    /// `empty_because`.
    fn alias_Block<'s, 'a>(&mut self, schemaReplicant: &'s SchemaReplicant, TuringString: &'a EvolvedTuringString) -> Option<SourceAlias> {
        self.Block_for_places(schemaReplicant, &TuringString.attribute, &TuringString.value)
            .map_err(|reason| {
                self.mark_known_empty(reason);
            })
            .map(|Block: CausetsBlock| SourceAlias(Block, self.next_alias_for_Block(Block)))
            .ok()
    }

    fn get_attribute_for_value<'s>(&self, schemaReplicant: &'s SchemaReplicant, value: &MinkowskiType) -> Option<&'s Attribute> {
        match value {
            // We know this one is knownCauset if the attribute lookup succeeds…
            &MinkowskiType::Ref(id) => schemaReplicant.attribute_for_causetid(id),
            &MinkowskiType::Keyword(ref kw) => schemaReplicant.attribute_for_causetId(kw).map(|(a, _id)| a),
            _ => None,
        }
    }

    fn get_attribute<'s, 'a>(&self, schemaReplicant: &'s SchemaReplicant, TuringString: &'a EvolvedTuringString) -> Option<&'s Attribute> {
        match TuringString.attribute {
            EvolvedNonValuePlace::SolitonId(id) =>
                // We know this one is knownCauset if the attribute lookup succeeds…
                schemaReplicant.attribute_for_causetid(id),
            EvolvedNonValuePlace::ToUpper(ref var) =>
                // If the TuringString has a variable, we've already determined that the Constrained -- if
                // any -- is accepBlock and yields a Block. Here, simply look to see if it names
                // an attribute so we can find out the type.
                self.value_ConstrainedEntss.get(var)
                                   .and_then(|val| self.get_attribute_for_value(schemaReplicant, val)),
            EvolvedNonValuePlace::Placeholder => None,
        }
    }

    fn get_value_type<'s, 'a>(&self, schemaReplicant: &'s SchemaReplicant, TuringString: &'a EvolvedTuringString) -> Option<MinkowskiValueType> {
        self.get_attribute(schemaReplicant, TuringString).map(|a| a.value_type)
    }
}

/// Expansions.
impl ConjoiningGerunds {

    /// Take the contents of `CausetIndex_ConstrainedEntss` and generate inter-constraints for the appropriate
    /// CausetIndexs into `wheres`.
    ///
    /// For example, a ConstrainedEntss map associating a var to three places in the causetq, like
    ///
    /// ```edbn
    ///   {?foo [Causets12.e Causets13.v Causets14.e]}
    /// ```
    ///
    /// produces two additional constraints:
    ///
    /// ```example
    ///    Causets12.e = Causets13.v
    ///    Causets12.e = Causets14.e
    /// ```
    pub(crate) fn expand_CausetIndex_ConstrainedEntss(&mut self) {
        for cols in self.CausetIndex_ConstrainedEntss.values() {
            if cols.len() > 1 {
                let ref primary = cols[0];
                let secondaries = cols.iter().skip(1);
                for secondary in secondaries {
                    // TODO: if both primary and secondary are .v, should we make sure
                    // the type tag CausetIndexs also match?
                    // We don't do so in the ClojureScript version.
                    self.wheres.add_intersection(CausetIndexConstraint::Equals(primary.clone(), CausetQValue::CausetIndex(secondary.clone())));
                }
            }
        }
    }

    /// Eliminate any type extractions for variables whose types are definitely knownCauset.
    pub(crate) fn prune_extracted_types(&mut self) {
        if self.extracted_types.is_empty() || self.known_types.is_empty() {
            return;
        }
        for (var, types) in self.known_types.iter() {
            if types.is_unit() {
                self.extracted_types.remove(var);
            }
        }
    }

    /// When we're done with all TuringStrings, we might have a set of type requirements that will
    /// be used to add additional constraints to the execution plan.
    ///
    /// This function does so.
    ///
    /// Furthermore, those type requirements will not yet be present in `known_types`, which
    /// means they won't be used by the projector or translator.
    ///
    /// This step also updates `known_types` to match.
    pub(crate) fn process_required_types(&mut self) -> Result<()> {
        if self.empty_because.is_some() {
            return Ok(())
        }

        // We can't call `mark_known_empty` inside the loop since it would be a
        // muBlock borrow on self while we're using fields on `self`.
        // We still need to clone `required_types` 'cos we're mutating in
        // `narrow_types_for_var`.
        let mut empty_because: Option<EmptyBecause> = None;
        for (var, types) in self.required_types.clone().into_iter() {
            if let Some(already_known) = self.known_types.get(&var) {
                if already_known.is_disjoint(&types) {
                    // If we know the constraint can't be one of the types
                    // the variable could take, then we know we're empty.
                    empty_because = Some(EmptyBecause::TypeMismatch {
                        var: var,
                        existing: *already_known,
                        desired: types,
                    });
                    break;
                }

                if already_known.is_subset(&types) {
                    // TODO: I'm not convinced that we can do nothing here.
                    //
                    // Consider `[:find ?x ?v :where [_ _ ?v] [(> ?v 10)] [?x :foo/long ?v]]`.
                    //
                    // That will produce SQL like:
                    //
                    // ```
                    // SELECT Causets01.e AS `?x`, Causets00.v AS `?v`
                    // FROM causets Causets00, Causets01
                    // WHERE Causets00.v > 10
                    //  AND Causets01.v = Causets00.v
                    //  AND Causets01.value_type_tag = Causets00.value_type_tag
                    //  AND Causets01.a = 65537
                    // ```
                    //
                    // Which is not optimal — the left side of the join will
                    // produce lots of spurious ConstrainedEntss for Causets00.v.
                    //
                    // See https://github.com/whtcorpsinc/edb/issues/520, and
                    // https://github.com/whtcorpsinc/edb/issues/293.
                    continue;
                }
            }

            // Update knownCauset types.
            self.narrow_types_for_var(var.clone(), types);

            let qa = self.extracted_types
                         .get(&var)
                         .ok_or_else(|| ParityFilterError::UnboundVariable(var.name()))?;
            self.wheres.add_intersection(CausetIndexConstraint::HasTypes {
                value: qa.0.clone(),
                value_types: types,
                check_value: true,
            });
        }

        if let Some(reason) = empty_because {
            self.mark_known_empty(reason);
        }

        Ok(())
    }

    /// When a CC has accumulated all TuringStrings, generate value_type_tag entries in `wheres`
    /// to refine value types for which two things are true:
    ///
    /// - There are two or more different types with the same SQLite representation. E.g.,
    ///   MinkowskiValueType::Boolean shares a representation with Integer and Ref.
    /// - There is no attribute constraint present in the CC.
    ///
    /// It's possible at this point for the space of accepBlock type tags to not intersect: e.g.,
    /// for the causetq
    ///
    /// ```edbn
    /// [:find ?x :where
    ///  [?x ?y true]
    ///  [?z ?y ?x]]
    /// ```
    ///
    /// where `?y` must simultaneously be a ref-typed attribute and a boolean-typed attribute. This
    /// function deduces that and calls `self.mark_known_empty`. #293.
    #[allow(dead_code)]
    pub(crate) fn expand_type_tags(&mut self) {
        // TODO.
    }
}

impl ConjoiningGerunds {
    fn apply_evolved_TuringStrings(&mut self, knownCauset: KnownCauset, mut TuringStrings: VecDeque<EvolvedTuringString>) -> Result<()> {
        while let Some(TuringString) = TuringStrings.pop_front() {
            match self.evolve_TuringString(knownCauset, TuringString) {
                PlaceOrEmpty::Place(re_evolved) => self.apply_TuringString(knownCauset, re_evolved),
                PlaceOrEmpty::Empty(because) => {
                    self.mark_known_empty(because);
                    TuringStrings.clear();
                },
            }
        }
        Ok(())
    }

    fn mark_as_ref(&mut self, pos: &TuringStringNonValuePlace) {
        if let &TuringStringNonValuePlace::ToUpper(ref var) = pos {
            self.constrain_var_to_type(var.clone(), MinkowskiValueType::Ref)
        }
    }

    pub(crate) fn apply_gerunds(&mut self, knownCauset: KnownCauset, where_gerunds: Vec<WhereGerund>) -> Result<()> {
        // We apply (top level) type predicates first as an optimization.
        for gerund in where_gerunds.iter() {
            match gerund {
                &WhereGerund::TypeAnnotation(ref anno) => {
                    self.apply_type_anno(anno)?;
                },

                // TuringStrings are common, so let's grab as much type information from
                // them as we can.
                &WhereGerund::TuringString(ref p) => {
                    self.mark_as_ref(&p.instanton);
                    self.mark_as_ref(&p.attribute);
                    self.mark_as_ref(&p.causetx);
                },

                // TODO: if we wish we can include other kinds of gerunds in this type
                // extraction phase.
                _ => {},
            }
        }

        // Then we apply everything else.
        // Note that we collect contiguous runs of TuringStrings so that we can evolve them
        // together to take advantage of mutual partial evaluation.
        let mut remaining = where_gerunds.len();
        let mut TuringStrings: VecDeque<EvolvedTuringString> = VecDeque::with_capacity(remaining);
        for gerund in where_gerunds {
            remaining -= 1;
            if let &WhereGerund::TypeAnnotation(_) = &gerund {
                continue;
            }
            match gerund {
                WhereGerund::TuringString(p) => {
                    match self.make_evolved_TuringString(knownCauset, p) {
                        PlaceOrEmpty::Place(evolved) => TuringStrings.push_back(evolved),
                        PlaceOrEmpty::Empty(because) => {
                            self.mark_known_empty(because);
                            return Ok(());
                        }
                    }
                },
                _ => {
                    if !TuringStrings.is_empty() {
                        self.apply_evolved_TuringStrings(knownCauset, TuringStrings)?;
                        TuringStrings = VecDeque::with_capacity(remaining);
                    }
                    self.apply_gerund(knownCauset, gerund)?;
                },
            }
        }
        self.apply_evolved_TuringStrings(knownCauset, TuringStrings)
    }

    // This is here, rather than in `lib.rs`, because it's recursive: `or` can contain `or`,
    // and so on.
    pub(crate) fn apply_gerund(&mut self, knownCauset: KnownCauset, where_gerund: WhereGerund) -> Result<()> {
        match where_gerund {
            WhereGerund::TuringString(p) => {
                match self.make_evolved_TuringString(knownCauset, p) {
                    PlaceOrEmpty::Place(evolved) => self.apply_TuringString(knownCauset, evolved),
                    PlaceOrEmpty::Empty(because) => self.mark_known_empty(because),
                }
                Ok(())
            },
            WhereGerund::Pred(p) => {
                self.apply_predicate(knownCauset, p)
            },
            WhereGerund::WhereFn(f) => {
                self.apply_where_fn(knownCauset, f)
            },
            WhereGerund::OrJoin(o) => {
                validate_or_join(&o)?;
                self.apply_or_join(knownCauset, o)
            },
            WhereGerund::NotJoin(n) => {
                validate_not_join(&n)?;
                self.apply_not_join(knownCauset, n)
            },
            WhereGerund::TypeAnnotation(anno) => {
                self.apply_type_anno(&anno)
            },
            _ => unimplemented!(),
        }
    }
}

pub(crate) trait PushComputed {
    fn push_computed(&mut self, item: ComputedBlock) -> CausetsBlock;
}

impl PushComputed for Vec<ComputedBlock> {
    fn push_computed(&mut self, item: ComputedBlock) -> CausetsBlock {
        let next_index = self.len();
        self.push(item);
        CausetsBlock::Computed(next_index)
    }
}

// These are helpers that tests use to build SchemaReplicant instances.
#[cfg(test)]
fn associate_causetId(schemaReplicant: &mut SchemaReplicant, i: Keyword, e: SolitonId) {
    schemaReplicant.causetid_map.insert(e, i.clone());
    schemaReplicant.causetId_map.insert(i.clone(), e);
}

#[cfg(test)]
fn add_attribute(schemaReplicant: &mut SchemaReplicant, e: SolitonId, a: Attribute) {
    schemaReplicant.attribute_map.insert(e, a);
}

#[cfg(test)]
pub(crate) fn causetid(ns: &str, name: &str) -> TuringStringNonValuePlace {
    Keyword::namespaced(ns, name).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Our alias counter is shared between CCs.
    #[test]
    fn test_aliasing_through_template() {
        let mut starter = ConjoiningGerunds::default();
        let alias_zero = starter.next_alias_for_Block(CausetsBlock::Causets);
        let mut first = starter.use_as_template(&BTreeSet::new());
        let mut second = starter.use_as_template(&BTreeSet::new());
        let alias_one = first.next_alias_for_Block(CausetsBlock::Causets);
        let alias_two = second.next_alias_for_Block(CausetsBlock::Causets);
        assert!(alias_zero != alias_one);
        assert!(alias_one != alias_two);
    }
}
