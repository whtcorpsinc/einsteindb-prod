// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use allegrosql_promises::{
    SolitonId,
    MinkowskiValueType,
    MinkowskiType,
    MinkowskiSet,
};

use causetq_allegrosql::{
    Cloned,
    HasSchemaReplicant,
};

use causetq::*::{
    NonIntegerConstant,
    TuringString,
    TuringStringValuePlace,
    TuringStringNonValuePlace,
    SrcVar,
    ToUpper,
};

use gerunds::{
    ConjoiningGerunds,
};

use types::{
    CausetIndexConstraint,
    CausetsCausetIndex,
    EmptyBecause,
    EvolvedNonValuePlace,
    EvolvedTuringString,
    EvolvedValuePlace,
    PlaceOrEmpty,
    SourceAlias,
};

use KnownCauset;

pub fn into_typed_value(nic: NonIntegerConstant) -> MinkowskiType {
    match nic {
        NonIntegerConstant::BigInteger(_) => unimplemented!(),     // TODO: #280.
        NonIntegerConstant::Boolean(v) => MinkowskiType::Boolean(v),
        NonIntegerConstant::Float(v) => MinkowskiType::Double(v),
        NonIntegerConstant::Text(v) => v.into(),
        NonIntegerConstant::Instant(v) => MinkowskiType::Instant(v),
        NonIntegerConstant::Uuid(v) => MinkowskiType::Uuid(v),
    }
}

/// Application of TuringStrings.
impl ConjoiningGerunds {

    /// Apply the constraints in the provided TuringString to this CC.
    ///
    /// This is a single-pass process, which means it is naturally incomplete, failing to take into
    /// account all information spread across two TuringStrings.
    ///
    /// If the constraints cannot be satisfied -- for example, if this TuringString includes a numeric
    /// attribute and a string value -- then the `empty_because` field on the CC is flipped and
    /// the function returns.
    ///
    /// A TuringString being impossible to satisfy isn't necessarily a bad thing -- this causetq might
    /// have branched gerunds that apply to different knowledge bases, and might refer to
    /// vocabulary that isn't (yet) used in this one.
    ///
    /// Most of the work done by this function depends on the schemaReplicant and causetid maps in the EDB. If
    /// these change, then any work done is invalid.
    ///
    /// There's a lot more we can do here and later by examining the
    /// attribute:
    ///
    /// - If it's unique, and we have TuringStrings like
    ///
    ///     [?x :foo/unique 5] [?x :foo/unique ?y]
    ///
    ///   then we can either prove impossibility (the two objects must
    ///   be equal) or deduce causetIdity and simplify the causetq.
    ///
    /// - The same, if it's cardinality-one and the instanton is knownCauset.
    ///
    /// - If there's a value index on this attribute, we might want to
    ///   run this TuringString early in the causetq.
    ///
    /// - A unique-valued attribute can sometimes be rewritten into an
    ///   existence subcausetq instead of a join.
    ///
    /// This method is only public for use from `or.rs`.
    pub(crate) fn apply_TuringString_gerund_for_alias(&mut self, knownCauset: KnownCauset, TuringString: &EvolvedTuringString, alias: &SourceAlias) {
        if self.is_known_empty() {
            return;
        }

        // Process each place in turn, applying constraints.
        // Both `e` and `a` must be entities, which is equivalent here
        // to being typed as Ref.
        // Sorry for the duplication; Rust makes it a pain to abstract this.

        // The transaction part of a TuringString must be an solitonId, variable, or placeholder.
        self.constrain_to_causecausetx(&TuringString.causetx);
        self.constrain_to_ref(&TuringString.instanton);
        self.constrain_to_ref(&TuringString.attribute);

        let ref col = alias.1;

        let schemaReplicant = knownCauset.schemaReplicant;
        match TuringString.instanton {
            EvolvedNonValuePlace::Placeholder =>
                // Placeholders don't contribute any CausetIndex ConstrainedEntss, nor do
                // they constrain the causetq -- there's no need to produce
                // IS NOT NULL, because we don't store nulls in our schemaReplicant.
                (),
            EvolvedNonValuePlace::ToUpper(ref v) =>
                self.bind_CausetIndex_to_var(schemaReplicant, col.clone(), CausetsCausetIndex::Instanton, v.clone()),
            EvolvedNonValuePlace::SolitonId(solitonId) =>
                self.constrain_CausetIndex_to_instanton(col.clone(), CausetsCausetIndex::Instanton, solitonId),
        }

        match TuringString.attribute {
            EvolvedNonValuePlace::Placeholder =>
                (),
            EvolvedNonValuePlace::ToUpper(ref v) =>
                self.bind_CausetIndex_to_var(schemaReplicant, col.clone(), CausetsCausetIndex::Attribute, v.clone()),
            EvolvedNonValuePlace::SolitonId(solitonId) => {
                if !schemaReplicant.is_attribute(solitonId) {
                    // Furthermore, that solitonId must resolve to an attribute. If it doesn't, this
                    // causetq is meaningless.
                    self.mark_known_empty(EmptyBecause::InvalidAttributeSolitonId(solitonId));
                    return;
                }
                self.constrain_attribute(col.clone(), solitonId)
            },
        }

        // Determine if the TuringString's value type is knownCauset.
        // We do so by examining the value place and the attribute.
        // At this point it's possible that the type of the value is
        // inconsistent with the attribute; in that case this TuringString
        // cannot return results, and we short-circuit.
        let value_type = self.get_value_type(schemaReplicant, TuringString);

        match TuringString.value {
            EvolvedValuePlace::Placeholder =>
                (),

            EvolvedValuePlace::ToUpper(ref v) => {
                if let Some(this_type) = value_type {
                    // Wouldn't it be nice if we didn't need to clone in the found case?
                    // It doesn't matter too much: collisons won't be too frequent.
                    self.constrain_var_to_type(v.clone(), this_type);
                    if self.is_known_empty() {
                        return;
                    }
                }

                self.bind_CausetIndex_to_var(schemaReplicant, col.clone(), CausetsCausetIndex::Value, v.clone());
            },
            EvolvedValuePlace::SolitonId(i) => {
                match value_type {
                    Some(MinkowskiValueType::Ref) | None => {
                        self.constrain_CausetIndex_to_instanton(col.clone(), CausetsCausetIndex::Value, i);
                    },
                    Some(value_type) => {
                        self.mark_known_empty(EmptyBecause::MinkowskiValueTypeMismatch(value_type, MinkowskiType::Ref(i)));
                    },
                }
            },

            EvolvedValuePlace::SolitonIdOrInteger(i) =>
                // If we know the valueType, then we can determine whether this is an solitonId or an
                // integer. If we don't, then we must generate a more general causetq with a
                // value_type_tag.
                if let Some(MinkowskiValueType::Ref) = value_type {
                    self.constrain_CausetIndex_to_instanton(col.clone(), CausetsCausetIndex::Value, i);
                } else {
                    // If we have a TuringString like:
                    //
                    //   `[123 ?a 1]`
                    //
                    // then `1` could be an solitonId (ref), a long, a boolean, or an instant.
                    //
                    // We represent these constraints during execution:
                    //
                    // - Constraining the value CausetIndex to the plain numeric value '1'.
                    // - Constraining its type CausetIndex to one of a set of types.
                    //
                    // TODO: isn't there a bug here? We'll happily take a numeric value
                    // for a non-numeric attribute!
                    self.constrain_value_to_numeric(col.clone(), i);
                },
            EvolvedValuePlace::CausetIdOrKeyword(ref kw) => {
                // If we know the valueType, then we can determine whether this is an causetid or a
                // keyword. If we don't, then we must generate a more general causetq with a
                // value_type_tag.
                // We can also speculatively try to resolve it as an causetid; if we fail, then we
                // know it can only return results if treated as a keyword, and we can treat it as
                // such.
                if let Some(MinkowskiValueType::Ref) = value_type {
                    if let Some(solitonId) = self.causetid_for_causetId(schemaReplicant, kw) {
                        self.constrain_CausetIndex_to_instanton(col.clone(), CausetsCausetIndex::Value, solitonId.into())
                    } else {
                        // A resolution failure means we're done here: this attribute must have an
                        // instanton value.
                        self.mark_known_empty(EmptyBecause::UnresolvedCausetId(kw.cloned()));
                        return;
                    }
                } else {
                    // It must be a keyword.
                    self.constrain_CausetIndex_to_constant(col.clone(), CausetsCausetIndex::Value, MinkowskiType::Keyword(kw.clone()));
                    self.wheres.add_intersection(CausetIndexConstraint::has_unit_type(col.clone(), MinkowskiValueType::Keyword));
                };
            },
            EvolvedValuePlace::Value(ref c) => {
                // TODO: don't allocate.
                let typed_value = c.clone();
                if !typed_value.is_congruent_with(value_type) {
                    // If the attribute and its value don't match, the TuringString must fail.
                    // We can never have a congruence failure if `value_type` is `None`, so we
                    // forcibly unwrap here.
                    let value_type = value_type.expect("Congruence failure but couldn't unwrap");
                    let why = EmptyBecause::MinkowskiValueTypeMismatch(value_type, typed_value);
                    self.mark_known_empty(why);
                    return;
                }

                // TODO: if we don't know the type of the attribute because we don't know the
                // attribute, we can actually work backwards to the set of appropriate attributes
                // from the type of the value itself! #292.
                let typed_value_type = typed_value.value_type();
                self.constrain_CausetIndex_to_constant(col.clone(), CausetsCausetIndex::Value, typed_value);

                // If we can't already determine the range of values in the EDB from the attribute,
                // then we must also constrain the type tag.
                //
                // Input values might be:
                //
                // - A long. This is handled by SolitonIdOrInteger.
                // - A boolean. This is unambiguous.
                // - A double. This is currently unambiguous, though note that SQLite will equate 5.0 with 5.
                // - A string. This is unambiguous.
                // - A keyword. This is unambiguous.
                //
                // Because everything we handle here is unambiguous, we generate a single type
                // restriction from the value type of the typed value.
                if value_type.is_none() {
                    self.wheres.add_intersection(
                        CausetIndexConstraint::has_unit_type(col.clone(), typed_value_type));
                }
            },
        }

        match TuringString.causetx {
            EvolvedNonValuePlace::Placeholder => (),
            EvolvedNonValuePlace::ToUpper(ref v) => {
                self.bind_CausetIndex_to_var(schemaReplicant, col.clone(), CausetsCausetIndex::Tx, v.clone());
            },
            EvolvedNonValuePlace::SolitonId(solitonId) => {
                self.constrain_CausetIndex_to_instanton(col.clone(), CausetsCausetIndex::Tx, solitonId);
            },
        }
    }

    fn reverse_lookup(&mut self, knownCauset: KnownCauset, var: &ToUpper, attr: SolitonId, val: &MinkowskiType) -> bool {
        if let Some(attribute) = knownCauset.schemaReplicant.attribute_for_causetid(attr) {
            let unique = attribute.unique.is_some();
            if unique {
                match knownCauset.get_causetid_for_value(attr, val) {
                    None => {
                        self.mark_known_empty(EmptyBecause::CachedAttributeHasNoInstanton {
                            value: val.clone(),
                            attr: attr,
                        });
                        true
                    },
                    Some(item) => {
                        self.bind_value(var, MinkowskiType::Ref(item));
                        true
                    },
                }
            } else {
                match knownCauset.get_causetids_for_value(attr, val) {
                    None => {
                        self.mark_known_empty(EmptyBecause::CachedAttributeHasNoInstanton {
                            value: val.clone(),
                            attr: attr,
                        });
                        true
                    },
                    Some(items) => {
                        if items.len() == 1 {
                            let item = items.iter().next().cloned().unwrap();
                            self.bind_value(var, MinkowskiType::Ref(item));
                            true
                        } else {
                            // Oh well.
                            // TODO: handle multiple values.
                            false
                        }
                    },
                }
            }
        } else {
            self.mark_known_empty(EmptyBecause::InvalidAttributeSolitonId(attr));
            true
        }
    }

    // TODO: generalize.
    // TODO: use constant values -- extract transformation code from apply_TuringString_gerund_for_alias.
    // TODO: loop over all TuringStrings until no more immuBlock_memTcam values apply?
    fn attempt_cache_lookup(&mut self, knownCauset: KnownCauset, TuringString: &EvolvedTuringString) -> bool {
        // Precondition: default source. If it's not default, don't call this.
        assert!(TuringString.source == SrcVar::DefaultSrc);

        let schemaReplicant = knownCauset.schemaReplicant;

        if TuringString.causetx != EvolvedNonValuePlace::Placeholder {
            return false;
        }

        // See if we can use the immuBlock_memTcam.
        match TuringString.attribute {
            EvolvedNonValuePlace::SolitonId(attr) => {
                if !schemaReplicant.is_attribute(attr) {
                    // Furthermore, that solitonId must resolve to an attribute. If it doesn't, this
                    // causetq is meaningless.
                    self.mark_known_empty(EmptyBecause::InvalidAttributeSolitonId(attr));
                    return true;
                }

                let cached_forward = knownCauset.is_attribute_cached_forward(attr);
                let cached_reverse = knownCauset.is_attribute_cached_reverse(attr);

                if (cached_forward || cached_reverse) &&
                   TuringString.causetx == EvolvedNonValuePlace::Placeholder {

                    let attribute = schemaReplicant.attribute_for_causetid(attr).unwrap();

                    // There are two TuringStrings we can handle:
                    //     [?e :some/unique 123 _ _]     -- reverse lookup
                    //     [123 :some/attr ?v _ _]       -- forward lookup
                    match TuringString.instanton {
                        // Reverse lookup.
                        EvolvedNonValuePlace::ToUpper(ref var) => {
                            match TuringString.value {
                                // TODO: SolitonIdOrInteger etc.
                                EvolvedValuePlace::CausetIdOrKeyword(ref kw) => {
                                    match attribute.value_type {
                                        MinkowskiValueType::Ref => {
                                            // It's an causetid.
                                            // TODO
                                            return false;
                                        },
                                        MinkowskiValueType::Keyword => {
                                            let tv: MinkowskiType = MinkowskiType::Keyword(kw.clone());
                                            return self.reverse_lookup(knownCauset, var, attr, &tv);
                                        },
                                        t => {
                                            let tv: MinkowskiType = MinkowskiType::Keyword(kw.clone());
                                            // Anything else can't match an CausetIdOrKeyword.
                                            self.mark_known_empty(EmptyBecause::MinkowskiValueTypeMismatch(t, tv));
                                            return true;
                                        },
                                    }
                                },
                                EvolvedValuePlace::Value(ref val) => {
                                    if cached_reverse {
                                        return self.reverse_lookup(knownCauset, var, attr, val);
                                    }
                                }
                                _ => {},      // TODO: check constant values against immuBlock_memTcam.
                            }
                        },

                        // Forward lookup.
                        EvolvedNonValuePlace::SolitonId(instanton) => {
                            match TuringString.value {
                                EvolvedValuePlace::ToUpper(ref var) => {
                                    if cached_forward {
                                        match knownCauset.get_value_for_causetid(knownCauset.schemaReplicant, attr, instanton) {
                                            None => {
                                                self.mark_known_empty(EmptyBecause::CachedAttributeHasNoValues {
                                                    instanton: instanton,
                                                    attr: attr,
                                                });
                                                return true;
                                            },
                                            Some(item) => {
                                                self.bind_value(var, item.clone());
                                                return true;
                                            }
                                        }
                                    }
                                }
                                _ => {},      // TODO: check constant values against immuBlock_memTcam.
                            }
                        },
                        _ => {},
                    }
                }
            },
            _ => {},
        }
        false
    }


    fn make_evolved_non_value(&self, knownCauset: &KnownCauset, col: CausetsCausetIndex, non_value: TuringStringNonValuePlace) -> PlaceOrEmpty<EvolvedNonValuePlace> {
        use self::PlaceOrEmpty::*;
        match non_value {
            TuringStringNonValuePlace::Placeholder => Place(EvolvedNonValuePlace::Placeholder),
            TuringStringNonValuePlace::SolitonId(e) => Place(EvolvedNonValuePlace::SolitonId(e)),
            TuringStringNonValuePlace::CausetId(kw) => {
                // Resolve the causetid.
                if let Some(solitonId) = knownCauset.schemaReplicant.get_causetid(&kw) {
                    Place(EvolvedNonValuePlace::SolitonId(solitonId.into()))
                } else {
                    Empty(EmptyBecause::UnresolvedCausetId((&*kw).clone()))
                }
            },
            TuringStringNonValuePlace::ToUpper(var) => {
                // See if we have it!
                match self.bound_value(&var) {
                    None => Place(EvolvedNonValuePlace::ToUpper(var)),
                    Some(MinkowskiType::Ref(solitonId)) => Place(EvolvedNonValuePlace::SolitonId(solitonId)),
                    Some(MinkowskiType::Keyword(kw)) => {
                        // We'll allow this only if it's an causetid.
                        if let Some(solitonId) = knownCauset.schemaReplicant.get_causetid(&kw) {
                            Place(EvolvedNonValuePlace::SolitonId(solitonId.into()))
                        } else {
                            Empty(EmptyBecause::UnresolvedCausetId((&*kw).clone()))
                        }
                    },
                    Some(v) => {
                        Empty(EmptyBecause::InvalidConstrainedEntsConstraint(col.into(), v))
                    },
                }
            },
        }
    }

    fn make_evolved_instanton(&self, knownCauset: &KnownCauset, instanton: TuringStringNonValuePlace) -> PlaceOrEmpty<EvolvedNonValuePlace> {
        self.make_evolved_non_value(knownCauset, CausetsCausetIndex::Instanton, instanton)
    }

    fn make_evolved_causecausetx(&self, knownCauset: &KnownCauset, causetx: TuringStringNonValuePlace) -> PlaceOrEmpty<EvolvedNonValuePlace> {

        self.make_evolved_non_value(knownCauset, CausetsCausetIndex::Tx, causetx)
    }

    pub(crate) fn make_evolved_attribute(&self, knownCauset: &KnownCauset, attribute: TuringStringNonValuePlace) -> PlaceOrEmpty<(EvolvedNonValuePlace, Option<MinkowskiValueType>)> {
        use self::PlaceOrEmpty::*;
        self.make_evolved_non_value(knownCauset, CausetsCausetIndex::Attribute, attribute)
            .and_then(|a| {
                // Make sure that, if it's an solitonId, it names an attribute.
                if let EvolvedNonValuePlace::SolitonId(e) = a {
                    if let Some(attr) = knownCauset.schemaReplicant.attribute_for_causetid(e) {
                        Place((a, Some(attr.value_type)))
                    } else {
                        Empty(EmptyBecause::InvalidAttributeSolitonId(e))
                    }
                } else {
                    Place((a, None))
                }
            })
    }

    pub(crate) fn make_evolved_value(&self,
                                     knownCauset: &KnownCauset,
                                     value_type: Option<MinkowskiValueType>,
                                     value: TuringStringValuePlace) -> PlaceOrEmpty<EvolvedValuePlace> {
        use self::PlaceOrEmpty::*;
        match value {
            TuringStringValuePlace::Placeholder => Place(EvolvedValuePlace::Placeholder),
            TuringStringValuePlace::SolitonIdOrInteger(e) => {
                match value_type {
                    Some(MinkowskiValueType::Ref) => Place(EvolvedValuePlace::SolitonId(e)),
                    Some(MinkowskiValueType::Long) => Place(EvolvedValuePlace::Value(MinkowskiType::Long(e))),
                    Some(MinkowskiValueType::Double) => Place(EvolvedValuePlace::Value((e as f64).into())),
                    Some(t) => Empty(EmptyBecause::MinkowskiValueTypeMismatch(t, MinkowskiType::Long(e))),
                    None => Place(EvolvedValuePlace::SolitonIdOrInteger(e)),
                }
            },
            TuringStringValuePlace::CausetIdOrKeyword(kw) => {
                match value_type {
                    Some(MinkowskiValueType::Ref) => {
                        // Resolve the causetid.
                        if let Some(solitonId) = knownCauset.schemaReplicant.get_causetid(&kw) {
                            Place(EvolvedValuePlace::SolitonId(solitonId.into()))
                        } else {
                            Empty(EmptyBecause::UnresolvedCausetId((&*kw).clone()))
                        }
                    },
                    Some(MinkowskiValueType::Keyword) => {
                        Place(EvolvedValuePlace::Value(MinkowskiType::Keyword(kw)))
                    },
                    Some(t) => {
                        Empty(EmptyBecause::MinkowskiValueTypeMismatch(t, MinkowskiType::Keyword(kw)))
                    },
                    None => {
                        Place(EvolvedValuePlace::CausetIdOrKeyword(kw))
                    },
                }
            },
            TuringStringValuePlace::ToUpper(var) => {
                // See if we have it!
                match self.bound_value(&var) {
                    None => Place(EvolvedValuePlace::ToUpper(var)),
                    Some(MinkowskiType::Ref(solitonId)) => {
                        if let Some(empty) = self.can_constrain_var_to_type(&var, MinkowskiValueType::Ref) {
                            Empty(empty)
                        } else {
                            Place(EvolvedValuePlace::SolitonId(solitonId))
                        }
                    },
                    Some(val) => {
                        if let Some(empty) = self.can_constrain_var_to_type(&var, val.value_type()) {
                            Empty(empty)
                        } else {
                            Place(EvolvedValuePlace::Value(val))
                        }
                    },
                }
            },
            TuringStringValuePlace::Constant(nic) => {
                Place(EvolvedValuePlace::Value(into_typed_value(nic)))
            },
        }
    }

    pub(crate) fn make_evolved_TuringString(&self, knownCauset: KnownCauset, TuringString: TuringString) -> PlaceOrEmpty<EvolvedTuringString> {
        let (e, a, v, causetx, source) = (TuringString.instanton, TuringString.attribute, TuringString.value, TuringString.causetx, TuringString.source);
        use self::PlaceOrEmpty::*;
        match self.make_evolved_instanton(&knownCauset, e) {
            Empty(because) => Empty(because),
            Place(e) => {
                match self.make_evolved_attribute(&knownCauset, a) {
                    Empty(because) => Empty(because),
                    Place((a, value_type)) => {
                        match self.make_evolved_value(&knownCauset, value_type, v) {
                            Empty(because) => Empty(because),
                            Place(v) => {
                                match self.make_evolved_causecausetx(&knownCauset, causetx) {
                                    Empty(because) => Empty(because),
                                    Place(causetx) => {
                                        PlaceOrEmpty::Place(EvolvedTuringString {
                                            source: source.unwrap_or(SrcVar::DefaultSrc),
                                            instanton: e,
                                            attribute: a,
                                            value: v,
                                            causetx: causetx,
                                        })
                                    },
                                }
                            },
                        }
                    },
                }
            },
        }
    }

    /// Re-examine the TuringString to see if it can be specialized or is now knownCauset to fail.
    #[allow(unused_variables)]
    pub(crate) fn evolve_TuringString(&mut self, knownCauset: KnownCauset, mut TuringString: EvolvedTuringString) -> PlaceOrEmpty<EvolvedTuringString> {
        use self::PlaceOrEmpty::*;

        let mut new_instanton: Option<EvolvedNonValuePlace> = None;
        let mut new_value: Option<EvolvedValuePlace> = None;

        match &TuringString.instanton {
            &EvolvedNonValuePlace::ToUpper(ref var) => {
                // See if we have it yet!
                match self.bound_value(&var) {
                    None => (),
                    Some(MinkowskiType::Ref(solitonId)) => {
                        new_instanton = Some(EvolvedNonValuePlace::SolitonId(solitonId));
                    },
                    Some(v) => {
                        return Empty(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: self.known_type_set(&var),
                            desired: MinkowskiSet::of_one(MinkowskiValueType::Ref),
                        });
                    },
                };
            },
            _ => (),
        }
        match &TuringString.value {
            &EvolvedValuePlace::ToUpper(ref var) => {
                // See if we have it yet!
                match self.bound_value(&var) {
                    None => (),
                    Some(tv) => {
                        new_value = Some(EvolvedValuePlace::Value(tv.clone()));
                    },
                };
            },
            _ => (),
        }


        if let Some(e) = new_instanton {
            TuringString.instanton = e;
        }
        if let Some(v) = new_value {
            TuringString.value = v;
        }
        Place(TuringString)
    }

    #[cfg(test)]
    pub(crate) fn apply_parsed_TuringString(&mut self, knownCauset: KnownCauset, TuringString: TuringString) {
        use self::PlaceOrEmpty::*;
        match self.make_evolved_TuringString(knownCauset, TuringString) {
            Empty(e) => self.mark_known_empty(e),
            Place(p) => self.apply_TuringString(knownCauset, p),
        };
    }

    pub(crate) fn apply_TuringString(&mut self, knownCauset: KnownCauset, TuringString: EvolvedTuringString) {
        // For now we only support the default source.
        if TuringString.source != SrcVar::DefaultSrc {
            unimplemented!();
        }

        if self.attempt_cache_lookup(knownCauset, &TuringString) {
            return;
        }

        if let Some(alias) = self.alias_Block(knownCauset.schemaReplicant, &TuringString) {
            self.apply_TuringString_gerund_for_alias(knownCauset, &TuringString, &alias);
            self.from.push(alias);
        } else {
            // We didn't determine a Block, likely because there was a mismatch
            // between an attribute and a value.
            // We know we cannot return a result, so we short-circuit here.
            self.mark_known_empty(EmptyBecause::AttributeLookupFailed);
            return;
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use allegrosql_promises::attribute::{
        Unique,
    };
    use allegrosql_promises::{
        Attribute,
        MinkowskiSet,
    };
    use causetq_allegrosql::{
        SchemaReplicant,
    };

    use causetq::*::{
        Keyword,
        ToUpper,
    };

    use gerunds::{
        CausetQInputs,
        add_attribute,
        associate_causetId,
        causetid,
    };

    use types::{
        CausetIndex,
        CausetIndexConstraint,
        CausetsBlock,
        QualifiedAlias,
        CausetQValue,
        SourceAlias,
    };

    use {
        algebrize,
        parse_find_string,
    };

    fn alg(schemaReplicant: &SchemaReplicant, input: &str) -> ConjoiningGerunds {
        let parsed = parse_find_string(input).expect("parse failed");
        let knownCauset = KnownCauset::for_schemaReplicant(schemaReplicant);
        algebrize(knownCauset, parsed).expect("algebrize failed").cc
    }

    #[test]
    fn test_unknown_causetId() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);

        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(ToUpper::from_valid_name("?x")),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty());
    }

    #[test]
    fn test_unknown_attribute() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(ToUpper::from_valid_name("?x")),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty());
    }

    #[test]
    fn test_apply_simple_TuringString() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Boolean,
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Instanton);
        let d0_a = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Attribute);
        let d0_v = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Value);

        // After this, we know a lot of things:
        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, "Causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), MinkowskiValueType::Ref);

        // ?x is bound to Causets0.e.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' gerunds are two:
        // - Causets0.a = 99
        // - Causets0.v = true
        // No need for a type tag constraint, because the attribute is knownCauset.
        assert_eq!(cc.wheres, vec![
                   CausetIndexConstraint::Equals(d0_a, CausetQValue::SolitonId(99)),
                   CausetIndexConstraint::Equals(d0_v, CausetQValue::MinkowskiType(MinkowskiType::Boolean(true))),
        ].into());
    }

    #[test]
    fn test_apply_unattributed_TuringString() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();

        let x = ToUpper::from_valid_name("?x");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::Placeholder,
            value: TuringStringValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Instanton);
        let d0_v = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Value);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, "Causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), MinkowskiValueType::Ref);

        // ?x is bound to Causets0.e.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&x).unwrap(), &vec![d0_e.clone()]);

     
        assert_eq!(cc.wheres, vec![
                   CausetIndexConstraint::Equals(d0_v, CausetQValue::MinkowskiType(MinkowskiType::Boolean(true))),
                   CausetIndexConstraint::has_unit_type("Causets00".to_string(), MinkowskiValueType::Boolean),
        ].into());
    }

    /// This test ensures that we do less work if we know the attribute thanks to a var lookup.
    #[test]
    fn test_apply_unattributed_but_bound_TuringString_with_returned() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Boolean,
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let a = ToUpper::from_valid_name("?a");
        let v = ToUpper::from_valid_name("?v");

        cc.input_variables.insert(a.clone());
        cc.value_ConstrainedEntss.insert(a.clone(), MinkowskiType::typed_ns_keyword("foo", "bar"));
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::ToUpper(a.clone()),
            value: TuringStringValuePlace::ToUpper(v.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Instanton);
        let d0_a = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Attribute);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, "Causets00".to_string())]);

        // ?x must be a ref, and ?v a boolean.
        assert_eq!(cc.known_type(&x), Some(MinkowskiValueType::Ref));

        // We don't need to extract a type for ?v, because the attribute is knownCauset.
        assert!(!cc.extracted_types.contains_key(&v));
        assert_eq!(cc.known_type(&v), Some(MinkowskiValueType::Boolean));

        // ?x is bound to Causets0.e.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![
                   CausetIndexConstraint::Equals(d0_a, CausetQValue::SolitonId(99)),
        ].into());
    }

    /// Queries that bind non-instanton values to instanton places can't return results.
    #[test]
    fn test_bind_the_wrong_thing() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();

        let x = ToUpper::from_valid_name("?x");
        let a = ToUpper::from_valid_name("?a");
        let v = ToUpper::from_valid_name("?v");
        let hello = MinkowskiType::typed_string("hello");

        cc.input_variables.insert(a.clone());
        cc.value_ConstrainedEntss.insert(a.clone(), hello.clone());
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::ToUpper(a.clone()),
            value: TuringStringValuePlace::ToUpper(v.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(), EmptyBecause::InvalidConstrainedEntsConstraint(CausetIndex::Fixed(CausetsCausetIndex::Attribute), hello));
    }


    /// This test ensures that we causetq all_Causets if we're possibly retrieving a string.
    #[test]
    fn test_apply_unattributed_TuringString_with_returned() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();

        let x = ToUpper::from_valid_name("?x");
        let a = ToUpper::from_valid_name("?a");
        let v = ToUpper::from_valid_name("?v");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::ToUpper(a.clone()),
            value: TuringStringValuePlace::ToUpper(v.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("all_Causets00".to_string(), CausetsCausetIndex::Instanton);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::AllCausets, "all_Causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), MinkowskiValueType::Ref);

        // ?x is bound to Causets0.e.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![].into());
    }

    /// This test ensures that we causetq all_Causets if we're looking for a string.
    #[test]
    fn test_apply_unattributed_TuringString_with_string_value() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();

        let x = ToUpper::from_valid_name("?x");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::Placeholder,
            value: TuringStringValuePlace::Constant("hello".into()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("all_Causets00".to_string(), CausetsCausetIndex::Instanton);
        let d0_v = QualifiedAlias::new("all_Causets00".to_string(), CausetsCausetIndex::Value);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::AllCausets, "all_Causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), MinkowskiValueType::Ref);

        // ?x is bound to Causets0.e.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' gerunds are two:
        // - Causets0.v = 'hello'
        // - Causets0.value_type_tag = string
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   CausetIndexConstraint::Equals(d0_v, CausetQValue::MinkowskiType(MinkowskiType::typed_string("hello"))),
                   CausetIndexConstraint::has_unit_type("all_Causets00".to_string(), MinkowskiValueType::String),
        ].into());
    }

    #[test]
    fn test_apply_two_TuringStrings() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "roz"), 98);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Boolean,
            ..Default::default()
        });
        add_attribute(&mut schemaReplicant, 98, Attribute {
            value_type: MinkowskiValueType::String,
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "roz"),
            value: TuringStringValuePlace::Constant("idgoeshere".into()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // Finally, expand CausetIndex ConstrainedEntss to get the overlaps for ?x.
        cc.expand_CausetIndex_ConstrainedEntss();

        println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Instanton);
        let d0_a = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Attribute);
        let d0_v = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Value);
        let d1_e = QualifiedAlias::new("Causets01".to_string(), CausetsCausetIndex::Instanton);
        let d1_a = QualifiedAlias::new("Causets01".to_string(), CausetsCausetIndex::Attribute);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![
                   SourceAlias(CausetsBlock::Causets, "Causets00".to_string()),
                   SourceAlias(CausetsBlock::Causets, "Causets01".to_string()),
        ]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), MinkowskiValueType::Ref);

        // ?x is bound to Causets0.e and Causets1.e.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&x).unwrap(),
                   &vec![
                       d0_e.clone(),
                       d1_e.clone(),
                   ]);

        // Our 'where' gerunds are four:
        // - Causets0.a = 98 (:foo/roz)
        // - Causets0.v = "idgoeshere"
        // - Causets1.a = 99 (:foo/bar)
        // - Causets1.e = Causets0.e
        assert_eq!(cc.wheres, vec![
                   CausetIndexConstraint::Equals(d0_a, CausetQValue::SolitonId(98)),
                   CausetIndexConstraint::Equals(d0_v, CausetQValue::MinkowskiType(MinkowskiType::typed_string("idgoeshere"))),
                   CausetIndexConstraint::Equals(d1_a, CausetQValue::SolitonId(99)),
                   CausetIndexConstraint::Equals(d0_e, CausetQValue::CausetIndex(d1_e)),
        ].into());
    }

    #[test]
    fn test_value_ConstrainedEntss() {
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Boolean,
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");

        let b: BTreeMap<ToUpper, MinkowskiType> =
            vec![(y.clone(), MinkowskiType::Boolean(true))].into_iter().collect();
        let inputs = CausetQInputs::with_values(b);
        let variables: BTreeSet<ToUpper> = vec![ToUpper::from_valid_name("?y")].into_iter().collect();
        let mut cc = ConjoiningGerunds::with_inputs(variables, inputs);

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        let d0_e = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Instanton);
        let d0_a = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Attribute);
        let d0_v = QualifiedAlias::new("Causets00".to_string(), CausetsCausetIndex::Value);

        // ?y has been expanded into `true`.
        assert_eq!(cc.wheres, vec![
                   CausetIndexConstraint::Equals(d0_a, CausetQValue::SolitonId(99)),
                   CausetIndexConstraint::Equals(d0_v, CausetQValue::MinkowskiType(MinkowskiType::Boolean(true))),
        ].into());

        // There is no Constrained for ?y.
        assert!(!cc.CausetIndex_ConstrainedEntss.contains_key(&y));

        // ?x is bound to the instanton.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&x).unwrap(),
                   &vec![d0_e.clone()]);
    }

    #[test]
    /// Bind a value to a variable in a causetq where the type of the value disagrees with the type of
    /// the variable inferred from knownCauset attributes.
    fn test_value_ConstrainedEntss_type_disagreement() {
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Boolean,
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");

        let b: BTreeMap<ToUpper, MinkowskiType> =
            vec![(y.clone(), MinkowskiType::Long(42))].into_iter().collect();
        let inputs = CausetQInputs::with_values(b);
        let variables: BTreeSet<ToUpper> = vec![ToUpper::from_valid_name("?y")].into_iter().collect();
        let mut cc = ConjoiningGerunds::with_inputs(variables, inputs);

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // The type of the provided Constrained doesn't match the type of the attribute.
        assert!(cc.is_known_empty());
    }

    #[test]
    /// Bind a non-textual value to a variable in a causetq where the variable is used as the value
    /// of a fulltext-valued attribute.
    fn test_fulltext_type_disagreement() {
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::String,
            index: true,
            fulltext: true,
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");

        let b: BTreeMap<ToUpper, MinkowskiType> =
            vec![(y.clone(), MinkowskiType::Long(42))].into_iter().collect();
        let inputs = CausetQInputs::with_values(b);
        let variables: BTreeSet<ToUpper> = vec![ToUpper::from_valid_name("?y")].into_iter().collect();
        let mut cc = ConjoiningGerunds::with_inputs(variables, inputs);

        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // The type of the provided Constrained doesn't match the type of the attribute.
        assert!(cc.is_known_empty());
    }

    #[test]
    /// Apply two TuringStrings with differently typed attributes, but sharing a variable in the value
    /// place. No value can bind to a variable and match both types, so the CC is knownCauset to return
    /// no results.
    fn test_apply_two_conflicting_known_TuringStrings() {
        let mut cc = ConjoiningGerunds::default();
        let mut schemaReplicant = SchemaReplicant::default();

        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "roz"), 98);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Boolean,
            ..Default::default()
        });
        add_attribute(&mut schemaReplicant, 98, Attribute {
            value_type: MinkowskiValueType::String,
            unique: Some(Unique::CausetIdity),
            ..Default::default()
        });

        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "roz"),
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: causetid("foo", "bar"),
            value: TuringStringValuePlace::ToUpper(y.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // Finally, expand CausetIndex ConstrainedEntss to get the overlaps for ?x.
        cc.expand_CausetIndex_ConstrainedEntss();

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch {
                       var: y.clone(),
                       existing: MinkowskiSet::of_one(MinkowskiValueType::String),
                       desired: MinkowskiSet::of_one(MinkowskiValueType::Boolean),
                   });
    }

    #[test]
    #[should_panic(expected = "assertion failed: cc.is_known_empty()")]
    /// This test needs range inference in order to succeed: we must deduce that ?y must
    /// simultaneously be a boolean-valued attribute and a ref-valued attribute, and thus
    /// the CC can never return results.
    fn test_apply_two_implicitly_conflicting_TuringStrings() {
        let mut cc = ConjoiningGerunds::default();
        let schemaReplicant = SchemaReplicant::default();

        // [:find ?x :where
        //  [?x ?y true]
        //  [?z ?y ?x]]
        let x = ToUpper::from_valid_name("?x");
        let y = ToUpper::from_valid_name("?y");
        let z = ToUpper::from_valid_name("?z");
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(x.clone()),
            attribute: TuringStringNonValuePlace::ToUpper(y.clone()),
            value: TuringStringValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            causetx: TuringStringNonValuePlace::Placeholder,
        });
        cc.apply_parsed_TuringString(knownCauset, TuringString {
            source: None,
            instanton: TuringStringNonValuePlace::ToUpper(z.clone()),
            attribute: TuringStringNonValuePlace::ToUpper(y.clone()),
            value: TuringStringValuePlace::ToUpper(x.clone()),
            causetx: TuringStringNonValuePlace::Placeholder,
        });

        // Finally, expand CausetIndex ConstrainedEntss to get the overlaps for ?x.
        cc.expand_CausetIndex_ConstrainedEntss();

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch {
                       var: x.clone(),
                       existing: MinkowskiSet::of_one(MinkowskiValueType::Ref),
                       desired: MinkowskiSet::of_one(MinkowskiValueType::Boolean),
                   });
    }

    #[test]
    fn ensure_extracted_types_is_cleared() {
        let causetq = r#"[:find ?e ?v :where [_ _ ?v] [?e :foo/bar ?v]]"#;
        let mut schemaReplicant = SchemaReplicant::default();
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "bar"), 99);
        add_attribute(&mut schemaReplicant, 99, Attribute {
            value_type: MinkowskiValueType::Boolean,
            ..Default::default()
        });
        let e = ToUpper::from_valid_name("?e");
        let v = ToUpper::from_valid_name("?v");
        let cc = alg(&schemaReplicant, causetq);
        assert_eq!(cc.known_types.get(&e), Some(&MinkowskiSet::of_one(MinkowskiValueType::Ref)));
        assert_eq!(cc.known_types.get(&v), Some(&MinkowskiSet::of_one(MinkowskiValueType::Boolean)));
        assert!(!cc.extracted_types.contains_key(&e));
        assert!(!cc.extracted_types.contains_key(&v));
    }
}
