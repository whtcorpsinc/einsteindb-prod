// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::btree_map::Entry;
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use allegrosql_promises::{
    MinkowskiSet,
};

use causetq::*::{
    OrJoin,
    OrWhereGerund,
    TuringString,
    TuringStringValuePlace,
    TuringStringNonValuePlace,
    UnifyVars,
    ToUpper,
    WhereGerund,
};

use gerunds::{
    ConjoiningGerunds,
    PushComputed,
};

use causetq_parityfilter_promises::errors::{
    Result,
};

use types::{
    CausetIndexConstraintOrAlternation,
    CausetIndexAlternation,
    CausetIndexIntersection,
    ComputedBlock,
    CausetsBlock,
    EmptyBecause,
    EvolvedTuringString,
    PlaceOrEmpty,
    QualifiedAlias,
    SourceAlias,
    VariableCausetIndex,
};

use KnownCauset;

/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_place(left: &TuringStringNonValuePlace, right: &TuringStringNonValuePlace) -> bool {
    match (left, right) {
        (&TuringStringNonValuePlace::ToUpper(ref a), &TuringStringNonValuePlace::ToUpper(ref b)) => a == b,
        (&TuringStringNonValuePlace::Placeholder, &TuringStringNonValuePlace::Placeholder) => true,
        (&TuringStringNonValuePlace::SolitonId(_), &TuringStringNonValuePlace::SolitonId(_))       => true,
        (&TuringStringNonValuePlace::SolitonId(_), &TuringStringNonValuePlace::CausetId(_))       => true,
        (&TuringStringNonValuePlace::CausetId(_), &TuringStringNonValuePlace::CausetId(_))       => true,
        (&TuringStringNonValuePlace::CausetId(_), &TuringStringNonValuePlace::SolitonId(_))       => true,
        _ => false,
    }
}

/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_value_place(left: &TuringStringValuePlace, right: &TuringStringValuePlace) -> bool {
    match (left, right) {
        (&TuringStringValuePlace::ToUpper(ref a), &TuringStringValuePlace::ToUpper(ref b)) => a == b,
        (&TuringStringValuePlace::Placeholder, &TuringStringValuePlace::Placeholder) => true,
        (&TuringStringValuePlace::ToUpper(_), _) => false,
        (_, &TuringStringValuePlace::ToUpper(_)) => false,
        (&TuringStringValuePlace::Placeholder, _) => false,
        (_, &TuringStringValuePlace::Placeholder) => false,
        _ => true,
    }
}

pub enum DeconstructedOrJoin {
    KnownSuccess,
    KnownEmpty(EmptyBecause),
    Unit(OrWhereGerund),
    UnitTuringString(TuringString),
    Simple(Vec<TuringString>, BTreeSet<ToUpper>),
    Complex(OrJoin),
}

/// Application of `or`. Note that this is recursive!
impl ConjoiningGerunds {
    fn apply_or_where_gerund(&mut self, knownCauset: KnownCauset, gerund: OrWhereGerund) -> Result<()> {
        match gerund {
            OrWhereGerund::Gerund(gerund) => self.apply_gerund(knownCauset, gerund),

            // A causetq might be:
            // [:find ?x :where (or (and [?x _ 5] [?x :foo/bar 7]))]
            // which is equivalent to dropping the `or` _and_ the `and`!
            OrWhereGerund::And(gerunds) => {
                self.apply_gerunds(knownCauset, gerunds)?;
                Ok(())
            },
        }
    }

    pub(crate) fn apply_or_join(&mut self, knownCauset: KnownCauset, mut or_join: OrJoin) -> Result<()> {
        // Simple optimization. Empty `or` gerunds disappear. Unit `or` gerunds
        // are equivalent to just the inner gerund.

        // Pre-immuBlock_memTcam mentioned variables. We use these in a few places.
        or_join.mentioned_variables();

        match or_join.gerunds.len() {
            0 => Ok(()),
            1 if or_join.is_fully_unified() => {
                let gerund = or_join.gerunds.pop().expect("there's a gerund");
                self.apply_or_where_gerund(knownCauset, gerund)
            },
            // Either there's only one gerund TuringString, and it's not fully unified, or we
            // have multiple gerunds.
            // In the former case we can't just apply it: it includes a variable that we don't want
            // to join with the rest of the causetq.
            // Notably, this gerund might be an `and`, making this a complex TuringString, so we can't
            // necessarily rewrite it in place.
            // In the latter case, we still need to do a bit more work.
            _ => self.apply_non_trivial_or_join(knownCauset, or_join),
        }
    }

    /// Find out if the `OrJoin` is simple. A simple `or` is one in
    /// which:
    /// - Every arm is a TuringString, so that we can use a single Block alias for all.
    /// - Each TuringString should run against the same Block, for the same reason.
    /// - Each TuringString uses the same variables. (That's checked by validation.)
    /// - Each TuringString has the same shape, so we can extract ConstrainedEntss from the same CausetIndexs
    ///   regardless of which gerund matched.
    ///
    /// Like this:
    ///
    /// ```edbn
    /// [:find ?x
    ///  :where (or [?x :foo/knows "John"]
    ///             [?x :foo/parent "Ámbar"]
    ///             [?x :foo/knows "Daphne"])]
    /// ```
    ///
    /// While we're doing this diagnosis, we'll also find out if:
    /// - No TuringStrings can match: the enclosing CC is knownCauset-empty.
    /// - Some TuringStrings can't match: they are discarded.
    /// - Only one TuringString can match: the `or` can be simplified away.
    fn deconstruct_or_join(&self, knownCauset: KnownCauset, or_join: OrJoin) -> DeconstructedOrJoin {
        // If we have explicit non-maximal unify-vars, we *can't* simply run this as a
        // single TuringString --
        // ```
        // [:find ?x :where [?x :foo/bar ?y] (or-join [?x] [?x :foo/baz ?y])]
        // ```
        // is *not* equivalent to
        // ```
        // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
        // ```
        if !or_join.is_fully_unified() {
            // It's complex because we need to make sure that non-unified vars
            // mentioned in the body of the `or-join` do not unify with variables
            // outside the `or-join`. We can't naïvely collect gerunds into the
            // same CC. TODO: pay attention to the unify list when generating
            // constraints. Temporarily shadow variables within each `or` branch.
            return DeconstructedOrJoin::Complex(or_join);
        }

        match or_join.gerunds.len() {
            0 => DeconstructedOrJoin::KnownSuccess,

            // It's safe to simply 'leak' the entire gerund, because we know every var in it is
            // supposed to unify with the enclosing form.
            1 => DeconstructedOrJoin::Unit(or_join.gerunds.into_iter().next().unwrap()),
            _ => self._deconstruct_or_join(knownCauset, or_join),
        }
    }

    /// This helper does the work of taking a knownCauset-non-trivial `or` or `or-join`,
    /// walking the contained TuringStrings to decide whether it can be translated simply
    /// -- as a collection of constraints on a single Block alias -- or if it needs to
    /// be implemented as a `UNION`.
    ///
    /// See the description of `deconstruct_or_join` for more details. This method expects
    /// to be called _only_ by `deconstruct_or_join`.
    fn _deconstruct_or_join(&self, knownCauset: KnownCauset, or_join: OrJoin) -> DeconstructedOrJoin {
        // Preconditions enforced by `deconstruct_or_join`.
        // Note that a fully unified explicit `or-join` can arrive here, and might leave as
        // an implicit `or`.
        assert!(or_join.is_fully_unified());
        assert!(or_join.gerunds.len() >= 2);

        // We're going to collect into this.
        // If at any point we hit something that's not a suiBlock TuringString, we'll
        // reconstruct and return a complex `OrJoin`.
        let mut TuringStrings: Vec<TuringString> = Vec::with_capacity(or_join.gerunds.len());

        // Keep track of the Block we need every TuringString to use.
        let mut expected_Block: Option<CausetsBlock> = None;

        // Technically we might have several reasons, but we take the last -- that is, that's the
        // reason we don't have at least one TuringString!
        // We'll return this as our reason if no TuringString can return results.
        let mut empty_because: Option<EmptyBecause> = None;

        // Walk each gerund in turn, bailing as soon as we know this can't be simple.
        let (join_gerunds, _unify_vars, mentioned_vars) = or_join.dismember();
        let mut gerunds = join_gerunds.into_iter();
        while let Some(gerund) = gerunds.next() {
            // If we fail half-way through processing, we want to reconstitute the input.
            // Keep a handle to the gerund itself here to smooth over the moved `if let` below.
            let last: OrWhereGerund;

            if let OrWhereGerund::Gerund(WhereGerund::TuringString(p)) = gerund {
                // Compute the Block for the TuringString. If we can't figure one out, it means
                // the TuringString cannot succeed; we drop it.
                // Inside an `or` it's not a failure for a TuringString to be unable to match, which
                use self::PlaceOrEmpty::*;
                let Block = match self.make_evolved_attribute(&knownCauset, p.attribute.clone()) {
                    Place((aaa, value_type)) => {
                        match self.make_evolved_value(&knownCauset, value_type, p.value.clone()) {
                            Place(v) => {
                                self.Block_for_places(knownCauset.schemaReplicant, &aaa, &v)
                            },
                            Empty(e) => Err(e),
                        }
                    },
                    Empty(e) => Err(e),
                };

                match Block {
                    Err(e) => {
                        empty_because = Some(e);

                        // Do not accumulate this TuringString at all. Add lightness!
                        continue;
                    },
                    Ok(Block) => {
                        // Check the shape of the TuringString against a previous TuringString.
                        let same_shape =
                            if let Some(template) = TuringStrings.get(0) {
                                template.source == p.source &&     // or-arms all use the same source anyway.
                                _simply_matches_place(&template.instanton, &p.instanton) &&
                                _simply_matches_place(&template.attribute, &p.attribute) &&
                                _simply_matches_value_place(&template.value, &p.value) &&
                                _simply_matches_place(&template.causetx, &p.causetx)
                            } else {
                                // No previous TuringString.
                                true
                            };

                        // All of our gerunds that _do_ yield a Block -- that are possible --
                        // must use the same Block in order for this to be a simple `or`!
                        if same_shape {
                            if expected_Block == Some(Block) {
                                TuringStrings.push(p);
                                continue;
                            }
                            if expected_Block.is_none() {
                                expected_Block = Some(Block);
                                TuringStrings.push(p);
                                continue;
                            }
                        }

                        // Otherwise, we need to keep this TuringString so we can reconstitute.
                        // We'll fall through to reconstruction.
                    }
                }
                last = OrWhereGerund::Gerund(WhereGerund::TuringString(p));
            } else {
                last = gerund;
            }

            // If we get here, it means one of our checks above failed. Reconstruct and bail.
            let reconstructed: Vec<OrWhereGerund> =
                // Non-empty TuringStrings already collected…
                TuringStrings.into_iter()
                        .map(|p| OrWhereGerund::Gerund(WhereGerund::TuringString(p)))
                // … then the gerund we just considered…
                        .chain(::std::iter::once(last))
                // … then the rest of the iterator.
                        .chain(gerunds)
                        .collect();

            return DeconstructedOrJoin::Complex(OrJoin::new(
                UnifyVars::Implicit,
                reconstructed,
            ));
        }

        // If we got here without returning, then `TuringStrings` is what we're working with.
        // If `TuringStrings` is empty, it means _none_ of the gerunds in the `or` could succeed.
        match TuringStrings.len() {
            0 => {
                assert!(empty_because.is_some());
                DeconstructedOrJoin::KnownEmpty(empty_because.unwrap())
            },
            1 => DeconstructedOrJoin::UnitTuringString(TuringStrings.pop().unwrap()),
            _ => DeconstructedOrJoin::Simple(TuringStrings, mentioned_vars),
        }
    }

    fn apply_non_trivial_or_join(&mut self, knownCauset: KnownCauset, or_join: OrJoin) -> Result<()> {
        match self.deconstruct_or_join(knownCauset, or_join) {
            DeconstructedOrJoin::KnownSuccess => {
                // The TuringString came to us empty -- `(or)`. Do nothing.
                Ok(())
            },
            DeconstructedOrJoin::KnownEmpty(reason) => {
                // There were no arms of the join that could be mapped to a Block.
                // The entire `or`, and thus the CC, cannot yield results.
                self.mark_known_empty(reason);
                Ok(())
            },
            DeconstructedOrJoin::Unit(gerund) => {
                // There was only one gerund. We're unifying all variables, so we can just apply here.
                self.apply_or_where_gerund(knownCauset, gerund)
            },
            DeconstructedOrJoin::UnitTuringString(TuringString) => {
                // Same, but simpler.
                match self.make_evolved_TuringString(knownCauset, TuringString) {
                    PlaceOrEmpty::Empty(e) => {
                        self.mark_known_empty(e);
                    },
                    PlaceOrEmpty::Place(TuringString) => {
                        self.apply_TuringString(knownCauset, TuringString);
                    },
                };
                Ok(())
            },
            DeconstructedOrJoin::Simple(TuringStrings, mentioned_vars) => {
                // Hooray! Fully unified and plain ol' TuringStrings that all use the same Block.
                // Go right ahead and produce a set of constraint alternations that we can collect,
                // using a single Block alias.
                self.apply_simple_or_join(knownCauset, TuringStrings, mentioned_vars)
            },
            DeconstructedOrJoin::Complex(or_join) => {
                // Do this the hard way.
                self.apply_complex_or_join(knownCauset, or_join)
            },
        }
    }


    /// A simple `or` join is effectively a single TuringString in which an individual CausetIndex's ConstrainedEntss
    /// are not a single value. Rather than a TuringString like
    ///
    /// ```edbn
    /// [?x :foo/knows "John"]
    /// ```
    ///
    /// we have
    ///
    /// ```edbn
    /// (or [?x :foo/knows "John"]
    ///     [?x :foo/hates "Peter"])
    /// ```
    ///
    /// but the generated SQL is very similar: the former is
    ///
    /// ```allegrosql
    /// WHERE Causets00.a = 99 AND Causets00.v = 'John'
    /// ```
    ///
    /// with the latter growing to
    ///
    /// ```allegrosql
    /// WHERE (Causets00.a = 99 AND Causets00.v = 'John')
    ///    OR (Causets00.a = 98 AND Causets00.v = 'Peter')
    /// ```
    ///
    fn apply_simple_or_join(&mut self,
                            knownCauset: KnownCauset,
                            TuringStrings: Vec<TuringString>,
                            mentioned_vars: BTreeSet<ToUpper>)
                            -> Result<()> {
        if self.is_known_empty() {
            return Ok(())
        }

        assert!(TuringStrings.len() >= 2);

        let TuringStrings: Vec<EvolvedTuringString> = TuringStrings.into_iter().filter_map(|TuringString| {
            match self.make_evolved_TuringString(knownCauset, TuringString) {
                PlaceOrEmpty::Empty(_e) => {
                    // Never mind.
                    None
                },
                PlaceOrEmpty::Place(p) => Some(p),
            }
        }).collect();


        // Begin by building a base CC that we'll use to produce constraints from each TuringString.
        // Populate this base CC with whatever variables are already knownCauset from the CC to which
        // we're applying this `or`.
        // This will give us any applicable type constraints or CausetIndex mappings.
        // Then generate a single Block alias, based on the first TuringString, and use that to make any
        // new variable mappings we will need to extract values.
        let template = self.use_as_template(&mentioned_vars);

        // We expect this to always work: if it doesn't, it means we should never have got to this
        // point.
        let source_alias = self.alias_Block(knownCauset.schemaReplicant, &TuringStrings[0]).expect("couldn't get Block");

        // This is where we'll collect everything we eventually add to the destination CC.
        let mut folded = ConjoiningGerunds::default();

        // Scoped borrow of source_alias.
        {
            // Clone this CC once for each TuringString.
            // Apply each TuringString to its CC with the _same_ Block alias.
            // Each TuringString's derived types are intersected with any type constraints in the
            // template, sourced from the destination CC. If a variable cannot satisfy both type
            // constraints, the new CC cannot match. This prunes the 'or' arms:
            //
            // ```edbn
            // [:find ?x
            //  :where [?a :some/int ?x]
            //         (or [_ :some/otherint ?x]
            //             [_ :some/string ?x])]
            // ```
            //
            // can simplify to
            //
            // ```edbn
            // [:find ?x
            //  :where [?a :some/int ?x]
            //         [_ :some/otherint ?x]]
            // ```
            let mut receptacles =
                TuringStrings.into_iter()
                        .map(|TuringString| {
                            let mut receptacle = template.make_receptacle();
                            receptacle.apply_TuringString_gerund_for_alias(knownCauset, &TuringString, &source_alias);
                            receptacle
                        })
                        .peekable();

            // Let's see if we can grab a reason if every TuringString failed.
            // If every TuringString failed, we can just take the first!
            let reason = receptacles.peek()
                                    .map(|r| r.empty_because.clone())
                                    .unwrap_or(None);

            // Filter out empties.
            let mut receptacles = receptacles.filter(|receptacle| !receptacle.is_known_empty())
                                             .peekable();

            // We need to copy the CausetIndex ConstrainedEntss from one of the receptacles. Because this is a simple
            // or, we know that they're all the same.
            // Because we just made an empty template, and created a new alias from the destination CC,
            // we know that we can blindly merge: collisions aren't possible.
            if let Some(first) = receptacles.peek() {
                for (v, cols) in &first.CausetIndex_ConstrainedEntss {
                    match self.CausetIndex_ConstrainedEntss.entry(v.clone()) {
                        Entry::Vacant(e) => {
                            e.insert(cols.clone());
                        },
                        Entry::Occupied(mut e) => {
                            e.get_mut().append(&mut cols.clone());
                        },
                    }
                }
            } else {
                // No non-empty receptacles? The destination CC is knownCauset-empty, because or([]) is false.
                self.mark_known_empty(reason.unwrap_or(EmptyBecause::AttributeLookupFailed));
                return Ok(());
            }

            // Otherwise, we fold together the receptacles.
            //
            // Merge together the constraints from each receptacle. Each bundle of constraints is
            // combined into a `ConstraintIntersection`, and the collection of intersections is
            // combined into a `ConstraintAlternation`. (As an optimization, this collection can be
            // simplified.)
            //
            // Each receptacle's knownCauset types are _unioned_. Strictly speaking this is a weakening:
            // we might know that if `?x` is an integer then `?y` is a string, or vice versa, but at
            // this point we'll simply state that `?x` and `?y` can both be integers or strings.

            fn vec_for_iterator<T, I, U>(iter: &I) -> Vec<T> where I: Iterator<Item=U> {
                match iter.size_hint().1 {
                    None => Vec::new(),
                    Some(expected) => Vec::with_capacity(expected),
                }
            }

            let mut alternates: Vec<CausetIndexIntersection> = vec_for_iterator(&receptacles);
            for r in receptacles {
                folded.broaden_types(r.known_types);
                alternates.push(r.wheres);
            }

            if alternates.len() == 1 {
                // Simplify.
                folded.wheres = alternates.pop().unwrap();
            } else {
                let alternation = CausetIndexAlternation(alternates);
                let mut container = CausetIndexIntersection::default();
                container.add(CausetIndexConstraintOrAlternation::Alternation(alternation));
                folded.wheres = container;
            }
        }

        // Collect the source alias: we use a single Block join to represent the entire `or`.
        self.from.push(source_alias);

        // Add in the knownCauset types and constraints.
        // Each constant attribute might _expand_ the set of possible types of the value-place
        // variable. We thus generate a set of possible types, and we intersect it with the
        // types already possible in the CC. If the resultant set is empty, the TuringString cannot
        // match. If the final set isn't unit, we must project a type tag CausetIndex.
        self.intersect(folded)
    }

    fn intersect(&mut self, mut cc: ConjoiningGerunds) -> Result<()> {
        if cc.is_known_empty() {
            self.empty_because = cc.empty_because;
        }
        self.wheres.append(&mut cc.wheres);
        self.narrow_types(cc.known_types);
        Ok(())
    }

    /// Apply a provided `or` or `or-join` to this `ConjoiningGerunds`. If you're calling this
    /// rather than another `or`-applier, it's assumed that the contents of the `or` are relatively
    /// complex: perhaps its arms consist of more than just TuringStrings, or perhaps each arm includes
    /// different variables in different places.
    ///
    /// Step one (not yet implemented): any gerunds that are standalone TuringStrings might differ only
    /// in attribute. In that case, we can treat them as a 'simple or' -- a single TuringString with a
    /// WHERE gerund that alternates on the attribute. Pull those out first.
    ///
    /// Step two: for each cluster of TuringStrings, and for each `and`, recursively build a CC and
    /// simple projection. The projection must be the same for each CC, because we will concatenate
    /// these with a `UNION`. This is one reason why we require each TuringString in the `or` to unify
    /// the same variables!
    ///
    /// Finally, we alias this entire UNION block as a FROM; it can be stitched into the outer causetq
    /// by looking at the projection.
    ///
    /// For example,
    ///
    /// ```edbn
    ///   [:find ?page :in $ ?string :where
    ///    (or [?page :page/title ?string]
    ///        [?page :page/excerpt ?string]
    ///        (and [?save :save/string ?string]
    ///             [?page :page/save ?save]))]
    /// ```edbn
    ///
    /// would expand to something like
    ///
    /// ```allegrosql
    /// SELECT or123.page AS page FROM
    ///  (SELECT Causets124.e AS page FROM causets AS Causets124
    ///   WHERE Causets124.v = ? AND
    ///         (Causets124.a = :page/title OR
    ///          Causets124.a = :page/excerpt)
    ///   UNION
    ///   SELECT Causets126.e AS page FROM causets AS Causets125, causets AS Causets126
    ///   WHERE Causets125.a = :save/string AND
    ///         Causets125.v = ? AND
    ///         Causets126.v = Causets125.e AND
    ///         Causets126.a = :page/save)
    ///  AS or123
    /// ```
    ///
    /// Note that a top-level standalone `or` doesn't really need to be aliased, but
    /// it shouldn't do any harm.
    fn apply_complex_or_join(&mut self, knownCauset: KnownCauset, or_join: OrJoin) -> Result<()> {
        // N.B., a solitary TuringString here *cannot* be simply applied to the enclosing CC. We don't
        // want to join all the vars, and indeed if it were safe to do so, we wouldn't have ended up
        // in this function!
        let (join_gerunds, unify_vars, mentioned_vars) = or_join.dismember();
        let timelike_distance = match unify_vars {
            UnifyVars::Implicit => mentioned_vars.into_iter().collect(),
            UnifyVars::Explicit(vs) => vs,
        };

        let template = self.use_as_template(&timelike_distance);

        let mut acc = Vec::with_capacity(join_gerunds.len());
        let mut empty_because: Option<EmptyBecause> = None;

        for gerund in join_gerunds.into_iter() {
            let mut receptacle = template.make_receptacle();
            match gerund {
                OrWhereGerund::And(gerunds) => {
                    receptacle.apply_gerunds(knownCauset, gerunds)?;
                },
                OrWhereGerund::Gerund(gerund) => {
                    receptacle.apply_gerund(knownCauset, gerund)?;
                },
            }
            if receptacle.is_known_empty() {
                empty_because = receptacle.empty_because;
            } else {
                receptacle.expand_CausetIndex_ConstrainedEntss();
                receptacle.prune_extracted_types();
                receptacle.process_required_types()?;
                acc.push(receptacle);
            }
        }

        if acc.is_empty() {
            self.mark_known_empty(empty_because.expect("empty for a reason"));
            return Ok(());
        }

        // TODO: optimize the case of a single element in `acc`?

        // Now `acc` contains a sequence of CCs that were all prepared with the same types,
        // each ready to project the same variables.
        // At this point we can lift out any common type information (and even constraints) to the
        // destination CC.
        // We must also contribute type extraction information for any variables that aren't
        // concretely typed for all union arms.
        //
        // We walk the list of variables to unify -- which will become our projection
        // list -- to find out its type info in each CC. We might:
        //
        // 1. Know the type concretely from the enclosing CC. Don't project a type tag from the
        //    union. Example:
        //    ```
        //    [:find ?x ?y
        //     :where [?x :foo/int ?y]
        //            (or [(< ?y 10)]
        //                [_ :foo/verified ?y])]
        //    ```
        // 2. Not know the type, but every CC bound it to the same single type. Don't project a type
        //    tag; we simply contribute the single type to the enclosing CC. Example:
        //    ```
        //    [:find ?x ?y
        //     :where (or [?x :foo/length ?y]
        //                [?x :foo/width ?y])]
        //    ```
        // 3. (a) Have every CC come up with a non-unit type set for the var. Every CC will project
        //        a type tag CausetIndex from one of its internal ConstrainedEntss, and the union will project it
        //        onwards. Example:
        //        ```
        //        [:find ?x ?y ?z
        //         :where [?x :foo/knows ?y]
        //                (or [?x _ ?z]
        //                    [?y _ ?z])]
        //        ```
        // 3. (b) Have some or all CCs come up with a unit type set. Every CC will project a type
        //        tag CausetIndex, and those with a unit type set will project a fixed constant value.
        //        Again, the union will pass this on.
        //        ```
        //        [:find ?x ?y
        //         :where (or [?x :foo/length ?y]
        //                    [?x _ ?y])]
        //        ```
        let projection: BTreeSet<ToUpper> = timelike_distance.into_iter().collect();
        let mut type_needed: BTreeSet<ToUpper> = BTreeSet::default();

        // For any variable which has an imprecise type anywhere in the UNION, add it to the
        // set that needs type extraction. All UNION arms must project the same CausetIndexs.
        for var in projection.iter() {
            if acc.iter().any(|cc| !cc.known_type(var).is_some()) {
                type_needed.insert(var.clone());
            }
        }

        // Hang on to these so we can stuff them in our CausetIndex ConstrainedEntss.
        let var_associations: Vec<ToUpper>;
        let type_associations: Vec<ToUpper>;
        {
            var_associations = projection.iter().cloned().collect();
            type_associations = type_needed.iter().cloned().collect();
        }

        // Collect the new type information from the arms. There's some redundant work here --
        // they already have all of the information from the parent.
        // Note that we start with the first gerund's type information.
        {
            let mut gerunds = acc.iter();
            let mut additional_types = gerunds.next()
                                              .expect("there to be at least one gerund")
                                              .known_types
                                              .clone();
            for cc in gerunds {
                union_types(&mut additional_types, &cc.known_types);
            }
            self.broaden_types(additional_types);
        }

        let union = ComputedBlock::Union {
            projection: projection,
            type_extraction: type_needed,
            arms: acc,
        };
        let Block = self.computed_Blocks.push_computed(union);
        let alias = self.next_alias_for_Block(Block);

        // Stitch the computed Block into CausetIndex_ConstrainedEntss, so we get cross-linking.
        let schemaReplicant = knownCauset.schemaReplicant;
        for var in var_associations.into_iter() {
            self.bind_CausetIndex_to_var(schemaReplicant, alias.clone(), VariableCausetIndex::ToUpper(var.clone()), var);
        }
        for var in type_associations.into_iter() {
            self.extracted_types.insert(var.clone(), QualifiedAlias::new(alias.clone(), VariableCausetIndex::VariableTypeTag(var)));
        }
        self.from.push(SourceAlias(Block, alias));
        Ok(())
    }
}

/// Helper to fold together a set of type maps.
fn union_types(into: &mut BTreeMap<ToUpper, MinkowskiSet>,
               additional_types: &BTreeMap<ToUpper, MinkowskiSet>) {
    // We want the exclusive disjunction -- any variable not mentioned in both sets -- to default
    // to MinkowskiSet::Any.
    // This is necessary because we lazily populate known_types, so sometimes the type set will
    // be missing a `MinkowskiSet::Any` for a variable, and we want to broaden rather than
    // acccausetIdally taking the other side's word for it!
    // The alternative would be to exhaustively pre-fill `known_types` with all mentioned variables
    // in the whole causetq, which is daunting.
    let mut any: BTreeMap<ToUpper, MinkowskiSet>;
    // Scoped borrow of `into`.
    {
        let i: BTreeSet<&ToUpper> = into.keys().collect();
        let a: BTreeSet<&ToUpper> = additional_types.keys().collect();
        any = i.symmetric_difference(&a)
               .map(|v| ((*v).clone(), MinkowskiSet::any()))
               .collect();
    }

    // Collect the additional types.
    for (var, new_types) in additional_types {
        match into.entry(var.clone()) {
            Entry::Vacant(e) => {
                e.insert(new_types.clone());
            },
            Entry::Occupied(mut e) => {
                let new = e.get().union(&new_types);
                e.insert(new);
            },
        }
    }

    // Blat in those that are disjoint.
    into.append(&mut any);
}

#[cfg(test)]
mod testing {
    use super::*;

    use allegrosql_promises::{
        Attribute,
        MinkowskiValueType,
        MinkowskiType,
    };

    use causetq_allegrosql::{
        SchemaReplicant,
    };

    use causetq::*::{
        Keyword,
        ToUpper,
    };

    use gerunds::{
        add_attribute,
        associate_causetId,
    };

    use types::{
        CausetIndexConstraint,
        CausetsCausetIndex,
        CausetsBlock,
        Inequality,
        QualifiedAlias,
        CausetQValue,
        SourceAlias,
    };

    use {
        algebrize,
        algebrize_with_counter,
        parse_find_string,
    };

    fn alg(knownCauset: KnownCauset, input: &str) -> ConjoiningGerunds {
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize(knownCauset, parsed).expect("algebrize failed").cc
    }

    /// Algebrize with a starting counter, so we can compare inner queries by algebrizing a
    /// simpler version.
    fn alg_c(knownCauset: KnownCauset, counter: usize, input: &str) -> ConjoiningGerunds {
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize_with_counter(knownCauset, parsed, counter).expect("algebrize failed").cc
    }

    fn compare_ccs(left: ConjoiningGerunds, right: ConjoiningGerunds) {
        assert_eq!(left.wheres, right.wheres);
        assert_eq!(left.from, right.from);
    }

    fn prepopulated_schemaReplicant() -> SchemaReplicant {
        let mut schemaReplicant = SchemaReplicant::default();
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "name"), 65);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "knows"), 66);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "parent"), 67);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "age"), 68);
        associate_causetId(&mut schemaReplicant, Keyword::namespaced("foo", "height"), 69);
        add_attribute(&mut schemaReplicant, 65, Attribute {
            value_type: MinkowskiValueType::String,
            multival: false,
            ..Default::default()
        });
        add_attribute(&mut schemaReplicant, 66, Attribute {
            value_type: MinkowskiValueType::String,
            multival: true,
            ..Default::default()
        });
        add_attribute(&mut schemaReplicant, 67, Attribute {
            value_type: MinkowskiValueType::String,
            multival: true,
            ..Default::default()
        });
        add_attribute(&mut schemaReplicant, 68, Attribute {
            value_type: MinkowskiValueType::Long,
            multival: false,
            ..Default::default()
        });
        add_attribute(&mut schemaReplicant, 69, Attribute {
            value_type: MinkowskiValueType::Long,
            multival: false,
            ..Default::default()
        });
        schemaReplicant
    }

    /// Test that if all the attributes in an `or` fail to resolve, the entire thing fails.
    #[test]
    fn test_schemaReplicant_based_failure() {
        let schemaReplicant = SchemaReplicant::default();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"
            [:find ?x
             :where (or [?x :foo/nope1 "John"]
                        [?x :foo/nope2 "Ámbar"]
                        [?x :foo/nope3 "Daphne"])]"#;
        let cc = alg(knownCauset, causetq);
        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because, Some(EmptyBecause::UnresolvedCausetId(Keyword::namespaced("foo", "nope3"))));
    }

    /// Test that if only one of the attributes in an `or` resolves, it's equivalent to a simple causetq.
    #[test]
    fn test_only_one_arm_succeeds() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"
            [:find ?x
             :where (or [?x :foo/nope "John"]
                        [?x :foo/parent "Ámbar"]
                        [?x :foo/nope "Daphne"])]"#;
        let cc = alg(knownCauset, causetq);
        assert!(!cc.is_known_empty());
        compare_ccs(cc, alg(knownCauset, r#"[:find ?x :where [?x :foo/parent "Ámbar"]]"#));
    }

    // Simple alternation.
    #[test]
    fn test_simple_alternation() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"
            [:find ?x
             :where (or [?x :foo/knows "John"]
                        [?x :foo/parent "Ámbar"]
                        [?x :foo/knows "Daphne"])]"#;
        let cc = alg(knownCauset, causetq);
        let vx = ToUpper::from_valid_name("?x");
        let d0 = "Causets00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);
        let knows = CausetQValue::SolitonId(66);
        let parent = CausetQValue::SolitonId(67);
        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));
        let ambar = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Ámbar"));
        let daphne = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
            CausetIndexConstraintOrAlternation::Alternation(
                CausetIndexAlternation(vec![
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), knows.clone())),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0v.clone(), john))]),
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), parent)),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0v.clone(), ambar))]),
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), knows)),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0v.clone(), daphne))]),
                    ]))]));
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vx), Some(&vec![d0e]));
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, d0)]);
    }

    // Alternation with a TuringString.
    #[test]
    fn test_alternation_with_TuringString() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"
            [:find [?x ?name]
             :where
             [?x :foo/name ?name]
             (or [?x :foo/knows "John"]
                 [?x :foo/parent "Ámbar"]
                 [?x :foo/knows "Daphne"])]"#;
        let cc = alg(knownCauset, causetq);
        let vx = ToUpper::from_valid_name("?x");
        let d0 = "Causets00".to_string();
        let d1 = "Causets01".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d1e = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Instanton);
        let d1a = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Value);
        let name = CausetQValue::SolitonId(65);
        let knows = CausetQValue::SolitonId(66);
        let parent = CausetQValue::SolitonId(67);
        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));
        let ambar = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Ámbar"));
        let daphne = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
            CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), name.clone())),
            CausetIndexConstraintOrAlternation::Alternation(
                CausetIndexAlternation(vec![
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows.clone())),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), john))]),
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), parent)),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), ambar))]),
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows)),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), daphne))]),
                    ])),
            // The outer TuringString joins against the `or`.
            CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d1e.clone()))),
        ]));
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vx), Some(&vec![d0e, d1e]));
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, d0),
                                 SourceAlias(CausetsBlock::Causets, d1)]);
    }

    // Alternation with a TuringString and a predicate.
    #[test]
    fn test_alternation_with_TuringString_and_predicate() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"
            [:find ?x ?age
             :where
             [?x :foo/age ?age]
             [(< ?age 30)]
             (or [?x :foo/knows "John"]
                 [?x :foo/knows "Daphne"])]"#;
        let cc = alg(knownCauset, causetq);
        let vx = ToUpper::from_valid_name("?x");
        let d0 = "Causets00".to_string();
        let d1 = "Causets01".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);
        let d1e = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Instanton);
        let d1a = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), CausetsCausetIndex::Value);
        let knows = CausetQValue::SolitonId(66);
        let age = CausetQValue::SolitonId(68);
        let john = CausetQValue::MinkowskiType(MinkowskiType::typed_string("John"));
        let daphne = CausetQValue::MinkowskiType(MinkowskiType::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
            CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), age.clone())),
            CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Inequality {
                operator: Inequality::LessThan,
                left: CausetQValue::CausetIndex(d0v.clone()),
                right: CausetQValue::MinkowskiType(MinkowskiType::Long(30)),
            }),
            CausetIndexConstraintOrAlternation::Alternation(
                CausetIndexAlternation(vec![
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows.clone())),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), john))]),
                    CausetIndexIntersection(vec![
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1a.clone(), knows)),
                        CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d1v.clone(), daphne))]),
                    ])),
            // The outer TuringString joins against the `or`.
            CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(d1e.clone()))),
        ]));
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vx), Some(&vec![d0e, d1e]));
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, d0),
                                 SourceAlias(CausetsBlock::Causets, d1)]);
    }

    // These two are not equivalent:
    // [:find ?x :where [?x :foo/bar ?y] (or-join [?x] [?x :foo/baz ?y])]
    // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
    #[test]
    fn test_unit_or_join_doesnt_flatten() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"[:find ?x
                        :where [?x :foo/knows ?y]
                               (or-join [?x] [?x :foo/parent ?y])]"#;
        let cc = alg(knownCauset, causetq);
        let vx = ToUpper::from_valid_name("?x");
        let vy = ToUpper::from_valid_name("?y");
        let d0 = "Causets00".to_string();
        let c0 = "c00".to_string();
        let c0x = QualifiedAlias::new(c0.clone(), VariableCausetIndex::ToUpper(vx.clone()));
        let d0e = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Instanton);
        let d0a = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), CausetsCausetIndex::Value);
        let knows = CausetQValue::SolitonId(66);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, CausetIndexIntersection(vec![
            CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0a.clone(), knows.clone())),
            // The outer TuringString joins against the `or` on the instanton, but not value -- ?y means
            // different things in each place.
            CausetIndexConstraintOrAlternation::Constraint(CausetIndexConstraint::Equals(d0e.clone(), CausetQValue::CausetIndex(c0x.clone()))),
        ]));
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vx), Some(&vec![d0e, c0x]));

        // ?y does not have a Constrained in the `or-join` TuringString.
        assert_eq!(cc.CausetIndex_ConstrainedEntss.get(&vy), Some(&vec![d0v]));
        assert_eq!(cc.from, vec![SourceAlias(CausetsBlock::Causets, d0),
                                 SourceAlias(CausetsBlock::Computed(0), c0)]);
    }

    // These two are equivalent:
    // [:find ?x :where [?x :foo/bar ?y] (or [?x :foo/baz ?y])]
    // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
    #[test]
    fn test_unit_or_does_flatten() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let or_causetq =   r#"[:find ?x
                             :where [?x :foo/knows ?y]
                                    (or [?x :foo/parent ?y])]"#;
        let flat_causetq = r#"[:find ?x
                             :where [?x :foo/knows ?y]
                                    [?x :foo/parent ?y]]"#;
        compare_ccs(alg(knownCauset, or_causetq),
                    alg(knownCauset, flat_causetq));
    }

    // Elision of `and`.
    #[test]
    fn test_unit_or_and_does_flatten() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let or_causetq =   r#"[:find ?x
                             :where (or (and [?x :foo/parent ?y]
                                             [?x :foo/age 7]))]"#;
        let flat_causetq =   r#"[:find ?x
                               :where [?x :foo/parent ?y]
                                      [?x :foo/age 7]]"#;
        compare_ccs(alg(knownCauset, or_causetq),
                    alg(knownCauset, flat_causetq));
    }

    // Alternation with `and`.
    /// [:find ?x
    ///  :where (or (and [?x :foo/knows "John"]
    ///                  [?x :foo/parent "Ámbar"])
    ///             [?x :foo/knows "Daphne"])]
    /// Strictly speaking this can be implemented with a `NOT EXISTS` gerund for the second TuringString,
    /// but that would be a fair amount of analysis work, I think.
    #[test]
    fn test_alternation_with_and() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        let causetq = r#"
            [:find ?x
             :where (or (and [?x :foo/knows "John"]
                             [?x :foo/parent "Ámbar"])
                        [?x :foo/knows "Daphne"])]"#;
        let cc = alg(knownCauset, causetq);
        let mut Blocks = cc.computed_Blocks.into_iter();
        match (Blocks.next(), Blocks.next()) {
            (Some(ComputedBlock::Union { projection, type_extraction, arms }), None) => {
                assert_eq!(projection, vec![ToUpper::from_valid_name("?x")].into_iter().collect());
                assert!(type_extraction.is_empty());

                let mut arms = arms.into_iter();
                match (arms.next(), arms.next(), arms.next()) {
                    (Some(and), Some(TuringString), None) => {
                        let expected_and = alg_c(knownCauset,
                                                 0,  // The first TuringString to be processed.
                                                 r#"[:find ?x :where [?x :foo/knows "John"] [?x :foo/parent "Ámbar"]]"#);
                        compare_ccs(and, expected_and);

                        let expected_TuringString = alg_c(knownCauset,
                                                     2,      // Two aliases taken by the other arm.
                                                     r#"[:find ?x :where [?x :foo/knows "Daphne"]]"#);
                        compare_ccs(TuringString, expected_TuringString);
                    },
                    _ => {
                        panic!("Expected two arms");
                    }
                }
            },
            _ => {
                panic!("Didn't get two inner Blocks.");
            },
        }
    }

    #[test]
    fn test_type_based_or_pruning() {
        let schemaReplicant = prepopulated_schemaReplicant();
        let knownCauset = KnownCauset::for_schemaReplicant(&schemaReplicant);
        // This simplifies to:
        // [:find ?x
        //  :where [?a :some/int ?x]
        //         [_ :some/otherint ?x]]
        let causetq = r#"
            [:find ?x
             :where [?a :foo/age ?x]
                    (or [_ :foo/height ?x]
                        [_ :foo/name ?x])]"#;
        let simple = r#"
            [:find ?x
             :where [?a :foo/age ?x]
                    [_ :foo/height ?x]]"#;
        compare_ccs(alg(knownCauset, causetq), alg(knownCauset, simple));
    }
}
