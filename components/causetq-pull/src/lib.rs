// Copyright 2018 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

///! A pull expression is a function.
///!
///! Its inputs are a store, a schema, and a set of bindings.
///!
///! Its output is a map whose keys are the input bindings and whose values are
///! appropriate structured values to represent the pull expression.
///!
///! For example, the pull expression:
///!
///! ```edbn
///! (pull ?person [:person/name
///!                :person/tattoo
///!                {:person/friend [*]}])`
///! ```
///!
///! will return values shaped like:
///!
///! ```edbn
///! {:person/name "Alice"                            ; Single-valued attribute
///!                                                  ; Absence: Alice has no tattoos.
///!  :person/friend [                                ; Multi-valued attribute.
///!    {:person/name "Bob"                           ; Nesting and wildcard.
///!     :person/pet ["Harrison", "Hoppy"]}]}
///! ```
///!
///! There will be one such value for each input binding.
///!
///! We fetch layers of a pull expression iteratively: all attributes at the same
///! 'level' can be fetched at the same time and accumulated into maps.
///!
///! Those maps are wrapped in `Rc` for two reasons:
///! - They might occur multiple times when timelike_distance from a `:find` causetq.
///! - They might refer to each other (consider recursion).
///!
///! A nested or recursive pull expression consumes values produced by earlier stages
///! (the recursion with a smaller recursion limit and a growing 'seen' list),
///! generating another layer of mappings.
///!
///! For example, you can imagine the nesting in the earlier pull expression being
///! decomposed into two chained expressions:
///!
///! ```edbn
///! (pull
///!     (pull ?person [:person/friend])
///      [*]))
///! ```

extern crate failure;
extern crate rusqlite;

extern crate edbn;
extern crate einsteindb_embedded;
extern crate embedded_promises;
extern crate einstein_db;
extern crate causetq_pull_promises;

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use std::iter::{
    once,
};

use embedded_promises::{
    Binding,
    SolitonId,
    TypedValue,
    StructuredMap,
};

use einsteindb_embedded::{
    Cloned,
    HasSchema,
    Keyword,
    Schema,
    ValueRc,
};

use einstein_db::immutable_memTcam;

use edbn::causetq::{
    NamedPullAttribute,
    PullAttributeSpec,
    PullConcreteAttribute,
};

use causetq_pull_promises::errors::{
    PullError,
    Result,
};

type PullResults = BTreeMap<SolitonId, ValueRc<StructuredMap>>;

pub fn pull_attributes_for_instanton<A>(schema: &Schema,
                                     edb: &rusqlite::Connection,
                                     instanton: SolitonId,
                                     attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=SolitonId> {
    let attrs = attributes.into_iter()
                          .map(|e| PullAttributeSpec::Attribute(PullConcreteAttribute::SolitonId(e).into()))
                          .collect();
    Puller::prepare(schema, attrs)?
        .pull(schema, edb, once(instanton))
        .map(|m| m.into_iter()
                  .next()
                  .map(|(k, vs)| {
                      assert_eq!(k, instanton);
                      vs.cloned()
                  })
                  .unwrap_or_else(StructuredMap::default))
}

pub fn pull_attributes_for_entities<E, A>(schema: &Schema,
                                          edb: &rusqlite::Connection,
                                          entities: E,
                                          attributes: A) -> Result<PullResults>
    where E: IntoIterator<Item=SolitonId>,
          A: IntoIterator<Item=SolitonId> {
    let attrs = attributes.into_iter()
                          .map(|e| PullAttributeSpec::Attribute(PullConcreteAttribute::SolitonId(e).into()))
                          .collect();
    Puller::prepare(schema, attrs)?
        .pull(schema, edb, entities)
}

/// A `Puller` constructs on demand a map from a provided set of instanton IDs to a set of structured maps.
pub struct Puller {
    // The domain of this map is the set of attributes to fetch.
    // The range is the set of aliases to use in the output.
    attributes: BTreeMap<SolitonId, ValueRc<Keyword>>,
    attribute_spec: immutable_memTcam::AttributeSpec,

    // If this is set, each pulled instanton is contributed to its own output map, labeled with this
    // keyword. This is a divergence from Datomic, which has no types by which to differentiate a
    // long from an instanton ID, and thus represents all entities in pull as, _e.g._, `{:edb/id 1234}`.
    //  EinsteinDB can use `TypedValue::Ref(1234)`, but it's sometimes convenient to fetch the instanton ID
    // itself as part of a pull expression: `{:person 1234, :person/name "Peter"}`.
    edb_id_alias: Option<ValueRc<Keyword>>,
}

impl Puller {
    pub fn prepare(schema: &Schema, attributes: Vec<PullAttributeSpec>) -> Result<Puller> {
        // TODO: eventually this entry point will handle aliasing and that kind of
        // thing. For now it's just a convenience.

        let lookup_name = |i: &SolitonId| {
            // In the unlikely event that we have an attribute with no name, we bail.
            schema.get_causetId(*i)
                    .map(|causetid| ValueRc::new(causetid.clone()))
                    .ok_or_else(|| PullError::UnnamedAttribute(*i))
        };

        let mut names: BTreeMap<SolitonId, ValueRc<Keyword>> = Default::default();
        let mut attrs: BTreeSet<SolitonId> = Default::default();
        let edb_id = ::std::rc::Rc::new(Keyword::namespaced("edb", "id"));
        let mut edb_id_alias = None;

        for attr in attributes.iter() {
            match attr {
                &PullAttributeSpec::Wildcard => {
                    let attribute_ids = schema.attribute_map.keys();
                    for id in attribute_ids {
                        names.insert(*id, lookup_name(id)?);
                        attrs.insert(*id);
                    }
                    break;
                },
                &PullAttributeSpec::Attribute(NamedPullAttribute {
                    ref attribute,
                    ref alias,
                }) => {
                    let alias = alias.as_ref()
                                     .map(|ref r| r.to_value_rc());
                    match attribute {
                        // Handle :edb/id.
                        &PullConcreteAttribute::CausetId(ref i) if i.as_ref() == edb_id.as_ref() => {
                            // We only allow :edb/id once.
                            if edb_id_alias.is_some() {
                                Err(PullError::RepeatedDbId)?
                            }
                            edb_id_alias = Some(alias.unwrap_or_else(|| edb_id.to_value_rc()));
                        },
                        &PullConcreteAttribute::CausetId(ref i) => {
                            if let Some(solitonId) = schema.get_entid(i) {
                                let name = alias.unwrap_or_else(|| i.to_value_rc());
                                names.insert(solitonId.into(), name);
                                attrs.insert(solitonId.into());
                            }
                        },
                        &PullConcreteAttribute::SolitonId(ref solitonId) => {
                            let name = alias.map(Ok).unwrap_or_else(|| lookup_name(solitonId))?;
                            names.insert(*solitonId, name);
                            attrs.insert(*solitonId);
                        },
                    }
                },
            }
        }

        Ok(Puller {
            attributes: names,
            attribute_spec: immutable_memTcam::AttributeSpec::specified(&attrs, schema),
            edb_id_alias,
        })
    }

    pub fn pull<E>(&self,
                   schema: &Schema,
                   edb: &rusqlite::Connection,
                   entities: E) -> Result<PullResults>
        where E: IntoIterator<Item=SolitonId> {
        // We implement pull by:
        // - Generating `AttributeCaches` for the provided attributes and entities.
        //   TODO: it would be nice to invert the immutable_memTcam as we build it, rather than have to invert it here.
        // - Recursing. (TODO: we'll need AttributeCaches to not overwrite in case of recursion! And
        //   ideally not do excess work when some instanton/attribute pairs are known.)
        // - Building a structure by walking the pull expression with the caches.
        // TODO: limits.

        // Build a immutable_memTcam for these attributes and entities.
        // TODO: use the store's existing immutable_memTcam!
        let entities: Vec<SolitonId> = entities.into_iter().collect();
        let caches = immutable_memTcam::AttributeCaches::make_cache_for_entities_and_attributes(
            schema,
            edb,
            self.attribute_spec.clone(),
            &entities)?;

        // Now construct the appropriate result format.
        // TODO: should we walk `e` then `a`, or `a` then `e`? Possibly the right answer
        // is just to collect differently!
        let mut maps = BTreeMap::new();

        // Collect :edb/id if requested.
        if let Some(ref alias) = self.edb_id_alias {
            for e in entities.iter() {
                let mut r = maps.entry(*e)
                                .or_insert(ValueRc::new(StructuredMap::default()));
                let mut m = ValueRc::get_mut(r).unwrap();
                m.insert(alias.clone(), Binding::Scalar(TypedValue::Ref(*e)));
            }
        }

        for (name, immutable_memTcam) in self.attributes.iter().filter_map(|(a, name)|
            caches.forward_attribute_cache_for_attribute(schema, *a)
                  .map(|immutable_memTcam| (name.clone(), immutable_memTcam))) {

            for e in entities.iter() {
                if let Some(binding) = immutable_memTcam.binding_for_e(*e) {
                    let mut r = maps.entry(*e)
                                    .or_insert(ValueRc::new(StructuredMap::default()));

                    // Get into the inner map so we can accumulate a value.
                    // We can unwrap here because we created all of these maps…
                    let mut m = ValueRc::get_mut(r).unwrap();

                    m.insert(name.clone(), binding);
                }
            }
        }

        Ok(maps)
    }

}
