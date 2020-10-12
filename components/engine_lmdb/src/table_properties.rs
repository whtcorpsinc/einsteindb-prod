// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::engine::LmdbEngine;
use crate::util;
use engine_promises::DecodeProperties;
use engine_promises::Cone;
use engine_promises::{Error, Result};
use engine_promises::{
    TableProperties, TablePropertiesCollectionIter, TablePropertiesKey, UserCollectedProperties,
};
use engine_promises::{TablePropertiesCollection, TablePropertiesExt};
use lmdb::table_properties_rc as raw;
use std::ops::Deref;

impl TablePropertiesExt for LmdbEngine {
    type TablePropertiesCollection = LmdbTablePropertiesCollection;
    type TablePropertiesCollectionIter = LmdbTablePropertiesCollectionIter;
    type TablePropertiesKey = LmdbTablePropertiesKey;
    type TableProperties = LmdbTableProperties;
    type UserCollectedProperties = LmdbUserCollectedProperties;

    fn get_properties_of_tables_in_cone(
        &self,
        causet: &Self::CAUSETHandle,
        cones: &[Cone],
    ) -> Result<Self::TablePropertiesCollection> {
        // FIXME: extra allocation
        let cones: Vec<_> = cones.iter().map(util::cone_to_rocks_cone).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_cone_rc(causet.as_inner(), &cones);
        let raw = raw.map_err(Error::Engine)?;
        Ok(LmdbTablePropertiesCollection::from_raw(raw))
    }
}

pub struct LmdbTablePropertiesCollection(raw::TablePropertiesCollection);

impl LmdbTablePropertiesCollection {
    fn from_raw(raw: raw::TablePropertiesCollection) -> LmdbTablePropertiesCollection {
        LmdbTablePropertiesCollection(raw)
    }
}

impl
    TablePropertiesCollection<
        LmdbTablePropertiesCollectionIter,
        LmdbTablePropertiesKey,
        LmdbTableProperties,
        LmdbUserCollectedProperties,
    > for LmdbTablePropertiesCollection
{
    fn iter(&self) -> LmdbTablePropertiesCollectionIter {
        LmdbTablePropertiesCollectionIter(self.0.iter())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct LmdbTablePropertiesCollectionIter(raw::TablePropertiesCollectionIter);

impl
    TablePropertiesCollectionIter<
        LmdbTablePropertiesKey,
        LmdbTableProperties,
        LmdbUserCollectedProperties,
    > for LmdbTablePropertiesCollectionIter
{
}

impl Iteron for LmdbTablePropertiesCollectionIter {
    type Item = (LmdbTablePropertiesKey, LmdbTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, props)| (LmdbTablePropertiesKey(key), LmdbTableProperties(props)))
    }
}

pub struct LmdbTablePropertiesKey(raw::TablePropertiesKey);

impl TablePropertiesKey for LmdbTablePropertiesKey {}

impl Deref for LmdbTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.deref()
    }
}

pub struct LmdbTableProperties(raw::TableProperties);

impl TableProperties<LmdbUserCollectedProperties> for LmdbTableProperties {
    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }

    fn user_collected_properties(&self) -> LmdbUserCollectedProperties {
        LmdbUserCollectedProperties(self.0.user_collected_properties())
    }
}

pub struct LmdbUserCollectedProperties(raw::UserCollectedProperties);

impl UserCollectedProperties for LmdbUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl DecodeProperties for LmdbUserCollectedProperties {
    fn decode(&self, k: &str) -> einsteindb_util::codec::Result<&[u8]> {
        self.get(k.as_bytes())
            .ok_or(einsteindb_util::codec::Error::KeyNotFound)
    }
}
