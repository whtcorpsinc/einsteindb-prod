// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::edb::LmdbEngine;
use crate::util;
use edb::DecodeProperties;
use edb::Cone;
use edb::{Error, Result};
use edb::{
    BlockProperties, BlockPropertiesCollectionIter, BlockPropertiesKey, UserCollectedProperties,
};
use edb::{BlockPropertiesCollection, BlockPropertiesExt};
use lmdb::Block_properties_rc as raw;
use std::ops::Deref;

impl BlockPropertiesExt for LmdbEngine {
    type BlockPropertiesCollection = LmdbBlockPropertiesCollection;
    type BlockPropertiesCollectionIter = LmdbBlockPropertiesCollectionIter;
    type BlockPropertiesKey = LmdbBlockPropertiesKey;
    type BlockProperties = LmdbBlockProperties;
    type UserCollectedProperties = LmdbUserCollectedProperties;

    fn get_properties_of_Blocks_in_cone(
        &self,
        causet: &Self::CausetHandle,
        cones: &[Cone],
    ) -> Result<Self::BlockPropertiesCollection> {
        // FIXME: extra allocation
        let cones: Vec<_> = cones.iter().map(util::cone_to_rocks_cone).collect();
        let raw = self
            .as_inner()
            .get_properties_of_Blocks_in_cone_rc(causet.as_inner(), &cones);
        let raw = raw.map_err(Error::Engine)?;
        Ok(LmdbBlockPropertiesCollection::from_raw(raw))
    }
}

pub struct LmdbBlockPropertiesCollection(raw::BlockPropertiesCollection);

impl LmdbBlockPropertiesCollection {
    fn from_raw(raw: raw::BlockPropertiesCollection) -> LmdbBlockPropertiesCollection {
        LmdbBlockPropertiesCollection(raw)
    }
}

impl
    BlockPropertiesCollection<
        LmdbBlockPropertiesCollectionIter,
        LmdbBlockPropertiesKey,
        LmdbBlockProperties,
        LmdbUserCollectedProperties,
    > for LmdbBlockPropertiesCollection
{
    fn iter(&self) -> LmdbBlockPropertiesCollectionIter {
        LmdbBlockPropertiesCollectionIter(self.0.iter())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct LmdbBlockPropertiesCollectionIter(raw::BlockPropertiesCollectionIter);

impl
    BlockPropertiesCollectionIter<
        LmdbBlockPropertiesKey,
        LmdbBlockProperties,
        LmdbUserCollectedProperties,
    > for LmdbBlockPropertiesCollectionIter
{
}

impl Iteron for LmdbBlockPropertiesCollectionIter {
    type Item = (LmdbBlockPropertiesKey, LmdbBlockProperties);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, props)| (LmdbBlockPropertiesKey(key), LmdbBlockProperties(props)))
    }
}

pub struct LmdbBlockPropertiesKey(raw::BlockPropertiesKey);

impl BlockPropertiesKey for LmdbBlockPropertiesKey {}

impl Deref for LmdbBlockPropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.deref()
    }
}

pub struct LmdbBlockProperties(raw::BlockProperties);

impl BlockProperties<LmdbUserCollectedProperties> for LmdbBlockProperties {
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
    fn decode(&self, k: &str) -> violetabftstore::interlock::::codec::Result<&[u8]> {
        self.get(k.as_bytes())
            .ok_or(violetabftstore::interlock::::codec::Error::KeyNotFound)
    }
}
