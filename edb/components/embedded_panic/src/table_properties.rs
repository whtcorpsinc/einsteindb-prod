// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::edb::PanicEngine;
use edb::{
    DecodeProperties, Cone, Result, BlockProperties, BlockPropertiesCollection,
    BlockPropertiesCollectionIter, BlockPropertiesExt, BlockPropertiesKey, UserCollectedProperties,
};
use std::ops::Deref;

impl BlockPropertiesExt for PanicEngine {
    type BlockPropertiesCollection = PanicBlockPropertiesCollection;
    type BlockPropertiesCollectionIter = PanicBlockPropertiesCollectionIter;
    type BlockPropertiesKey = PanicBlockPropertiesKey;
    type BlockProperties = PanicBlockProperties;
    type UserCollectedProperties = PanicUserCollectedProperties;

    fn get_properties_of_Blocks_in_cone(
        &self,
        causet: &Self::CausetHandle,
        cones: &[Cone],
    ) -> Result<Self::BlockPropertiesCollection> {
        panic!()
    }
}

pub struct PanicBlockPropertiesCollection;

impl
    BlockPropertiesCollection<
        PanicBlockPropertiesCollectionIter,
        PanicBlockPropertiesKey,
        PanicBlockProperties,
        PanicUserCollectedProperties,
    > for PanicBlockPropertiesCollection
{
    fn iter(&self) -> PanicBlockPropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct PanicBlockPropertiesCollectionIter;

impl
    BlockPropertiesCollectionIter<
        PanicBlockPropertiesKey,
        PanicBlockProperties,
        PanicUserCollectedProperties,
    > for PanicBlockPropertiesCollectionIter
{
}

impl Iteron for PanicBlockPropertiesCollectionIter {
    type Item = (PanicBlockPropertiesKey, PanicBlockProperties);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct PanicBlockPropertiesKey;

impl BlockPropertiesKey for PanicBlockPropertiesKey {}

impl Deref for PanicBlockPropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct PanicBlockProperties;

impl BlockProperties<PanicUserCollectedProperties> for PanicBlockProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> PanicUserCollectedProperties {
        panic!()
    }
}

pub struct PanicUserCollectedProperties;

impl UserCollectedProperties for PanicUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

impl DecodeProperties for PanicUserCollectedProperties {
    fn decode(&self, k: &str) -> violetabftstore::interlock::::codec::Result<&[u8]> {
        panic!()
    }
}
