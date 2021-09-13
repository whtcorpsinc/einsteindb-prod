// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::properties::DecodeProperties;
use crate::cone::Cone;
use crate::CausetHandleExt;
use std::ops::Deref;

pub trait BlockPropertiesExt: CausetHandleExt {
    type BlockPropertiesCollection: BlockPropertiesCollection<
        Self::BlockPropertiesCollectionIter,
        Self::BlockPropertiesKey,
        Self::BlockProperties,
        Self::UserCollectedProperties,
    >;
    type BlockPropertiesCollectionIter: BlockPropertiesCollectionIter<
        Self::BlockPropertiesKey,
        Self::BlockProperties,
        Self::UserCollectedProperties,
    >;
    type BlockPropertiesKey: BlockPropertiesKey;
    type BlockProperties: BlockProperties<Self::UserCollectedProperties>;
    type UserCollectedProperties: UserCollectedProperties;

    fn get_properties_of_Blocks_in_cone(
        &self,
        causet: &Self::CausetHandle,
        cones: &[Cone],
    ) -> Result<Self::BlockPropertiesCollection>;

    fn get_cone_properties_causet(
        &self,
        causetname: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
    ) -> Result<Self::BlockPropertiesCollection> {
        let causet = self.causet_handle(causetname)?;
        let cone = Cone::new(spacelike_key, lightlike_key);
        Ok(self.get_properties_of_Blocks_in_cone(causet, &[cone])?)
    }
}

pub trait BlockPropertiesCollection<I, PKey, P, UCP>
where
    I: BlockPropertiesCollectionIter<PKey, P, UCP>,
    PKey: BlockPropertiesKey,
    P: BlockProperties<UCP>,
    UCP: UserCollectedProperties,
{
    fn iter(&self) -> I;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait BlockPropertiesCollectionIter<PKey, P, UCP>: Iteron<Item = (PKey, P)>
where
    PKey: BlockPropertiesKey,
    P: BlockProperties<UCP>,
    UCP: UserCollectedProperties,
{
}

pub trait BlockPropertiesKey: Deref<Target = str> {}

pub trait BlockProperties<UCP>
where
    UCP: UserCollectedProperties,
{
    fn num_entries(&self) -> u64;

    fn user_collected_properties(&self) -> UCP;
}

pub trait UserCollectedProperties: DecodeProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
