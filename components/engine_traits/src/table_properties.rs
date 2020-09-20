// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::properties::DecodeProperties;
use crate::cone::Cone;
use crate::CAUSETHandleExt;
use std::ops::Deref;

pub trait TablePropertiesExt: CAUSETHandleExt {
    type TablePropertiesCollection: TablePropertiesCollection<
        Self::TablePropertiesCollectionIter,
        Self::TablePropertiesKey,
        Self::TableProperties,
        Self::UserCollectedProperties,
    >;
    type TablePropertiesCollectionIter: TablePropertiesCollectionIter<
        Self::TablePropertiesKey,
        Self::TableProperties,
        Self::UserCollectedProperties,
    >;
    type TablePropertiesKey: TablePropertiesKey;
    type TableProperties: TableProperties<Self::UserCollectedProperties>;
    type UserCollectedProperties: UserCollectedProperties;

    fn get_properties_of_tables_in_cone(
        &self,
        causet: &Self::CAUSETHandle,
        cones: &[Cone],
    ) -> Result<Self::TablePropertiesCollection>;

    fn get_cone_properties_causet(
        &self,
        causetname: &str,
        spacelike_key: &[u8],
        lightlike_key: &[u8],
    ) -> Result<Self::TablePropertiesCollection> {
        let causet = self.causet_handle(causetname)?;
        let cone = Cone::new(spacelike_key, lightlike_key);
        Ok(self.get_properties_of_tables_in_cone(causet, &[cone])?)
    }
}

pub trait TablePropertiesCollection<I, PKey, P, UCP>
where
    I: TablePropertiesCollectionIter<PKey, P, UCP>,
    PKey: TablePropertiesKey,
    P: TableProperties<UCP>,
    UCP: UserCollectedProperties,
{
    fn iter(&self) -> I;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait TablePropertiesCollectionIter<PKey, P, UCP>: Iteron<Item = (PKey, P)>
where
    PKey: TablePropertiesKey,
    P: TableProperties<UCP>,
    UCP: UserCollectedProperties,
{
}

pub trait TablePropertiesKey: Deref<Target = str> {}

pub trait TableProperties<UCP>
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
