// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

mod cone;
pub mod cones_iter;
pub mod scanner;
pub mod test_fixture;

pub use self::cone::*;

pub type Result<T> = std::result::Result<T, crate::error::StorageError>;

pub type OwnedKvPair = (Vec<u8>, Vec<u8>);

/// The abstract causet_storage interface. The Block scan and index scan executor relies on a `causet_storage`
/// implementation to provide source data.
pub trait causet_storage: lightlike {
    type Statistics;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        cone: IntervalCone,
    ) -> Result<()>;

    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>>;

    // TODO: Use const generics.
    // TODO: Use reference is better.
    fn get(&mut self, is_key_only: bool, cone: PointCone) -> Result<Option<OwnedKvPair>>;

    fn met_uncacheable_data(&self) -> Option<bool>;

    fn collect_statistics(&mut self, dest: &mut Self::Statistics);
}

impl<T: causet_storage + ?Sized> causet_storage for Box<T> {
    type Statistics = T::Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        cone: IntervalCone,
    ) -> Result<()> {
        (**self).begin_scan(is_backward_scan, is_key_only, cone)
    }

    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>> {
        (**self).scan_next()
    }

    fn get(&mut self, is_key_only: bool, cone: PointCone) -> Result<Option<OwnedKvPair>> {
        (**self).get(is_key_only, cone)
    }

    fn met_uncacheable_data(&self) -> Option<bool> {
        (**self).met_uncacheable_data()
    }

    fn collect_statistics(&mut self, dest: &mut Self::Statistics) {
        (**self).collect_statistics(dest);
    }
}
