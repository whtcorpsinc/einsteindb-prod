// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::collections::{btree_map, BTreeMap};
use std::sync::Arc;

use super::cone::*;
use super::Result;

type ErrorBuilder = Box<dyn lightlike + Sync + Fn() -> crate::error::StorageError>;

type FixtureValue = std::result::Result<Vec<u8>, ErrorBuilder>;

/// A `causet_storage` implementation that returns fixed source data (i.e. fixture). Useful in tests.
#[derive(Clone)]
pub struct FixtureStorage {
    data: Arc<BTreeMap<Vec<u8>, FixtureValue>>,
    data_view_unsafe: Option<btree_map::Cone<'static, Vec<u8>, FixtureValue>>,
    is_backward_scan: bool,
    is_key_only: bool,
}

impl FixtureStorage {
    pub fn new(data: BTreeMap<Vec<u8>, FixtureValue>) -> Self {
        Self {
            data: Arc::new(data),
            data_view_unsafe: None,
            is_backward_scan: false,
            is_key_only: false,
        }
    }
}

impl<'a, 'b> From<&'b [(&'a [u8], &'a [u8])]> for FixtureStorage {
    fn from(v: &'b [(&'a [u8], &'a [u8])]) -> FixtureStorage {
        let tree: BTreeMap<_, _> = v
            .iter()
            .map(|(k, v)| (k.to_vec(), Ok(v.to_vec())))
            .collect();
        Self::new(tree)
    }
}

impl From<Vec<(Vec<u8>, Vec<u8>)>> for FixtureStorage {
    fn from(v: Vec<(Vec<u8>, Vec<u8>)>) -> FixtureStorage {
        let tree: BTreeMap<_, _> = v.into_iter().map(|(k, v)| (k, Ok(v))).collect();
        Self::new(tree)
    }
}

impl super::causet_storage for FixtureStorage {
    type Statistics = ();

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        cone: IntervalCone,
    ) -> Result<()> {
        let data_view = self
            .data
            .cone(cone.lower_inclusive..cone.upper_exclusive);
        // Erase the lifetime to be 'static.
        self.data_view_unsafe = unsafe { Some(std::mem::transmute(data_view)) };
        self.is_backward_scan = is_backward_scan;
        self.is_key_only = is_key_only;
        Ok(())
    }

    fn scan_next(&mut self) -> Result<Option<super::OwnedKvPair>> {
        let value = if !self.is_backward_scan {
            // During the call of this function, `data` must be valid and we are only returning
            // data clones to outside, so this access is safe.
            self.data_view_unsafe.as_mut().unwrap().next()
        } else {
            self.data_view_unsafe.as_mut().unwrap().next_back()
        };
        match value {
            None => Ok(None),
            Some((k, Ok(v))) => {
                if !self.is_key_only {
                    Ok(Some((k.clone(), v.clone())))
                } else {
                    Ok(Some((k.clone(), Vec::new())))
                }
            }
            Some((_k, Err(err_producer))) => Err(err_producer()),
        }
    }

    fn get(&mut self, is_key_only: bool, cone: PointCone) -> Result<Option<super::OwnedKvPair>> {
        let r = self.data.get(&cone.0);
        match r {
            None => Ok(None),
            Some(Ok(v)) => {
                if !is_key_only {
                    Ok(Some((cone.0, v.clone())))
                } else {
                    Ok(Some((cone.0, Vec::new())))
                }
            }
            Some(Err(err_producer)) => Err(err_producer()),
        }
    }

    fn collect_statistics(&mut self, _dest: &mut Self::Statistics) {}

    fn met_uncacheable_data(&self) -> Option<bool> {
        None
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::causet_storage::causet_storage;

    #[test]
    fn test_basic() {
        let data: &[(&'static [u8], &'static [u8])] = &[
            (b"foo", b"1"),
            (b"bar", b"2"),
            (b"foo_2", b"3"),
            (b"bar_2", b"4"),
            (b"foo_3", b"5"),
        ];
        let mut causet_storage = FixtureStorage::from(data);

        // Get Key only = false
        assert_eq!(causet_storage.get(false, PointCone::from("a")).unwrap(), None);
        assert_eq!(
            causet_storage.get(false, PointCone::from("foo")).unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );

        // Get Key only = true
        assert_eq!(causet_storage.get(true, PointCone::from("a")).unwrap(), None);
        assert_eq!(
            causet_storage.get(true, PointCone::from("foo")).unwrap(),
            Some((b"foo".to_vec(), Vec::new()))
        );

        // Scan Backward = false, Key only = false
        causet_storage
            .begin_scan(false, false, IntervalCone::from(("foo", "foo_3")))
            .unwrap();

        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );

        let mut s2 = causet_storage.clone();
        assert_eq!(
            s2.scan_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );

        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(causet_storage.scan_next().unwrap(), None);
        assert_eq!(causet_storage.scan_next().unwrap(), None);

        assert_eq!(s2.scan_next().unwrap(), None);
        assert_eq!(s2.scan_next().unwrap(), None);

        // Scan Backward = false, Key only = false
        causet_storage
            .begin_scan(false, false, IntervalCone::from(("bar", "bar_2")))
            .unwrap();

        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"bar".to_vec(), b"2".to_vec()))
        );
        assert_eq!(causet_storage.scan_next().unwrap(), None);

        // Scan Backward = false, Key only = true
        causet_storage
            .begin_scan(false, true, IntervalCone::from(("bar", "foo_")))
            .unwrap();

        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"bar".to_vec(), Vec::new()))
        );
        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"bar_2".to_vec(), Vec::new()))
        );
        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"foo".to_vec(), Vec::new()))
        );
        assert_eq!(causet_storage.scan_next().unwrap(), None);

        // Scan Backward = true, Key only = false
        causet_storage
            .begin_scan(true, false, IntervalCone::from(("foo", "foo_3")))
            .unwrap();

        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            causet_storage.scan_next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );
        assert_eq!(causet_storage.scan_next().unwrap(), None);
        assert_eq!(causet_storage.scan_next().unwrap(), None);

        // Scan empty cone
        causet_storage
            .begin_scan(false, false, IntervalCone::from(("faa", "fab")))
            .unwrap();
        assert_eq!(causet_storage.scan_next().unwrap(), None);

        causet_storage
            .begin_scan(false, false, IntervalCone::from(("foo", "foo")))
            .unwrap();
        assert_eq!(causet_storage.scan_next().unwrap(), None);
    }
}
