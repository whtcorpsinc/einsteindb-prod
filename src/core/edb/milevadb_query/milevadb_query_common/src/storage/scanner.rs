// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use super::cone::*;
use super::cones_iter::*;
use super::{OwnedKvPair, causet_storage};
use crate::error::StorageError;

const KEY_BUFFER_CAPACITY: usize = 64;

/// A scanner that scans over multiple cones. Each cone can be a point cone containing only
/// one Evcausetidx, or an interval cone containing multiple events.
pub struct ConesScanner<T> {
    causet_storage: T,
    cones_iter: ConesIterator,

    scan_backward_in_cone: bool,
    is_key_only: bool,

    scanned_rows_per_cone: Vec<usize>,

    // The following fields are only used for calculating scanned cone. Scanned cone is only
    // useful in streaming mode, where the client need to know the underlying physical data cone
    // of each response slice, so that partial retry can be non-overlapping.
    is_scanned_cone_aware: bool,
    current_cone: IntervalCone,
    working_cone_begin_key: Vec<u8>,
    working_cone_lightlike_key: Vec<u8>,
}

pub struct ConesScannerOptions<T> {
    pub causet_storage: T,
    pub cones: Vec<Cone>,
    pub scan_backward_in_cone: bool, // TODO: This can be const generics
    pub is_key_only: bool,            // TODO: This can be const generics
    pub is_scanned_cone_aware: bool, // TODO: This can be const generics
}

impl<T: causet_storage> ConesScanner<T> {
    pub fn new(
        ConesScannerOptions {
            causet_storage,
            cones,
            scan_backward_in_cone,
            is_key_only,
            is_scanned_cone_aware,
        }: ConesScannerOptions<T>,
    ) -> ConesScanner<T> {
        let cones_len = cones.len();
        let cones_iter = ConesIterator::new(cones);
        ConesScanner {
            causet_storage,
            cones_iter,
            scan_backward_in_cone,
            is_key_only,
            scanned_rows_per_cone: Vec::with_capacity(cones_len),
            is_scanned_cone_aware,
            current_cone: IntervalCone {
                lower_inclusive: Vec::with_capacity(KEY_BUFFER_CAPACITY),
                upper_exclusive: Vec::with_capacity(KEY_BUFFER_CAPACITY),
            },
            working_cone_begin_key: Vec::with_capacity(KEY_BUFFER_CAPACITY),
            working_cone_lightlike_key: Vec::with_capacity(KEY_BUFFER_CAPACITY),
        }
    }

    /// Fetches next Evcausetidx.
    // Note: This is not implemented over `Iteron` since it can fail.
    // TODO: Change to use reference to avoid alloation and copy.
    pub fn next(&mut self) -> Result<Option<OwnedKvPair>, StorageError> {
        loop {
            let cone = self.cones_iter.next();
            let some_row = match cone {
                IterStatus::NewCone(Cone::Point(r)) => {
                    if self.is_scanned_cone_aware {
                        self.fidelio_scanned_cone_from_new_point(&r);
                    }
                    self.cones_iter.notify_drained();
                    self.scanned_rows_per_cone.push(0);
                    self.causet_storage.get(self.is_key_only, r)?
                }
                IterStatus::NewCone(Cone::Interval(r)) => {
                    if self.is_scanned_cone_aware {
                        self.fidelio_scanned_cone_from_new_cone(&r);
                    }
                    self.scanned_rows_per_cone.push(0);
                    self.causet_storage
                        .begin_scan(self.scan_backward_in_cone, self.is_key_only, r)?;
                    self.causet_storage.scan_next()?
                }
                IterStatus::Continue => self.causet_storage.scan_next()?,
                IterStatus::Drained => {
                    if self.is_scanned_cone_aware {
                        self.fidelio_working_cone_lightlike_key();
                    }
                    return Ok(None); // drained
                }
            };
            if self.is_scanned_cone_aware {
                self.fidelio_scanned_cone_from_scanned_row(&some_row);
            }
            if some_row.is_some() {
                // Retrieved one Evcausetidx from point cone or interval cone.
                if let Some(r) = self.scanned_rows_per_cone.last_mut() {
                    *r += 1;
                }

                return Ok(some_row);
            } else {
                // No more Evcausetidx in the cone.
                self.cones_iter.notify_drained();
            }
        }
    }

    /// Applightlikes causet_storage statistics collected so far to the given container and clears the
    /// collected statistics.
    pub fn collect_causet_storage_stats(&mut self, dest: &mut T::Statistics) {
        self.causet_storage.collect_statistics(dest)
    }

    /// Applightlikes scanned events of each cone so far to the given container and clears the
    /// collected statistics.
    pub fn collect_scanned_rows_per_cone(&mut self, dest: &mut Vec<usize>) {
        dest.applightlike(&mut self.scanned_rows_per_cone);
        self.scanned_rows_per_cone.push(0);
    }

    /// Returns scanned cone since last call.
    pub fn take_scanned_cone(&mut self) -> IntervalCone {
        assert!(self.is_scanned_cone_aware);

        let mut cone = IntervalCone::default();
        if !self.scan_backward_in_cone {
            std::mem::swap(
                &mut cone.lower_inclusive,
                &mut self.working_cone_begin_key,
            );
            std::mem::swap(&mut cone.upper_exclusive, &mut self.working_cone_lightlike_key);

            self.working_cone_begin_key
                .extlightlike_from_slice(&cone.upper_exclusive);
        } else {
            std::mem::swap(&mut cone.lower_inclusive, &mut self.working_cone_lightlike_key);
            std::mem::swap(
                &mut cone.upper_exclusive,
                &mut self.working_cone_begin_key,
            );

            self.working_cone_begin_key
                .extlightlike_from_slice(&cone.lower_inclusive);
        }

        cone
    }

    #[inline]
    pub fn can_be_cached(&self) -> bool {
        self.causet_storage.met_uncacheable_data() == Some(false)
    }

    fn fidelio_scanned_cone_from_new_point(&mut self, point: &PointCone) {
        assert!(self.is_scanned_cone_aware);

        self.fidelio_working_cone_lightlike_key();
        self.current_cone.lower_inclusive.clear();
        self.current_cone.upper_exclusive.clear();
        self.current_cone
            .lower_inclusive
            .extlightlike_from_slice(&point.0);
        self.current_cone
            .upper_exclusive
            .extlightlike_from_slice(&point.0);
        self.current_cone.upper_exclusive.push(0);
        self.fidelio_working_cone_begin_key();
    }

    fn fidelio_scanned_cone_from_new_cone(&mut self, cone: &IntervalCone) {
        assert!(self.is_scanned_cone_aware);

        self.fidelio_working_cone_lightlike_key();
        self.current_cone.lower_inclusive.clear();
        self.current_cone.upper_exclusive.clear();
        self.current_cone
            .lower_inclusive
            .extlightlike_from_slice(&cone.lower_inclusive);
        self.current_cone
            .upper_exclusive
            .extlightlike_from_slice(&cone.upper_exclusive);
        self.fidelio_working_cone_begin_key();
    }

    fn fidelio_working_cone_begin_key(&mut self) {
        assert!(self.is_scanned_cone_aware);

        if self.working_cone_begin_key.is_empty() {
            if !self.scan_backward_in_cone {
                self.working_cone_begin_key
                    .extlightlike(&self.current_cone.lower_inclusive);
            } else {
                self.working_cone_begin_key
                    .extlightlike(&self.current_cone.upper_exclusive);
            }
        }
    }

    fn fidelio_working_cone_lightlike_key(&mut self) {
        assert!(self.is_scanned_cone_aware);

        self.working_cone_lightlike_key.clear();
        if !self.scan_backward_in_cone {
            self.working_cone_lightlike_key
                .extlightlike(&self.current_cone.upper_exclusive);
        } else {
            self.working_cone_lightlike_key
                .extlightlike(&self.current_cone.lower_inclusive);
        }
    }

    fn fidelio_scanned_cone_from_scanned_row(&mut self, some_row: &Option<OwnedKvPair>) {
        assert!(self.is_scanned_cone_aware);

        if let Some((key, _)) = some_row {
            self.working_cone_lightlike_key.clear();
            self.working_cone_lightlike_key.extlightlike(key);
            if !self.scan_backward_in_cone {
                self.working_cone_lightlike_key.push(0);
            }
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;

    use crate::causet_storage::test_fixture::FixtureStorage;
    use crate::causet_storage::{IntervalCone, PointCone, Cone};

    fn create_causet_storage() -> FixtureStorage {
        let data: &[(&'static [u8], &'static [u8])] = &[
            (b"bar", b"2"),
            (b"bar_2", b"4"),
            (b"foo", b"1"),
            (b"foo_2", b"3"),
            (b"foo_3", b"5"),
        ];
        FixtureStorage::from(data)
    }

    #[test]
    fn test_next() {
        let causet_storage = create_causet_storage();

        // Currently we accept unordered cones.
        let cones: Vec<Cone> = vec![
            IntervalCone::from(("foo", "foo_2a")).into(),
            PointCone::from("foo_2b").into(),
            PointCone::from("foo_3").into(),
            IntervalCone::from(("a", "c")).into(),
        ];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: false,
        });
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo_3".to_vec(), b"5".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"bar".to_vec(), b"2".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"bar_2".to_vec(), b"4".to_vec()))
        );
        assert_eq!(scanner.next().unwrap(), None);

        // Backward in cone
        let cones: Vec<Cone> = vec![
            IntervalCone::from(("foo", "foo_2a")).into(),
            PointCone::from("foo_2b").into(),
            PointCone::from("foo_3").into(),
            IntervalCone::from(("a", "bar_2")).into(),
        ];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: true,
            is_key_only: false,
            is_scanned_cone_aware: false,
        });
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo_2".to_vec(), b"3".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo".to_vec(), b"1".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo_3".to_vec(), b"5".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"bar".to_vec(), b"2".to_vec()))
        );
        assert_eq!(scanner.next().unwrap(), None);

        // Key only
        let cones: Vec<Cone> = vec![
            IntervalCone::from(("bar", "foo_2a")).into(),
            PointCone::from("foo_3").into(),
            PointCone::from("bar_3").into(),
        ];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage,
            cones,
            scan_backward_in_cone: false,
            is_key_only: true,
            is_scanned_cone_aware: false,
        });
        assert_eq!(scanner.next().unwrap(), Some((b"bar".to_vec(), Vec::new())));
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"bar_2".to_vec(), Vec::new()))
        );
        assert_eq!(scanner.next().unwrap(), Some((b"foo".to_vec(), Vec::new())));
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo_2".to_vec(), Vec::new()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((b"foo_3".to_vec(), Vec::new()))
        );
        assert_eq!(scanner.next().unwrap(), None);
    }

    #[test]
    fn test_scanned_rows() {
        let causet_storage = create_causet_storage();

        let cones: Vec<Cone> = vec![
            IntervalCone::from(("foo", "foo_2a")).into(),
            PointCone::from("foo_2b").into(),
            PointCone::from("foo_3").into(),
            IntervalCone::from(("a", "z")).into(),
        ];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage,
            cones,
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: false,
        });
        let mut scanned_rows_per_cone = Vec::new();

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo");
        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_2");
        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_3");

        scanner.collect_scanned_rows_per_cone(&mut scanned_rows_per_cone);
        assert_eq!(scanned_rows_per_cone, vec![2, 0, 1]);
        scanned_rows_per_cone.clear();

        scanner.collect_scanned_rows_per_cone(&mut scanned_rows_per_cone);
        assert_eq!(scanned_rows_per_cone, vec![0]);
        scanned_rows_per_cone.clear();

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"bar");
        assert_eq!(&scanner.next().unwrap().unwrap().0, b"bar_2");

        scanner.collect_scanned_rows_per_cone(&mut scanned_rows_per_cone);
        assert_eq!(scanned_rows_per_cone, vec![0, 2]);
        scanned_rows_per_cone.clear();

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo");

        scanner.collect_scanned_rows_per_cone(&mut scanned_rows_per_cone);
        assert_eq!(scanned_rows_per_cone, vec![1]);
        scanned_rows_per_cone.clear();

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_2");
        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_3");
        assert_eq!(scanner.next().unwrap(), None);

        scanner.collect_scanned_rows_per_cone(&mut scanned_rows_per_cone);
        assert_eq!(scanned_rows_per_cone, vec![2]);
        scanned_rows_per_cone.clear();

        assert_eq!(scanner.next().unwrap(), None);

        scanner.collect_scanned_rows_per_cone(&mut scanned_rows_per_cone);
        assert_eq!(scanned_rows_per_cone, vec![0]);
        scanned_rows_per_cone.clear();
    }

    #[test]
    fn test_scanned_cone_forward() {
        let causet_storage = create_causet_storage();

        // No cone
        let cones = vec![];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        // Empty interval cone
        let cones = vec![IntervalCone::from(("x", "xb")).into()];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"xb");

        // Empty point cone
        let cones = vec![PointCone::from("x").into()];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"x\0");

        // Filled interval cone
        let cones = vec![IntervalCone::from(("foo", "foo_8")).into()];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo");
        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_2");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo_2\0");

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_3");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo_2\0");
        assert_eq!(&r.upper_exclusive, b"foo_3\0");

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo_3\0");
        assert_eq!(&r.upper_exclusive, b"foo_8");

        // Multiple cones
        // TODO: caller should not pass in unordered cones otherwise scanned cones would be
        // unsound.
        let cones = vec![
            IntervalCone::from(("foo", "foo_3")).into(),
            IntervalCone::from(("foo_5", "foo_50")).into(),
            IntervalCone::from(("bar", "bar_")).into(),
            PointCone::from("bar_2").into(),
            PointCone::from("bar_3").into(),
            IntervalCone::from(("bar_4", "box")).into(),
        ];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage,
            cones,
            scan_backward_in_cone: false,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo\0");

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_2");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo\0");
        assert_eq!(&r.upper_exclusive, b"foo_2\0");

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"bar");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo_2\0");
        assert_eq!(&r.upper_exclusive, b"bar\0");

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"bar_2");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"bar\0");
        assert_eq!(&r.upper_exclusive, b"bar_2\0");

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"bar_2\0");
        assert_eq!(&r.upper_exclusive, b"box");
    }

    #[test]
    fn test_scanned_cone_backward() {
        let causet_storage = create_causet_storage();

        // No cone
        let cones = vec![];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: true,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"");
        assert_eq!(&r.upper_exclusive, b"");

        // Empty interval cone
        let cones = vec![IntervalCone::from(("x", "xb")).into()];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: true,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"xb");

        // Empty point cone
        let cones = vec![PointCone::from("x").into()];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: true,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"x");
        assert_eq!(&r.upper_exclusive, b"x\0");

        // Filled interval cone
        let cones = vec![IntervalCone::from(("foo", "foo_8")).into()];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage: causet_storage.clone(),
            cones,
            scan_backward_in_cone: true,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_3");
        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_2");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo_2");
        assert_eq!(&r.upper_exclusive, b"foo_8");

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo_2");

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo");

        // Multiple cones
        let cones = vec![
            IntervalCone::from(("bar_4", "box")).into(),
            PointCone::from("bar_3").into(),
            PointCone::from("bar_2").into(),
            IntervalCone::from(("bar", "bar_")).into(),
            IntervalCone::from(("foo_5", "foo_50")).into(),
            IntervalCone::from(("foo", "foo_3")).into(),
        ];
        let mut scanner = ConesScanner::new(ConesScannerOptions {
            causet_storage,
            cones,
            scan_backward_in_cone: true,
            is_key_only: false,
            is_scanned_cone_aware: true,
        });

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"bar_2");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"bar_2");
        assert_eq!(&r.upper_exclusive, b"box");

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"bar");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"bar");
        assert_eq!(&r.upper_exclusive, b"bar_2");

        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo_2");
        assert_eq!(&scanner.next().unwrap().unwrap().0, b"foo");

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"bar");

        assert_eq!(scanner.next().unwrap(), None);

        let r = scanner.take_scanned_cone();
        assert_eq!(&r.lower_inclusive, b"foo");
        assert_eq!(&r.upper_exclusive, b"foo");
    }
}
