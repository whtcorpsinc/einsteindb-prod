// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use super::cone::Cone;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum IterStatus {
    /// All cones are consumed.
    Drained,

    /// Last cone is drained or this iteration is a fresh spacelike so that caller should scan
    /// on a new cone.
    NewCone(Cone),

    /// Last interval cone is not drained and the caller should continue scanning without changing
    /// the scan cone.
    Continue,
}

/// An Iteron like structure that produces user key cones.
///
/// For each `next()`, it produces one of the following:
/// - a new cone
/// - a flag indicating continuing last interval cone
/// - a flag indicating that all cones are consumed
///
/// If a new cone is returned, caller can then scan unknown amount of key(s) within this new cone.
/// The caller must inform the structure so that it will emit a new cone next time by calling
/// `notify_drained()` after current cone is drained. Multiple `notify_drained()` without `next()`
/// will have no effect.
pub struct ConesIterator {
    /// Whether or not we are processing a valid cone. If we are not processing a cone, or there
    /// is no cone any more, this field is `false`.
    in_cone: bool,

    iter: std::vec::IntoIter<Cone>,
}

impl ConesIterator {
    #[inline]
    pub fn new(user_key_cones: Vec<Cone>) -> Self {
        Self {
            in_cone: false,
            iter: user_key_cones.into_iter(),
        }
    }

    /// Continues iterating.
    #[inline]
    pub fn next(&mut self) -> IterStatus {
        if self.in_cone {
            return IterStatus::Continue;
        }
        match self.iter.next() {
            None => IterStatus::Drained,
            Some(cone) => {
                self.in_cone = true;
                IterStatus::NewCone(cone)
            }
        }
    }

    /// Notifies that current cone is drained.
    #[inline]
    pub fn notify_drained(&mut self) {
        self.in_cone = false;
    }
}

#[causet(test)]
mod tests {
    use super::super::cone::IntervalCone;
    use super::*;

    use std::sync::atomic;

    static RANGE_INDEX: atomic::AtomicU64 = atomic::AtomicU64::new(1);

    fn new_cone() -> Cone {
        use byteorder::{BigEndian, WriteBytesExt};

        let v = RANGE_INDEX.fetch_add(2, atomic::Ordering::SeqCst);
        let mut r = IntervalCone::from(("", ""));
        r.lower_inclusive.write_u64::<BigEndian>(v).unwrap();
        r.upper_exclusive.write_u64::<BigEndian>(v + 2).unwrap();
        Cone::Interval(r)
    }

    #[test]
    fn test_basic() {
        // Empty
        let mut c = ConesIterator::new(vec![]);
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        assert_eq!(c.next(), IterStatus::Drained);

        // Non-empty
        let cones = vec![new_cone(), new_cone(), new_cone()];
        let mut c = ConesIterator::new(cones.clone());
        assert_eq!(c.next(), IterStatus::NewCone(cones[0].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::NewCone(cones[1].clone()));
        assert_eq!(c.next(), IterStatus::Continue);
        assert_eq!(c.next(), IterStatus::Continue);
        c.notify_drained();
        c.notify_drained(); // multiple consumes will not take effect
        assert_eq!(c.next(), IterStatus::NewCone(cones[2].clone()));
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
        c.notify_drained();
        assert_eq!(c.next(), IterStatus::Drained);
    }
}
