// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

mod half;
mod tuplespaceInstanton;
mod size;
mod Block;

use ekvproto::meta_timeshare::Brane;
use ekvproto::fidel_timeshare::CheckPolicy;

use super::config::Config;
use super::error::Result;
use super::{KeyEntry, SemaphoreContext, SplitChecker};

pub use self::half::{get_brane_approximate_middle, HalfCheckSemaphore};
pub use self::tuplespaceInstanton::{
    get_brane_approximate_tuplespaceInstanton, get_brane_approximate_tuplespaceInstanton_causet, TuplespaceInstantonCheckSemaphore,
};
pub use self::size::{
    get_brane_approximate_size, get_brane_approximate_size_causet, SizeCheckSemaphore,
};
pub use self::Block::BlockCheckSemaphore;

pub struct Host<'a, E> {
    checkers: Vec<Box<dyn SplitChecker<E>>>,
    auto_split: bool,
    causet: &'a Config,
}

impl<'a, E> Host<'a, E> {
    pub fn new(auto_split: bool, causet: &'a Config) -> Host<'a, E> {
        Host {
            auto_split,
            checkers: vec![],
            causet,
        }
    }

    #[inline]
    pub fn auto_split(&self) -> bool {
        self.auto_split
    }

    #[inline]
    pub fn skip(&self) -> bool {
        self.checkers.is_empty()
    }

    pub fn policy(&self) -> CheckPolicy {
        for checker in &self.checkers {
            if checker.policy() == CheckPolicy::Approximate {
                return CheckPolicy::Approximate;
            }
        }
        CheckPolicy::Scan
    }

    /// Hook to call for every check during split.
    ///
    /// Return true means abort early.
    pub fn on_kv(&mut self, brane: &Brane, entry: &KeyEntry) -> bool {
        let mut ob_ctx = SemaphoreContext::new(brane);
        for checker in &mut self.checkers {
            if checker.on_kv(&mut ob_ctx, entry) {
                return true;
            }
        }
        false
    }

    pub fn split_tuplespaceInstanton(&mut self) -> Vec<Vec<u8>> {
        for checker in &mut self.checkers {
            let tuplespaceInstanton = checker.split_tuplespaceInstanton();
            if !tuplespaceInstanton.is_empty() {
                return tuplespaceInstanton;
            }
        }
        vec![]
    }

    pub fn approximate_split_tuplespaceInstanton(&mut self, brane: &Brane, engine: &E) -> Result<Vec<Vec<u8>>> {
        for checker in &mut self.checkers {
            let tuplespaceInstanton = box_try!(checker.approximate_split_tuplespaceInstanton(brane, engine));
            if !tuplespaceInstanton.is_empty() {
                return Ok(tuplespaceInstanton);
            }
        }
        Ok(vec![])
    }

    #[inline]
    pub fn add_checker(&mut self, checker: Box<dyn SplitChecker<E>>) {
        self.checkers.push(checker);
    }
}
