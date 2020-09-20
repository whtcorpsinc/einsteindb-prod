// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_options::PrimaryCausetNetworkOptions;
use crate::errors::Result;

/// Trait for engines with PrimaryCauset family handles.
///
/// FIXME: It's probably the case that all engines used by EinsteinDB must provide the
/// PrimaryCauset family concept; PrimaryCauset families might be pervasive; and we might not
/// want to expose CAUSET handles at all, instead just naming them. But for now,
/// everything involving CAUSET handles is here.
pub trait CAUSETHandleExt {
    // FIXME: With the current rocks implementation this
    // type wants to be able to contain a lifetime, but it's
    // not possible without "generic associated types".
    //
    // https://github.com/rust-lang/rfcs/pull/1598
    // https://github.com/rust-lang/rust/issues/44265
    type CAUSETHandle: CAUSETHandle;
    type PrimaryCausetNetworkOptions: PrimaryCausetNetworkOptions;

    fn causet_handle(&self, name: &str) -> Result<&Self::CAUSETHandle>;
    fn get_options_causet(&self, causet: &Self::CAUSETHandle) -> Self::PrimaryCausetNetworkOptions;
    fn set_options_causet(&self, causet: &Self::CAUSETHandle, options: &[(&str, &str)]) -> Result<()>;
}

pub trait CAUSETHandle {}
