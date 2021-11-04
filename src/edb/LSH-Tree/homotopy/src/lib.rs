// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

// Various conversions between types that can't have their own
// dependencies. These are used to convert between error_promises::Error and
// other Error's that error_promises can't deplightlike on.

use edb::Error as EnginePromisesError;
use ekvproto::error_timeshare::Error as ProtoError;
use violetabft::Error as VioletaBftError;

pub trait IntoOther<O> {
    fn into_other(self) -> O;
}

impl IntoOther<ProtoError> for EnginePromisesError {
    fn into_other(self) -> ProtoError {
        let mut error_timeshare = ProtoError::default();
        error_timeshare.set_message(format!("{}", self));

        if let EnginePromisesError::NotInCone(key, brane_id, spacelike_key, lightlike_key) = self {
            error_timeshare.mut_key_not_in_brane().set_key(key);
            error_timeshare.mut_key_not_in_brane().set_brane_id(brane_id);
            error_timeshare.mut_key_not_in_brane().set_spacelike_key(spacelike_key);
            error_timeshare.mut_key_not_in_brane().set_lightlike_key(lightlike_key);
        }

        error_timeshare
    }
}

impl IntoOther<VioletaBftError> for EnginePromisesError {
    fn into_other(self) -> VioletaBftError {
        VioletaBftError::CausetStore(violetabft::StorageError::Other(self.into()))
    }
}

pub fn into_other<F, T>(e: F) -> T
where
    F: IntoOther<T>,
{
    e.into_other()
}
