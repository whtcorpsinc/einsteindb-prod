// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

// Various conversions between types that can't have their own
// interdeplightlikeencies. These are used to convert between error_promises::Error and
// other Error's that error_promises can't deplightlike on.

use edb::Error as EnginePromisesError;
use ekvproto::errorpb::Error as ProtoError;
use violetabft::Error as VioletaBftError;

pub trait IntoOther<O> {
    fn into_other(self) -> O;
}

impl IntoOther<ProtoError> for EnginePromisesError {
    fn into_other(self) -> ProtoError {
        let mut errorpb = ProtoError::default();
        errorpb.set_message(format!("{}", self));

        if let EnginePromisesError::NotInCone(key, brane_id, spacelike_key, lightlike_key) = self {
            errorpb.mut_key_not_in_brane().set_key(key);
            errorpb.mut_key_not_in_brane().set_brane_id(brane_id);
            errorpb.mut_key_not_in_brane().set_spacelike_key(spacelike_key);
            errorpb.mut_key_not_in_brane().set_lightlike_key(lightlike_key);
        }

        errorpb
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
