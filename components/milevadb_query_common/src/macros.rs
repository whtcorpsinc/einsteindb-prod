// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

#[macro_export]
macro_rules! other_err {
    ($msg:tt) => ({
        milevadb_query_common::error::Error::from(milevadb_query_common::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $msg), file!(), line!())
        ))
    });
    ($f:tt, $($arg:expr),+) => ({
        milevadb_query_common::error::Error::from(milevadb_query_common::error::EvaluateError::Other(
            format!(concat!("[{}:{}]: ", $f), file!(), line!(), $($arg),+)
        ))
    });
}
