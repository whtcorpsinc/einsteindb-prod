// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use ekvproto::fidel_timeshare::*;
use fidel_client::RECONNECT_INTERVAL_SEC;

use super::*;

#[derive(Debug)]
pub struct Retry {
    retry: usize,
    count: AtomicUsize,
}

impl Retry {
    pub fn new(retry: usize) -> Retry {
        info!("[Retry] return Ok(_) every {:?} times", retry);

        Retry {
            retry,
            count: AtomicUsize::new(0),
        }
    }

    fn is_ok(&self) -> bool {
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count != 0 && count % self.retry == 0 {
            // it's ok.
            return true;
        }
        // let's sleep awhile, so that client will fidelio its connection.
        thread::sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC));
        false
    }
}

impl FidelMocker for Retry {
    fn get_brane_by_id(&self, _: &GetBraneByIdRequest) -> Option<Result<GetBraneResponse>> {
        if self.is_ok() {
            info!("[Retry] get_brane_by_id returns Ok(_)");
            Some(Ok(GetBraneResponse::default()))
        } else {
            info!("[Retry] get_brane_by_id returns Err(_)");
            Some(Err("please retry".to_owned()))
        }
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        if self.is_ok() {
            info!("[Retry] get_store returns Ok(_)");
            Some(Ok(GetStoreResponse::default()))
        } else {
            info!("[Retry] get_store returns Err(_)");
            Some(Err("please retry".to_owned()))
        }
    }
}

#[derive(Debug)]
pub struct NotRetry {
    is_visited: AtomicBool,
}

impl NotRetry {
    pub fn new() -> NotRetry {
        info!(
            "[NotRetry] return error response for the first time and return Ok() for reset times."
        );

        NotRetry {
            is_visited: AtomicBool::new(false),
        }
    }
}

impl FidelMocker for NotRetry {
    fn get_brane_by_id(&self, _: &GetBraneByIdRequest) -> Option<Result<GetBraneResponse>> {
        if !self.is_visited.swap(true, Ordering::Relaxed) {
            info!(
                "[NotRetry] get_brane_by_id returns Ok(_) with header has IncompatibleVersion error"
            );
            let mut err = Error::default();
            err.set_type(ErrorType::IncompatibleVersion);
            let mut resp = GetBraneResponse::default();
            resp.mut_header().set_error(err);
            Some(Ok(resp))
        } else {
            info!("[NotRetry] get_brane_by_id returns Ok()");
            Some(Ok(GetBraneResponse::default()))
        }
    }

    fn get_store(&self, _: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        if !self.is_visited.swap(true, Ordering::Relaxed) {
            info!(
                "[NotRetry] get_brane_by_id returns Ok(_) with header has IncompatibleVersion error"
            );
            let mut err = Error::default();
            err.set_type(ErrorType::IncompatibleVersion);
            let mut resp = GetStoreResponse::default();
            resp.mut_header().set_error(err);
            Some(Ok(resp))
        } else {
            info!("[NotRetry] get_brane_by_id returns Ok()");
            Some(Ok(GetStoreResponse::default()))
        }
    }
}
