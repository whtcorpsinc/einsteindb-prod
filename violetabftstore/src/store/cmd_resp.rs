// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::error;

use crate::Error;
use ekvproto::violetabft_cmd_timeshare::VioletaBftCmdResponse;

pub fn bind_term(resp: &mut VioletaBftCmdResponse, term: u64) {
    if term == 0 {
        return;
    }

    resp.mut_header().set_current_term(term);
}

pub fn bind_error(resp: &mut VioletaBftCmdResponse, err: Error) {
    resp.mut_header().set_error(err.into());
}

pub fn new_error(err: Error) -> VioletaBftCmdResponse {
    let mut resp = VioletaBftCmdResponse::default();
    bind_error(&mut resp, err);
    resp
}

pub fn err_resp(e: Error, term: u64) -> VioletaBftCmdResponse {
    let mut resp = new_error(e);
    bind_term(&mut resp, term);
    resp
}

pub fn message_error<E>(err: E) -> VioletaBftCmdResponse
where
    E: Into<Box<dyn error::Error + lightlike + Sync>>,
{
    new_error(Error::Other(err.into()))
}
