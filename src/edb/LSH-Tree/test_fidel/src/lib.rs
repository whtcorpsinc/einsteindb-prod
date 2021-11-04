// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

#[macro_use]
extern crate violetabftstore::interlock::;
#[macro_use]
extern crate slog_global;

pub mod mocker;
mod server;
pub mod util;

pub use self::mocker::FidelMocker;
pub use self::server::Server;
