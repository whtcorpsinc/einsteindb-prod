// Copyright 2017 EinsteinDB Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate einsteindb_util;
#[macro_use]
extern crate slog_global;

pub mod mocker;
mod server;
pub mod util;

pub use self::mocker::FidelMocker;
pub use self::server::Server;
