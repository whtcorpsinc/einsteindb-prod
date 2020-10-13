// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

pub(crate) mod metrics;
mod violetabft_client;

pub mod config;
pub mod debug;
pub mod errors;
pub mod gc_worker;
pub mod load_statistics;
pub mod lock_manager;
pub mod node;
pub mod violetabftkv;
pub mod resolve;
pub mod server;
pub mod service;
pub mod snap;
pub mod status_server;
pub mod transport;

pub use self::config::{Config, DEFAULT_CLUSTER_ID, DEFAULT_LISTENING_ADDR};
pub use self::errors::{Error, Result};
pub use self::metrics::CONFIG_LMDB_GAUGE;
pub use self::metrics::CPU_CORES_QUOTA_GAUGE;
pub use self::node::{create_violetabft_causetStorage, Node};
pub use self::violetabft_client::VioletaBftClient;
pub use self::violetabftkv::VioletaBftKv;
pub use self::resolve::{FidelStoreAddrResolver, StoreAddrResolver};
pub use self::server::Server;
pub use self::transport::ServerTransport;
