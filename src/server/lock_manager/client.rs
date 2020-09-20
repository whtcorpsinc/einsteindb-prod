// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};
use futures::channel::mpsc::{self, UnboundedSlightlikeer};
use futures::future::{self, BoxFuture};
use futures::sink::SinkExt;
use futures::stream::{StreamExt, TryStreamExt};
use grpcio::{ChannelBuilder, EnvBuilder, Environment, WriteFlags};
use ekvproto::deadlock::*;
use security::SecurityManager;
use std::sync::Arc;
use std::time::Duration;

type DeadlockFuture<T> = BoxFuture<'static, Result<T>>;

pub type Callback = Box<dyn Fn(DeadlockResponse) + Slightlike>;

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "deadlock";

/// Builds the `Environment` of deadlock clients. All clients should use the same instance.
pub fn env() -> Arc<Environment> {
    Arc::new(
        EnvBuilder::new()
            .cq_count(CQ_COUNT)
            .name_prefix(thd_name!(CLIENT_PREFIX))
            .build(),
    )
}

#[derive(Clone)]
pub struct Client {
    addr: String,
    client: DeadlockClient,
    slightlikeer: Option<UnboundedSlightlikeer<DeadlockRequest>>,
}

impl Client {
    pub fn new(env: Arc<Environment>, security_mgr: Arc<SecurityManager>, addr: &str) -> Self {
        let cb = ChannelBuilder::new(env)
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3));
        let channel = security_mgr.connect(cb, addr);
        let client = DeadlockClient::new(channel);
        Self {
            addr: addr.to_owned(),
            client,
            slightlikeer: None,
        }
    }

    pub fn register_detect_handler(
        &mut self,
        cb: Callback,
    ) -> (DeadlockFuture<()>, DeadlockFuture<()>) {
        let (tx, rx) = mpsc::unbounded();
        let (sink, receiver) = self.client.detect().unwrap();
        let slightlike_task = Box::pin(async move {
            let mut sink = sink.sink_map_err(Error::Grpc);
            let res = sink
                .slightlike_all(&mut rx.map(|r| Ok((r, WriteFlags::default()))))
                .await
                .map(|_| {
                    info!("cancel detect slightlikeer");
                    sink.get_mut().cancel();
                });
            res
        });
        self.slightlikeer = Some(tx);

        let recv_task = Box::pin(receiver.map_err(Error::Grpc).try_for_each(move |resp| {
            cb(resp);
            future::ok(())
        }));

        (slightlike_task, recv_task)
    }

    pub fn detect(&self, req: DeadlockRequest) -> Result<()> {
        self.slightlikeer
            .as_ref()
            .unwrap()
            .unbounded_slightlike(req)
            .map_err(|e| Error::Other(box_err!(e)))
    }
}
