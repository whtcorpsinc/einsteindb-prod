use std::sync::atomic::*;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::{FutureExt, SinkExt, StreamExt, TryFutureExt};
use grpcio::{self, *};
use ekvproto::backup::*;
use security::{check_common_name, SecurityManager};
use violetabftstore::interlock::::worker::*;

use super::Task;

/// Service handles the RPC messages for the `Backup` service.
#[derive(Clone)]
pub struct Service {
    interlock_semaphore: Interlock_Semaphore<Task>,
    security_mgr: Arc<SecurityManager>,
}

impl Service {
    /// Create a new backup service.
    pub fn new(interlock_semaphore: Interlock_Semaphore<Task>, security_mgr: Arc<SecurityManager>) -> Service {
        Service {
            interlock_semaphore,
            security_mgr,
        }
    }
}

impl Backup for Service {
    fn backup(
        &mut self,
        ctx: RpcContext,
        req: BackupRequest,
        mut sink: ServerStreamingSink<BackupResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let mut cancel = None;
        // TODO: make it a bounded channel.
        let (tx, rx) = mpsc::unbounded();
        if let Err(status) = match Task::new(req, tx) {
            Ok((task, c)) => {
                cancel = Some(c);
                self.interlock_semaphore.schedule(task).map_err(|e| {
                    RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e)))
                })
            }
            Err(e) => Err(RpcStatus::new(
                RpcStatusCode::UNKNOWN,
                Some(format!("{:?}", e)),
            )),
        } {
            error!("backup task initiate failed"; "error" => ?status);
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("backup failed to lightlike error"; "error" => ?e)),
            );
            return;
        };

        let lightlike_task = async move {
            let mut s = rx.map(|resp| Ok((resp, WriteFlags::default())));
            sink.lightlike_all(&mut s).await?;
            sink.close().await?;
            Ok(())
        }
        .map(|res: Result<()>| {
            match res {
                Ok(_) => {
                    info!("backup lightlike half closed");
                }
                Err(e) => {
                    if let Some(c) = cancel {
                        // Cancel the running task.
                        c.store(true, Ordering::SeqCst);
                    }
                    error!("backup canceled"; "error" => ?e);
                }
            }
        });

        ctx.spawn(lightlike_task);
    }
}

#[causet(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::lightlikepoint::tests::*;
    use external_causet_storage::make_local_backlightlike;
    use security::*;
    use edb::causet_storage::tail_pointer::tests::*;
    use edb::causet_storage::txn::tests::must_commit;
    use violetabftstore::interlock::::mpsc::Receiver;
    use txn_types::TimeStamp;

    fn new_rpc_suite() -> (Server, BackupClient, Receiver<Option<Task>>) {
        let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
        let env = Arc::new(EnvBuilder::new().build());
        let (interlock_semaphore, rx) = dummy_interlock_semaphore();
        let backup_service = super::Service::new(interlock_semaphore, security_mgr);
        let builder =
            ServerBuilder::new(env.clone()).register_service(create_backup(backup_service));
        let mut server = builder.bind("127.0.0.1", 0).build().unwrap();
        server.spacelike();
        let (_, port) = server.bind_addrs().next().unwrap();
        let addr = format!("127.0.0.1:{}", port);
        let channel = ChannelBuilder::new(env).connect(&addr);
        let client = BackupClient::new(channel);
        (server, client, rx)
    }

    #[test]
    fn test_client_stop() {
        let (_server, client, rx) = new_rpc_suite();

        let (tmp, lightlikepoint) = new_lightlikepoint();
        let engine = lightlikepoint.engine.clone();
        lightlikepoint.brane_info.set_branes(vec![
            (b"".to_vec(), b"2".to_vec(), 1),
            (b"2".to_vec(), b"5".to_vec(), 2),
        ]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        for i in 0..5 {
            let spacelike = alloc_ts();
            let key = format!("{}", i);
            must_prewrite_put(
                &engine,
                key.as_bytes(),
                key.as_bytes(),
                key.as_bytes(),
                spacelike,
            );
            let commit = alloc_ts();
            must_commit(&engine, key.as_bytes(), spacelike, commit);
        }

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_spacelike_key(vec![]);
        req.set_lightlike_key(vec![b'5']);
        req.set_spacelike_version(now.into_inner());
        req.set_lightlike_version(now.into_inner());
        // Set an unique path to avoid AlreadyExists error.
        req.set_causet_storage_backlightlike(make_local_backlightlike(&tmp.path().join(now.to_string())));

        let stream = client.backup(&req).unwrap();
        let task = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Drop stream without spacelike receiving will cause cancel error.
        drop(stream);
        // A stopped remote must not cause panic.
        lightlikepoint.handle_backup_task(task.unwrap());

        // Set an unique path to avoid AlreadyExists error.
        req.set_causet_storage_backlightlike(make_local_backlightlike(&tmp.path().join(alloc_ts().to_string())));
        let mut stream = client.backup(&req).unwrap();
        // Drop steam once it received something.
        client.spawn(async move {
            let _ = stream.next().await;
        });
        let task = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // A stopped remote must not cause panic.
        lightlikepoint.handle_backup_task(task.unwrap());

        // Set an unique path to avoid AlreadyExists error.
        req.set_causet_storage_backlightlike(make_local_backlightlike(&tmp.path().join(alloc_ts().to_string())));
        let stream = client.backup(&req).unwrap();
        let task = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        // Drop stream without spacelike receiving will cause cancel error.
        drop(stream);
        // Wait util the task is canceled in map_err.
        loop {
            std::thread::sleep(Duration::from_millis(100));
            if task.resp.unbounded_lightlike(Default::default()).is_err() {
                break;
            }
        }
        // The task should be canceled.
        assert!(task.has_canceled());
        // A stopped remote must not cause panic.
        lightlikepoint.handle_backup_task(task);
    }
}
