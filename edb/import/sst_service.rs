//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::f64::INFINITY;
use std::sync::Arc;

use edb::{name_to_causet, CompactExt, MiscExt, Causet_DEFAULT, Causet_WRITE};
use futures::executor::{ThreadPool, ThreadPoolBuilder};
use futures::{TryFutureExt, TryStreamExt};
use grpcio::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use ekvproto::error_timeshare;

#[causet(feature = "prost-codec")]
use ekvproto::import_sst_timeshare::write_request::*;
#[causet(feature = "protobuf-codec")]
use ekvproto::import_sst_timeshare::WriteRequest_oneof_Soliton as Soliton;
use ekvproto::import_sst_timeshare::*;

use ekvproto::violetabft_cmd_timeshare::*;

use crate::server::CONFIG_LMDB_GAUGE;
use engine_lmdb::LmdbEngine;
use edb::{SstExt, SstWriterBuilder};
use violetabftstore::router::VioletaBftStoreRouter;
use violetabftstore::store::Callback;
use security::{check_common_name, SecurityManager};
use sst_importer::lightlike_rpc_response;
use violetabftstore::interlock::::future::create_stream_with_buffer;
use violetabftstore::interlock::::future::paired_future_callback;
use violetabftstore::interlock::::time::{Instant, Limiter};

use sst_importer::import_mode::*;
use sst_importer::metrics::*;
use sst_importer::service::*;
use sst_importer::{error_inc, Config, Error, SSTImporter};

/// ImportSSTService provides edb-server with the ability to ingest SST files.
///
/// It saves the SST sent from client to a file and then lightlikes a command to
/// violetabftstore to trigger the ingest process.
#[derive(Clone)]
pub struct ImportSSTService<Router> {
    causet: Config,
    router: Router,
    engine: LmdbEngine,
    threads: ThreadPool,
    importer: Arc<SSTImporter>,
    switcher: ImportModeSwitcher<LmdbEngine>,
    limiter: Limiter,
    security_mgr: Arc<SecurityManager>,
}

impl<Router: VioletaBftStoreRouter<LmdbEngine>> ImportSSTService<Router> {
    pub fn new(
        causet: Config,
        router: Router,
        engine: LmdbEngine,
        importer: Arc<SSTImporter>,
        security_mgr: Arc<SecurityManager>,
    ) -> ImportSSTService<Router> {
        let threads = ThreadPoolBuilder::new()
            .pool_size(causet.num_threads)
            .name_prefix("sst-importer")
            .after_spacelike(move |_| edb_alloc::add_thread_memory_accessor())
            .before_stop(move |_| edb_alloc::remove_thread_memory_accessor())
            .create()
            .unwrap();
        let switcher = ImportModeSwitcher::new(&causet, &threads, engine.clone());
        ImportSSTService {
            causet,
            router,
            engine,
            threads,
            importer,
            switcher,
            limiter: Limiter::new(INFINITY),
            security_mgr,
        }
    }
}

impl<Router: VioletaBftStoreRouter<LmdbEngine>> ImportSst for ImportSSTService<Router> {
    fn switch_mode(
        &mut self,
        ctx: RpcContext<'_>,
        req: SwitchModeRequest,
        sink: UnarySink<SwitchModeResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let label = "switch_mode";
        let timer = Instant::now_coarse();

        let res = {
            fn mf(causet: &str, name: &str, v: f64) {
                CONFIG_LMDB_GAUGE.with_label_values(&[causet, name]).set(v);
            }

            match req.get_mode() {
                SwitchMode::Normal => self.switcher.enter_normal_mode(mf),
                SwitchMode::Import => self.switcher.enter_import_mode(mf),
            }
        };
        match res {
            Ok(_) => info!("switch mode"; "mode" => ?req.get_mode()),
            Err(ref e) => error!(%e; "switch mode failed"; "mode" => ?req.get_mode(),),
        }

        let task = async move {
            let res = Ok(SwitchModeResponse::default());
            lightlike_rpc_response!(res, sink, label, timer);
        };
        ctx.spawn(task);
    }

    /// Receive SST from client and save the file for later ingesting.
    fn upload(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<UploadRequest>,
        sink: ClientStreamingSink<UploadResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let label = "upload";
        let timer = Instant::now_coarse();
        let import = self.importer.clone();
        let (rx, buf_driver) = create_stream_with_buffer(stream, self.causet.stream_channel_window);
        let mut rx = rx.map_err(Error::from);

        let handle_task = async move {
            let res = async move {
                let first_Soliton = rx.try_next().await?;
                let meta = match first_Soliton {
                    Some(ref Soliton) if Soliton.has_meta() => Soliton.get_meta(),
                    _ => return Err(Error::InvalidSoliton),
                };
                let file = import.create(meta)?;
                let mut file = rx
                    .try_fold(file, |mut file, Soliton| async move {
                        let spacelike = Instant::now_coarse();
                        let data = Soliton.get_data();
                        if data.is_empty() {
                            return Err(Error::InvalidSoliton);
                        }
                        file.applightlike(data)?;
                        IMPORT_UPLOAD_Soliton_BYTES.observe(data.len() as f64);
                        IMPORT_UPLOAD_Soliton_DURATION.observe(spacelike.elapsed_secs());
                        Ok(file)
                    })
                    .await?;
                file.finish().map(|_| UploadResponse::default())
            }
            .await;
            lightlike_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(buf_driver);
        self.threads.spawn_ok(handle_task);
    }

    /// Downloads the file and performs key-rewrite for later ingesting.
    fn download(
        &mut self,
        ctx: RpcContext<'_>,
        req: DownloadRequest,
        sink: UnarySink<DownloadResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let label = "download";
        let timer = Instant::now_coarse();
        let importer = Arc::clone(&self.importer);
        let limiter = self.limiter.clone();
        let sst_writer = <LmdbEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&self.engine)
            .set_causet(name_to_causet(req.get_sst().get_causet_name()).unwrap())
            .build(self.importer.get_path(req.get_sst()).to_str().unwrap())
            .unwrap();

        let handle_task = async move {
            // FIXME: download() should be an async fn, to allow BR to cancel
            // a download task.
            // Unfortunately, this currently can't happen because the S3Storage
            // is not lightlike + Sync. See the documentation of S3Storage for reason.
            let res = importer.download::<LmdbEngine>(
                req.get_sst(),
                req.get_causet_storage_backlightlike(),
                req.get_name(),
                req.get_rewrite_rule(),
                limiter,
                sst_writer,
            );
            let mut resp = DownloadResponse::default();
            match res {
                Ok(cone) => match cone {
                    Some(r) => resp.set_cone(r),
                    None => resp.set_is_empty(true),
                },
                Err(e) => resp.set_error(e.into()),
            }
            let resp = Ok(resp);
            lightlike_rpc_response!(resp, sink, label, timer);
        };

        self.threads.spawn_ok(handle_task);
    }

    /// Ingest the file by lightlikeing a violetabft command to violetabftstore.
    ///
    /// If the ingestion fails because the brane is not found or the epoch does
    /// not match, the remaining files will eventually be cleaned up by
    /// CleanupSSTWorker.
    fn ingest(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: IngestRequest,
        sink: UnarySink<IngestResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let label = "ingest";
        let timer = Instant::now_coarse();

        if self.switcher.get_mode() == SwitchMode::Normal
            && self
                .engine
                .ingest_maybe_slowdown_writes(Causet_DEFAULT)
                .expect("causet")
        {
            let err = "too many sst files are ingesting";
            let mut server_is_busy_err = error_timeshare::ServerIsBusy::default();
            server_is_busy_err.set_reason(err.to_string());
            let mut error_timeshare = error_timeshare::Error::default();
            error_timeshare.set_message(err.to_string());
            error_timeshare.set_server_is_busy(server_is_busy_err);
            let mut resp = IngestResponse::default();
            resp.set_error(error_timeshare);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("lightlike rpc failed"; "err" => %e)),
            );
            return;
        }
        // Make ingest command.
        let mut ingest = Request::default();
        ingest.set_cmd_type(CmdType::IngestSst);
        ingest.mut_ingest_sst().set_sst(req.take_sst());
        let mut context = req.take_context();
        let mut header = VioletaBftRequestHeader::default();
        header.set_peer(context.take_peer());
        header.set_brane_id(context.get_brane_id());
        header.set_brane_epoch(context.take_brane_epoch());
        let mut cmd = VioletaBftCmdRequest::default();
        cmd.set_header(header);
        cmd.mut_requests().push(ingest);

        let (cb, future) = paired_future_callback();
        if let Err(e) = self.router.lightlike_command(cmd, Callback::Write(cb)) {
            let mut resp = IngestResponse::default();
            resp.set_error(e.into());
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("lightlike rpc failed"; "err" => %e)),
            );
            return;
        }

        let ctx_task = async move {
            let res = future.await.map_err(Error::from);
            let res = match res {
                Ok(mut res) => {
                    let mut resp = IngestResponse::default();
                    let mut header = res.response.take_header();
                    if header.has_error() {
                        resp.set_error(header.take_error());
                    }
                    Ok(resp)
                }
                Err(e) => Err(e),
            };
            lightlike_rpc_response!(res, sink, label, timer);
        };
        ctx.spawn(ctx_task);
    }

    fn compact(
        &mut self,
        ctx: RpcContext<'_>,
        req: CompactRequest,
        sink: UnarySink<CompactResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let label = "compact";
        let timer = Instant::now_coarse();
        let engine = self.engine.clone();

        let handle_task = async move {
            let (spacelike, lightlike) = if !req.has_cone() {
                (None, None)
            } else {
                (
                    Some(req.get_cone().get_spacelike()),
                    Some(req.get_cone().get_lightlike()),
                )
            };
            let output_level = if req.get_output_level() == -1 {
                None
            } else {
                Some(req.get_output_level())
            };

            let res = engine.compact_files_in_cone(spacelike, lightlike, output_level);
            match res {
                Ok(_) => info!(
                    "compact files in cone";
                    "spacelike" => spacelike.map(log_wrappers::Key),
                    "lightlike" => lightlike.map(log_wrappers::Key),
                    "output_level" => ?output_level, "takes" => ?timer.elapsed()
                ),
                Err(ref e) => error!(%e;
                    "compact files in cone failed";
                    "spacelike" => spacelike.map(log_wrappers::Key),
                    "lightlike" => lightlike.map(log_wrappers::Key),
                    "output_level" => ?output_level,
                ),
            }
            let res = engine.compact_files_in_cone(spacelike, lightlike, output_level);
            match res {
                Ok(_) => info!(
                    "compact files in cone";
                    "spacelike" => spacelike.map(log_wrappers::Key),
                    "lightlike" => lightlike.map(log_wrappers::Key),
                    "output_level" => ?output_level, "takes" => ?timer.elapsed()
                ),
                Err(ref e) => error!(
                    "compact files in cone failed";
                    "spacelike" => spacelike.map(log_wrappers::Key),
                    "lightlike" => lightlike.map(log_wrappers::Key),
                    "output_level" => ?output_level, "err" => %e
                ),
            }
            let res = res
                .map_err(|e| Error::Engine(box_err!(e)))
                .map(|_| CompactResponse::default());
            lightlike_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(handle_task);
    }

    fn set_download_speed_limit(
        &mut self,
        ctx: RpcContext<'_>,
        req: SetDownloadSpeedLimitRequest,
        sink: UnarySink<SetDownloadSpeedLimitResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let label = "set_download_speed_limit";
        let timer = Instant::now_coarse();

        let speed_limit = req.get_speed_limit();
        self.limiter.set_speed_limit(if speed_limit > 0 {
            speed_limit as f64
        } else {
            INFINITY
        });

        let ctx_task = async move {
            let res = Ok(SetDownloadSpeedLimitResponse::default());
            lightlike_rpc_response!(res, sink, label, timer);
        };

        ctx.spawn(ctx_task);
    }

    fn write(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<WriteRequest>,
        sink: ClientStreamingSink<WriteResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let label = "write";
        let timer = Instant::now_coarse();
        let import = self.importer.clone();
        let engine = self.engine.clone();
        let (rx, buf_driver) = create_stream_with_buffer(stream, self.causet.stream_channel_window);
        let mut rx = rx.map_err(Error::from);

        let handle_task = async move {
            let res = async move {
                let first_req = rx.try_next().await?;
                let meta = match first_req {
                    Some(r) => match r.Soliton {
                        Some(Soliton::Meta(m)) => m,
                        _ => return Err(Error::InvalidSoliton),
                    },
                    _ => return Err(Error::InvalidSoliton),
                };

                let name = import.get_path(&meta);

                let default = <LmdbEngine as SstExt>::SstWriterBuilder::new()
                    .set_in_memory(true)
                    .set_db(&engine)
                    .set_causet(Causet_DEFAULT)
                    .build(&name.to_str().unwrap())?;
                let write = <LmdbEngine as SstExt>::SstWriterBuilder::new()
                    .set_in_memory(true)
                    .set_db(&engine)
                    .set_causet(Causet_WRITE)
                    .build(&name.to_str().unwrap())?;
                let writer = match import.new_writer::<LmdbEngine>(default, write, meta) {
                    Ok(w) => w,
                    Err(e) => {
                        error!("build writer failed {:?}", e);
                        return Err(Error::InvalidSoliton);
                    }
                };
                let writer = rx
                    .try_fold(writer, |mut writer, req| async move {
                        let spacelike = Instant::now_coarse();
                        let batch = match req.Soliton {
                            Some(Soliton::Batch(b)) => b,
                            _ => return Err(Error::InvalidSoliton),
                        };
                        writer.write(batch)?;
                        IMPORT_WRITE_Soliton_DURATION.observe(spacelike.elapsed_secs());
                        Ok(writer)
                    })
                    .await?;

                writer.finish().map(|metas| {
                    let mut resp = WriteResponse::default();
                    resp.set_metas(metas.into());
                    resp
                })
            }
            .await;
            lightlike_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(buf_driver);
        self.threads.spawn_ok(handle_task);
    }
}
