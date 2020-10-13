// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::error;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::result;

use futures::channel::oneshot::Canceled;
use grpcio::Error as GrpcError;
use hyper::Error as HttpError;
use openssl::error::ErrorStack as OpenSSLError;
use protobuf::ProtobufError;

use super::snap::Task as SnapTask;
use crate::causetStorage::kv::Error as EngineError;
use crate::causetStorage::Error as StorageError;
use engine_promises::Error as EngineTraitError;
use fidel_client::Error as FidelError;
use violetabftstore::Error as VioletaBftServerError;
use einsteindb_util::codec::Error as CodecError;
use einsteindb_util::worker::ScheduleError;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Sync + Slightlike>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }
        // Following is for From other errors.
        Io(err: IoError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            display("{}", err)
        }
        Grpc(err: GrpcError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        Codec(err: CodecError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        AddrParse(err: AddrParseError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        VioletaBftServer(err: VioletaBftServerError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        Engine(err: EngineError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        EngineTrait(err: EngineTraitError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        CausetStorage(err: StorageError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        Fidel(err: FidelError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        SnapWorkerStopped(err: ScheduleError<SnapTask>) {
            from()
            display("{:?}", err)
        }
        Sink {
            display("failed to poll from mpsc receiver")
        }
        RecvError(err: Canceled) {
            from()
            cause(err)
            display("{:?}", err)
        }
        Http(err: HttpError) {
            from()
            cause(err)
            display("{:?}", err)
        }
        OpenSSL(err: OpenSSLError) {
            from()
            cause(err)
            display("{:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
