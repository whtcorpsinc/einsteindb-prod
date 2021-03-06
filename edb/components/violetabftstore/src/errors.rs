// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use std::error;
use std::io;
use std::net;
use std::result;

use crossbeam::TrySlightlikeError;
#[causet(feature = "prost-codec")]
use prost::{DecodeError, EncodeError};
use protobuf::ProtobufError;

use error_code::{self, ErrorCode, ErrorCodeExt};
use ekvproto::{errorpb, metapb};
use einsteindb_util::codec;

use super::interlock::Error as CopError;
use super::store::SnapError;

pub const VIOLETABFTSTORE_IS_BUSY: &str = "violetabftstore is busy";

/// Describes why a message is discarded.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DiscardReason {
    /// Channel is disconnected, message can't be delivered.
    Disconnected,
    /// Message is dropped due to some filter rules, usually in tests.
    Filtered,
    /// Channel runs out of capacity, message can't be delivered.
    Full,
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        VioletaBftEntryTooLarge(brane_id: u64, entry_size: u64) {
            display("violetabft entry is too large, brane {}, entry size {}", brane_id, entry_size)
        }
        StoreNotMatch(to_store_id: u64, my_store_id: u64) {
            display("to store id {}, mine {}", to_store_id, my_store_id)
        }
        BraneNotFound(brane_id: u64) {
            display("brane {} not found", brane_id)
        }
        BraneNotInitialized(brane_id: u64) {
            display("brane {} not initialized yet", brane_id)
        }
        NotLeader(brane_id: u64, leader: Option<metapb::Peer>) {
            display("peer is not leader for brane {}, leader may {:?}", brane_id, leader)
        }
        KeyNotInBrane(key: Vec<u8>, brane: metapb::Brane) {
            display("key {} is not in brane key cone [{}, {}) for brane {}",
                    hex::encode_upper(key),
                    hex::encode_upper(brane.get_spacelike_key()),
                    hex::encode_upper(brane.get_lightlike_key()),
                    brane.get_id())
        }
        Other(err: Box<dyn error::Error + Sync + Slightlike>) {
            from()
            cause(err.as_ref())
            display("{:?}", err)
        }

        // Following is for From other errors.
        Io(err: io::Error) {
            from()
            cause(err)
            display("Io {}", err)
        }
        Engine(err: engine_promises::Error) {
            from()
            display("Engine {:?}", err)
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            display("Protobuf {}", err)
        }
        #[causet(feature = "prost-codec")]
        ProstDecode(err: DecodeError) {
            cause(err)
            display("DecodeError {}", err)
        }
        #[causet(feature = "prost-codec")]
        ProstEncode(err: EncodeError) {
            cause(err)
            display("EncodeError {}", err)
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            display("Codec {}", err)
        }
        AddrParse(err: net::AddrParseError) {
            from()
            cause(err)
            display("AddrParse {}", err)
        }
        Fidel(err: fidel_client::Error) {
            from()
            cause(err)
            display("Fidel {}", err)
        }
        VioletaBft(err: violetabft::Error) {
            from()
            cause(err)
            display("VioletaBft {}", err)
        }
        Timeout(msg: String) {
            display("Timeout {}", msg)
        }
        EpochNotMatch(msg: String, new_branes: Vec<metapb::Brane>) {
            display("EpochNotMatch {}", msg)
        }
        StaleCommand {
            display("stale command")
        }
        Interlock(err: CopError) {
            from()
            cause(err)
            display("Interlock {}", err)
        }
        Transport(reason: DiscardReason) {
            display("Discard due to {:?}", reason)
        }
        Snapshot(err: SnapError) {
            from()
            cause(err)
            display("Snapshot {}", err)
        }
        SstImporter(err: sst_importer::Error) {
            from()
            cause(err)
            display("SstImporter {}", err)
        }
        Encryption(err: encryption::Error) {
            from()
            display("Encryption {}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for errorpb::Error {
    fn from(err: Error) -> errorpb::Error {
        let mut errorpb = errorpb::Error::default();
        errorpb.set_message(format!("{}", err));

        match err {
            Error::BraneNotFound(brane_id) => {
                errorpb.mut_brane_not_found().set_brane_id(brane_id);
            }
            Error::NotLeader(brane_id, leader) => {
                if let Some(leader) = leader {
                    errorpb.mut_not_leader().set_leader(leader);
                }
                errorpb.mut_not_leader().set_brane_id(brane_id);
            }
            Error::VioletaBftEntryTooLarge(brane_id, entry_size) => {
                errorpb.mut_violetabft_entry_too_large().set_brane_id(brane_id);
                errorpb
                    .mut_violetabft_entry_too_large()
                    .set_entry_size(entry_size);
            }
            Error::StoreNotMatch(to_store_id, my_store_id) => {
                errorpb
                    .mut_store_not_match()
                    .set_request_store_id(to_store_id);
                errorpb
                    .mut_store_not_match()
                    .set_actual_store_id(my_store_id);
            }
            Error::KeyNotInBrane(key, brane) => {
                errorpb.mut_key_not_in_brane().set_key(key);
                errorpb
                    .mut_key_not_in_brane()
                    .set_brane_id(brane.get_id());
                errorpb
                    .mut_key_not_in_brane()
                    .set_spacelike_key(brane.get_spacelike_key().to_vec());
                errorpb
                    .mut_key_not_in_brane()
                    .set_lightlike_key(brane.get_lightlike_key().to_vec());
            }
            Error::EpochNotMatch(_, new_branes) => {
                let mut e = errorpb::EpochNotMatch::default();
                e.set_current_branes(new_branes.into());
                errorpb.set_epoch_not_match(e);
            }
            Error::StaleCommand => {
                errorpb.set_stale_command(errorpb::StaleCommand::default());
            }
            Error::Transport(reason) if reason == DiscardReason::Full => {
                let mut server_is_busy_err = errorpb::ServerIsBusy::default();
                server_is_busy_err.set_reason(VIOLETABFTSTORE_IS_BUSY.to_owned());
                errorpb.set_server_is_busy(server_is_busy_err);
            }
            Error::Engine(engine_promises::Error::NotInCone(key, brane_id, spacelike_key, lightlike_key)) => {
                errorpb.mut_key_not_in_brane().set_key(key);
                errorpb.mut_key_not_in_brane().set_brane_id(brane_id);
                errorpb
                    .mut_key_not_in_brane()
                    .set_spacelike_key(spacelike_key.to_vec());
                errorpb
                    .mut_key_not_in_brane()
                    .set_lightlike_key(lightlike_key.to_vec());
            }
            _ => {}
        };

        errorpb
    }
}

impl<T> From<TrySlightlikeError<T>> for Error {
    #[inline]
    fn from(e: TrySlightlikeError<T>) -> Error {
        match e {
            TrySlightlikeError::Full(_) => Error::Transport(DiscardReason::Full),
            TrySlightlikeError::Disconnected(_) => Error::Transport(DiscardReason::Disconnected),
        }
    }
}

#[causet(feature = "prost-codec")]
impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Error {
        Error::ProstEncode(err.into())
    }
}

#[causet(feature = "prost-codec")]
impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Error {
        Error::ProstDecode(err.into())
    }
}

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::VioletaBftEntryTooLarge(_, _) => error_code::violetabftstore::ENTRY_TOO_LARGE,
            Error::StoreNotMatch(_, _) => error_code::violetabftstore::STORE_NOT_MATCH,
            Error::BraneNotFound(_) => error_code::violetabftstore::REGION_NOT_FOUND,
            Error::NotLeader(_, _) => error_code::violetabftstore::NOT_LEADER,
            Error::StaleCommand => error_code::violetabftstore::STALE_COMMAND,
            Error::BraneNotInitialized(_) => error_code::violetabftstore::REGION_NOT_INITIALIZED,
            Error::KeyNotInBrane(_, _) => error_code::violetabftstore::KEY_NOT_IN_REGION,
            Error::Io(_) => error_code::violetabftstore::IO,
            Error::Engine(e) => e.error_code(),
            Error::Protobuf(_) => error_code::violetabftstore::PROTOBUF,
            Error::Codec(e) => e.error_code(),
            Error::AddrParse(_) => error_code::violetabftstore::ADDR_PARSE,
            Error::Fidel(e) => e.error_code(),
            Error::VioletaBft(e) => e.error_code(),
            Error::Timeout(_) => error_code::violetabftstore::TIMEOUT,
            Error::EpochNotMatch(_, _) => error_code::violetabftstore::EPOCH_NOT_MATCH,
            Error::Interlock(e) => e.error_code(),
            Error::Transport(_) => error_code::violetabftstore::TRANSPORT,
            Error::Snapshot(e) => e.error_code(),
            Error::SstImporter(e) => e.error_code(),
            Error::Encryption(e) => e.error_code(),
            #[causet(feature = "prost-codec")]
            Error::ProstDecode(_) => error_code::violetabftstore::PROTOBUF,
            #[causet(feature = "prost-codec")]
            Error::ProstEncode(_) => error_code::violetabftstore::PROTOBUF,

            Error::Other(_) => error_code::violetabftstore::UNKNOWN,
        }
    }
}
