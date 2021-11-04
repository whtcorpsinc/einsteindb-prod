// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use super::ErrorCodeExt;
use ekvproto::error_timeshare;

define_error_codes!(
    "KV-VioletaBftstore-",

    ENTRY_TOO_LARGE => ("EntryTooLarge", "", ""),
    NOT_LEADER => ("NotLeader", "", ""),
    STORE_NOT_MATCH => ("StoreNotMatch", "", ""),
    REGION_NOT_FOUND => ("BraneNotFound", "", ""),
    REGION_NOT_INITIALIZED => ("BraneNotInitialized", "", ""),
    KEY_NOT_IN_REGION => ("KeyNotInBrane", "", ""),
    STALE_COMMAND => ("StaleCommand", "", ""),
    TRANSPORT => ("Transport", "", ""),
    INTERLOCK => ("Interlock", "", ""),
    IO => ("IO", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    ADDR_PARSE => ("AddressParse", "", ""),
    TIMEOUT => ("Timeout", "", ""),
    EPOCH_NOT_MATCH => ("EpochNotMatch", "", ""),
    UNKNOWN => ("Unknown", "", ""),
    SERVER_IS_BUSY => ("ServerIsBusy", "", ""),

    SNAP_ABORT => ("SnapAbort", "", ""),
    SNAP_TOO_MANY => ("SnapTooMany", "", ""),
    SNAP_UNKNOWN => ("SnapUnknown", "", "")
);

impl ErrorCodeExt for error_timeshare::Error {
    fn error_code(&self) -> ErrorCode {
        if self.has_not_leader() {
            NOT_LEADER
        } else if self.has_brane_not_found() {
            REGION_NOT_FOUND
        } else if self.has_key_not_in_brane() {
            KEY_NOT_IN_REGION
        } else if self.has_epoch_not_match() {
            EPOCH_NOT_MATCH
        } else if self.has_server_is_busy() {
            SERVER_IS_BUSY
        } else if self.has_stale_command() {
            STALE_COMMAND
        } else if self.has_store_not_match() {
            STORE_NOT_MATCH
        } else if self.has_violetabft_entry_too_large() {
            ENTRY_TOO_LARGE
        } else {
            UNKNOWN
        }
    }
}
