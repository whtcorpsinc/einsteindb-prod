// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

define_error_codes!(
    "KV-Interlock-",

    LOCKED => ("Locked", "", ""),
    DEADLINE_EXCEEDED => ("DeadlineExceeded", "", ""),
    MAX_PENDING_TASKS_EXCEEDED => ("MaxPlightlikeingTasksExceeded", "", ""),

    INVALID_DATA_TYPE => ("InvalidDataType", "", ""),
    ENCODING => ("Encoding", "", ""),
    PrimaryCauset_OFFSET => ("PrimaryCausetOffset", "", ""),
    UNKNOWN_SIGNATURE => ("UnknownSignature", "", ""),
    EVAL => ("Eval", "", ""),

    STORAGE_ERROR => ("StorageError", "", ""),
    INVALID_CHARACTER_STRING => ("InvalidCharacterString", "", "")
);
