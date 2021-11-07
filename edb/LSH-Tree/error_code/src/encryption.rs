// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

define_error_codes!(
    "KV-Encryption-",

    ROCKS => ("Lmdb", "", ""),
    IO => ("IO", "", ""),
    CRYPTER => ("Crypter", "", ""),
    PROTO => ("Proto", "", ""),
    UNKNOWN_ENCRYPTION => ("UnknownEncryption", "", ""),
    WRONG_MASTER_KEY => ("WrongMasterKey", "", ""),
    BOTH_MASTER_KEY_FAIL => ("BothMasterKeyFail", "", "")
);
