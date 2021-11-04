// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

define_error_codes!(
    "KV-SSTImporter-",

    IO => ("Io", "", ""),
    GRPC => ("gRPC", "", ""),
    UUID => ("Uuid", "", ""),
    FUTURE => ("Future", "", ""),
    LMDB => ("Lmdb", "", ""),
    PARSE_INT_ERROR => ("ParseIntError", "", ""),
    FILE_EXISTS => ("FileExists", "", ""),
    FILE_CORRUPTED => ("FileCorrupted", "", ""),
    INVALID_SST_PATH => ("InvalidSSTPath", "",""),
    INVALID_Soliton => ("InvalidSoliton", "", ""),
    ENGINE => ("Engine", "", ""),
    CANNOT_READ_EXTERNAL_STORAGE => ("CannotReadExternalStorage", "", ""),
    WRONG_KEY_PREFIX => ("WrongKeyPrefix", "", ""),
    BAD_FORMAT => ("BadFormat", "", "")
);
