// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

define_error_codes!(
    "KV-Engine-",

    ENGINE => ("Engine", "", ""),
    NOT_IN_RANGE => ("NotInCone", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    IO => ("IO", "", ""),
    Causet_NAME => ("CausetName", "", ""),
    CODEC => ("Codec", "", ""),
    DATALOSS => ("DataLoss", "", ""),
    DATACOMPACTED => ("DataCompacted", "", "")
);
