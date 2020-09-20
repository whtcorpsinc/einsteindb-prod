// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV-Engine-",

    ENGINE => ("Engine", "", ""),
    NOT_IN_RANGE => ("NotInCone", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    IO => ("IO", "", ""),
    CAUSET_NAME => ("CAUSETName", "", ""),
    CODEC => ("Codec", "", ""),
    DATALOSS => ("DataLoss", "", ""),
    DATACOMPACTED => ("DataCompacted", "", "")
);
