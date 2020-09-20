// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV-FIDel-",

    IO => ("IO", "", ""),
    CLUSTER_BOOTSTRAPPED => ("ClusterBootstraped", "", ""),
    CLUSTER_NOT_BOOTSTRAPPED => ("ClusterNotBootstraped", "", ""),
    INCOMPATIBLE => ("Imcompatible", "", ""),
    GRPC => ("gRPC", "", ""),
    REGION_NOT_FOUND => ("BraneNotFound", "", ""),
    STORE_TOMBSTONE => ("StoreTombstone", "", ""),
    UNKNOWN => ("Unknown", "", "")
);
