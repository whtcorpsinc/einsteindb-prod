// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

pub trait CAUSETNamesExt {
    fn causet_names(&self) -> Vec<&str>;
}
