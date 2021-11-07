// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::*;

pub trait SyncMuBlock {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_causet(&self, causet: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_causet(&self, causet: &str, key: &[u8]) -> Result<()>;

    fn delete_cone_causet(&self, causet: &str, begin_key: &[u8], lightlike_key: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }

    fn put_msg_causet<M: protobuf::Message>(&self, causet: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_causet(causet, key, &m.write_to_bytes()?)
    }
}
