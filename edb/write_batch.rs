// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::options::WriteOptions;

pub trait WriteBatchExt: Sized {
    type WriteBatch: WriteBatch<Self>;
    /// `WriteBatchVec` is used for `multi_batch_write` of LmdbEngine and other Engine could also
    /// implement another kind of WriteBatch according to their needs.
    type WriteBatchVec: WriteBatch<Self>;

    const WRITE_BATCH_MAX_KEYS: usize;

    fn write_opt(&self, wb: &Self::WriteBatch, opts: &WriteOptions) -> Result<()>;
    fn write_vec_opt(&self, wb: &Self::WriteBatchVec, opts: &WriteOptions) -> Result<()>;
    fn support_write_batch_vec(&self) -> bool;
    fn write(&self, wb: &Self::WriteBatch) -> Result<()> {
        self.write_opt(wb, &WriteOptions::default())
    }
    fn write_batch(&self) -> Self::WriteBatch;
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch;
}

pub trait MuBlock: lightlike {
    fn data_size(&self) -> usize;
    fn count(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn should_write_to_engine(&self) -> bool;

    fn clear(&mut self);
    fn set_save_point(&mut self);
    fn pop_save_point(&mut self) -> Result<()>;
    fn rollback_to_save_point(&mut self) -> Result<()>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn put_causet(&mut self, causet: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;
    fn delete_causet(&mut self, causet: &str, key: &[u8]) -> Result<()>;

    fn delete_cone_causet(&mut self, causet: &str, begin_key: &[u8], lightlike_key: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&mut self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }
    fn put_msg_causet<M: protobuf::Message>(&mut self, causet: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_causet(causet, key, &m.write_to_bytes()?)
    }
}

pub trait WriteBatch<E: WriteBatchExt + Sized>: MuBlock {
    fn with_capacity(e: &E, cap: usize) -> Self;
    fn write_to_engine(&self, e: &E, opts: &WriteOptions) -> Result<()>;
}
