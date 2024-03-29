// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::io::Result;

pub trait EncryptionKeyManager: Sync + lightlike {
    fn get_file(&self, fname: &str) -> Result<FileEncryptionInfo>;
    fn new_file(&self, fname: &str) -> Result<FileEncryptionInfo>;
    fn delete_file(&self, fname: &str) -> Result<()>;
    fn link_file(&self, src_fname: &str, dst_fname: &str) -> Result<()>;
    fn rename_file(&self, src_fname: &str, dst_fname: &str) -> Result<()>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct FileEncryptionInfo {
    pub method: EncryptionMethod,
    pub key: Vec<u8>,
    pub iv: Vec<u8>,
}
impl Default for FileEncryptionInfo {
    fn default() -> Self {
        FileEncryptionInfo {
            method: EncryptionMethod::Unknown,
            key: vec![],
            iv: vec![],
        }
    }
}

impl Debug for FileEncryptionInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "FileEncryptionInfo [method={:?}, key=...<{} bytes>, iv=...<{} bytes>]",
            self.method,
            self.key.len(),
            self.iv.len()
        )
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EncryptionMethod {
    Unknown = 0,
    Plaintext = 1,
    Aes128Ctr = 2,
    Aes192Ctr = 3,
    Aes256Ctr = 4,
}
