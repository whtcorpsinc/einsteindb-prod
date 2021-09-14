// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use ekvproto::encryption_timeshare::EncryptedContent;

use super::{Backlightlike, MemAesGcmBacklightlike};
use crate::AesGcmCrypter;
use crate::{Error, Iv, Result};

pub struct FileBacklightlike {
    backlightlike: MemAesGcmBacklightlike,
}

impl FileBacklightlike {
    pub fn new(key_path: &Path) -> Result<FileBacklightlike> {
        // FileBacklightlike uses Aes256-GCM.
        let key_len = AesGcmCrypter::KEY_LEN;
        let mut file = File::open(key_path)?;
        // Check file size to avoid reading a gigantic file accidentally.
        let file_len = file.metadata()?.len() as usize;
        if file_len != key_len * 2 + 1 {
            return Err(box_err!(
                "mismatch master key file size, expected {}, actual {}.",
                key_len * 2 + 1,
                file_len
            ));
        }
        let mut content = vec![];
        let read_len = file.read_to_lightlike(&mut content)?;
        if read_len != file_len {
            return Err(box_err!(
                "mismatch master key file size read, expected {}, actual {}",
                file_len,
                read_len
            ));
        }
        if content.last() != Some(&b'\n') {
            return Err(box_err!("master key file should lightlike with newline."));
        }
        let key = hex::decode(&content[..file_len - 1])
            .map_err(|e| Error::Other(box_err!("failed to decode master key from file: {}", e)))?;
        let backlightlike = MemAesGcmBacklightlike::new(key)?;
        Ok(FileBacklightlike { backlightlike })
    }
}

impl Backlightlike for FileBacklightlike {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let iv = Iv::new_gcm();
        self.backlightlike.encrypt_content(plaintext, iv)
    }

    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        self.backlightlike.decrypt_content(content)
    }

    fn is_secure(&self) -> bool {
        true
    }
}

#[causet(test)]
mod tests {
    use hex::FromHex;
    use matches::assert_matches;
    use std::{fs::File, io::Write, path::PathBuf};
    use tempfile::TempDir;

    use super::super::metadata::MetadataKey;

    use super::*;
    use crate::*;

    fn create_key_file(val: &str) -> (PathBuf, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("key");
        let mut file = File::create(path.clone()).unwrap();
        file.write_all(format!("{}\n", val).as_bytes()).unwrap();
        (path, tmp_dir)
    }

    #[test]
    fn test_file_backlightlike_ase_256_gcm() {
        // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
        let pt = Vec::from_hex("25431587e9ecausetfc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749")
            .unwrap();
        let ct = Vec::from_hex("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980")
            .unwrap();
        let iv = Vec::from_hex("cafabd9672ca6c79a2fbdc22").unwrap();

        let (key_path, _tmp_key_dir) =
            create_key_file("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139");
        let backlightlike = FileBacklightlike::new(&key_path).unwrap();

        let iv = Iv::from_slice(iv.as_slice()).unwrap();
        let encrypted_content = backlightlike.backlightlike.encrypt_content(&pt, iv).unwrap();
        assert_eq!(encrypted_content.get_content(), ct.as_slice());
        let plaintext = backlightlike.decrypt(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);
    }

    #[test]
    fn test_file_backlightlike_authenticate() {
        let pt = vec![1u8, 2, 3];

        let (key_path, _tmp_key_dir) =
            create_key_file("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139");
        let backlightlike = FileBacklightlike::new(&key_path).unwrap();

        let encrypted_content = backlightlike.encrypt(&pt).unwrap();
        let plaintext = backlightlike.decrypt(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);

        // Must checksum mismatch
        let mut encrypted_content1 = encrypted_content.clone();
        encrypted_content1
            .mut_metadata()
            .get_mut(MetadataKey::AesGcmTag.as_str())
            .unwrap()[0] ^= 0b11111111u8;
        assert_matches!(
            backlightlike.decrypt(&encrypted_content1).unwrap_err(),
            Error::WrongMasterKey(_)
        );

        // Must checksum not found
        let mut encrypted_content2 = encrypted_content;
        encrypted_content2
            .mut_metadata()
            .remove(MetadataKey::AesGcmTag.as_str());
        assert_matches!(
            backlightlike.decrypt(&encrypted_content2).unwrap_err(),
            Error::Other(_)
        );
    }
}
