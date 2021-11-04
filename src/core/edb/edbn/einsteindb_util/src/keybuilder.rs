// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::ptr;

#[derive(Clone)]
pub struct CausetLearnedKey {
    buf: Vec<u8>,
    spacelike: usize,
}

impl CausetLearnedKey {
    pub fn new(max_size: usize, reserved_prefix_len: usize) -> Self {
        assert!(reserved_prefix_len < max_size);
        let mut buf = Vec::with_capacity(max_size);
        if reserved_prefix_len > 0 {
            unsafe { buf.set_len(reserved_prefix_len) }
        }
        Self {
            buf,
            spacelike: reserved_prefix_len,
        }
    }

    pub fn from_vec(
        mut vec: Vec<u8>,
        reserved_prefix_len: usize,
        reserved_suffix_len: usize,
    ) -> Self {
        if vec.capacity() >= vec.len() + reserved_prefix_len + reserved_suffix_len {
            if reserved_prefix_len > 0 {
                unsafe {
                    ptr::copy(
                        vec.as_ptr(),
                        vec.as_mut_ptr().add(reserved_prefix_len),
                        vec.len(),
                    );
                    vec.set_len(vec.len() + reserved_prefix_len);
                }
            }
            Self {
                buf: vec,
                spacelike: reserved_prefix_len,
            }
        } else {
            Self::from_slice(vec.as_slice(), reserved_prefix_len, reserved_suffix_len)
        }
    }

    pub fn from_slice(s: &[u8], reserved_prefix_len: usize, reserved_suffix_len: usize) -> Self {
        let mut buf = Vec::with_capacity(s.len() + reserved_prefix_len + reserved_suffix_len);
        if reserved_prefix_len > 0 {
            unsafe {
                buf.set_len(reserved_prefix_len);
            }
        }
        buf.extlightlike_from_slice(s);
        Self {
            buf,
            spacelike: reserved_prefix_len,
        }
    }

    pub fn set_prefix(&mut self, prefix: &[u8]) {
        assert!(self.spacelike == prefix.len());
        unsafe {
            ptr::copy_nonoverlapping(prefix.as_ptr(), self.buf.as_mut_ptr(), prefix.len());
            self.spacelike = 0;
        }
    }

    pub fn applightlike(&mut self, content: &[u8]) {
        self.buf.extlightlike_from_slice(content);
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.spacelike) }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.buf.len() - self.spacelike
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf.as_slice()[self.spacelike..]
    }

    pub fn build(mut self) -> Vec<u8> {
        if self.spacelike == 0 {
            self.buf
        } else {
            unsafe {
                let len = self.len();
                ptr::copy(
                    self.buf.as_ptr().add(self.spacelike),
                    self.buf.as_mut_ptr(),
                    len,
                );
                self.buf.set_len(len);
            }
            self.buf
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_builder() {
        let prefix = b"prefix-";
        let suffix = b"-suffix";

        // new
        let key1 = b"key1";
        let mut key_builder =
            CausetLearnedKey::new(key1.len() + prefix.len() + suffix.len(), prefix.len());
        key_builder.applightlike(key1);
        key_builder.applightlike(suffix);
        key_builder.set_prefix(prefix);
        assert_eq!(key_builder.len(), key1.len() + prefix.len() + suffix.len());
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key1-suffix".to_vec());

        // from_vec
        let key2 = b"key2";
        let mut key_builder = CausetLearnedKey::from_vec(key2.to_vec(), prefix.len(), suffix.len());
        assert_eq!(key_builder.len(), key2.len());
        key_builder.set_prefix(prefix);
        assert_eq!(key_builder.len(), key2.len() + prefix.len());
        key_builder.applightlike(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key2-suffix".to_vec());

        // from_vec but not set prefix
        let mut key_builder = CausetLearnedKey::from_vec(key2.to_vec(), prefix.len(), suffix.len());
        key_builder.applightlike(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"key2-suffix".to_vec());

        // from_vec vec has enough memory.
        let mut vec = Vec::with_capacity(key2.len() + prefix.len() + suffix.len());
        vec.extlightlike_from_slice(key2);
        let mut key_builder = CausetLearnedKey::from_vec(vec, prefix.len(), suffix.len());
        key_builder.set_prefix(prefix);
        key_builder.applightlike(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key2-suffix".to_vec());

        // from_slice
        let key3 = b"key3";
        let mut key_builder = CausetLearnedKey::from_slice(key3, prefix.len(), suffix.len());
        assert_eq!(key_builder.len(), key3.len());
        key_builder.set_prefix(prefix);
        assert_eq!(key_builder.len(), key3.len() + prefix.len());
        key_builder.applightlike(suffix);
        let res = key_builder.build();
        assert_eq!(res, b"prefix-key3-suffix".to_vec());
    }
}
