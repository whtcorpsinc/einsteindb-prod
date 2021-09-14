// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! External causet_storage support.
//!
//! This crate define an abstraction of external causet_storage. Currently, it
//! supports local causet_storage.

#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate edb_alloc;

use std::io;
use std::marker::Unpin;
use std::path::Path;
use std::sync::Arc;

use futures_io::AsyncRead;
#[causet(feature = "protobuf-codec")]
use ekvproto::backup::StorageBacklightlike_oneof_backlightlike as Backlightlike;
#[causet(feature = "prost-codec")]
use ekvproto::backup::{causet_storage_backlightlike::Backlightlike, Local};
use ekvproto::backup::{Gcs, Noop, StorageBacklightlike, S3};

mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
mod s3;
pub use s3::S3Storage;
mod gcs;
pub use gcs::GCSStorage;
mod util;
pub use util::block_on_external_io;

pub const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

/// Create a new causet_storage from the given causet_storage backlightlike description.
pub fn create_causet_storage(backlightlike: &StorageBacklightlike) -> io::Result<Arc<dyn ExternalStorage>> {
    match &backlightlike.backlightlike {
        Some(Backlightlike::Local(local)) => {
            let p = Path::new(&local.path);
            LocalStorage::new(p).map(|s| Arc::new(s) as _)
        }
        Some(Backlightlike::Noop(_)) => Ok(Arc::new(NoopStorage::default()) as _),
        Some(Backlightlike::S3(config)) => S3Storage::new(config).map(|s| Arc::new(s) as _),
        Some(Backlightlike::Gcs(config)) => GCSStorage::new(config).map(|s| Arc::new(s) as _),
        _ => {
            let u = url_of_backlightlike(backlightlike);
            error!("unknown causet_storage"; "scheme" => u.scheme());
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unknown causet_storage {}", u),
            ))
        }
    }
}

/// Formats the causet_storage backlightlike as a URL.
pub fn url_of_backlightlike(backlightlike: &StorageBacklightlike) -> url::Url {
    let mut u = url::Url::parse("unknown:///").unwrap();
    match &backlightlike.backlightlike {
        Some(Backlightlike::Local(local)) => {
            u.set_scheme("local").unwrap();
            u.set_path(&local.path);
        }
        Some(Backlightlike::Noop(_)) => {
            u.set_scheme("noop").unwrap();
        }
        Some(Backlightlike::S3(s3)) => {
            u.set_scheme("s3").unwrap();
            if let Err(e) = u.set_host(Some(&s3.bucket)) {
                warn!("ignoring invalid S3 bucket name"; "bucket" => &s3.bucket, "error" => %e);
            }
            u.set_path(s3.get_prefix());
        }
        Some(Backlightlike::Gcs(gcs)) => {
            u.set_scheme("gcs").unwrap();
            if let Err(e) = u.set_host(Some(&gcs.bucket)) {
                warn!("ignoring invalid GCS bucket name"; "bucket" => &gcs.bucket, "error" => %e);
            }
            u.set_path(gcs.get_prefix());
        }
        None => {}
    }
    u
}

/// Creates a local `StorageBacklightlike` to the given path.
pub fn make_local_backlightlike(path: &Path) -> StorageBacklightlike {
    let path = path.display().to_string();
    #[causet(feature = "prost-codec")]
    {
        StorageBacklightlike {
            backlightlike: Some(Backlightlike::Local(Local { path })),
        }
    }
    #[causet(feature = "protobuf-codec")]
    {
        let mut backlightlike = StorageBacklightlike::default();
        backlightlike.mut_local().set_path(path);
        backlightlike
    }
}

/// Creates a noop `StorageBacklightlike`.
pub fn make_noop_backlightlike() -> StorageBacklightlike {
    let noop = Noop::default();
    #[causet(feature = "prost-codec")]
    {
        StorageBacklightlike {
            backlightlike: Some(Backlightlike::Noop(noop)),
        }
    }
    #[causet(feature = "protobuf-codec")]
    {
        let mut backlightlike = StorageBacklightlike::default();
        backlightlike.set_noop(noop);
        backlightlike
    }
}

// Creates a S3 `StorageBacklightlike`
pub fn make_s3_backlightlike(config: S3) -> StorageBacklightlike {
    #[causet(feature = "prost-codec")]
    {
        StorageBacklightlike {
            backlightlike: Some(Backlightlike::S3(config)),
        }
    }
    #[causet(feature = "protobuf-codec")]
    {
        let mut backlightlike = StorageBacklightlike::default();
        backlightlike.set_s3(config);
        backlightlike
    }
}

// Creates a GCS `StorageBacklightlike`
pub fn make_gcs_backlightlike(config: Gcs) -> StorageBacklightlike {
    #[causet(feature = "prost-codec")]
    {
        StorageBacklightlike {
            backlightlike: Some(Backlightlike::Gcs(config)),
        }
    }
    #[causet(feature = "protobuf-codec")]
    {
        let mut backlightlike = StorageBacklightlike::default();
        backlightlike.set_gcs(config);
        backlightlike
    }
}

/// An abstraction of an external causet_storage.
// TODO: these should all be returning a future (i.e. async fn).
pub trait ExternalStorage: 'static {
    /// Write all contents of the read to the given path.
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + lightlike + Unpin>,
        content_length: u64,
    ) -> io::Result<()>;
    /// Read all contents of the given path.
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_>;
}

impl ExternalStorage for Arc<dyn ExternalStorage> {
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + lightlike + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        (**self).write(name, reader, content_length)
    }
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        (**self).read(name)
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_causet_storage() {
        let backlightlike = make_local_backlightlike(Path::new("/tmp/a"));
        create_causet_storage(&backlightlike).unwrap();

        let backlightlike = make_noop_backlightlike();
        create_causet_storage(&backlightlike).unwrap();

        let backlightlike = StorageBacklightlike::default();
        assert!(create_causet_storage(&backlightlike).is_err());
    }

    #[test]
    fn test_url_of_backlightlike() {
        use ekvproto::backup::S3;

        let backlightlike = make_local_backlightlike(Path::new("/tmp/a"));
        assert_eq!(url_of_backlightlike(&backlightlike).to_string(), "local:///tmp/a");

        let backlightlike = make_noop_backlightlike();
        assert_eq!(url_of_backlightlike(&backlightlike).to_string(), "noop:///");

        let mut backlightlike = StorageBacklightlike::default();
        backlightlike.backlightlike = Some(Backlightlike::S3(S3 {
            bucket: "bucket".to_owned(),
            prefix: "/backup 01/prefix/".to_owned(),
            lightlikepoint: "http://lightlikepoint.com".to_owned(),
            // ^ only 'bucket' and 'prefix' should be visible in url_of_backlightlike()
            ..S3::default()
        }));
        assert_eq!(
            url_of_backlightlike(&backlightlike).to_string(),
            "s3://bucket/backup%2001/prefix/"
        );

        backlightlike.backlightlike = Some(Backlightlike::Gcs(Gcs {
            bucket: "bucket".to_owned(),
            prefix: "/backup 02/prefix/".to_owned(),
            lightlikepoint: "http://lightlikepoint.com".to_owned(),
            // ^ only 'bucket' and 'prefix' should be visible in url_of_backlightlike()
            ..Gcs::default()
        }));
        assert_eq!(
            url_of_backlightlike(&backlightlike).to_string(),
            "gcs://bucket/backup%2002/prefix/"
        );
    }
}
