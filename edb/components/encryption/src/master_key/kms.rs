// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::{self, TryFutureExt};
use ekvproto::encryption_timeshare::EncryptedContent;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::HttpClient;
use rusoto_core::RusotoError;
use rusoto_kms::DecryptError;
use rusoto_kms::{DecryptRequest, GenerateDataKeyRequest, Kms, KmsClient};
use tokio::runtime::{Builder, Runtime};

use super::{metadata::MetadataKey, Backlightlike, MemAesGcmBacklightlike};
use crate::config::KmsConfig;
use crate::crypter::Iv;
use crate::{Error, Result};
use rusoto_util::new_client;

const AWS_KMS_DATA_KEY_SPEC: &str = "AES_256";
const AWS_KMS_VENDOR_NAME: &[u8] = b"AWS";

struct AwsKms {
    client: KmsClient,
    current_key_id: String,
    runtime: Runtime,
    // The current implementation (rosoto 0.43.0 + hyper 0.13.3) is not `lightlike`
    // in practical. See more https://github.com/edb/edb/issues/7236.
    // FIXME: remove it.
    _not_lightlike: PhantomData<*const ()>,
}

impl AwsKms {
    fn with_request_dispatcher<D>(config: &KmsConfig, dispatcher: D) -> Result<AwsKms>
    where
        D: DispatchSignedRequest + lightlike + Sync + 'static,
    {
        Self::check_config(config)?;

        // Create and run the client in the same thread.
        let client = new_client!(KmsClient, config, dispatcher);
        // Basic interlock_semaphore executes futures in the current thread.
        let runtime = Builder::new()
            .basic_interlock_semaphore()
            .thread_name("kms-runtime")
            .core_threads(1)
            .enable_all()
            .build()?;

        Ok(AwsKms {
            client,
            current_key_id: config.key_id.clone(),
            runtime,
            _not_lightlike: PhantomData::default(),
        })
    }

    fn check_config(config: &KmsConfig) -> Result<()> {
        if config.key_id.is_empty() {
            return Err(box_err!("KMS key id can not be empty"));
        }
        Ok(())
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is possible that
    // a wrong master key has been used, or other error otherwise.
    fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let decrypt_request = DecryptRequest {
            ciphertext_blob: ciphertext.to_vec().into(),
            // Use default algorithm SYMMETRIC_DEFAULT.
            encryption_algorithm: None,
            // Use key_id encoded in ciphertext.
            key_id: Some(self.current_key_id.clone()),
            // Encryption context and grant tokens are not used.
            encryption_context: None,
            grant_tokens: None,
        };
        let runtime = &mut self.runtime;
        let client = &self.client;
        let decrypt_response = retry(runtime, || {
            client.decrypt(decrypt_request.clone()).map_err(|e| {
                if let RusotoError::Service(DecryptError::IncorrectKey(e)) = e {
                    Error::WrongMasterKey(e.into())
                } else {
                    // To keep it simple, retry all errors, even though only
                    // some of them are retriable.
                    Error::Other(e.into())
                }
            })
        })?;
        let plaintext = decrypt_response.plaintext.unwrap().as_ref().to_vec();
        Ok(plaintext)
    }

    fn generate_data_key(&mut self) -> Result<(Vec<u8>, Vec<u8>)> {
        let generate_request = GenerateDataKeyRequest {
            encryption_context: None,
            grant_tokens: None,
            key_id: self.current_key_id.clone(),
            key_spec: Some(AWS_KMS_DATA_KEY_SPEC.to_owned()),
            number_of_bytes: None,
        };
        let runtime = &mut self.runtime;
        let client = &self.client;
        let generate_response = retry(runtime, || {
            client
                .generate_data_key(generate_request.clone())
                .map_err(|e| Error::Other(e.into()))
        })
        .unwrap();
        let ciphertext_key = generate_response.ciphertext_blob.unwrap().as_ref().to_vec();
        let plaintext_key = generate_response.plaintext.unwrap().as_ref().to_vec();
        Ok((ciphertext_key, plaintext_key))
    }
}

fn retry<T, U, F>(runtime: &mut Runtime, mut func: F) -> Result<T>
where
    F: FnMut() -> U,
    U: Future<Output = Result<T>> + std::marker::Unpin,
{
    let retry_limit = 6;
    let timeout_duration = Duration::from_secs(10);
    let mut last_err = None;
    for _ in 0..retry_limit {
        let fut = func();

        match runtime.block_on(async move {
            let timeout = tokio::time::delay_for(timeout_duration);
            future::select(fut, timeout).await
        }) {
            future::Either::Left((Ok(resp), _)) => return Ok(resp),
            future::Either::Left((Err(e), _)) => {
                error!("kms request failed"; "error"=>?e);
                if let Error::WrongMasterKey(e) = e {
                    return Err(Error::WrongMasterKey(e));
                }
                last_err = Some(e);
            }
            future::Either::Right((_, _)) => {
                error!("kms request timeout"; "timeout" => ?timeout_duration);
            }
        }
    }
    Err(Error::Other(box_err!("{:?}", last_err)))
}

struct Inner {
    config: KmsConfig,
    backlightlike: Option<MemAesGcmBacklightlike>,
    cached_ciphertext_key: Vec<u8>,
}

impl Inner {
    fn maybe_fidelio_backlightlike(&mut self, ciphertext_key: Option<&Vec<u8>>) -> Result<()> {
        let http_dispatcher = HttpClient::new().unwrap();
        self.maybe_fidelio_backlightlike_with(ciphertext_key, http_dispatcher)
    }

    fn maybe_fidelio_backlightlike_with<D>(
        &mut self,
        ciphertext_key: Option<&Vec<u8>>,
        dispatcher: D,
    ) -> Result<()>
    where
        D: DispatchSignedRequest + lightlike + Sync + 'static,
    {
        if self.backlightlike.is_some()
            && ciphertext_key.map_or(true, |key| *key == self.cached_ciphertext_key)
        {
            return Ok(());
        }

        let mut kms = AwsKms::with_request_dispatcher(&self.config, dispatcher)?;
        let key = if let Some(ciphertext_key) = ciphertext_key {
            self.cached_ciphertext_key = ciphertext_key.to_owned();
            kms.decrypt(ciphertext_key)?
        } else {
            let (ciphertext_key, plaintext_key) = kms.generate_data_key()?;
            self.cached_ciphertext_key = ciphertext_key;
            plaintext_key
        };
        if self.cached_ciphertext_key == key {
            panic!(
                "ciphertext key should not be the same as master key, \
                otherwise it leaks master key!"
            );
        }

        // Always use AES 256 for encrypting master key.
        self.backlightlike = Some(MemAesGcmBacklightlike::new(key)?);
        Ok(())
    }
}

pub struct KmsBacklightlike {
    inner: Mutex<Inner>,
}

impl KmsBacklightlike {
    pub fn new(config: KmsConfig) -> Result<KmsBacklightlike> {
        let inner = Inner {
            backlightlike: None,
            config,
            cached_ciphertext_key: Vec::new(),
        };

        Ok(KmsBacklightlike {
            inner: Mutex::new(inner),
        })
    }

    fn encrypt_content(&self, plaintext: &[u8], iv: Iv) -> Result<EncryptedContent> {
        let mut inner = self.inner.dagger().unwrap();
        inner.maybe_fidelio_backlightlike(None)?;
        let mut content = inner
            .backlightlike
            .as_ref()
            .unwrap()
            .encrypt_content(plaintext, iv)?;

        // Set extra metadata for KmsBacklightlike.
        // For now, we only support AWS.
        content.metadata.insert(
            MetadataKey::KmsVlightlikeor.as_str().to_owned(),
            AWS_KMS_VENDOR_NAME.to_vec(),
        );
        if inner.cached_ciphertext_key.is_empty() {
            return Err(box_err!("KMS ciphertext key not found"));
        }
        content.metadata.insert(
            MetadataKey::KmsCiphertextKey.as_str().to_owned(),
            inner.cached_ciphertext_key.clone(),
        );

        Ok(content)
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is possible that
    // a wrong master key has been used, or other error otherwise.
    fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        match content.metadata.get(MetadataKey::KmsVlightlikeor.as_str()) {
            // For now, we only support AWS.
            Some(val) if val.as_slice() == AWS_KMS_VENDOR_NAME => (),
            None => {
                return Err(
                    // If vlightlikeer is missing in metadata, it could be the encrypted content is invalid
                    // or corrupted, but it is also possible that the content is encrypted using the
                    // FileBacklightlike. Return WrongMasterKey anyway.
                    Error::WrongMasterKey(box_err!("missing KMS vlightlikeor")),
                );
            }
            other => {
                return Err(box_err!(
                    "KMS vlightlikeor mismatch expect {:?} got {:?}",
                    AWS_KMS_VENDOR_NAME,
                    other
                ))
            }
        }

        let mut inner = self.inner.dagger().unwrap();
        let ciphertext_key = content.metadata.get(MetadataKey::KmsCiphertextKey.as_str());
        if ciphertext_key.is_none() {
            return Err(box_err!("KMS ciphertext key not found"));
        }
        inner.maybe_fidelio_backlightlike(ciphertext_key)?;
        inner.backlightlike.as_ref().unwrap().decrypt_content(content)
    }
}

impl Backlightlike for KmsBacklightlike {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        self.encrypt_content(plaintext, Iv::new_gcm())
    }

    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        self.decrypt_content(content)
    }

    fn is_secure(&self) -> bool {
        true
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use hex::FromHex;
    use matches::assert_matches;
    use rusoto_kms::{DecryptResponse, GenerateDataKeyResponse};
    use rusoto_mock::MockRequestDispatcher;

    #[test]
    fn test_aws_kms() {
        let magic_contents = b"5678";
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            brane: "ap-southeast-2".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            lightlikepoint: String::new(),
        };

        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(magic_contents.as_ref().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(magic_contents.as_ref().into()),
            });
        let mut aws_kms = AwsKms::with_request_dispatcher(&config, dispatcher).unwrap();
        let (ciphertext, plaintext) = aws_kms.generate_data_key().unwrap();
        assert_eq!(ciphertext, magic_contents);
        assert_eq!(plaintext, magic_contents);

        let dispatcher = MockRequestDispatcher::with_status(200).with_json_body(DecryptResponse {
            plaintext: Some(magic_contents.as_ref().into()),
            key_id: Some("test_key_id".to_string()),
            encryption_algorithm: None,
        });
        let mut aws_kms = AwsKms::with_request_dispatcher(&config, dispatcher).unwrap();
        let plaintext = aws_kms.decrypt(ciphertext.as_slice()).unwrap();
        assert_eq!(plaintext, magic_contents);
    }

    #[test]
    fn test_fidelio_backlightlike() {
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            brane: "ap-southeast-2".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            lightlikepoint: String::new(),
        };

        let plaintext_key = vec![5u8; 32]; // 32 * 8 = 256 bits
        let ciphertext_key1 = vec![7u8; 32]; // 32 * 8 = 256 bits
        let ciphertext_key2 = vec![8u8; 32]; // 32 * 8 = 256 bits

        let mut inner = Inner {
            config,
            backlightlike: None,
            cached_ciphertext_key: vec![],
        };

        // fidelio mem backlightlike
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(ciphertext_key1.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner.maybe_fidelio_backlightlike_with(None, dispatcher).unwrap();
        assert!(inner.backlightlike.is_some());
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key1.to_vec());

        // Do not fidelio mem backlightlike if ciphertext_key is None.
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(plaintext_key.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner.maybe_fidelio_backlightlike_with(None, dispatcher).unwrap();
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key1.to_vec());

        // Do not fidelio mem backlightlike if cached_ciphertext_key equals to ciphertext_key.
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(ciphertext_key2.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner
            .maybe_fidelio_backlightlike_with(Some(&ciphertext_key1.to_vec()), dispatcher)
            .unwrap();
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key1.to_vec());

        // fidelio mem backlightlike if cached_ciphertext_key does not equal to ciphertext_key.
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(ciphertext_key2.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner
            .maybe_fidelio_backlightlike_with(Some(&ciphertext_key2.to_vec()), dispatcher)
            .unwrap();
        assert!(inner.backlightlike.is_some());
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key2.to_vec());
    }

    #[test]
    fn test_kms_backlightlike() {
        // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
        let pt = Vec::from_hex("25431587e9ecausetfc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749")
            .unwrap();
        let ct = Vec::from_hex("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980")
            .unwrap();
        let key = Vec::from_hex("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139")
            .unwrap();
        let iv = Vec::from_hex("cafabd9672ca6c79a2fbdc22").unwrap();

        let backlightlike = MemAesGcmBacklightlike::new(key.clone()).unwrap();

        let inner = Inner {
            config: KmsConfig::default(),
            backlightlike: Some(backlightlike),
            cached_ciphertext_key: key,
        };
        let backlightlike = KmsBacklightlike {
            inner: Mutex::new(inner),
        };
        let iv = Iv::from_slice(iv.as_slice()).unwrap();
        let encrypted_content = backlightlike.encrypt_content(&pt, iv).unwrap();
        assert_eq!(encrypted_content.get_content(), ct.as_slice());
        let plaintext = backlightlike.decrypt_content(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);

        let mut vlightlikeor_not_found = encrypted_content.clone();
        vlightlikeor_not_found
            .metadata
            .remove(MetadataKey::KmsVlightlikeor.as_str());
        assert_matches!(
            backlightlike.decrypt_content(&vlightlikeor_not_found).unwrap_err(),
            Error::WrongMasterKey(_)
        );

        let mut invalid_vlightlikeor = encrypted_content.clone();
        let mut invalid_suffix = b"_invalid".to_vec();
        invalid_vlightlikeor
            .metadata
            .get_mut(MetadataKey::KmsVlightlikeor.as_str())
            .unwrap()
            .applightlike(&mut invalid_suffix);
        assert_matches!(
            backlightlike.decrypt_content(&invalid_vlightlikeor).unwrap_err(),
            Error::Other(_)
        );

        let mut ciphertext_key_not_found = encrypted_content;
        ciphertext_key_not_found
            .metadata
            .remove(MetadataKey::KmsCiphertextKey.as_str());
        assert_matches!(
            backlightlike
                .decrypt_content(&ciphertext_key_not_found)
                .unwrap_err(),
            Error::Other(_)
        );
    }

    #[test]
    fn test_kms_wrong_key_id() {
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            brane: "ap-southeast-2".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            lightlikepoint: String::new(),
        };

        // IncorrectKeyException
        //
        // HTTP Status Code: 400
        // Json, see:
        // https://github.com/rusoto/rusoto/blob/mock-v0.43.0/rusoto/services/kms/src/generated.rs#L1970
        // https://github.com/rusoto/rusoto/blob/mock-v0.43.0/rusoto/core/src/proto/json/error.rs#L7
        // https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html#API_Decrypt_Errors
        let dispatcher = MockRequestDispatcher::with_status(400).with_body(
            r#"{
                "__type": "IncorrectKeyException",
                "Message": "mock"
            }"#,
        );
        let mut aws_kms = AwsKms::with_request_dispatcher(&config, dispatcher).unwrap();
        match aws_kms.decrypt(b"invalid") {
            Err(Error::WrongMasterKey(_)) => (),
            other => panic!("{:?}", other),
        }
    }
}
