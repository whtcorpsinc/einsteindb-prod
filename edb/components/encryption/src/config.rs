// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use ekvproto::encryption_timeshare::EncryptionMethod;
use violetabftstore::interlock::::config::ReadableDuration;

#[causet(test)]
use crate::master_key::Backlightlike;
#[causet(test)]
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EncryptionConfig {
    // Encryption configs.
    #[serde(with = "encryption_method_serde")]
    #[config(skip)]
    pub data_encryption_method: EncryptionMethod,
    #[config(skip)]
    pub data_key_rotation_period: ReadableDuration,
    #[config(skip)]
    pub master_key: MasterKeyConfig,
    #[config(skip)]
    pub previous_master_key: MasterKeyConfig,
}

impl Default for EncryptionConfig {
    fn default() -> EncryptionConfig {
        EncryptionConfig {
            data_encryption_method: EncryptionMethod::Plaintext,
            data_key_rotation_period: ReadableDuration::days(7),
            master_key: MasterKeyConfig::default(),
            previous_master_key: MasterKeyConfig::default(),
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FileConfig {
    pub path: String,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct KmsConfig {
    pub key_id: String,

    // Providing access_key and secret_access_key is recommlightlikeed as it has
    // security risk.
    #[doc(hidden)]
    // We don's want to write access_key and secret_access_key to config file
    // accidentally.
    #[serde(skip_serializing)]
    #[config(skip)]
    pub access_key: String,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[config(skip)]
    pub secret_access_key: String,

    pub brane: String,
    pub lightlikepoint: String,
}

#[causet(test)]
#[derive(Clone, Debug)]
pub struct Mock(pub Arc<dyn Backlightlike>);
#[causet(test)]
impl PartialEq for Mock {
    fn eq(&self, _: &Self) -> bool {
        false
    }
}
#[causet(test)]
impl Eq for Mock {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum MasterKeyConfig {
    // CausetStore encryption metadata as plaintext. Data still get encrypted. Not allowed to use if
    // encryption is enabled. (i.e. when encryption_config.method != Plaintext).
    Plaintext,

    // Pass master key from a file, with key encoded as a readable hex string. The file should lightlike
    // with newline.
    #[serde(rename_all = "kebab-case")]
    File {
        #[serde(flatten)]
        config: FileConfig,
    },

    #[serde(rename_all = "kebab-case")]
    Kms {
        #[serde(flatten)]
        config: KmsConfig,
    },

    #[causet(test)]
    #[serde(skip)]
    Mock(Mock),
}

impl Default for MasterKeyConfig {
    fn default() -> Self {
        MasterKeyConfig::Plaintext
    }
}

mod encryption_method_serde {
    use super::EncryptionMethod;
    use std::fmt;

    use serde::de::{self, Unexpected, Visitor};
    use serde::{Deserializer, Serializer};

    const UNKNOWN: &str = "unknown";
    const PLAINTEXT: &str = "plaintext";
    const AES128_CTR: &str = "aes128-ctr";
    const AES192_CTR: &str = "aes192-ctr";
    const AES256_CTR: &str = "aes256-ctr";

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(method: &EncryptionMethod, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match method {
            EncryptionMethod::Unknown => serializer.serialize_str(UNKNOWN),
            EncryptionMethod::Plaintext => serializer.serialize_str(PLAINTEXT),
            EncryptionMethod::Aes128Ctr => serializer.serialize_str(AES128_CTR),
            EncryptionMethod::Aes192Ctr => serializer.serialize_str(AES192_CTR),
            EncryptionMethod::Aes256Ctr => serializer.serialize_str(AES256_CTR),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<EncryptionMethod, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EncryptionMethodVisitor;

        impl<'de> Visitor<'de> for EncryptionMethodVisitor {
            type Value = EncryptionMethod;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "valid encryption method")
            }

            fn visit_str<E>(self, value: &str) -> Result<EncryptionMethod, E>
            where
                E: de::Error,
            {
                match value.to_lowercase().as_ref() {
                    UNKNOWN => Ok(EncryptionMethod::Unknown),
                    PLAINTEXT => Ok(EncryptionMethod::Plaintext),
                    AES128_CTR => Ok(EncryptionMethod::Aes128Ctr),
                    AES192_CTR => Ok(EncryptionMethod::Aes192Ctr),
                    AES256_CTR => Ok(EncryptionMethod::Aes256Ctr),
                    _ => Err(E::invalid_value(Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_str(EncryptionMethodVisitor)
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kms_config() {
        let kms_causet = EncryptionConfig {
            data_encryption_method: EncryptionMethod::Aes128Ctr,
            data_key_rotation_period: ReadableDuration::days(14),
            master_key: MasterKeyConfig::Kms {
                config: KmsConfig {
                    key_id: "key_id".to_owned(),
                    access_key: "access_key".to_owned(),
                    secret_access_key: "secret_access_key".to_owned(),
                    brane: "brane".to_owned(),
                    lightlikepoint: "lightlikepoint".to_owned(),
                },
            },
            previous_master_key: MasterKeyConfig::Plaintext,
        };
        let kms_str = r#"
        data-encryption-method = "aes128-ctr"
        data-key-rotation-period = "14d"
        [previous-master-key]
        type = "plaintext"
        [master-key]
        type = "kms"
        key-id = "key_id"
        access-key = "access_key"
        secret-access-key = "secret_access_key"
        brane = "brane"
        lightlikepoint = "lightlikepoint"
        "#;
        let causet: EncryptionConfig = toml::from_str(kms_str).unwrap();
        assert_eq!(
            causet,
            kms_causet,
            "\n{}\n",
            toml::to_string_pretty(&kms_causet).unwrap()
        );
    }
}
