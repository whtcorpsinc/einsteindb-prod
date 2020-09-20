// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};

pub use configuration_derive::*;

pub type ConfigChange = HashMap<String, ConfigValue>;

#[derive(Clone, PartialEq)]
pub enum ConfigValue {
    Duration(u64),
    Size(u64),
    U64(u64),
    F64(f64),
    I32(i32),
    U32(u32),
    Usize(usize),
    Bool(bool),
    String(String),
    BlobRunMode(String),
    OptionSize(Option<u64>),
    Module(ConfigChange),
    Skip,
}

impl Display for ConfigValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConfigValue::Duration(v) => write!(f, "{}ms", v),
            ConfigValue::Size(v) => write!(f, "{}b", v),
            ConfigValue::OptionSize(Some(v)) => write!(f, "{}b", v),
            ConfigValue::OptionSize(None) => write!(f, ""),
            ConfigValue::U64(v) => write!(f, "{}", v),
            ConfigValue::F64(v) => write!(f, "{}", v),
            ConfigValue::I32(v) => write!(f, "{}", v),
            ConfigValue::U32(v) => write!(f, "{}", v),
            ConfigValue::Usize(v) => write!(f, "{}", v),
            ConfigValue::Bool(v) => write!(f, "{}", v),
            ConfigValue::String(v) => write!(f, "{}", v),
            ConfigValue::BlobRunMode(v) => write!(f, "{}", v),
            ConfigValue::Module(v) => write!(f, "{:?}", v),
            ConfigValue::Skip => write!(f, "ConfigValue::Skip"),
        }
    }
}

impl Debug for ConfigValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

macro_rules! impl_from {
    ($from: ty, $to: tt) => {
        impl From<$from> for ConfigValue {
            fn from(r: $from) -> ConfigValue {
                ConfigValue::$to(r)
            }
        }
    };
}
impl_from!(u64, U64);
impl_from!(f64, F64);
impl_from!(i32, I32);
impl_from!(u32, U32);
impl_from!(usize, Usize);
impl_from!(bool, Bool);
impl_from!(String, String);
impl_from!(ConfigChange, Module);

macro_rules! impl_into {
    ($into: ty, $from: tt) => {
        impl Into<$into> for ConfigValue {
            fn into(self) -> $into {
                if let ConfigValue::$from(v) = self {
                    v
                } else {
                    panic!(
                        "expect: {:?}, got: {:?}",
                        format!("ConfigValue::{}", stringify!($from)),
                        self
                    );
                }
            }
        }
    };
}
impl_into!(u64, U64);
impl_into!(f64, F64);
impl_into!(i32, I32);
impl_into!(u32, U32);
impl_into!(usize, Usize);
impl_into!(bool, Bool);
impl_into!(String, String);
impl_into!(ConfigChange, Module);

/// The Configuration trait
///
/// There are four type of fields inside derived Configuration struct:
/// 1. `#[config(skip)]` field, these fields will not return
/// by `diff` method and have not effect of `ufidelate` method
/// 2. `#[config(hidden)]` field, these fields have the same effect of
/// `#[config(skip)]` field, in addition, these fields will not appear
/// at the output of serializing `Self::Encoder`
/// 3. `#[config(submodule)]` field, these fields represent the
/// submodule, and should also derive `Configuration`
/// 4. normal fields, the type of these fields should be implment
/// `Into` and `From` for `ConfigValue`
pub trait Configuration<'a> {
    type Encoder: serde::Serialize;
    /// Compare to other config, return the difference
    fn diff(&self, _: &Self) -> ConfigChange;
    /// Ufidelate config with difference returned by `diff`
    fn ufidelate(&mut self, _: ConfigChange);
    /// Get encoder that can be serialize with `serde::Serializer`
    /// with the disappear of `#[config(hidden)]` field
    fn get_encoder(&'a self) -> Self::Encoder;
    /// Get all fields and their type of the config
    fn typed(&self) -> ConfigChange;
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait ConfigManager: Slightlike + Sync {
    fn dispatch(&mut self, _: ConfigChange) -> Result<()>;
}

#[causetg(test)]
mod tests {
    use super::*;
    use crate as configuration;

    #[derive(Clone, Configuration, Debug, Default, PartialEq)]
    pub struct TestConfig {
        field1: usize,
        field2: String,
        #[config(skip)]
        skip_field: u64,
        #[config(submodule)]
        submodule_field: SubConfig,
    }

    #[derive(Clone, Configuration, Debug, Default, PartialEq)]
    pub struct SubConfig {
        field1: u64,
        field2: bool,
        #[config(skip)]
        skip_field: String,
    }

    #[test]
    fn test_ufidelate_fields() {
        let mut causetg = TestConfig::default();
        let mut ufidelated_causetg = causetg.clone();
        {
            // ufidelate fields
            ufidelated_causetg.field1 = 100;
            ufidelated_causetg.field2 = "1".to_owned();
            ufidelated_causetg.submodule_field.field1 = 1000;
            ufidelated_causetg.submodule_field.field2 = true;
        }
        let diff = causetg.diff(&ufidelated_causetg);
        {
            let mut diff = diff.clone();
            assert_eq!(diff.len(), 3);
            assert_eq!(diff.remove("field1").map(Into::into), Some(100usize));
            assert_eq!(diff.remove("field2").map(Into::into), Some("1".to_owned()));
            // submodule should also be ufidelated
            let sub_m = diff.remove("submodule_field").map(Into::into);
            assert!(sub_m.is_some());
            let mut sub_diff: ConfigChange = sub_m.unwrap();
            assert_eq!(sub_diff.len(), 2);
            assert_eq!(sub_diff.remove("field1").map(Into::into), Some(1000u64));
            assert_eq!(sub_diff.remove("field2").map(Into::into), Some(true));
        }
        causetg.ufidelate(diff);
        assert_eq!(causetg, ufidelated_causetg, "causetg should be ufidelated");
    }

    #[test]
    fn test_not_ufidelate() {
        let mut causetg = TestConfig::default();
        let diff = causetg.diff(&causetg.clone());
        assert!(diff.is_empty(), "diff should be empty");

        causetg.ufidelate(diff);
        assert_eq!(causetg, TestConfig::default(), "causetg should not be ufidelated");
    }

    #[test]
    fn test_ufidelate_skip_field() {
        let mut causetg = TestConfig::default();
        let mut ufidelated_causetg = causetg.clone();

        ufidelated_causetg.skip_field = 100;
        assert!(causetg.diff(&ufidelated_causetg).is_empty(), "diff should be empty");

        let mut diff = HashMap::new();
        diff.insert("skip_field".to_owned(), ConfigValue::U64(123));
        causetg.ufidelate(diff);
        assert_eq!(causetg, TestConfig::default(), "causetg should not be ufidelated");
    }

    #[test]
    fn test_ufidelate_submodule() {
        let mut causetg = TestConfig::default();
        let mut ufidelated_causetg = causetg.clone();

        ufidelated_causetg.submodule_field.field1 = 12345;
        ufidelated_causetg.submodule_field.field2 = true;

        let diff = causetg.diff(&ufidelated_causetg);
        {
            let mut diff = diff.clone();
            assert_eq!(diff.len(), 1);
            let mut sub_diff: ConfigChange =
                diff.remove("submodule_field").map(Into::into).unwrap();
            assert_eq!(sub_diff.len(), 2);
            assert_eq!(sub_diff.remove("field1").map(Into::into), Some(12345u64));
            assert_eq!(sub_diff.remove("field2").map(Into::into), Some(true));
        }

        causetg.ufidelate(diff);
        assert_eq!(
            causetg.submodule_field, ufidelated_causetg.submodule_field,
            "submodule should be ufidelated"
        );
        assert_eq!(causetg, ufidelated_causetg, "causetg should be ufidelated");
    }

    #[test]
    fn test_hidden_field() {
        use serde::Serialize;

        #[derive(Configuration, Default, Serialize)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct TestConfig {
            #[config(skip)]
            skip_field: String,
            #[config(hidden)]
            hidden_field: u64,
            #[config(submodule)]
            submodule_field: SubConfig,
        }

        #[derive(Configuration, Default, Serialize)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct SubConfig {
            #[serde(rename = "rename_field")]
            bool_field: bool,
            #[config(hidden)]
            hidden_field: usize,
        }

        let causetg = SubConfig::default();
        assert_eq!(
            toml::to_string(&causetg).unwrap(),
            "rename_field = false\nhidden-field = 0\n"
        );
        assert_eq!(
            toml::to_string(&causetg.get_encoder()).unwrap(),
            "rename_field = false\n"
        );

        let causetg = TestConfig::default();
        assert_eq!(
            toml::to_string(&causetg).unwrap(),
            "skip-field = \"\"\nhidden-field = 0\n\n[submodule-field]\nrename_field = false\nhidden-field = 0\n"
        );
        assert_eq!(
            toml::to_string(&causetg.get_encoder()).unwrap(),
            "skip-field = \"\"\n\n[submodule-field]\nrename_field = false\n"
        );
    }
}
