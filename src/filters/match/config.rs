/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use serde::{Deserialize, Serialize};

use super::proto;
use crate::{config::ConfigType, filters::ConvertProtoConfigError};

/// Configuration for [`Match`][super::Match].
#[derive(Debug, Deserialize, Serialize, PartialEq, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Configuration for [`Filter::read`][crate::filters::Filter::read].
    pub on_read: Option<DirectionalConfig>,
    /// Configuration for [`Filter::write`][crate::filters::Filter::write].
    pub on_write: Option<DirectionalConfig>,
}

impl TryFrom<Config> for proto::Match {
    type Error = crate::filters::Error;

    fn try_from(config: Config) -> Result<Self, Self::Error> {
        Ok(Self {
            on_read: config.on_read.map(TryFrom::try_from).transpose()?,
            on_write: config.on_write.map(TryFrom::try_from).transpose()?,
        })
    }
}

impl TryFrom<proto::Match> for Config {
    type Error = ConvertProtoConfigError;

    fn try_from(value: proto::Match) -> Result<Self, Self::Error> {
        Ok(Self {
            on_read: value
                .on_read
                .map(proto::r#match::Config::try_into)
                .transpose()
                .map_err(|error: eyre::Report| {
                    ConvertProtoConfigError::new(error, Some("on_read".into()))
                })?,
            on_write: value
                .on_write
                .map(proto::r#match::Config::try_into)
                .transpose()
                .map_err(|error: eyre::Report| {
                    ConvertProtoConfigError::new(error, Some("on_write".into()))
                })?,
        })
    }
}

/// Configuration for a specific direction.
#[derive(Debug, Deserialize, Serialize, PartialEq, schemars::JsonSchema)]
pub struct DirectionalConfig {
    /// The key for the metadata to compare against.
    #[serde(rename = "metadataKey")]
    pub metadata_key: String,
    /// List of filters to compare and potentially run if any match.
    pub branches: Vec<Branch>,
    /// The behaviour for when none of the `branches` match.
    #[serde(default)]
    pub fallthrough: Fallthrough,
}

impl TryFrom<DirectionalConfig> for proto::r#match::Config {
    type Error = crate::filters::Error;

    fn try_from(config: DirectionalConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            metadata_key: Some(config.metadata_key),
            branches: config
                .branches
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
            fallthrough: config.fallthrough.try_into().map(Some)?,
        })
    }
}

impl TryFrom<proto::r#match::Config> for DirectionalConfig {
    type Error = eyre::Report;

    fn try_from(value: proto::r#match::Config) -> Result<Self, Self::Error> {
        Ok(Self {
            metadata_key: value.metadata_key.ok_or_else(|| {
                ConvertProtoConfigError::new("Missing", Some("metadata_key".into()))
            })?,
            branches: value
                .branches
                .into_iter()
                .map(proto::r#match::Branch::try_into)
                .collect::<Result<_, _>>()?,
            fallthrough: value
                .fallthrough
                .ok_or_else(|| ConvertProtoConfigError::new("Missing", Some("fallthrough".into())))?
                .try_into()
                .map(Fallthrough)?,
        })
    }
}

/// A specific match branch. The filter is run when `value` matches the value
/// defined in `metadata_key`.
#[derive(Debug, Deserialize, Serialize, PartialEq, schemars::JsonSchema)]
pub struct Branch {
    /// The value to compare against the dynamic metadata.
    pub value: crate::metadata::Value,
    /// The filter to run on successful matches.
    #[serde(flatten)]
    pub filter: Filter,
}

impl TryFrom<Branch> for proto::r#match::Branch {
    type Error = crate::filters::Error;

    fn try_from(branch: Branch) -> Result<Self, Self::Error> {
        Ok(Self {
            value: Some(branch.value.into()),
            filter: branch.filter.try_into().map(Some)?,
        })
    }
}

impl TryFrom<proto::r#match::Branch> for Branch {
    type Error = eyre::Report;

    fn try_from(branch: proto::r#match::Branch) -> Result<Self, Self::Error> {
        Ok(Self {
            value: branch
                .value
                .ok_or_else(|| ConvertProtoConfigError::new("Missing", Some("value".into())))?
                .try_into()?,
            filter: branch
                .filter
                .map(|filter| filter.try_into())
                .transpose()?
                .ok_or_else(|| ConvertProtoConfigError::new("Missing", Some("filter".into())))?,
        })
    }
}

#[derive(Debug, PartialEq, Serialize, schemars::JsonSchema)]
pub struct Filter {
    /// The identifier for the filter to run.
    pub id: String,
    /// The configuration for the filter to run, if any.
    pub config: Option<ConfigType>,
}

impl Filter {
    pub fn new<I: Into<String>>(id: I) -> Self {
        Self {
            id: id.into(),
            config: None,
        }
    }

    pub fn with_config<I: Into<String>, C: Into<ConfigType>>(id: I, config: C) -> Self {
        Self {
            id: id.into(),
            config: Some(config.into()),
        }
    }
}

impl From<String> for Filter {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&'static str> for Filter {
    fn from(id: &'static str) -> Self {
        Self::new(id)
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FilterVisitor;

        impl<'de> serde::de::Visitor<'de> for FilterVisitor {
            type Value = Filter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("string or map")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Filter::new(value))
            }

            fn visit_string<E>(self, id: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Filter::new(id))
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                const ID_KEY: &str = "id";
                const CONFIG_KEY: &str = "config";
                const KEYS: &[&str] = &[ID_KEY, CONFIG_KEY];
                let mut id: Option<String> = None;
                let mut config: Option<ConfigType> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match &*key {
                        ID_KEY => match id {
                            Some(_) => return Err(serde::de::Error::duplicate_field(ID_KEY)),
                            None => {
                                id.replace(map.next_value()?);
                            }
                        },
                        CONFIG_KEY => match config {
                            Some(_) => return Err(serde::de::Error::duplicate_field(CONFIG_KEY)),
                            None => {
                                config.replace(map.next_value()?);
                            }
                        },
                        key => return Err(serde::de::Error::unknown_field(key, KEYS)),
                    };
                }

                let id = id.ok_or_else(|| serde::de::Error::missing_field("id"))?;

                Ok(Filter { id, config })
            }
        }

        deserializer.deserialize_any(FilterVisitor)
    }
}

impl TryFrom<Filter> for proto::r#match::Filter {
    type Error = crate::filters::Error;

    fn try_from(filter: Filter) -> Result<Self, Self::Error> {
        Ok(Self {
            config: match filter.config {
                Some(ConfigType::Dynamic(any)) => Some(any),
                Some(ConfigType::Static(value)) => {
                    crate::filters::FilterRegistry::get_factory(&filter.id)
                        .ok_or_else(|| crate::filters::Error::NotFound(filter.id.clone()))?
                        .encode_config_to_protobuf(value)
                        .map(Some)?
                }
                None => None,
            },
            id: Some(filter.id),
        })
    }
}

impl TryFrom<proto::r#match::Filter> for Filter {
    type Error = eyre::Report;

    fn try_from(filter: proto::r#match::Filter) -> Result<Self, Self::Error> {
        let id: String = filter
            .id
            .ok_or_else(|| eyre::eyre!("missing `filter` field in Fallthrough configuration"))?;

        Ok(Self {
            id,
            config: filter.config.map(ConfigType::Dynamic),
        })
    }
}

/// The behaviour when the none of branches match. Defaults to dropping packets.
#[derive(Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct Fallthrough(pub Filter);

impl Default for Fallthrough {
    fn default() -> Self {
        Self(Filter::new(crate::filters::drop::NAME))
    }
}

impl TryFrom<Fallthrough> for proto::r#match::Filter {
    type Error = crate::filters::Error;
    fn try_from(fallthrough: Fallthrough) -> Result<Self, Self::Error> {
        fallthrough.0.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde() {
        let matches_yaml = "
on_read:
    metadataKey: quilkin.dev/captured_bytes
    branches:
        - value: abc
          id: quilkin.filters.debug.v1alpha1.Debug
        ";

        let config = serde_yaml::from_str::<Config>(matches_yaml).unwrap();

        assert_eq!(
            config,
            Config {
                on_read: Some(DirectionalConfig {
                    metadata_key: "quilkin.dev/captured_bytes".into(),
                    branches: vec![Branch {
                        value: String::from("abc").into(),
                        filter: "quilkin.filters.debug.v1alpha1.Debug".into(),
                    }],
                    fallthrough: <_>::default(),
                }),
                on_write: None,
            }
        )
    }
}
