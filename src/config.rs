/*
 * Copyright 2020 Google LLC
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

//! Quilkin configuration.

use std::{net::SocketAddr, sync::Arc};

use arc_swap::ArcSwap;
use base64_serde::base64_serde_type;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

mod builder;
mod config_type;
mod error;
mod metrics;

use crate::{cluster::ClusterLocalities, endpoint::Endpoint, xds::Resource};
use metrics::Metrics;

pub use self::{builder::Builder, config_type::ConfigType, error::ValidationError};

base64_serde_type!(Base64Standard, base64::STANDARD);

// For some log messages on the hot path (potentially per-packet), we log 1 out
// of every `LOG_SAMPLING_RATE` occurrences to avoid spamming the logs.
pub(crate) const LOG_SAMPLING_RATE: u64 = 1000;

/// Config is the configuration of a proxy
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    #[serde(default)]
    pub admin: Option<Admin>,
    #[schemars(with = "Vec::<Endpoint>")]
    #[serde(default)]
    pub endpoints: Arc<ArcSwap<Vec<Endpoint>>>,
    #[serde(default)]
    pub filters: crate::filters::SharedFilterChain,
    #[serde(default)]
    #[schemars(with = "Vec::<ManagementServer>")]
    pub management_servers: Arc<ArcSwap<Vec<ManagementServer>>>,
    #[serde(default)]
    pub proxy: Proxy,
    pub version: Version,

    #[serde(default, skip)]
    metrics: Metrics,
}

impl Config {
    /// Returns a new empty [`Builder`] for [`Config`].
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Attempts to deserialize `input` as a YAML object representing `Self`.
    pub fn from_reader<R: std::io::Read>(input: R) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_reader(input)
    }

    #[tracing::instrument(skip_all, fields(response = response.type_url()))]
    pub fn apply(&self, response: &Resource) -> crate::Result<()> {
        match response {
            Resource::Endpoint(cla) => {
                let cluster = ClusterLocalities::try_from(cla.clone())?;
                let endpoints = cluster
                    .into_values()
                    .flat_map(|locality| locality.endpoints)
                    .collect::<Vec<_>>();

                tracing::info!(endpoints = ?endpoints.iter().map(|endpoint| endpoint.address.clone()).collect::<Vec<_>>(), "new endpoints");

                self.endpoints.store(Arc::new(endpoints));
            }
            Resource::Listener(listener) => {
                let chain = listener
                    .filter_chains
                    .get(0)
                    .map(|chain| chain.filters.clone())
                    .unwrap_or_default()
                    .into_iter()
                    .map(Filter::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                self.filters.store(&chain)?;
                let chain = listener
                    .filter_chains
                    .get(0)
                    .map(|chain| chain.filters.clone())
                    .unwrap_or_default()
                    .into_iter()
                    .map(Filter::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                self.filters.store(&chain)?;
            }
            Resource::Cluster(cluster) => {
                if let Some(cluster) = cluster
                    .load_assignment
                    .clone()
                    .map(ClusterLocalities::try_from)
                    .transpose()?
                {
                    let endpoints = cluster
                        .into_values()
                        .flat_map(|locality| locality.endpoints)
                        .collect::<Vec<_>>();
                    tracing::info!(endpoints = ?endpoints.iter().map(|endpoint| endpoint.address.clone()).collect::<Vec<_>>(), "new endpoints");
                    self.endpoints.store(Arc::new(endpoints));
                }
            }
        }

        self.apply_metrics();

        Ok(())
    }

    fn apply_metrics(&self) {
        let endpoints = self.endpoints.load();

        self.metrics
            .active_clusters
            .set(!endpoints.is_empty() as i64);
        self.metrics.active_endpoints.set(endpoints.len() as i64);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum Version {
    #[serde(rename = "v1alpha1")]
    V1Alpha1,
}

impl Default for Version {
    fn default() -> Self {
        Self::V1Alpha1
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Proxy {
    #[serde(default = "default_proxy_id")]
    pub id: String,
    #[serde(default = "default_proxy_port")]
    pub port: u16,
}

#[cfg(not(target_os = "linux"))]
fn default_proxy_id() -> String {
    Uuid::new_v4().to_hyphenated().to_string()
}

#[cfg(target_os = "linux")]
fn default_proxy_id() -> String {
    sys_info::hostname().unwrap_or_else(|_| Uuid::new_v4().to_hyphenated().to_string())
}

fn default_proxy_port() -> u16 {
    7000
}

impl Default for Proxy {
    fn default() -> Self {
        Proxy {
            id: default_proxy_id(),
            port: default_proxy_port(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Admin {
    pub address: SocketAddr,
}

impl Default for Admin {
    fn default() -> Self {
        Admin {
            address: (std::net::Ipv4Addr::UNSPECIFIED, 9091).into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagementServer {
    pub address: String,
}

/// Filter is the configuration for a single filter
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub name: String,
    pub config: Option<serde_json::Value>,
}

impl TryFrom<crate::xds::config::listener::v3::Filter> for Filter {
    type Error = eyre::Error;

    fn try_from(filter: crate::xds::config::listener::v3::Filter) -> Result<Self, Self::Error> {
        use crate::xds::config::listener::v3::filter::ConfigType;

        let config = if let Some(config_type) = filter.config_type {
            let config = match config_type {
                ConfigType::TypedConfig(any) => any,
                ConfigType::ConfigDiscovery(_) => {
                    return Err(eyre::eyre!("Config discovery is not supported."))
                }
            };
            Some(
                crate::filters::FilterRegistry::get_factory(&filter.name)
                    .ok_or_else(|| eyre::eyre!("Missing filter"))?
                    .encode_config_to_json(config)?,
            )
        } else {
            None
        };

        Ok(Self {
            name: filter.name,
            config,
        })
    }
}

impl TryFrom<Filter> for crate::xds::config::listener::v3::Filter {
    type Error = eyre::Error;

    fn try_from(filter: Filter) -> Result<Self, Self::Error> {
        use crate::xds::config::listener::v3::filter::ConfigType;

        let config = if let Some(config) = filter.config {
            Some(
                crate::filters::FilterRegistry::get_factory(&filter.name)
                    .ok_or_else(|| eyre::eyre!("Missing filter"))?
                    .encode_config_to_protobuf(config)?,
            )
        } else {
            None
        };

        Ok(Self {
            name: filter.name,
            config_type: config.map(ConfigType::TypedConfig),
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::endpoint::Metadata;

    fn parse_config(yaml: &str) -> Config {
        Config::from_reader(yaml.as_bytes()).unwrap()
    }

    #[test]
    fn deserialise_client() {
        let config = Builder::default()
            .port(7000)
            .endpoints(vec![Endpoint::new("127.0.0.1:25999".parse().unwrap())])
            .build()
            .unwrap();
        let _ = serde_yaml::to_string(&config).unwrap();
    }

    #[test]
    fn deserialise_server() {
        let config = Builder::default()
            .port(7000)
            .endpoints(vec![
                Endpoint::new("127.0.0.1:26000".parse().unwrap()),
                Endpoint::new("127.0.0.1:26001".parse().unwrap()),
            ])
            .build()
            .unwrap();
        let _ = serde_yaml::to_string(&config).unwrap();
    }

    #[test]
    fn parse_default_values() {
        let config: Config = serde_json::from_value(json!({
            "version": "v1alpha1",
            "endpoints": [{
                "address": "127.0.0.1:25999",
            }],
        }))
        .unwrap();

        assert_eq!(config.proxy.port, 7000);
        assert!(config.proxy.id.len() > 1);
    }

    #[test]
    fn parse_proxy() {
        let yaml = "
version: v1alpha1
proxy:
  id: server-proxy
  port: 7000
endpoints:
  - address: 127.0.0.1:25999
  ";
        let config = parse_config(yaml);

        assert_eq!(config.proxy.port, 7000);
        assert_eq!(config.proxy.id.as_str(), "server-proxy");
    }

    #[test]
    fn parse_client() {
        let config: Config = serde_json::from_value(json!({
            "version": "v1alpha1",
            "endpoints": [{
                "address": "127.0.0.1:25999"
            }]
        }))
        .unwrap();

        assert_eq!(
            **config.endpoints.load(),
            vec![Endpoint::new((std::net::Ipv4Addr::LOCALHOST, 25999).into())],
        );
    }

    #[test]
    fn parse_server() {
        let config: Config = serde_json::from_value(json!({
            "version": "v1alpha1",
            "endpoints": [
                {
                    "address": "127.0.0.1:26000",
                    "metadata": {
                        "quilkin.dev": {
                            "tokens": ["MXg3aWp5Ng==", "OGdqM3YyaQ=="],
                        }
                    }
                },
                {
                    "address": "127.0.0.1:26001",
                    "metadata": {
                        "quilkin.dev": {
                            "tokens": ["bmt1eTcweA=="],
                        }
                    }
                },
            ]
        }))
        .unwrap();

        assert_eq!(
            **config.endpoints.load(),
            vec![
                Endpoint::with_metadata(
                    "127.0.0.1:26000".parse().unwrap(),
                    Metadata {
                        tokens: vec!["1x7ijy6", "8gj3v2i"]
                            .into_iter()
                            .map(From::from)
                            .collect(),
                    },
                ),
                Endpoint::with_metadata(
                    "127.0.0.1:26001".parse().unwrap(),
                    Metadata {
                        tokens: vec!["nkuy70x"].into_iter().map(From::from).collect(),
                    },
                ),
            ],
        );
    }

    #[test]
    fn parse_management_servers() {
        let config: Config = serde_json::from_value(json!({
            "version": "v1alpha1",
            "management_servers": [
                { "address": "127.0.0.1:25999" },
                { "address": "127.0.0.1:30000" },
            ],
        }))
        .unwrap();

        assert_eq!(
            **config.management_servers.load(),
            vec![
                ManagementServer {
                    address: "127.0.0.1:25999".into(),
                },
                ManagementServer {
                    address: "127.0.0.1:30000".into(),
                },
            ],
        );
    }

    #[test]
    fn deny_unused_fields() {
        let configs = vec![
            "
version: v1alpha1
foo: bar
endpoints:
  - address: 127.0.0.1:7001
",
            "
# proxy
version: v1alpha1
proxy:
  foo: bar
  id: client-proxy
  port: 7000
endpoints:
  - address: 127.0.0.1:7001
",
            "
# admin
version: v1alpha1
admin:
    foo: bar
    address: 127.0.0.1:7001
",
            "
# static.endpoints
version: v1alpha1
endpoints:
  - address: 127.0.0.1:7001
    connection_ids:
    - Mxg3aWp5Ng==
",
            "
# static.filters
version: v1alpha1
filters:
  - name: quilkin.core.v1.rate-limiter
    foo: bar
",
            "
# dynamic.management_servers
version: v1alpha1
dynamic:
  management_servers:
    - address: 127.0.0.1:25999
      foo: bar
",
        ];

        for config in configs {
            let result = Config::from_reader(config.as_bytes());
            let error = result.unwrap_err();
            assert!(format!("{error:?}").contains("unknown field"));
        }
    }
}
