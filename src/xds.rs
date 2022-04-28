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

// We don't control the codegen, so disable any code warnings in the
// proto modules.
#[allow(warnings)]
mod xds {
    pub mod core {
        pub mod v3 {
            #![doc(hidden)]
            tonic::include_proto!("xds.core.v3");
        }
    }

    pub mod r#type {
        pub mod matcher {
            pub mod v3 {
                pub use super::super::super::config::common::matcher::v3::*;
                tonic::include_proto!("envoy.r#type.matcher.v3");
            }
        }
        pub mod metadata {
            pub mod v3 {
                tonic::include_proto!("envoy.r#type.metadata.v3");
            }
        }
        pub mod tracing {
            pub mod v3 {
                tonic::include_proto!("envoy.r#type.tracing.v3");
            }
        }
        pub mod v3 {
            tonic::include_proto!("envoy.r#type.v3");
        }
    }
    pub mod config {
        pub mod accesslog {
            pub mod v3 {
                tonic::include_proto!("envoy.config.accesslog.v3");
            }
        }
        pub mod cluster {
            pub mod v3 {
                tonic::include_proto!("envoy.config.cluster.v3");
            }
        }
        pub mod common {
            pub mod matcher {
                pub mod v3 {
                    tonic::include_proto!("envoy.config.common.matcher.v3");
                }
            }
        }
        pub mod core {
            pub mod v3 {
                tonic::include_proto!("envoy.config.core.v3");
            }
        }
        pub mod endpoint {
            pub mod v3 {
                tonic::include_proto!("envoy.config.endpoint.v3");
            }
        }
        pub mod listener {
            pub mod v3 {
                tonic::include_proto!("envoy.config.listener.v3");
            }
        }
        pub mod route {
            pub mod v3 {
                tonic::include_proto!("envoy.config.route.v3");
            }
        }
    }
    pub mod service {
        pub mod discovery {
            pub mod v3 {
                tonic::include_proto!("envoy.service.discovery.v3");

                impl TryFrom<DiscoveryResponse> for DiscoveryRequest {
                    type Error = eyre::Error;

                    fn try_from(response: DiscoveryResponse) -> Result<Self, Self::Error> {
                        Ok(Self {
                            version_info: response.version_info,
                            resource_names: response
                                .resources
                                .into_iter()
                                .map(crate::xds::Resource::try_from)
                                .map(|result| result.map(|resource| resource.name().to_owned()))
                                .collect::<Result<Vec<_>, _>>()?,
                            type_url: response.type_url,
                            response_nonce: response.nonce,
                            ..<_>::default()
                        })
                    }
                }
            }
        }
        pub mod cluster {
            pub mod v3 {
                tonic::include_proto!("envoy.service.cluster.v3");
            }
        }
    }
}

#[allow(warnings)]
mod google {
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}

pub(crate) mod client;
mod metrics;
pub mod provider;
mod resource;
pub(crate) mod server;

use service::discovery::v3::aggregated_discovery_service_server::AggregatedDiscoveryServiceServer;

pub use client::Client;
pub use provider::DiscoveryServiceProvider;
pub use resource::{Resource, ResourceType};
pub use server::ControlPlane;
pub use service::discovery::v3::aggregated_discovery_service_client::AggregatedDiscoveryServiceClient;
pub use xds::*;

#[tracing::instrument(skip_all)]
pub async fn manage(
    config: crate::Config,
    provider: std::sync::Arc<dyn DiscoveryServiceProvider>,
) -> crate::Result<()> {
    let port = config.proxy.port;
    tracing::info!("Serving management server at {}", port);

    let server = AggregatedDiscoveryServiceServer::new(ControlPlane::from_arc(config, provider)?);
    let server = tonic::transport::Server::builder().add_service(server);
    Ok(server
        .serve((std::net::Ipv4Addr::UNSPECIFIED, port).into())
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use tracing_subscriber::fmt::format::FmtSpan;

    use crate::{
        config::Config,
        endpoint::{Endpoint, EndpointAddress},
        filters::{
            concatenate_bytes::{ConcatenateBytes, Config as ConcatenateBytesConfig, Strategy},
            StaticFilter,
        },
        xds::{
            config::{
                cluster::v3::{cluster::ClusterDiscoveryType, Cluster},
                endpoint::v3::{ClusterLoadAssignment, LocalityLbEndpoints},
                listener::v3::{Filter, FilterChain, Listener},
            },
            service::discovery::v3::DiscoveryResponse,
        },
    };

    const CLUSTER_TYPE: &str = ResourceType::Cluster.type_url();
    const LISTENER_TYPE: &str = ResourceType::Listener.type_url();

    #[tokio::test]
    async fn send_cds_and_lds_updates() {
        tracing_subscriber::fmt()
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .init();

        let config: Config = serde_json::from_value(serde_json::json!({
            "version": "v1alpha1",
            "proxy": {
                "id": "test-proxy",
                "port": 23456u16,
            },
            "management_servers": [{
                "address": "http://127.0.0.1:23456",
            }]
        }))
        .unwrap();

        let (provider, sender) = crate::test_utils::MessageServiceProvider::new();
        tokio::spawn(manage(config.clone(), Arc::from(provider)));

        let config = Arc::new(config);
        let client = Client::connect(config.clone()).await.unwrap();
        let mut stream = client.stream().await.unwrap();

        // Each time, we create a new upstream endpoint and send a cluster update for it.
        let concat_bytes = vec![("b", "c,"), ("d", "e")];
        for (i, (b1, b2)) in concat_bytes.into_iter().enumerate() {
            let upstream_address: crate::endpoint::EndpointAddress =
                (std::net::Ipv4Addr::UNSPECIFIED, i as u16).into();
            // Send a cluster update.
            let cluster_update = cluster_discovery_response(
                "cluster-1".into(),
                i.to_string().as_str(),
                i.to_string().as_str(),
                upstream_address.clone(),
            );

            sender
                .send((ResourceType::Cluster, cluster_update))
                .unwrap();

            let filters = vec![
                ConcatenateBytes::to_config(ConcatenateBytesConfig {
                    on_read: Strategy::Append,
                    on_write: <_>::default(),
                    bytes: b1.as_bytes().to_vec(),
                })
                .unwrap(),
                ConcatenateBytes::to_config(ConcatenateBytesConfig {
                    on_read: Strategy::Append,
                    on_write: <_>::default(),
                    bytes: b2.as_bytes().to_vec(),
                })
                .unwrap(),
            ];

            // Send a filter update.
            let filter_update = concat_listener_discovery_response(
                i.to_string().as_str(),
                i.to_string().as_str(),
                filters
                    .iter()
                    .cloned()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()
                    .unwrap(),
            );
            sender
                .send((ResourceType::Listener, filter_update))
                .unwrap();

            let mut request = stream.send(ResourceType::Cluster, &[]).await.unwrap();
            request.changed().await.unwrap();
            assert_eq!(
                *config.endpoints.load().iter().next().unwrap(),
                upstream_address
            );
            let mut request = stream.send(ResourceType::Listener, &[]).await.unwrap();
            request.changed().await.unwrap();
            let changed_filters = config.filters.load();

            assert_eq!(changed_filters.len(), 2);

            let mut iter = changed_filters.iter();
            assert_eq!(iter.next().unwrap(), filters[0]);
            assert_eq!(iter.next().unwrap(), filters[1]);
        }
    }

    fn concat_listener_discovery_response(
        version_info: &str,
        nonce: &str,
        filters: Vec<Filter>,
    ) -> DiscoveryResponse {
        let filter_chain = create_lds_filter_chain(filters);
        let listener_name = "listener-1";
        let listener = create_lds_listener(listener_name.into(), vec![filter_chain]);
        let lds_resource = prost_types::Any {
            type_url: LISTENER_TYPE.into(),
            value: crate::prost::encode(&listener).unwrap(),
        };

        DiscoveryResponse {
            version_info: version_info.into(),
            resources: vec![lds_resource],
            canary: false,
            type_url: LISTENER_TYPE.into(),
            nonce: nonce.into(),
            control_plane: None,
        }
    }

    fn cluster_discovery_response(
        name: String,
        version_info: &str,
        nonce: &str,
        endpoint_addr: EndpointAddress,
    ) -> DiscoveryResponse {
        let cluster = create_cluster_resource(&name, endpoint_addr);
        let resource = prost_types::Any {
            type_url: CLUSTER_TYPE.into(),
            value: crate::prost::encode(&cluster).unwrap(),
        };

        DiscoveryResponse {
            type_url: CLUSTER_TYPE.into(),
            version_info: version_info.into(),
            nonce: nonce.into(),
            resources: vec![resource],
            canary: false,
            control_plane: None,
        }
    }

    fn create_cluster_resource(name: &str, endpoint_addr: EndpointAddress) -> Cluster {
        Cluster {
            name: name.into(),
            load_assignment: Some(create_endpoint_resource(name, endpoint_addr)),
            cluster_discovery_type: Some(ClusterDiscoveryType::Type(0)),
            ..<_>::default()
        }
    }

    fn create_endpoint_resource(
        cluster_name: &str,
        address: EndpointAddress,
    ) -> ClusterLoadAssignment {
        ClusterLoadAssignment {
            cluster_name: cluster_name.into(),
            endpoints: vec![LocalityLbEndpoints {
                lb_endpoints: vec![Endpoint::new(address).into()],
                ..<_>::default()
            }],
            ..<_>::default()
        }
    }

    fn create_lds_filter_chain(filters: Vec<Filter>) -> FilterChain {
        FilterChain {
            name: "test-lds-filter-chain".into(),
            filters,
            ..<_>::default()
        }
    }

    fn create_lds_listener(name: String, filter_chains: Vec<FilterChain>) -> Listener {
        Listener {
            name,
            filter_chains,
            ..<_>::default()
        }
    }
}
