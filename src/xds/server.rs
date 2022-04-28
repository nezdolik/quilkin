use std::{pin::Pin, sync::Arc};

use cached::{Cached, CachedAsync};
use futures::Stream;
use prometheus::Error as PrometheusError;
use tokio_stream::StreamExt;
use tracing::Instrument;
use tracing::{error, info, warn};

use crate::{
    config::Config,
    xds::{
        metrics::{ServerMetrics as Metrics, StreamConnectionMetrics},
        service::discovery::v3::{
            aggregated_discovery_service_server::AggregatedDiscoveryService, DeltaDiscoveryRequest,
            DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
        },
        DiscoveryServiceProvider, ResourceType,
    },
};

pub struct ControlPlane {
    metrics: Metrics,
    _config: Arc<Config>,
    provider: Arc<dyn DiscoveryServiceProvider>,
}

impl ControlPlane {
    /// Creates a new server for a [DiscoveryServiceProvider].
    pub fn new<P: DiscoveryServiceProvider + 'static>(config: Config, provider: P) -> Self {
        Self::try_new(config, provider).unwrap()
    }

    pub fn try_new<P: DiscoveryServiceProvider + 'static>(
        config: Config,
        provider: P,
    ) -> Result<Self, PrometheusError> {
        Self::from_arc(config, Arc::from(provider))
    }

    /// Creates a new server from a dynamic reference
    /// counted [DiscoveryServiceProvider].
    pub fn from_arc(
        config: Config,
        provider: Arc<dyn DiscoveryServiceProvider>,
    ) -> Result<Self, PrometheusError> {
        let config = Arc::new(config);
        let metrics = Metrics::new()?;
        if config.admin.is_some() {
            tokio::spawn(crate::admin::server(config.clone()));
        }
        Ok(Self {
            _config: config,
            metrics,
            provider,
        })
    }

    pub async fn stream_aggregated_resources(
        &self,
        mut streaming: Pin<Box<dyn Stream<Item = Result<DiscoveryRequest, tonic::Status>> + Send>>,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<DiscoveryResponse, tonic::Status>> + Send>>,
        tonic::Status,
    > {
        let message = streaming.next().await.ok_or_else(|| {
            error!("No message found");
            tonic::Status::invalid_argument("No message found")
        })??;

        if message.node.is_none() {
            error!("Node identifier was not found");
            return Err(tonic::Status::invalid_argument("Node identifier required"));
        }

        let node = message.node.clone().unwrap();
        let resource_type = message.type_url.parse()?;
        let provider = self.provider.clone();
        let metrics = self.metrics.clone();

        Ok(Box::pin(async_stream::try_stream! {
            let span = tracing::info_span!(
                "xds_server_stream",
                id = %message.node.as_ref().map(|node| &*node.id).unwrap_or_default(),
                r#type = %resource_type
            );
            let _guard = span.enter();

            let _conn_metrics = StreamConnectionMetrics::new(metrics.total_active_connections.clone());
            let chage_rate_metrics = metrics.change_rate.clone();

            // Short cache for inflight requests that have yet to be ACKed.
            let mut inflight_cache = cached::TimedCache::with_lifespan(5);
            let mut resource_versions = std::collections::HashMap::<ResourceType, u64>::new();
            let version = resource_versions.get(&resource_type).copied().unwrap_or_default();
            let mut response = match provider.discovery_request(&node.id, version, resource_type, &message.resource_names).in_current_span().await {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(?error, "Discovery request failed");
                    Err(tonic::Status::from(error))?;
                    return
                }
            };

            let nonce = uuid::Uuid::new_v4();
            response.nonce = nonce.to_string();
            tracing::info!(?response.type_url, "Response Type");

            inflight_cache.cache_set(nonce, response.clone());

            drop(_guard);
            yield response;

            while let Some(new_message) = streaming.next().await.transpose()? {
                let resource_type = new_message.type_url.parse::<ResourceType>()?;
                let mut version: u64 = resource_versions.get(&resource_type).copied().unwrap_or_default();
                let span = tracing::info_span!(
                    "xds_server_stream",
                    id = %new_message.node.as_ref().map(|node| &*node.id).unwrap_or_default(),
                    r#type = %resource_type,
                    version,
                );
                let _guard = span.enter();

                let mut response = if let Some(error) = &new_message.error_detail {
                    error!(?error, "NACK");
                    // Currently just resend previous discovery response.
                    let nonce = uuid::Uuid::parse_str(&new_message.response_nonce).map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
                    inflight_cache.try_get_or_set_with(nonce.clone(), ||{
                        provider.discovery_request(&node.id, version, resource_type, &message.resource_names)
                    }).await?.clone()
                } else {
                    if let Ok(uuid) = uuid::Uuid::parse_str(&new_message.response_nonce) {
                        if inflight_cache.cache_get(&uuid).is_some() {
                            info!("ACK");
                            inflight_cache.cache_remove(&uuid);
                            continue;
                        } else {
                            warn!(node_id = &*node.id, ?resource_type, version=&*new_message.version_info, "Unknown nonce: could not be found in cache");
                            Err(tonic::Status::invalid_argument("Unknown nonce"))?;
                            continue;
                        }
                    } else {
                        tracing::info!("Getting new version");
                        version += 1;
                        let provider = provider.discovery_request(&node.id, version, resource_type, &new_message.resource_names).await?;
                        chage_rate_metrics.inc();
                        provider
                    }
                };

                resource_versions.insert(resource_type, version);
                response.nonce = uuid::Uuid::new_v4().to_string();
                inflight_cache.cache_set(nonce, response.clone());
                yield response;
            }
        }))
    }
}

#[tonic::async_trait]
impl AggregatedDiscoveryService for ControlPlane {
    type StreamAggregatedResourcesStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<DiscoveryResponse, tonic::Status>> + Send>>;
    type DeltaAggregatedResourcesStream =
        tokio_stream::wrappers::ReceiverStream<Result<DeltaDiscoveryResponse, tonic::Status>>;

    #[tracing::instrument(skip_all)]
    async fn stream_aggregated_resources(
        &self,
        request: tonic::Request<tonic::Streaming<DiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::StreamAggregatedResourcesStream>, tonic::Status> {
        let streaming = request.into_inner();

        Ok(tonic::Response::new(
            self.stream_aggregated_resources(Box::pin(streaming))
                .in_current_span()
                .await?,
        ))
    }

    async fn delta_aggregated_resources(
        &self,
        _request: tonic::Request<tonic::Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<tonic::Response<Self::DeltaAggregatedResourcesStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Quilkin doesn't currently support Delta xDS",
        ))
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::timeout;

    use super::*;
    use crate::{
        test_utils::TestProvider,
        xds::{
            config::{
                cluster::v3::{
                    cluster::{ClusterDiscoveryType, DiscoveryType},
                    Cluster,
                },
                core::v3::Node,
                endpoint::v3::{ClusterLoadAssignment, LocalityLbEndpoints},
                listener::v3::{FilterChain, Listener},
            },
            service::discovery::v3::DiscoveryResponse,
            ResourceType,
        },
    };

    const TIMEOUT_DURATION: std::time::Duration = std::time::Duration::from_secs(10);

    #[tokio::test]
    async fn valid_response() {
        const RESOURCE: ResourceType = ResourceType::Endpoint;
        const LISTENER_TYPE: ResourceType = ResourceType::Listener;

        let mut response = DiscoveryResponse {
            version_info: 0u8.to_string(),
            resources: vec![prost_types::Any {
                type_url: RESOURCE.type_url().into(),
                value: crate::prost::encode(&Cluster {
                    name: "quilkin".into(),
                    load_assignment: Some(ClusterLoadAssignment {
                        cluster_name: "quilkin".into(),
                        endpoints: vec![LocalityLbEndpoints { ..<_>::default() }],
                        ..<_>::default()
                    }),
                    cluster_discovery_type: Some(ClusterDiscoveryType::Type(
                        DiscoveryType::Static as i32,
                    )),
                    ..<_>::default()
                })
                .unwrap(),
            }],
            type_url: RESOURCE.type_url().into(),
            ..<_>::default()
        };

        let mut listener_response = DiscoveryResponse {
            version_info: 0u8.to_string(),
            resources: vec![prost_types::Any {
                type_url: LISTENER_TYPE.type_url().into(),
                value: crate::prost::encode(&Listener {
                    filter_chains: vec![FilterChain {
                        filters: vec![],
                        ..<_>::default()
                    }],
                    ..<_>::default()
                })
                .unwrap(),
            }],
            type_url: RESOURCE.type_url().into(),
            ..<_>::default()
        };

        let provider = TestProvider::new(<_>::from([
            (RESOURCE, response.clone()),
            (LISTENER_TYPE, listener_response.clone()),
        ]));
        let client = ControlPlane::new(<_>::default(), provider);
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        let mut request = DiscoveryRequest {
            node: Some(Node {
                id: "quilkin".into(),
                user_agent_name: "quilkin".into(),
                ..Node::default()
            }),
            resource_names: vec![],
            type_url: RESOURCE.type_url().into(),
            ..DiscoveryRequest::default()
        };

        let mut listener_request = DiscoveryRequest {
            node: Some(Node {
                id: "quilkin".into(),
                user_agent_name: "quilkin".into(),
                ..Node::default()
            }),
            resource_names: vec![],
            type_url: LISTENER_TYPE.type_url().into(),
            ..DiscoveryRequest::default()
        };

        timeout(TIMEOUT_DURATION, tx.send(Ok(request.clone())))
            .await
            .unwrap()
            .unwrap();

        let mut stream = timeout(
            TIMEOUT_DURATION,
            client.stream_aggregated_resources(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            )),
        )
        .await
        .unwrap()
        .unwrap();

        let message = timeout(TIMEOUT_DURATION, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        response.nonce = message.nonce.clone();
        request.response_nonce = message.nonce.clone();

        assert_eq!(response, message);

        timeout(TIMEOUT_DURATION, tx.send(Ok(request.clone())))
            .await
            .unwrap()
            .unwrap();

        timeout(TIMEOUT_DURATION, tx.send(Ok(listener_request.clone())))
            .await
            .unwrap()
            .unwrap();

        let message = timeout(TIMEOUT_DURATION, stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        listener_response.nonce = message.nonce.clone();
        listener_request.response_nonce = message.nonce.clone();

        assert_eq!(listener_response, message);

        timeout(TIMEOUT_DURATION, tx.send(Ok(listener_request.clone())))
            .await
            .unwrap()
            .unwrap();
    }
}
