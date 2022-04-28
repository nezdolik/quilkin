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

use std::{collections::HashMap, sync::Arc, time::Duration};

use rand::Rng;
use tokio::sync::{mpsc, watch, RwLock};
use tonic::transport::{channel::Channel as TonicChannel, Error as TonicError};
use tracing::Instrument;
use tryhard::{
    backoff_strategies::{BackoffStrategy, ExponentialBackoff},
    RetryFutureConfig, RetryPolicy,
};

use crate::{
    config::Config,
    xds::{
        config::core::v3::Node,
        metrics::Metrics,
        service::discovery::v3::{
            aggregated_discovery_service_client::AggregatedDiscoveryServiceClient, DiscoveryRequest,
        },
        Resource, ResourceType,
    },
    Result,
};

/// Client that can talk to an XDS server using the aDS protocol.
#[derive(Clone)]
pub struct Client {
    client: AggregatedDiscoveryServiceClient<TonicChannel>,
    config: Arc<Config>,
    metrics: Metrics,
}

impl Client {
    #[tracing::instrument(skip_all, fields(servers = ?config.management_servers.load().iter().map(|server| &server.address).collect::<Vec<_>>()))]
    pub async fn connect(config: Arc<Config>) -> Result<Self> {
        const BACKOFF_INITIAL_DELAY_MILLISECONDS: u64 = 500;
        const BACKOFF_MAX_DELAY_SECONDS: u64 = 30;
        const BACKOFF_MAX_JITTER_MILLISECONDS: u64 = 2000;

        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(BACKOFF_INITIAL_DELAY_MILLISECONDS));
        let max_delay = Duration::from_secs(BACKOFF_MAX_DELAY_SECONDS);

        let retry_config = RetryFutureConfig::new(u32::MAX).custom_backoff(|attempt, error: &_| {
            tracing::info!(attempt, "Retrying to connect");
            // reset after success
            if attempt <= 1 {
                backoff = ExponentialBackoff::new(Duration::from_millis(
                    BACKOFF_INITIAL_DELAY_MILLISECONDS,
                ));
            }

            // max delay + jitter of up to 2 seconds
            let mut delay = backoff.delay(attempt, &error);
            if delay > max_delay {
                delay = max_delay;
            }
            delay += Duration::from_millis(
                rand::thread_rng().gen_range(0..BACKOFF_MAX_JITTER_MILLISECONDS),
            );

            match error {
                RpcSessionError::InitialConnect(ref error) => {
                    tracing::error!(?error, "Unable to connect to the XDS server");

                    // Do not retry if this is an invalid URL error that we cannot recover from.
                    // Need to use {:?} as the Display output only returns 'transport error'
                    let err_description = format!("{error:?}");
                    if err_description.to_lowercase().contains("invalid url") {
                        RetryPolicy::Break
                    } else {
                        RetryPolicy::Delay(delay)
                    }
                }

                RpcSessionError::Receive(ref status) => {
                    tracing::error!(status = ?status, "Failed to receive response from XDS server");
                    RetryPolicy::Delay(delay)
                }
            }
        });

        let management_servers = config.management_servers.load();
        let mut addresses = management_servers
            .iter()
            .cycle()
            .map(|server| server.address.clone());
        let connect_to_server = tryhard::retry_fn(|| {
            let address = addresses.next();
            async {
                match address {
                    None => Err(RpcSessionError::Receive(tonic::Status::internal(
                        "Failed initial connection",
                    ))),
                    Some(endpoint) => AggregatedDiscoveryServiceClient::connect(endpoint)
                        .instrument(tracing::debug_span!(
                            "AggregatedDiscoveryServiceClient::connect"
                        ))
                        .await
                        .map_err(RpcSessionError::InitialConnect),
                }
            }
        })
        .with_config(retry_config);
        let client = connect_to_server
            .instrument(tracing::info_span!("xds_client_connect"))
            .await?;
        tracing::info!("Connected to xDS server");
        Ok(Self {
            client,
            config,
            metrics: Metrics::new()?,
        })
    }

    /// Starts a new stream to the xDS management server.
    pub async fn stream(&self) -> Result<Stream> {
        Stream::connect(self.clone()).await
    }
}

/// An active xDS gRPC management stream.
pub struct Stream {
    metrics: Metrics,
    config: Arc<Config>,
    requests: mpsc::UnboundedSender<DiscoveryRequest>,
    responses: tokio::task::JoinHandle<Result<()>>,
    in_flight_requests:
        Arc<RwLock<HashMap<ResourceType, (watch::Sender<()>, watch::Receiver<()>)>>>,
}

impl Stream {
    #[tracing::instrument(skip_all)]
    async fn connect(xds: Client) -> Result<Self> {
        let (requests, rx) = tokio::sync::mpsc::unbounded_channel();
        let Client {
            mut client,
            metrics,
            config,
        } = xds;

        let in_flight_requests = Arc::<RwLock<HashMap<_, (watch::Sender<()>, _)>>>::default();
        let responses = tokio::spawn({
            let in_flight_requests = in_flight_requests.clone();
            let metrics = metrics.clone();
            let requests = requests.clone();
            let config = config.clone();
            async move {
                let mut responses = client
                    .stream_aggregated_resources(
                        tokio_stream::wrappers::UnboundedReceiverStream::from(rx),
                    )
                    .in_current_span()
                    .await?
                    .into_inner();

                // We are now connected to the server.
                metrics.connected_state.set(1);

                while let Some(response) = responses.message().await? {
                    tracing::debug!(
                        id = &*response.version_info,
                        r#type = &*response.type_url,
                        "Received response"
                    );
                    metrics.requests_total.inc();
                    let resources = response
                        .resources
                        .iter()
                        .cloned()
                        .map(Resource::try_from)
                        .collect::<Result<Vec<_>, _>>()?;
                    let resource_type = response.type_url.parse()?;

                    let mut request = DiscoveryRequest::try_from(response)?;
                    if let Err(error) = resources
                        .iter()
                        .map(|resource| config.apply(resource))
                        .collect::<Result<(), _>>()
                    {
                        metrics.update_failure_total.inc();
                        request.error_detail = Some(crate::xds::google::rpc::Status {
                            code: 3,
                            message: error.to_string(),
                            ..<_>::default()
                        });
                    } else {
                        metrics.update_success_total.inc();

                        if let Some((sender, _)) = in_flight_requests
                            .write()
                            .in_current_span()
                            .await
                            .get(&resource_type)
                        {
                            sender.send(())?;
                        }
                    }

                    requests.send(request)?;
                }

                Ok(())
            }
            .instrument(tracing::info_span!("handle_discovery_response"))
        });

        Ok(Self {
            config,
            metrics,
            requests,
            responses,
            in_flight_requests,
        })
    }

    #[tracing::instrument(skip(self))]
    pub async fn send(
        &mut self,
        resource_type: ResourceType,
        names: &[String],
    ) -> Result<watch::Receiver<()>> {
        let request = DiscoveryRequest {
            node: Some(Node {
                id: self.config.proxy.id.clone(),
                user_agent_name: "quilkin".into(),
                ..Node::default()
            }),
            resource_names: names.to_vec(),
            type_url: resource_type.type_url().into(),
            ..DiscoveryRequest::default()
        };

        let recv = {
            self.in_flight_requests
                .read()
                .await
                .get(&resource_type)
                .map(|(_, rx)| rx.clone())
        };

        let recv = if let Some(recv) = recv {
            recv
        } else {
            let (tx, rx) = watch::channel(());
            self.in_flight_requests
                .write()
                .await
                .insert(resource_type, (tx, rx.clone()));
            rx
        };

        tracing::info!("sending discovery request");
        self.requests.send(request)?;

        Ok(recv)
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // We are no longer connected.
        self.metrics.connected_state.set(0);
        self.responses.abort();
    }
}

#[derive(Debug, thiserror::Error)]
enum RpcSessionError {
    #[error("Failed to establish initial connection.\n {0:?}")]
    InitialConnect(TonicError),

    #[error("Error occured while receiving data. Status: {0}")]
    Receive(tonic::Status),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    /// If we get an invalid URL, we should return immediately rather
    /// than backoff or retry.
    async fn invalid_url() {
        let config = crate::Config::builder()
            .management_servers(["localhost:18000".into()])
            .build()
            .unwrap();
        let run = Client::connect(Arc::new(config));

        let execution_result =
            tokio::time::timeout(std::time::Duration::from_millis(10000), run).await;
        assert!(execution_result
            .expect("client should bail out immediately")
            .is_err());
    }
}
