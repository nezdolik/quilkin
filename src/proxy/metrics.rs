/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use hyper::{Body, Response, StatusCode};
use prometheus::{Encoder, TextEncoder};

/// Metrics contains metrics configuration for the server.
#[derive(Clone)]
pub struct Metrics {
    pub(crate) registry: &'static prometheus::Registry,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = crate::metrics::registry();
        Metrics { registry }
    }

    pub fn collect_metrics(&self) -> Response<Body> {
        let mut response = Response::new(Body::empty());
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let body = encoder
            .encode(&self.registry.gather(), &mut buffer)
            .map_err(|error| tracing::warn!(%error, "Failed to encode metrics"))
            .and_then(|_| {
                String::from_utf8(buffer)
                    .map(Body::from)
                    .map_err(|error| tracing::warn!(%error, "Failed to convert metrics to utf8"))
            });

        match body {
            Ok(body) => {
                *response.body_mut() = body;
            }
            Err(_) => {
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            }
        };

        response
    }
}

#[cfg(test)]
mod tests {
    use hyper::StatusCode;

    use crate::proxy::Metrics;

    #[tokio::test]
    async fn collect_metrics() {
        let metrics = Metrics::new();
        let response = metrics.collect_metrics();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
