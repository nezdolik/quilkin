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

mod shared;

use prometheus::{exponential_buckets, Histogram};

use crate::{
    config::Filter as FilterConfig,
    filters::{prelude::*, FilterRegistry},
    metrics::{histogram_opts, CollectorExt},
};

const FILTER_LABEL: &str = "filter";

/// Start the histogram bucket at an eighth of a millisecond, as we bucketed the full filter
/// chain processing starting at a quarter of a millisecond, so we we will want finer granularity
/// here.
const BUCKET_START: f64 = 0.000125;

const BUCKET_FACTOR: f64 = 2.5;

/// At an exponential factor of 2.5 (BUCKET_FACTOR), 11 iterations gets us to just over half a
/// second. Any processing that occurs over half a second is far too long, so we end
/// the bucketing there as we don't care about granularity past this value.
const BUCKET_COUNT: usize = 11;

pub use shared::SharedFilterChain;

/// A chain of [`Filter`]s to be executed in order.
///
/// Executes each filter, passing the [`ReadContext`] and [`WriteContext`]
/// between each filter's execution, returning the result of data that has gone
/// through all of the filters in the chain. If any of the filters in the chain
/// return `None`, then the chain is broken, and `None` is returned.
#[derive(Default)]
pub struct FilterChain {
    filters: Vec<(String, FilterInstance)>,
    filter_read_duration_seconds: Vec<Histogram>,
    filter_write_duration_seconds: Vec<Histogram>,
}

impl FilterChain {
    pub fn new(filters: Vec<(String, FilterInstance)>) -> Result<Self, Error> {
        let subsystem = "filter";

        Ok(Self {
            filter_read_duration_seconds: filters
                .iter()
                .map(|(name, _)| {
                    Histogram::with_opts(
                        histogram_opts(
                            "read_duration_seconds",
                            subsystem,
                            "Seconds taken to execute a given filter's `read`.",
                            Some(
                                exponential_buckets(BUCKET_START, BUCKET_FACTOR, BUCKET_COUNT)
                                    .unwrap(),
                            ),
                        )
                        .const_label(FILTER_LABEL, name),
                    )
                    .and_then(|histogram| histogram.register_if_not_exists())
                })
                .collect::<Result<_, prometheus::Error>>()?,
            filter_write_duration_seconds: filters
                .iter()
                .map(|(name, _)| {
                    Histogram::with_opts(
                        histogram_opts(
                            "write_duration_seconds",
                            subsystem,
                            "Seconds taken to execute a given filter's `write`.",
                            Some(exponential_buckets(0.000125, 2.5, 11).unwrap()),
                        )
                        .const_label(FILTER_LABEL, name),
                    )
                    .and_then(|histogram| histogram.register_if_not_exists())
                })
                .collect::<Result<_, prometheus::Error>>()?,
            filters,
        })
    }

    /// Validates the filter configurations in the provided config and constructs
    /// a FilterChain if all configurations are valid.
    pub fn try_create(filter_configs: &[FilterConfig]) -> Result<Self, Error> {
        Self::try_from(filter_configs)
    }

    pub fn len(&self) -> usize {
        self.filters.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = crate::config::Filter> + '_ {
        self.filters
            .iter()
            .map(|(name, instance)| crate::config::Filter {
                name: name.clone(),
                config: match &*instance.config {
                    serde_json::Value::Null => None,
                    value => Some(value.clone()),
                },
            })
    }
}

impl std::fmt::Debug for FilterChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut filters = f.debug_struct("Filters");

        for (id, instance) in &self.filters {
            filters.field(id, &*instance.config);
        }

        filters.finish()
    }
}

impl<const N: usize> TryFrom<&[FilterConfig; N]> for FilterChain {
    type Error = Error;

    fn try_from(filter_configs: &[FilterConfig; N]) -> Result<Self, Error> {
        Self::try_from(&filter_configs[..])
    }
}

impl<const N: usize> TryFrom<[FilterConfig; N]> for FilterChain {
    type Error = Error;

    fn try_from(filter_configs: [FilterConfig; N]) -> Result<Self, Error> {
        Self::try_from(&filter_configs[..])
    }
}

impl TryFrom<Vec<FilterConfig>> for FilterChain {
    type Error = Error;

    fn try_from(filter_configs: Vec<FilterConfig>) -> Result<Self, Error> {
        Self::try_from(&filter_configs[..])
    }
}

impl TryFrom<&[FilterConfig]> for FilterChain {
    type Error = Error;

    fn try_from(filter_configs: &[FilterConfig]) -> Result<Self, Error> {
        let mut filters = Vec::new();

        for filter_config in filter_configs {
            let filter = FilterRegistry::get(
                &filter_config.name,
                CreateFilterArgs::fixed(filter_config.config.clone()),
            )?;

            filters.push((filter_config.name.clone(), filter));
        }

        Self::new(filters)
    }
}

impl<'de> serde::Deserialize<'de> for FilterChain {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let filters = <Vec<FilterConfig>>::deserialize(de)?;

        Self::try_from(filters).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for FilterChain {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let filters = self
            .filters
            .iter()
            .map(|(name, instance)| crate::config::Filter {
                name: name.clone(),
                config: Some(serde_json::Value::clone(&instance.config)),
            })
            .collect::<Vec<_>>();

        filters.serialize(ser)
    }
}

impl schemars::JsonSchema for FilterChain {
    fn schema_name() -> String {
        <Vec<FilterConfig>>::schema_name()
    }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        <Vec<FilterConfig>>::json_schema(gen)
    }

    fn is_referenceable() -> bool {
        <Vec<FilterConfig>>::is_referenceable()
    }
}

impl Filter for FilterChain {
    fn read(&self, ctx: ReadContext) -> Option<ReadResponse> {
        self.filters
            .iter()
            .zip(self.filter_read_duration_seconds.iter())
            .try_fold(ctx, |ctx, ((_, instance), histogram)| {
                Some(ReadContext::with_response(
                    ctx.source.clone(),
                    histogram.observe_closure_duration(|| instance.filter.read(ctx))?,
                ))
            })
            .map(ReadResponse::from)
    }

    fn write(&self, ctx: WriteContext) -> Option<WriteResponse> {
        self.filters
            .iter()
            .rev()
            .zip(self.filter_write_duration_seconds.iter().rev())
            .try_fold(ctx, |ctx, ((_, instance), histogram)| {
                Some(WriteContext::with_response(
                    ctx.endpoint,
                    ctx.source.clone(),
                    ctx.dest.clone(),
                    histogram.observe_closure_duration(|| instance.filter.write(ctx))?,
                ))
            })
            .map(WriteResponse::from)
    }
}

#[cfg(test)]
mod tests {
    use std::{str::from_utf8, sync::Arc};

    use crate::{
        config,
        endpoint::{Endpoint, Endpoints, UpstreamEndpoints},
        filters::Debug,
        test_utils::{new_test_config, TestFilter},
    };

    use super::*;

    #[test]
    fn from_config() {
        let provider = Debug::factory();

        // everything is fine
        let filter_configs = &[config::Filter {
            name: provider.name().into(),
            config: Some(serde_json::Map::default().into()),
        }];

        let chain = FilterChain::try_create(filter_configs).unwrap();
        assert_eq!(1, chain.filters.len());

        // uh oh, something went wrong
        let filter_configs = &[config::Filter {
            name: "this is so wrong".into(),
            config: Default::default(),
        }];
        let result = FilterChain::try_create(filter_configs);
        assert!(result.is_err());
    }

    fn endpoints() -> Vec<Endpoint> {
        vec![
            Endpoint::new("127.0.0.1:80".parse().unwrap()),
            Endpoint::new("127.0.0.1:90".parse().unwrap()),
        ]
    }

    fn upstream_endpoints(endpoints: Vec<Endpoint>) -> UpstreamEndpoints {
        Endpoints::new(endpoints).into()
    }

    #[test]
    fn chain_single_test_filter() {
        crate::test_utils::load_test_filters();
        let config = new_test_config();
        let endpoints_fixture = endpoints();

        let response = config
            .filters
            .read(ReadContext::new(
                upstream_endpoints(endpoints_fixture.clone()),
                "127.0.0.1:70".parse().unwrap(),
                b"hello".to_vec(),
            ))
            .unwrap();

        let expected = endpoints_fixture.clone();
        assert_eq!(
            expected,
            response.endpoints.iter().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            "hello:odr:127.0.0.1:70",
            from_utf8(response.contents.as_slice()).unwrap()
        );
        assert_eq!(
            "receive",
            response.metadata[&"downstream".to_string()]
                .as_string()
                .unwrap()
        );

        let response = config
            .filters
            .write(WriteContext::new(
                &endpoints_fixture[0],
                endpoints_fixture[0].address.clone(),
                "127.0.0.1:70".parse().unwrap(),
                b"hello".to_vec(),
            ))
            .unwrap();

        assert_eq!(
            "receive",
            response.metadata[&"upstream".to_string()]
                .as_string()
                .unwrap()
        );
        assert_eq!(
            "hello:our:127.0.0.1:80:127.0.0.1:70",
            from_utf8(response.contents.as_slice()).unwrap()
        );
    }

    #[test]
    fn chain_double_test_filter() {
        let chain = FilterChain::new(vec![
            (
                TestFilter::NAME.into(),
                FilterInstance {
                    config: Arc::new(serde_json::json!(null)),
                    filter: Box::new(TestFilter),
                },
            ),
            (
                TestFilter::NAME.into(),
                FilterInstance {
                    config: Arc::new(serde_json::json!(null)),
                    filter: Box::new(TestFilter),
                },
            ),
        ])
        .unwrap();

        let endpoints_fixture = endpoints();

        let response = chain
            .read(ReadContext::new(
                upstream_endpoints(endpoints_fixture.clone()),
                "127.0.0.1:70".parse().unwrap(),
                b"hello".to_vec(),
            ))
            .unwrap();

        let expected = endpoints_fixture.clone();
        assert_eq!(
            expected,
            response.endpoints.iter().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            "hello:odr:127.0.0.1:70:odr:127.0.0.1:70",
            from_utf8(response.contents.as_slice()).unwrap()
        );
        assert_eq!(
            "receive:receive",
            response.metadata[&"downstream".to_string()]
                .as_string()
                .unwrap()
        );

        let response = chain
            .write(WriteContext::new(
                &endpoints_fixture[0],
                endpoints_fixture[0].address.clone(),
                "127.0.0.1:70".parse().unwrap(),
                b"hello".to_vec(),
            ))
            .unwrap();
        assert_eq!(
            "hello:our:127.0.0.1:80:127.0.0.1:70:our:127.0.0.1:80:127.0.0.1:70",
            from_utf8(response.contents.as_slice()).unwrap()
        );
        assert_eq!(
            "receive:receive",
            response.metadata[&"upstream".to_string()]
                .as_string()
                .unwrap()
        );
    }

    #[test]
    fn get_configs() {
        struct TestFilter2;
        impl Filter for TestFilter2 {}

        let filter_chain = FilterChain::new(vec![
            (
                "TestFilter".into(),
                FilterInstance {
                    config: Arc::new(serde_json::json!(null)),
                    filter: Box::new(TestFilter),
                },
            ),
            (
                "TestFilter2".into(),
                FilterInstance {
                    config: Arc::new(serde_json::json!({
                        "k1": "v1",
                        "k2": 2
                    })),
                    filter: Box::new(TestFilter2),
                },
            ),
        ])
        .unwrap();

        let configs = filter_chain.iter().collect::<Vec<_>>();
        assert_eq!(
            vec![
                crate::config::Filter {
                    name: "TestFilter".into(),
                    config: None,
                },
                crate::config::Filter {
                    name: "TestFilter2".into(),
                    config: Some(serde_json::json!({
                        "k1": "v1",
                        "k2": 2
                    }))
                },
            ],
            configs
        )
    }
}
