/*
 * Copyright 2022 Google LLC
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

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::filters::{prelude::*, FilterChain};

/// SharedFilterChain creates and updates the filter chain.
#[derive(Clone, Debug, Default)]
pub struct SharedFilterChain {
    /// The current filter chain.
    filter_chain: Arc<ArcSwap<FilterChain>>,
}

impl SharedFilterChain {
    /// Updates the filter chain to set of filters provided, if valid.
    pub fn store(&self, filters: &[crate::config::Filter]) -> crate::Result<()> {
        self.store_chain(FilterChain::try_create(filters)?);
        Ok(())
    }

    pub(crate) fn store_chain(&self, chain: FilterChain) {
        self.filter_chain.store(Arc::new(chain));
    }

    /// Returns a projection into current filter chain. Useful for quick reads,
    /// see [`ArcSwap::load`] for further explanation.
    pub fn load(&self) -> arc_swap::Guard<Arc<FilterChain>> {
        self.filter_chain.load()
    }

    /// Returns a copy current filter chain. See [`ArcSwap::load_full`] for
    /// further explanation.
    pub fn load_full(&self) -> Arc<FilterChain> {
        self.filter_chain.load_full()
    }
}

impl Filter for SharedFilterChain {
    fn read(&self, ctx: ReadContext) -> Option<ReadResponse> {
        self.filter_chain.load().read(ctx)
    }

    fn write(&self, ctx: WriteContext) -> Option<WriteResponse> {
        self.filter_chain.load().write(ctx)
    }
}

impl From<FilterChain> for SharedFilterChain {
    fn from(chain: FilterChain) -> Self {
        Self {
            filter_chain: Arc::new(ArcSwap::new(Arc::new(chain))),
        }
    }
}

impl TryFrom<Vec<crate::config::Filter>> for SharedFilterChain {
    type Error = Error;

    fn try_from(filters: Vec<crate::config::Filter>) -> Result<Self, Self::Error> {
        Self::try_from(&filters[..])
    }
}

impl<const N: usize> TryFrom<&[crate::config::Filter; N]> for SharedFilterChain {
    type Error = Error;

    fn try_from(filters: &[crate::config::Filter; N]) -> Result<Self, Self::Error> {
        Self::try_from(&filters[..])
    }
}

impl<const N: usize> TryFrom<[crate::config::Filter; N]> for SharedFilterChain {
    type Error = Error;

    fn try_from(filters: [crate::config::Filter; N]) -> Result<Self, Self::Error> {
        Self::try_from(&filters[..])
    }
}

impl TryFrom<&[crate::config::Filter]> for SharedFilterChain {
    type Error = Error;

    fn try_from(filters: &[crate::config::Filter]) -> Result<Self, Self::Error> {
        Ok(Self {
            filter_chain: Arc::new(ArcSwap::new(Arc::new(FilterChain::try_create(filters)?))),
        })
    }
}

impl<'de> serde::Deserialize<'de> for SharedFilterChain {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        FilterChain::deserialize(de).map(Self::from)
    }
}

impl serde::Serialize for SharedFilterChain {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        self.filter_chain.load().serialize(ser)
    }
}

impl schemars::JsonSchema for SharedFilterChain {
    fn schema_name() -> String {
        FilterChain::schema_name()
    }
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        FilterChain::json_schema(gen)
    }

    fn is_referenceable() -> bool {
        FilterChain::is_referenceable()
    }
}

#[cfg(test)]
mod tests {
    use super::SharedFilterChain;
    use crate::filters::{Filter, ReadContext};

    use std::time::Duration;

    use crate::endpoint::{Endpoint, Endpoints, UpstreamEndpoints};
    use tokio::time::sleep;

    #[tokio::test]
    async fn dynamic_filter_manager_update_filter_chain() {
        let filter_chain = SharedFilterChain::default();
        let test_endpoints = Endpoints::new(vec![Endpoint::new("127.0.0.1:8080".parse().unwrap())]);
        let response = filter_chain.read(ReadContext::new(
            UpstreamEndpoints::from(test_endpoints.clone()),
            "127.0.0.1:8081".parse().unwrap(),
            vec![],
        ));
        assert!(response.is_some());

        filter_chain
            .store(&[crate::config::Filter {
                name: crate::filters::drop::NAME.into(),
                config: None,
            }])
            .unwrap();

        let mut num_iterations = 0;
        loop {
            if filter_chain
                .read(ReadContext::new(
                    UpstreamEndpoints::from(test_endpoints.clone()),
                    "127.0.0.1:8081".parse().unwrap(),
                    vec![],
                ))
                .is_none()
            {
                break;
            }

            num_iterations += 1;
            if num_iterations > 1000 {
                unreachable!("timed-out waiting for new filter chain to be applied");
            }

            sleep(Duration::from_millis(10)).await;
        }
    }
}
