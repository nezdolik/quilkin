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

use std::collections::HashMap;

use crate::{
    endpoint::{Endpoint, EndpointAddress},
    metadata::MetadataView,
    xds::config::endpoint::v3::lb_endpoint,
};

pub type ClusterLocalities = HashMap<Option<Locality>, LocalityEndpoints>;

impl TryFrom<crate::xds::config::endpoint::v3::ClusterLoadAssignment> for ClusterLocalities {
    type Error = eyre::Error;

    fn try_from(
        mut cla: crate::xds::config::endpoint::v3::ClusterLoadAssignment,
    ) -> Result<Self, Self::Error> {
        let mut existing_endpoints = HashMap::new();

        for lb_locality in cla.endpoints {
            let locality = lb_locality.locality.map(|locality| Locality {
                region: locality.region,
                zone: locality.zone,
                sub_zone: locality.sub_zone,
            });

            // Extract components of the endpoint that we care about.
            let mut endpoints = vec![];
            for (host_identifier, metadata) in
                lb_locality
                    .lb_endpoints
                    .into_iter()
                    .filter_map(|lb_endpoint| {
                        let metadata = lb_endpoint.metadata;
                        lb_endpoint
                            .host_identifier
                            .map(|host_identifier| (host_identifier, metadata))
                    })
            {
                let endpoint = match host_identifier {
                    lb_endpoint::HostIdentifier::Endpoint(endpoint) => Ok(endpoint),
                    lb_endpoint::HostIdentifier::EndpointName(name_reference) => {
                        match cla.named_endpoints.remove(&name_reference) {
                            Some(endpoint) => Ok(endpoint),
                            None => Err(eyre::eyre!(
                                "no endpoint found name reference {}",
                                name_reference
                            )),
                        }
                    }
                }?;

                // Extract the endpoint's address.
                let address: EndpointAddress = endpoint
                    .address
                    .and_then(|address| address.address)
                    .ok_or_else(|| eyre::eyre!("No address provided."))?
                    .try_into()?;

                endpoints.push(Endpoint::with_metadata(
                    address,
                    metadata
                        .map(MetadataView::try_from)
                        .transpose()?
                        .unwrap_or_default(),
                ));
            }

            existing_endpoints.insert(locality, LocalityEndpoints { endpoints });
        }

        Ok(existing_endpoints)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Locality {
    pub region: String,
    pub zone: String,
    pub sub_zone: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalityEndpoints {
    pub endpoints: Vec<Endpoint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cluster {
    pub localities: ClusterLocalities,
}

/// Represents a full snapshot of all clusters.
#[derive(Clone, Debug, Default)]
pub struct ClusterMap(HashMap<String, Cluster>);

impl From<HashMap<String, Cluster>> for ClusterMap {
    fn from(value: HashMap<String, Cluster>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for ClusterMap {
    type Target = HashMap<String, Cluster>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ClusterMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<const N: usize> From<[(String, Cluster); N]> for ClusterMap {
    fn from(value: [(String, Cluster); N]) -> Self {
        Self(value.into())
    }
}

impl FromIterator<(String, Cluster)> for ClusterMap {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (String, Cluster)>,
    {
        Self(iter.into_iter().collect())
    }
}
