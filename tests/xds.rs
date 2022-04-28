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

use quilkin::{
    config::Config,
    endpoint::{Endpoint, EndpointAddress},
    test_utils::TestHelper,
    xds::{
        config::{
            cluster::v3::{cluster::ClusterDiscoveryType, Cluster},
            endpoint::v3::{ClusterLoadAssignment, LocalityLbEndpoints},
            listener::v3::{filter::ConfigType, Filter, FilterChain, Listener},
        },
        service::discovery::v3::DiscoveryResponse,
        DiscoveryServiceProvider, ResourceType,
    },
};

tonic::include_proto!("quilkin.filters.concatenate_bytes.v1alpha1");

use concatenate_bytes::{Strategy, StrategyValue};

use prost::Message;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex},
    time,
};

const CLUSTER_TYPE: &str = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
const LISTENER_TYPE: &str = "type.googleapis.com/envoy.config.listener.v3.Listener";
