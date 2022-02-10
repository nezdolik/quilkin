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

package filterchain

import (
	"context"

	envoylistener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
)

// ProxyFilterChain represents a filter chain for a specific proxy.
type ProxyFilterChain struct {
	// ProxyID is the ID of the associated proxy.
	// this is the same as the XDS node id provided by the proxy
	// when it first connects.
	ProxyID string
	// FilterChain is the filter chain for the associated proxy.
	FilterChain *envoylistener.FilterChain
}

// Provider is an abstraction over the source of filter chains.
type Provider interface {
	// Run returns a channel that the server reads filter chain
	// updates from.
	Run(ctx context.Context) <-chan ProxyFilterChain
}
