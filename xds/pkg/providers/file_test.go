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

package providers

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"quilkin.dev/xds-management-server/pkg/cluster"
	"quilkin.dev/xds-management-server/pkg/filterchain"
	"quilkin.dev/xds-management-server/pkg/filters"
	"sigs.k8s.io/yaml"
)

func testFileProvider(configFilePath string) (*FileProvider, chan<- string) {
	proxyIDCh := make(chan string)
	return NewFileProvider(configFilePath, proxyIDCh), proxyIDCh
}

func TestFileProviderRun(t *testing.T) {
	configFile, err := ioutil.TempFile("", "")
	require.NoError(t, err, "failed to create temp file")
	defer func() {
		_ = os.Remove(configFile.Name())
	}()

	filterConfigTestData := fmt.Sprintf(`
name: %s
config:
  id: hello
`, filters.DebugFilterName)
	filterConfigTestDataYaml := map[interface{}]interface{}{
		"typed_config": map[interface{}]interface{}{
			"@type": filters.DebugFilterName,
			"id":    "hello",
		},
	}
	require.NoError(t, yaml.Unmarshal([]byte(filterConfigTestData), filterConfigTestDataYaml), "failed to unmarshal test data filter config")

	logger := &log.Logger{}
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&log.TextFormatter{})
	logger.SetLevel(log.WarnLevel)

	type expectedFilterChain struct {
		ProxyID            string
		EachFilterContains []string
	}
	tests := []struct {
		name                 string
		config               string
		wantClusters         []cluster.Cluster
		wantProxyFilterChain expectedFilterChain
	}{
		{
			name: "add initial config",
			config: `
clusters:
- name: cluster-a
  endpoints:
  - ip: 127.0.0.1
    port: 8080
    metadata:
      'quilkin.dev':
        tokens:
        - MXg3aWp5Ng==
`,
			wantClusters: []cluster.Cluster{
				{
					Name: "cluster-a",
					Endpoints: []cluster.Endpoint{{
						IP:   "127.0.0.1",
						Port: 8080,
						Metadata: map[string]interface{}{
							"quilkin.dev": map[string]interface{}{
								"tokens": []interface{}{"MXg3aWp5Ng=="},
							},
						},
					}},
				},
			},
		},
		{
			name: "update config 1 - add new cluster",
			config: `
clusters:
- name: cluster-a
  endpoints:
  - ip: 127.0.0.1
    port: 8080
    metadata:
      'quilkin.dev':
        tokens:
        - MXg3aWp5Ng==
- name: cluster-b
  endpoints:
  - ip: 127.0.0.2
    port: 8082
`,
			wantClusters: []cluster.Cluster{
				{
					Name: "cluster-a",
					Endpoints: []cluster.Endpoint{{
						IP:   "127.0.0.1",
						Port: 8080,
						Metadata: map[string]interface{}{
							"quilkin.dev": map[string]interface{}{
								"tokens": []interface{}{"MXg3aWp5Ng=="},
							},
						},
					}},
				},
				{
					Name: "cluster-b",
					Endpoints: []cluster.Endpoint{{
						IP:   "127.0.0.2",
						Port: 8082,
					}},
				},
			},
		},
		{
			name: "update config 2 - remove cluster, add filter",
			config: fmt.Sprintf(`
clusters:
- name: cluster-b
  endpoints:
  - ip: 127.0.0.2
    port: 8082
filters:
- name: %s
  config:
    id: hello
`, filters.DebugFilterName),
			wantClusters: []cluster.Cluster{
				{
					Name: "cluster-b",
					Endpoints: []cluster.Endpoint{{
						IP:   "127.0.0.2",
						Port: 8082,
					}},
				},
			},
			wantProxyFilterChain: expectedFilterChain{
				ProxyID:            "proxy-1",
				EachFilterContains: []string{"hello"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			require.NoError(t, ioutil.WriteFile(configFile.Name(), []byte(tt.config), 0644))

			p, proxyIDCh := testFileProvider(configFile.Name())
			defer close(proxyIDCh)

			clusterCh, filterChainCh, errorCh := p.Run(ctx, logger)

			expectingFilterChainUpdate := tt.wantProxyFilterChain.ProxyID != ""
			if expectingFilterChainUpdate {
				proxyIDCh <- tt.wantProxyFilterChain.ProxyID
			}

			expectingClusters := len(tt.wantClusters) > 0

			var clusters []cluster.Cluster
			var filterChain filterchain.ProxyFilterChain

			wg := sync.WaitGroup{}

			if expectingFilterChainUpdate {
				wg.Add(1)
				go func() {
					filterChain = <-filterChainCh
					wg.Done()
				}()
			}

			if expectingClusters {
				wg.Add(1)
				go func() {
					clusters = <-clusterCh
					wg.Done()
				}()
			}

			waitCtx, waitCancel := context.WithCancel(ctx)
			go func() {
				wg.Wait()
				waitCancel()
			}()

			select {
			case <-waitCtx.Done():
				if expectingFilterChainUpdate {
					require.NotNil(t, filterChain.FilterChain)
					require.Len(t, filterChain.FilterChain.Filters, len(tt.wantProxyFilterChain.EachFilterContains))
					require.EqualValues(t, tt.wantProxyFilterChain.ProxyID, filterChain.ProxyID)
					for i, fc := range filterChain.FilterChain.Filters {
						require.Contains(t, fc.String(), tt.wantProxyFilterChain.EachFilterContains[i])
					}
				}

				require.EqualValues(t, tt.wantClusters, clusters)
			case err := <-errorCh:
				require.NoError(t, err, "received error from provider")
			}

			// Cancel the context to shutdown the provider.
			cancel()

			// Then check for any errors or unexpected resource updates.
			err, more := <-errorCh
			require.False(t, more, "received error from provider at shutdown: %v", err)

			clusterUpdate, more := <-clusterCh
			require.False(t, more, "received unexpected cluster update %v", clusterUpdate)

			filterChainUpdate, more := <-filterChainCh
			require.False(t, more, "received unexpected filter chain update %v", filterChainUpdate)
		})
	}
}
