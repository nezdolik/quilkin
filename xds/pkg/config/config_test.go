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

package config

import (
    "testing"
	"github.com/stretchr/testify/require"
)

func TestMakeFilterChain(t *testing.T) {
    const (
        debugId string = "quilkin.extensions.filters.debug.v1alpha1.Debug"
        rateLimitId string = "quilkin.extensions.filters.rate_limit.v1alpha1.RateLimit"
    )

	dbgFilter := Filter{
        Name: debugId,
        Config: &map[string]interface{}{
            "id": "hello",
        },
    }

	rateLimitFilter := Filter{
        Name: rateLimitId,
        Config: &map[string]interface{}{
            "max_packets": 400,
        },
    }

	got, err := FilterChainFromJson([]Filter{dbgFilter, rateLimitFilter})
    require.NoError(t, err)

	require.EqualValues(t, "", got.Name)
	require.Len(t, got.Filters, 2)

	require.EqualValues(t, debugId, got.Filters[0].Name)
	require.EqualValues(t, rateLimitId, got.Filters[1].Name)

	require.Contains(t, got.Filters[0].String(), "id:{value:\"hello\"}")
	require.Contains(t, got.Filters[1].String(), "max_packets:400")
}

func TestMakeFilterChainInvalid(t *testing.T) {
    tests := []struct {name string; config Filter}{

		{
			name: "invalid filter config",
			config: Filter{
                Name: "quilkin.extensions.filters.debug.v1alpha1.Debug",
                Config: &map[string]interface{}{
                    "id": "hello",
                },
            },
		},
		{
			name: "missing proto",
			config: Filter{
                Name: "quilkin.extensions.filters.debug.v1alpha1.Debug2",
                Config: &map[string]interface{}{
                    "id": "hello",
                },
            },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FilterChainFromJson([]Filter{tt.config})
			require.Error(t, err)
		})
	}
}
