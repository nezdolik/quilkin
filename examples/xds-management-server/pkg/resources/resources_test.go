package resources

import (
	"testing"

	envoycore "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyendpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	envoylistener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"quilkin.dev/xds-management-server/pkg/cluster"
	"quilkin.dev/xds-management-server/pkg/filterchain"
	"quilkin.dev/xds-management-server/pkg/filters"
	debugfilterv1alpha "quilkin.dev/xds-management-server/pkg/filters/debug/v1alpha1"
	ratelimitv1alpha "quilkin.dev/xds-management-server/pkg/filters/local_rate_limit/v1alpha1"
)

func TestMakeEndpoint(t *testing.T) {
	ep, err := makeEndpoint("cluster-a", []cluster.Endpoint{{
		IP:   "127.0.0.1",
		Port: 22,
		Metadata: map[string]interface{}{
			"key-1": map[string]interface{}{
				"key-1-a": "value-1",
			},
		},
	}})

	require.NoError(t, err)
	require.Len(t, ep.Endpoints, 1)
	require.Len(t, ep.Endpoints[0].LbEndpoints, 1)

	lbEndpoint := ep.Endpoints[0].LbEndpoints[0]
	endpoint := lbEndpoint.HostIdentifier.(*envoyendpoint.LbEndpoint_Endpoint)
	socketAddress := endpoint.Endpoint.Address.Address.(*envoycore.Address_SocketAddress).SocketAddress
	require.EqualValues(t, "127.0.0.1", socketAddress.Address)
	require.EqualValues(t, 22, socketAddress.PortSpecifier.(*envoycore.SocketAddress_PortValue).PortValue)

	md := lbEndpoint.Metadata.FilterMetadata
	require.Len(t, md, 1)
	value, found := md["key-1"]
	require.True(t, found)

	require.Len(t, value.Fields, 1)
	nestedValue, found := value.Fields["key-1-a"]
	require.True(t, found)

	require.EqualValues(t, "value-1", nestedValue.GetStringValue())
}

func TestMakeMultipleEndpointsInTheSameLocality(t *testing.T) {
	ep, err := makeEndpoint("cluster-a", []cluster.Endpoint{{
		IP:   "127.0.0.1",
		Port: 22,
	}, {
		IP:   "127.0.0.2",
		Port: 23,
	}})

	require.NoError(t, err)
	require.Len(t, ep.Endpoints, 1)
	require.Len(t, ep.Endpoints[0].LbEndpoints, 2)

	lbe1 := ep.Endpoints[0].LbEndpoints[0]
	lbe2 := ep.Endpoints[0].LbEndpoints[1]

	gotEp := lbe1.HostIdentifier.(*envoyendpoint.LbEndpoint_Endpoint)
	socketAddress := gotEp.Endpoint.Address.Address.(*envoycore.Address_SocketAddress).SocketAddress
	require.EqualValues(t, "127.0.0.1", socketAddress.Address)
	require.EqualValues(t, 22, socketAddress.PortSpecifier.(*envoycore.SocketAddress_PortValue).PortValue)

	gotEp = lbe2.HostIdentifier.(*envoyendpoint.LbEndpoint_Endpoint)
	socketAddress = gotEp.Endpoint.Address.Address.(*envoycore.Address_SocketAddress).SocketAddress
	require.EqualValues(t, "127.0.0.2", socketAddress.Address)
	require.EqualValues(t, 23, socketAddress.PortSpecifier.(*envoycore.SocketAddress_PortValue).PortValue)
}

func TestMakeEndpointWithoutMetadata(t *testing.T) {
	ep, err := makeEndpoint("cluster-a", []cluster.Endpoint{{
		IP:   "127.0.0.1",
		Port: 22,
	}})

	require.NoError(t, err)
	require.Len(t, ep.Endpoints, 1)
	require.Len(t, ep.Endpoints[0].LbEndpoints, 1)

	lbEndpoint := ep.Endpoints[0].LbEndpoints[0]
	endpoint := lbEndpoint.HostIdentifier.(*envoyendpoint.LbEndpoint_Endpoint)
	socketAddress := endpoint.Endpoint.Address.Address.(*envoycore.Address_SocketAddress).SocketAddress
	require.EqualValues(t, "127.0.0.1", socketAddress.Address)
	require.EqualValues(t, 22, socketAddress.PortSpecifier.(*envoycore.SocketAddress_PortValue).PortValue)

	require.Len(t, lbEndpoint.Metadata.FilterMetadata, 0)
}

func TestMakeCluster(t *testing.T) {
	cs, err := makeCluster(cluster.Cluster{
		Name: "cluster-1",
		Endpoints: []cluster.Endpoint{{
			IP:   "127.0.0.1",
			Port: 22,
		}},
	})

	require.NoError(t, err)
	require.Len(t, cs.LoadAssignment.Endpoints, 1)

	require.EqualValues(t, "cluster-1", cs.Name)

	lbEndpoint := cs.LoadAssignment.Endpoints[0].LbEndpoints[0]
	endpoint := lbEndpoint.HostIdentifier.(*envoyendpoint.LbEndpoint_Endpoint)
	socketAddress := endpoint.Endpoint.Address.Address.(*envoycore.Address_SocketAddress).SocketAddress
	require.EqualValues(t, "127.0.0.1", socketAddress.Address)
	require.EqualValues(t, 22, socketAddress.PortSpecifier.(*envoycore.SocketAddress_PortValue).PortValue)
}

func TestGenerateSnapshot(t *testing.T) {
	clusters := []cluster.Cluster{
		{
			Name: "cluster-1",
			Endpoints: []cluster.Endpoint{{
				IP:   "127.0.0.1",
				Port: 22,
			}},
		},
		{
			Name: "cluster-2",
			Endpoints: []cluster.Endpoint{{
				IP:   "127.0.0.3",
				Port: 23,
			}},
		},
	}

	debugFilter := func(t *testing.T, value string) *envoylistener.Filter {
		filter, err := filterchain.CreateXdsFilter(filters.DebugFilterName,
			&debugfilterv1alpha.Debug{
				Id: &wrapperspb.StringValue{Value: value},
			})
		require.NoError(t, err)

		return filter
	}
	rateLimitFilter := func(t *testing.T, value uint64) *envoylistener.Filter {
		filter, err := filterchain.CreateXdsFilter(filters.RateLimitFilterName,
			&ratelimitv1alpha.LocalRateLimit{
				MaxPackets: value,
			})
		require.NoError(t, err)

		return filter
	}

	snapshot, err := GenerateSnapshot(19, clusters, filterchain.ProxyFilterChain{
		ProxyID: "proxy-1",
		FilterChains: []*envoylistener.FilterChain{
			{
				FilterChainMatch: &envoylistener.FilterChainMatch{
					ApplicationProtocols: []string{"abc", "xyz"},
				},
				Filters: []*envoylistener.Filter{
					debugFilter(t, "hello"), rateLimitFilter(t, 400),
				},
			},
			{
				FilterChainMatch: &envoylistener.FilterChainMatch{
					ApplicationProtocols: []string{"123", "456"},
				},
				Filters: []*envoylistener.Filter{
					debugFilter(t, "world"), rateLimitFilter(t, 500),
				},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, snapshot.Consistent())

	for _, rsc := range snapshot.Resources {
		require.EqualValues(t, "19", rsc.Version)
	}

	// Cluster
	clusterResource := snapshot.Resources[1]
	require.Len(t, clusterResource.Items, 2)

	cluster1, found := clusterResource.Items["cluster-1"]
	require.True(t, found, "cluster-1 is missing from map")
	cluster2, found := clusterResource.Items["cluster-2"]
	require.True(t, found, "cluster-2 is missing from map")

	require.Contains(t, cluster1.Resource.String(), "127.0.0.1")
	require.Contains(t, cluster2.Resource.String(), "127.0.0.3")

	// Listener
	listenerResource := snapshot.Resources[3]
	require.Len(t, listenerResource.Items, 1)

	filterChain, found := listenerResource.Items[""]
	require.True(t, found, "missing default filter chain")

	require.Contains(t, filterChain.Resource.String(), filters.DebugFilterName)
	require.Contains(t, filterChain.Resource.String(), "hello")
	require.Contains(t, filterChain.Resource.String(), "world")

	require.Contains(t, filterChain.Resource.String(), filters.RateLimitFilterName)
	require.Contains(t, filterChain.Resource.String(), "max_packets:400")
	require.Contains(t, filterChain.Resource.String(), "max_packets:500")

	// Check the application protocols field we set versions in.
	for _, version := range []string{"abc", "xyz", "123", "456"} {
		require.Contains(t, filterChain.Resource.String(), version)
	}
}
