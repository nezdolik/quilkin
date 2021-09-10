package agones

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	agonesv1 "agones.dev/agones/pkg/apis/agones/v1"
	agones "agones.dev/agones/pkg/client/clientset/versioned"
	log "github.com/sirupsen/logrus"
	k8sv1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"quilkin.dev/xds-management-server/pkg/cluster"
)

// Provider implements the Provider interface, exposing Agones GameServers as endpoints.
type Provider struct {
	config       Config
	logger       *log.Logger
	agonesClient agones.Interface
}

// Config contains the Agones provider's configuration.
type Config struct {
	K8sConfig               *rest.Config
	GameServersNamespace    string
	GameServersPollInterval time.Duration
}

// NewProvider returns a new Provider instance.
func NewProvider(logger *log.Logger, config Config) (*Provider, error) {
	agonesClient, err := agones.NewForConfig(config.K8sConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Agones clientset")
	}

	return &Provider{
		agonesClient: agonesClient,
		logger:       logger,
		config:       config,
	}, nil
}

// CheckHealth implements health check.
func (p *Provider) CheckHealth(ctx context.Context) error {
	if _, err := p.agonesClient.
		AgonesV1().
		RESTClient().
		Get().
		AbsPath("/healthz").
		DoRaw(ctx); err != nil {
		return fmt.Errorf("agones cluster provider failed to reach k8s api: %w", err)
	}
	if _, err := p.agonesClient.
		AgonesV1().
		GameServers(p.config.GameServersNamespace).
		List(ctx, k8sv1.ListOptions{Limit: 1}); err != nil {
		return fmt.Errorf("agones cluster provider failed to list GameServers objects: %w", err)
	}
	return nil
}

// Run spawns a goroutine in the background that watches Agones GameServers
// and exposes them as endpoints via the returned Cluster channel.
func (p *Provider) Run(ctx context.Context) (<-chan []cluster.Cluster, error) {
	gameServerListWatch := cache.NewListWatchFromClient(
		p.agonesClient.AgonesV1().RESTClient(),
		"gameservers",
		p.config.GameServersNamespace,
		fields.Everything())

	gameServerInformer := cache.NewSharedInformer(gameServerListWatch, &agonesv1.GameServer{}, 0)
	gameServerStore := gameServerInformer.GetStore()
	_ = gameServerInformer.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
		if err == io.EOF {
			// The informer shutdown successfully.
			return
		}
		p.logger.WithError(err).Warn("GameServer SharedInformer encountered an error")
	})
	go gameServerInformer.Run(ctx.Done())

	clusterCh := make(chan []cluster.Cluster)

	go runClusterWatch(
		ctx,
		p.logger,
		p.config.GameServersPollInterval,
		gameServerStore,
		clusterCh)

	return clusterCh, nil
}

func runClusterWatch(
	ctx context.Context,
	logger *log.Logger,
	gameServersPollInterval time.Duration,
	gameServerStore cache.Store,
	clusterCh chan<- []cluster.Cluster,
) {
	defer close(clusterCh)

	ticker := time.NewTicker(gameServersPollInterval)
	defer ticker.Stop()

	prevEndpoints := map[string]cluster.Endpoint{}
	for {
		select {
		case <-ticker.C:
			currEndpoints := getEndpointsFromStore(logger, gameServerStore)
			if reflect.DeepEqual(currEndpoints, prevEndpoints) {
				continue
			}
			prevEndpoints = currEndpoints

			fmt.Printf("curr endpoints: %v\n", currEndpoints)
			endpoints := make([]cluster.Endpoint, 0)
			for _, ep := range currEndpoints {
				endpoints = append(endpoints, ep)
			}
			clusterCh <- []cluster.Cluster{{
				Name:      "default-quilkin-cluster",
				Endpoints: endpoints,
			}}
		case <-ctx.Done():
			logger.Debug("Exiting cluster watch loop: context cancelled")

		}
	}
}

func getEndpointsFromStore(
	logger *log.Logger,
	gameServerStore cache.Store,
) map[string]cluster.Endpoint {
	gameServers := gameServerStore.List()

	endpoints := make(map[string]cluster.Endpoint)
	for i := range gameServers {
		gs := gameServers[i].(*agonesv1.GameServer)

		gsLogger := logger.WithFields(log.Fields{
			"gameserver": gs.Name,
		})

		if gs.Status.State != agonesv1.GameServerStateAllocated {
			//continue
		}

		if gs.Status.Address == "" {
			continue
		}

		numPorts := len(gs.Status.Ports)
		if numPorts == 0 {
			continue
		}

		var metadata map[string]interface{}
		tokenStr, found := gs.Annotations["quilkin.dev/tokens"]
		if found {
			metadata = map[string]interface{}{
				"quilkin.dev": map[string]interface{}{
					"tokens": strings.Split(tokenStr, ","),
				},
			}
		}

		gsPort := int(getGameServerPort(gsLogger, gs.Status.Ports))
		endpoints[fmt.Sprintf("%s:%d", gs.Status.Address, gsPort)] = cluster.Endpoint{
			IP:       gs.Status.Address,
			Port:     gsPort,
			Metadata: metadata,
		}
	}

	return endpoints
}

func getGameServerPort(logger *log.Entry, ports []agonesv1.GameServerStatusPort) int32 {
	if len(ports) == 0 {
		return 0
	}

	if len(ports) == 1 {
		return ports[0].Port
	}

	for _, port := range ports {
		if port.Name == "default" {
			return port.Port
		}
	}

	logger.Warnf("found %d ports: will pick the first one %v", len(ports), ports[0])
	return ports[0].Port
}
