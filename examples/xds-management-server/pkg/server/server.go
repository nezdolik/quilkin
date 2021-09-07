package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	discoveryservice "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	grpcMaxConcurrentStreams = 1000000
)

// HealthCheck implements the health check logic exposed by the server.
// It is implemented by internal components of the server.
type HealthCheck interface {
	// CheckHealth returns an error if the implementation is unhealthy.
	CheckHealth(ctx context.Context) error
}

// Server is a wrapper around go-control-plane's xds server.
type Server struct {
	logger *log.Logger
	// xdsPort is the port the xds server runs on.
	xdsPort int16
	// snapshotCache is passed updated by us and read by go-control-plane
	// to provided proxies with config from.
	snapshotCache cache.SnapshotCache
	// nodeIDCh is nilable. It is used as a callback mechanism from go-control-plane's server to
	// let us know when a new proxy has connected. The proxy's ID is passed on the channel.
	nodeIDCh chan<- string
	// healthCheck provides health check information exposed by the server.
	healthCheck HealthCheck
	// adminPort is the port the admin server runs on.
	adminPort int16
}

// NewServer returns a new Server
func New(
	logger *log.Logger,
	xdsPort int16,
	snapshotCache cache.SnapshotCache,
	nodeIDCh chan<- string,
	healthCheck HealthCheck,
	adminPort int16,
) *Server {
	return &Server{
		logger: logger.WithFields(log.Fields{
			"component": "server",
		}).Logger,
		xdsPort:       xdsPort,
		snapshotCache: snapshotCache,
		nodeIDCh:      nodeIDCh,
		healthCheck:   healthCheck,
		adminPort:     adminPort,
	}
}

func (s *Server) startAdminServer(ctx context.Context, address string) {
	srv := &http.Server{Addr: address}

	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		statusMessage := "OK"
		statusCode := http.StatusOK
		if err := s.healthCheck.CheckHealth(r.Context()); err != nil {
			statusCode = http.StatusInternalServerError
			statusMessage = err.Error()
		}

		w.WriteHeader(statusCode)
		_, writeErr := w.Write([]byte(statusMessage))
		if writeErr != nil {
			s.logger.WithError(writeErr).Warn("Failed to write /healthz response")
		}
	})

	go func() {
		s.logger.Infof("starting admin server on %s", address)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.WithError(err).Warn("Admin server shutdown prematurely")
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			s.logger.WithError(err).Warn("Admin server did not shut down properly")
		}
	}()
}

// Run starts a go-control-plane, xds server in a background goroutine.
// The server is bounded by the provided context.
func (s *Server) Run(ctx context.Context) error {

	cbs := &callbacks{log: s.logger, nodeIDCh: s.nodeIDCh}

	srv := server.NewServer(ctx, s.snapshotCache, cbs)

	var grpcOptions []grpc.ServerOption
	grpcOptions = append(grpcOptions, grpc.MaxConcurrentStreams(grpcMaxConcurrentStreams))
	grpcServer := grpc.NewServer(grpcOptions...)

	discoveryservice.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, srv)

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
		close(s.nodeIDCh)
	}()

	s.startAdminServer(ctx, fmt.Sprintf(":%d", s.adminPort))

	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", s.xdsPort))
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	log.Infof("management server listening on %d\n", s.xdsPort)

	go func() {
		if err = grpcServer.Serve(listen); err != nil {
			log.WithError(err).Warn("gRPC server returned an error")
		}
	}()

	return nil
}
