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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	envoylistener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	"quilkin.dev/xds-management-server/pkg/cluster"
	"quilkin.dev/xds-management-server/pkg/config"
	"quilkin.dev/xds-management-server/pkg/filterchain"
	"sigs.k8s.io/yaml"

	"github.com/cenkalti/backoff"
	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
)

// FileProvider watches a file on disk for resources.
type FileProvider struct {
	configFilePath string
	proxyIDCh      <-chan string
}

// NewFileProvider creates a new FileProvider.
func NewFileProvider(configFilePath string, proxyIDCh chan string) *FileProvider {
	return &FileProvider{
		configFilePath: configFilePath,
		proxyIDCh:      proxyIDCh,
	}
}

// Run runs the FileProvider.
func (p *FileProvider) Run(ctx context.Context, logger *log.Logger) (
	<-chan []cluster.Cluster, <-chan filterchain.ProxyFilterChain, <-chan error) {
	clusterCh := make(chan []cluster.Cluster, 10)
	filterChainCh := make(chan filterchain.ProxyFilterChain, 10)
	resourcesCh := make(chan config.Json)

	errorCh := make(chan error)

	go runUpdater(
		ctx,
		logger,
		p.proxyIDCh,
		resourcesCh,
		clusterCh,
		filterChainCh)

	go runConfigFileProvider(
		ctx,
		logger,
		p.configFilePath,
		resourcesCh,
		errorCh)

	return clusterCh, filterChainCh, errorCh
}

func runUpdater(
	ctx context.Context,
	logger *log.Logger,
	proxyIDCh <-chan string,
	resourcesCh <-chan config.Json,
	clusterCh chan<- []cluster.Cluster,
	filterChainCh chan<- filterchain.ProxyFilterChain,
) {
	defer func() {
		close(clusterCh)
		close(filterChainCh)
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	proxyIDs := make(map[string]struct{})
	var pendingUpdate bool

	var currClusters []cluster.Cluster
	var currFilterChain *envoylistener.FilterChain

	for {
		select {
		case proxyID := <-proxyIDCh:
			proxyIDs[proxyID] = struct{}{}
		case json := <-resourcesCh:
			filterChain, err := config.FilterChainFromJson(json.Filters)
			if err != nil {
				logger.WithError(err).Warn("failed to create filter chain")
				continue
			}
			pendingUpdate = true
			currFilterChain = filterChain
			currClusters = json.Clusters
		case <-ticker.C:
			if !pendingUpdate {
				continue
			}

			clusterCh <- currClusters

			for proxyID := range proxyIDs {
				if currFilterChain != nil {
					filterChainCh <- filterchain.ProxyFilterChain{
						ProxyID:     proxyID,
						FilterChain: currFilterChain,
					}
				}
			}
		case <-ctx.Done():
			logger.Debug("Exiting update loop due to context cancelled.")
			return
		}
	}
}

func runConfigFileProvider(
	ctx context.Context,
	logger *log.Logger,
	configFilePath string,
	resourcesCh chan<- config.Json,
	errorCh chan<- error,
) {
	logger = logger.WithFields(log.Fields{
		"component":   "FileProvider",
		"config_file": configFilePath,
	}).Logger

	defer func() {
		close(resourcesCh)
		close(errorCh)
	}()

	reloadFileEventCh := make(chan struct{}, 1)
	defer close(reloadFileEventCh)

	// Reads the resource file from disk, parses and send the config to the receiver.
	reloadFile := func() {
		fileBytes, err := ioutil.ReadFile(configFilePath)
		if err != nil {
			log.WithError(err).Warn("failed to read resources config file")
			return
		}

		r := config.Json{}
		jsonBytes, err := yaml.YAMLToJSON(fileBytes)
		if err != nil {
			log.WithError(err).Warn("failed to convert file from YAML to JSON")
			return
		}

		if err := json.Unmarshal(jsonBytes, &r); err != nil {
			log.WithError(err).Warn("failed to YAML unmarshal resources config file")
			return
		}

		resourcesCh <- r
	}

	fileWatcherErrorCh := make(chan error)
	go runConfigFileWatch(
		ctx,
		logger,
		configFilePath,
		reloadFileEventCh,
		fileWatcherErrorCh)

	for {
		select {
		case <-reloadFileEventCh:
			reloadFile()
		case <-ctx.Done():
			logger.Debugf("Exiting: context cancelled")
			return
		case err := <-fileWatcherErrorCh:
			errorCh <- fmt.Errorf("failed to watch config file %s: %w", configFilePath, err)
			return
		}
	}
}

// runConfigFileWatch runs a loop that watches the config file
// for changes and sends an event on the provided channel on each change.
func runConfigFileWatch(
	ctx context.Context,
	base *log.Logger,
	configFilePath string,
	reloadFileEventCh chan<- struct{},
	errorCh chan<- error,
) {
	logger := base.WithFields(log.Fields{
		"component":   "ConfigFileWatcher",
		"config_file": configFilePath,
	})

	defer close(errorCh)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		errorCh <- fmt.Errorf("failed to create file watcher: %w", err)
		return
	}
	defer func() {
		if err := watcher.Close(); err != nil {
			logger.WithError(err).Warn("failed to close watcher successfully")
		}
	}()

	backOff := backoff.NewExponentialBackOff()
	err = backoff.Retry(func() error {
		if err := watcher.Add(configFilePath); err != nil {
			logger.WithError(err).Warnf("failed to watch file")
			return err
		}
		defer func() {
			if err := watcher.Remove(configFilePath); err != nil {
				logger.WithError(err).Warnf("failed to remove watch")
			}
		}()

		backOff.Reset()

		// Load the initial file contents.
		reloadFileEventCh <- struct{}{}

		for {
			select {
			case <-ctx.Done():
				logger.Debugf("Exiting: context cancelled")
				return nil
			case event, ok := <-watcher.Events:
				if !ok {
					logger.WithError(err).Warn("received watch error event")
					return err
				}

				if event.Op&fsnotify.Remove == fsnotify.Remove {
					return fmt.Errorf("resources file was removed")
				}
				if event.Op&fsnotify.Rename == fsnotify.Rename {
					return fmt.Errorf("resources file was renamed")
				}

				isCreate := event.Op&fsnotify.Create == fsnotify.Create
				isWrite := event.Op&fsnotify.Write == fsnotify.Write
				if !(isCreate || isWrite) {
					continue
				}

				// Wait for a bit before reading the file because potential race conditions
				//  between getting the event and the file updated actually being
				//  reflected on disk.
				time.Sleep(1 * time.Second)

				// Write event. We can reload the config file
				reloadFileEventCh <- struct{}{}
			}
		}

	}, backOff)
	if err != nil {
		errorCh <- err
	}
}
