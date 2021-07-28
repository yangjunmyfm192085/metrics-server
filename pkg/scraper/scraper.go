// Copyright 2018 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scraper

import (
	"context"
	"fmt"
	//"math/rand"
	"sync"
	"time"

	"sigs.k8s.io/metrics-server/pkg/scraper/client"

	corev1 "k8s.io/api/core/v1"
	//"k8s.io/apimachinery/pkg/labels"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"

	"sigs.k8s.io/metrics-server/pkg/storage"
)

const (
	maxDelayMs       = 4 * 1000
	delayPerSourceMs = 8
)

var (
	requestDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace: "metrics_server",
			Subsystem: "kubelet",
			Name:      "request_duration_seconds",
			Help:      "Duration of requests to Kubelet API in seconds",
			Buckets:   metrics.DefBuckets,
		},
		[]string{"node"},
	)
	requestTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace: "metrics_server",
			Subsystem: "kubelet",
			Name:      "request_total",
			Help:      "Number of requests sent to Kubelet API",
		},
		[]string{"success"},
	)
	lastRequestTime = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace: "metrics_server",
			Subsystem: "kubelet",
			Name:      "last_request_time_seconds",
			Help:      "Time of last request performed to Kubelet API since unix epoch in seconds",
		},
		[]string{"node"},
	)
)

// RegisterScraperMetrics registers rate, errors, and duration metrics on
// Kubelet API scrapes.
func RegisterScraperMetrics(registrationFunc func(metrics.Registerable) error) error {
	for _, metric := range []metrics.Registerable{
		requestDuration,
		requestTotal,
		lastRequestTime,
	} {
		err := registrationFunc(metric)
		if err != nil {
			return err
		}
	}
	return nil
}

//func NewScraper(nodeLister v1listers.NodeLister, client client.KubeletMetricsInterface, scrapeTimeout time.Duration) *scraper {
//	return &scraper{
//		nodeLister:    nodeLister,
//		kubeletClient: client,
//		scrapeTimeout: scrapeTimeout,
//	}
//}

func NewManageNodeScraper(client client.KubeletMetricsInterface, scrapeTimeout time.Duration, metricResolution time.Duration, store storage.Storage) *manageNodeScraper {
	return &manageNodeScraper{
		kubeletClient:    client,
		scrapeTimeout:    scrapeTimeout,
		metricResolution: metricResolution,
		stop:             map[apitypes.UID]chan struct{}{},
		isWorking:        map[apitypes.UID]bool{},
		storage:          store,
	}
}

//type scraper struct {
//	nodeLister    v1listers.NodeLister
//	kubeletClient client.KubeletMetricsInterface
//	scrapeTimeout time.Duration
//}

type manageNodeScraper struct {
	nodeLock         sync.Mutex
	kubeletClient    client.KubeletMetricsInterface
	scrapeTimeout    time.Duration
	metricResolution time.Duration
	// Tracks all running per-node goroutines - per-node goroutine will be
	// processing updates received through its corresponding channel.
	stop map[apitypes.UID]chan struct{}
	// Track the current state of per-node goroutines.
	// Currently all update request for a given node coming when another
	// update of this node is being processed are ignored.
	isWorking map[apitypes.UID]bool

	storage storage.Storage
}

//var _ Scraper = (*scraper)(nil)

// NodeInfo contains the information needed to identify and connect to a particular node
// (node name and preferred address).
type NodeInfo struct {
	Name           string
	ConnectAddress string
}

func (m *manageNodeScraper) AddNodeScraper(node *corev1.Node) error {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	if working, exists := m.isWorking[node.UID]; exists && working {
		klog.V(1).ErrorS(fmt.Errorf("Scrape in node is already running"), "node", klog.KObj(node))
		return fmt.Errorf("Scrape in node is already running, node:%v", node)
	}
	klog.V(1).InfoS("Start scrape metrics from node", "node", klog.KObj(node))
	if _, exists := m.stop[node.UID]; !exists {
		m.stop[node.UID] = make(chan struct{})
	}
	go func() {
		// make the timeout a bit shorter to account for staggering, so we still preserve
		// the overall timeout
		ticker := time.NewTicker(m.metricResolution)
		defer ticker.Stop()
		m.ScrapeAndStoreData(node, time.Now())
		for {
			select {
			case startTime := <-ticker.C:
				m.ScrapeAndStoreData(node, startTime)
			case <-m.stop[node.UID]:
				klog.V(1).InfoS("Scrape metrics from node exist", "node", klog.KObj(node))
				return
			}
		}
	}()
	if _, exists := m.isWorking[node.UID]; !exists || !m.isWorking[node.UID] {
		m.isWorking[node.UID] = true
	}
	return nil
}
func (m *manageNodeScraper) ScrapeAndStoreData(node *corev1.Node, startTime time.Time) error {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	klog.V(6).InfoS("Scraping metrics from node", "node", klog.KObj(node))
	ctx, cancelTimeout := context.WithTimeout(baseCtx, m.scrapeTimeout)
	defer cancelTimeout()
	defer func() {
		requestDuration.WithLabelValues(node.Name).Observe(float64(myClock.Since(startTime)) / float64(time.Second))
		lastRequestTime.WithLabelValues(node.Name).Set(float64(myClock.Now().Unix()))
	}()
	ms, err := m.kubeletClient.GetMetrics(ctx, node)
	if err != nil {
		klog.ErrorS(err, "Failed to scrape node", "node", klog.KObj(node))
		return err
	}
	klog.InfoS("", "ms", ms)
	res := &storage.MetricsBatch{
		Nodes: map[string]storage.MetricsPoint{},
		Pods:  map[apitypes.NamespacedName]storage.PodMetricsPoint{},
	}
	for nodeName, nodeMetricsPoint := range ms.Nodes {
		if _, nodeFind := res.Nodes[nodeName]; nodeFind {
			klog.ErrorS(nil, "Got duplicate node point", "node", klog.KRef("", nodeName))
			continue
		}
		res.Nodes[nodeName] = nodeMetricsPoint
	}
	for podRef, podMetricsPoint := range ms.Pods {
		if _, podFind := res.Pods[podRef]; podFind {
			klog.ErrorS(nil, "Got duplicate pod point", "pod", klog.KRef(podRef.Namespace, podRef.Name))
			continue
		}
		res.Pods[podRef] = podMetricsPoint
	}
	klog.V(6).InfoS("Storing metrics from node", "node", klog.KObj(node))
	m.storage.Store(res)
	return nil
}
func (m *manageNodeScraper) UpdateNodeScraper(node *corev1.Node) error {
	if working, exists := m.isWorking[node.UID]; exists && working {
		klog.V(1).ErrorS(fmt.Errorf("Scrape in node is already running"), "node", klog.KObj(node))
		return fmt.Errorf("Scrape in node is already running, node:%v", node)
	} else {
		return m.AddNodeScraper(node)
	}
}

func (m *manageNodeScraper) DeleteNodeScraper(node *corev1.Node) error {
	m.nodeLock.Lock()
	defer m.nodeLock.Unlock()
	if working, exists := m.isWorking[node.UID]; exists && working {
		klog.V(1).InfoS("Stop scrape metrics from node", "node", klog.KObj(node))
		delete(m.isWorking, node.UID)
		if _, exists := m.stop[node.UID]; exists {
			m.stop[node.UID] <- struct{}{}
			delete(m.stop, node.UID)
		}
	}
	return nil
}

//func (c *scraper) Scrape(baseCtx context.Context) *storage.MetricsBatch {
//	nodes, err := c.nodeLister.List(labels.Everything())
//	if err != nil {
//		// report the error and continue on in case of partial results
//		klog.ErrorS(err, "Failed to list nodes")
//	}
//	klog.V(1).InfoS("Scraping metrics from nodes", "nodeCount", len(nodes))
//
//	responseChannel := make(chan *storage.MetricsBatch, len(nodes))
//	defer close(responseChannel)
//
//	startTime := myClock.Now()
//
//	// TODO(serathius): re-evaluate this code -- do we really need to stagger fetches like this?
//	delayMs := delayPerSourceMs * len(nodes)
//	if delayMs > maxDelayMs {
//		delayMs = maxDelayMs
//	}
//
//	for _, node := range nodes {
//		go func(node *corev1.Node) {
//			// Prevents network congestion.
//			sleepDuration := time.Duration(rand.Intn(delayMs)) * time.Millisecond
//			time.Sleep(sleepDuration)
//			// make the timeout a bit shorter to account for staggering, so we still preserve
//			// the overall timeout
//			ctx, cancelTimeout := context.WithTimeout(baseCtx, c.scrapeTimeout-sleepDuration)
//			defer cancelTimeout()
//			klog.V(2).InfoS("Scraping node", "node", klog.KObj(node))
//			m, err := c.collectNode(ctx, node)
//			if err != nil {
//				klog.ErrorS(err, "Failed to scrape node", "node", klog.KObj(node))
//			}
//			responseChannel <- m
//		}(node)
//	}
//
//	res := &storage.MetricsBatch{
//		Nodes: map[string]storage.MetricsPoint{},
//		Pods:  map[apitypes.NamespacedName]storage.PodMetricsPoint{},
//	}
//
//	for range nodes {
//		srcBatch := <-responseChannel
//		if srcBatch == nil {
//			continue
//		}
//		for nodeName, nodeMetricsPoint := range srcBatch.Nodes {
//			if _, nodeFind := res.Nodes[nodeName]; nodeFind {
//				klog.ErrorS(nil, "Got duplicate node point", "node", klog.KRef("", nodeName))
//				continue
//			}
//			res.Nodes[nodeName] = nodeMetricsPoint
//		}
//		for podRef, podMetricsPoint := range srcBatch.Pods {
//			if _, podFind := res.Pods[podRef]; podFind {
//				klog.ErrorS(nil, "Got duplicate pod point", "pod", klog.KRef(podRef.Namespace, podRef.Name))
//				continue
//			}
//			res.Pods[podRef] = podMetricsPoint
//		}
//	}
//
//	klog.V(1).InfoS("Scrape finished", "duration", myClock.Since(startTime), "nodeCount", len(res.Nodes), "podCount", len(res.Pods))
//	return res
//}

//func (c *scraper) collectNode(ctx context.Context, node *corev1.Node) (*storage.MetricsBatch, error) {
//	startTime := myClock.Now()
//	defer func() {
//		requestDuration.WithLabelValues(node.Name).Observe(float64(myClock.Since(startTime)) / float64(time.Second))
//		lastRequestTime.WithLabelValues(node.Name).Set(float64(myClock.Now().Unix()))
//	}()
//	ms, err := c.kubeletClient.GetMetrics(ctx, node)
//
//	if err != nil {
//		requestTotal.WithLabelValues("false").Inc()
//		return nil, err
//	}
//	requestTotal.WithLabelValues("true").Inc()
//	return ms, nil
//}

type clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

type realClock struct{}

func (realClock) Now() time.Time                  { return time.Now() }
func (realClock) Since(d time.Time) time.Duration { return time.Since(d) }

var myClock clock = &realClock{}
