package statsd

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type (
	countsMap       map[string]*countMetric
	gaugesMap       map[string]*gaugeMetric
	setsMap         map[string]*setMetric
	histogramMap    map[string]*histogramMetric
	distributionMap map[string]*distributionMetric
)

type aggregator struct {
	nbContextGauge        int32
	nbContextCount        int32
	nbContextSet          int32
	nbContextHistogram    int32
	nbContextDistribution int32

	countsM       sync.RWMutex
	gaugesM       sync.RWMutex
	setsM         sync.RWMutex
	histogramsM   sync.RWMutex
	distributionM sync.RWMutex

	gauges        gaugesMap
	counts        countsMap
	sets          setsMap
	histograms    histogramMap
	distributions distributionMap

	closed chan struct{}
	exited chan struct{}

	client *Client
}

type aggregatorMetrics struct {
	nbContext             int32
	nbContextGauge        int32
	nbContextCount        int32
	nbContextSet          int32
	nbContextHistogram    int32
	nbContextDistribution int32
}

func newAggregator(c *Client) *aggregator {
	return &aggregator{
		client:        c,
		counts:        countsMap{},
		gauges:        gaugesMap{},
		sets:          setsMap{},
		histograms:    histogramMap{},
		distributions: distributionMap{},
		closed:        make(chan struct{}),
		exited:        make(chan struct{}),
	}
}

func (a *aggregator) start(flushInterval time.Duration) {
	ticker := time.NewTicker(flushInterval)

	go func() {
		for {
			select {
			case <-ticker.C:
				a.sendMetrics()
			case <-a.closed:
				close(a.exited)
				return
			}
		}
	}()
}

func (a *aggregator) sendMetrics() {
	for _, m := range a.flushMetrics() {
		a.client.send(m)
	}
}

func (a *aggregator) stop() {
	close(a.closed)
	<-a.exited
	a.sendMetrics()
}

func (a *aggregator) flushTelemetryMetrics() *aggregatorMetrics {
	if a == nil {
		return nil
	}

	am := &aggregatorMetrics{
		nbContextGauge:        atomic.SwapInt32(&a.nbContextGauge, 0),
		nbContextCount:        atomic.SwapInt32(&a.nbContextCount, 0),
		nbContextSet:          atomic.SwapInt32(&a.nbContextSet, 0),
		nbContextHistogram:    atomic.SwapInt32(&a.nbContextHistogram, 0),
		nbContextDistribution: atomic.SwapInt32(&a.nbContextDistribution, 0),
	}

	am.nbContext = am.nbContextGauge + am.nbContextCount + am.nbContextSet + am.nbContextHistogram + am.nbContextDistribution
	return am
}

func (a *aggregator) flushMetrics() []metric {
	metrics := []metric{}

	// We reset the values to avoid sending 'zero' values for metrics not
	// sampled during this flush interval

	a.setsM.Lock()
	sets := a.sets
	a.sets = setsMap{}
	a.setsM.Unlock()

	for _, s := range sets {
		metrics = append(metrics, s.flushUnsafe()...)
	}

	a.gaugesM.Lock()
	gauges := a.gauges
	a.gauges = gaugesMap{}
	a.gaugesM.Unlock()

	for _, g := range gauges {
		metrics = append(metrics, g.flushUnsafe())
	}

	a.countsM.Lock()
	counts := a.counts
	a.counts = countsMap{}
	a.countsM.Unlock()

	for _, c := range counts {
		metrics = append(metrics, c.flushUnsafe())
	}

	a.histogramsM.Lock()
	histograms := a.histograms
	a.histograms = histogramMap{}
	a.histogramsM.Unlock()

	for _, h := range histograms {
		metrics = append(metrics, h.flushUnsafe())
	}

	a.distributionM.Lock()
	distributions := a.distributions
	a.distributions = distributionMap{}
	a.distributionM.Unlock()

	for _, d := range distributions {
		metrics = append(metrics, d.flushUnsafe())
	}

	atomic.AddInt32(&a.nbContextCount, int32(len(counts)))
	atomic.AddInt32(&a.nbContextGauge, int32(len(gauges)))
	atomic.AddInt32(&a.nbContextSet, int32(len(sets)))
	atomic.AddInt32(&a.nbContextHistogram, int32(len(histograms)))
	atomic.AddInt32(&a.nbContextDistribution, int32(len(distributions)))
	return metrics
}

func getContext(name string, tags []string) string {
	return name + ":" + strings.Join(tags, tagSeparatorSymbol)
}

func getContextAndTags(name string, tags []string) (string, string) {
	stringTags := strings.Join(tags, tagSeparatorSymbol)
	return name + ":" + stringTags, stringTags
}

func (a *aggregator) count(name string, value int64, tags []string) error {
	context := getContext(name, tags)
	a.countsM.RLock()
	if count, found := a.counts[context]; found {
		count.sample(value)
		a.countsM.RUnlock()
		return nil
	}
	a.countsM.RUnlock()

	a.countsM.Lock()
	a.counts[context] = newCountMetric(name, value, tags)
	a.countsM.Unlock()
	return nil
}

func (a *aggregator) gauge(name string, value float64, tags []string) error {
	context := getContext(name, tags)
	a.gaugesM.RLock()
	if gauge, found := a.gauges[context]; found {
		gauge.sample(value)
		a.gaugesM.RUnlock()
		return nil
	}
	a.gaugesM.RUnlock()

	gauge := newGaugeMetric(name, value, tags)

	a.gaugesM.Lock()
	a.gauges[context] = gauge
	a.gaugesM.Unlock()
	return nil
}

func (a *aggregator) set(name string, value string, tags []string) error {
	context := getContext(name, tags)
	a.setsM.RLock()
	if set, found := a.sets[context]; found {
		set.sample(value)
		a.setsM.RUnlock()
		return nil
	}
	a.setsM.RUnlock()

	a.setsM.Lock()
	a.sets[context] = newSetMetric(name, value, tags)
	a.setsM.Unlock()
	return nil
}

func (a *aggregator) histogram(name string, value float64, tags []string) error {
	context, stringTags := getContextAndTags(name, tags)
	a.histogramsM.RLock()
	if histogram, found := a.histograms[context]; found {
		histogram.sample(value)
		a.histogramsM.RUnlock()
		return nil
	}
	a.histogramsM.RUnlock()

	a.histogramsM.Lock()
	a.histograms[context] = newHistogramMetric(name, value, stringTags)
	a.histogramsM.Unlock()
	return nil
}

func (a *aggregator) distribution(name string, value float64, tags []string) error {
	context, stringTags := getContextAndTags(name, tags)
	a.distributionM.RLock()
	if distribution, found := a.distributions[context]; found {
		distribution.sample(value)
		a.distributionM.RUnlock()
		return nil
	}
	a.distributionM.RUnlock()

	a.distributionM.Lock()
	a.distributions[context] = newDistributionMetric(name, value, stringTags)
	a.distributionM.Unlock()
	return nil
}
