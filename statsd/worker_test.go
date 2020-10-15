package statsd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func initWorker(bufferSize int) (*bufferPool, *sender, *worker) {
	pool := newBufferPool(10, bufferSize, 5)
	// manually create the sender so the sender loop is not started. All we
	// need is the queue
	s := &sender{
		queue: make(chan *statsdBuffer, 10),
		pool:  pool,
	}

	w := newWorker(pool, s)
	return pool, s, w
}

func TestWorkerGauge(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: gauge,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_gauge",
		fvalue:     21,
		tags:       []string{"tag1", "tag2"},
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_gauge:21|g|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerCount(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: count,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_count",
		ivalue:     21,
		tags:       []string{"tag1", "tag2"},
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_count:21|c|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerHistogram(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: histogram,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_histogram",
		fvalue:     21,
		tags:       []string{"tag1", "tag2"},
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_histogram:21|h|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerDistribution(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: distribution,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_distribution",
		fvalue:     21,
		tags:       []string{"tag1", "tag2"},
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_distribution:21|d|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerSet(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: set,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_set",
		svalue:     "value:1",
		tags:       []string{"tag1", "tag2"},
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_set:value:1|s|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerTiming(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: timing,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_timing",
		fvalue:     1.2,
		tags:       []string{"tag1", "tag2"},
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_timing:1.200000|ms|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerHistogramAggregated(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: histogramAggregated,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_histogram",
		fvalues:    []float64{1.2},
		stags:      "tag1,tag2",
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_histogram:1.2|h|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerHistogramAggregatedMultiple(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: histogramAggregated,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_histogram",
		fvalues:    []float64{1.1, 2.2, 3.3, 4.4},
		stags:      "tag1,tag2",
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_histogram:1.1:2.2:3.3:4.4|h|#globalTags,globalTags2,tag1,tag2", string(data.buffer))

	// reducing buffer size so not all values fit in one packet
	_, s, w = initWorker(70)

	err = w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data = <-s.queue
	assert.Equal(t, "namespace.test_histogram:1.1:2.2|h|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
	data = <-s.queue
	assert.Equal(t, "namespace.test_histogram:3.3:4.4|h|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerDistributionAggregated(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: distributionAggregated,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_distribution",
		fvalues:    []float64{1.2},
		stags:      "tag1,tag2",
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_distribution:1.2|d|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}

func TestWorkerDistributionAggregatedMultiple(t *testing.T) {
	_, s, w := initWorker(100)

	m := metric{
		metricType: distributionAggregated,
		namespace:  "namespace.",
		globalTags: []string{"globalTags", "globalTags2"},
		name:       "test_distribution",
		fvalues:    []float64{1.1, 2.2, 3.3, 4.4},
		stags:      "tag1,tag2",
		rate:       1,
	}
	err := w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data := <-s.queue
	assert.Equal(t, "namespace.test_distribution:1.1:2.2:3.3:4.4|d|#globalTags,globalTags2,tag1,tag2", string(data.buffer))

	// reducing buffer size so not all values fit in one packet
	_, s, w = initWorker(72)

	err = w.processMetric(m)
	assert.Nil(t, err)

	w.flush()
	data = <-s.queue
	assert.Equal(t, "namespace.test_distribution:1.1:2.2|d|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
	data = <-s.queue
	assert.Equal(t, "namespace.test_distribution:3.3:4.4|d|#globalTags,globalTags2,tag1,tag2", string(data.buffer))
}
