package statsd_test

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
)

func setupUDSClientServer(b *testing.B, options []statsd.Option) (*statsd.Client, net.Listener) {
	sockAddr := "/tmp/test.sock"
	if err := os.RemoveAll(sockAddr); err != nil {
		log.Fatal(err)
	}
	conn, err := net.Listen("unix", sockAddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go func() {
		for {
			_, err := conn.Accept()
			if err != nil {
				return
			}
		}
	}()
	client, err := statsd.New("unix://"+sockAddr, options...)
	if err != nil {
		b.Error(err)
	}
	return client, conn
}

func setupUDPClientServer(b *testing.B, options []statsd.Option) (*statsd.Client, *net.UDPConn) {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		b.Error(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		b.Error(err)
	}

	client, err := statsd.New(conn.LocalAddr().String(), options...)
	if err != nil {
		b.Error(err)
	}
	return client, conn
}

func setupClient(b *testing.B, transport string, extraOptions []statsd.Option) (*statsd.Client, io.Closer) {
	options := []statsd.Option{statsd.WithMaxMessagesPerPayload(1024), statsd.WithoutTelemetry()}
	options = append(options, extraOptions...)

	if transport == "udp" {
		return setupUDPClientServer(b, options)
	}
	return setupUDSClientServer(b, options)
}

func benchmarkStatsdDifferentMetrics(b *testing.B, transport string, extraOptions ...statsd.Option) {
	client, conn := setupClient(b, transport, extraOptions)
	defer conn.Close()

	n := int32(0)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		testNumber := atomic.AddInt32(&n, 1)
		name := fmt.Sprintf("test.metric%d", testNumber)
		for pb.Next() {
			client.Gauge(name, 1, []string{"tag:tag"}, 1)
		}
	})
	client.Flush()
	t := client.FlushTelemetryMetrics()
	reportMetric(b, float64(t.TotalDroppedOnReceive)/float64(t.TotalMetrics)*100, "%_dropRate")

	b.StopTimer()
	client.Close()
}

func benchmarkStatsdSameMetrics(b *testing.B, transport string, extraOptions ...statsd.Option) {
	client, conn := setupClient(b, transport, extraOptions)
	defer conn.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client.Gauge("test.metric", 1, []string{"tag:tag"}, 1)
		}
	})
	client.Flush()
	t := client.FlushTelemetryMetrics()
	reportMetric(b, float64(t.TotalDroppedOnReceive)/float64(t.TotalMetrics)*100, "%_dropRate")

	b.StopTimer()
	client.Close()
}

/*
UDP with the same metric
*/

// blocking + no aggregation
func BenchmarkStatsdUDPSameMetricBLocking(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "udp", statsd.WithBlockOnSendMode(), statsd.WithoutClientSideAggregation())
}

// dropping + no aggregation
func BenchmarkStatsdUDPSameMetricDropping(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "udp", statsd.WithDropOnSendMode(), statsd.WithoutClientSideAggregation())
}

// blocking + aggregation
func BenchmarkStatsdUDPSameMetricBLockingAggregation(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "udp", statsd.WithBlockOnSendMode(), statsd.WithClientSideAggregation())
}

// dropping + aggregation
func BenchmarkStatsdUDPSameMetricDroppingAggregation(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "udp", statsd.WithDropOnSendMode(), statsd.WithClientSideAggregation())
}

/*
UDP with the different metrics
*/

// blocking + no aggregation
func BenchmarkStatsdUDPDifferentMetricBlocking(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "udp", statsd.WithBlockOnSendMode(), statsd.WithoutClientSideAggregation())
}

// dropping + no aggregation
func BenchmarkStatsdUDPDifferentMetricDropping(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "udp", statsd.WithDropOnSendMode(), statsd.WithoutClientSideAggregation())
}

// blocking + aggregation
func BenchmarkStatsdUDPDifferentMetricBlockingAggregation(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "udp", statsd.WithBlockOnSendMode(), statsd.WithClientSideAggregation())
}

// dropping + aggregation
func BenchmarkStatsdUDPDifferentMetricDroppingAggregation(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "udp", statsd.WithDropOnSendMode(), statsd.WithClientSideAggregation())
}

/*
UDS with the same metric
*/
// blocking + no aggregation
func BenchmarkStatsdUDSSameMetricBLocking(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "uds", statsd.WithBlockOnSendMode(), statsd.WithoutClientSideAggregation())
}

// dropping + no aggregation
func BenchmarkStatsdUDSSameMetricDropping(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "uds", statsd.WithDropOnSendMode(), statsd.WithoutClientSideAggregation())
}

// blocking + aggregation
func BenchmarkStatsdUDSSameMetricBLockingAggregation(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "uds", statsd.WithBlockOnSendMode(), statsd.WithClientSideAggregation())
}

// dropping + aggregation
func BenchmarkStatsdUDSSameMetricDroppingAggregation(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "uds", statsd.WithDropOnSendMode(), statsd.WithClientSideAggregation())
}

/*
UDS with different metrics
*/
// blocking + no aggregation
func BenchmarkStatsdUDPSifferentMetricBlocking(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "uds", statsd.WithBlockOnSendMode(), statsd.WithoutClientSideAggregation())
}

// dropping + no aggregation
func BenchmarkStatsdUDSDifferentMetricDropping(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "uds", statsd.WithDropOnSendMode(), statsd.WithoutClientSideAggregation())
}

// blocking + aggregation
func BenchmarkStatsdUDPSifferentMetricBlockingAggregation(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "uds", statsd.WithBlockOnSendMode(), statsd.WithClientSideAggregation())
}

// dropping + aggregation
func BenchmarkStatsdUDSDifferentMetricDroppingAggregation(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "uds", statsd.WithDropOnSendMode(), statsd.WithClientSideAggregation())
}
