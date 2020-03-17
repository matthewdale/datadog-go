package statsd

import (
	"math"
	"time"
)

var (
	// DefaultNamespace is the default value for the Namespace option
	DefaultNamespace = ""
	// DefaultTags is the default value for the Tags option
	DefaultTags = []string{}
	// DefaultMaxBytesPerPayload is the default value for the MaxBytesPerPayload option
	DefaultMaxBytesPerPayload = 0
	// DefaultMaxMessagesPerPayload is the default value for the MaxMessagesPerPayload option
	DefaultMaxMessagesPerPayload = math.MaxInt32
	// DefaultBufferPoolSize is the default value for the DefaultBufferPoolSize option
	DefaultBufferPoolSize = 0
	// DefaultBufferFlushInterval is the default value for the BufferFlushInterval option
	DefaultBufferFlushInterval = 100 * time.Millisecond
	// DefaultBufferShardCount is the default value for the BufferShardCount option
	DefaultBufferShardCount = 32
	// DefaultSenderQueueSize is the default value for the DefaultSenderQueueSize option
	DefaultSenderQueueSize = 0
	// DefaultWriteTimeoutUDS is the default value for the WriteTimeoutUDS option
	DefaultWriteTimeoutUDS = 1 * time.Millisecond
	// DefaultTelemetry is the default value for the Telemetry option
	DefaultTelemetry = true
	// DefaultSendingMode is the default behavior when sending metrics
	DefaultSendingMode = BlockOnSend
	// DefaultDropSendingModeBufferSizer is the default size of the channel holding incoming metrics
	DefaultDropSendingModeBufferSizer = 2048
	// DefaultAggregationFlushInterval is the default interval for the aggregator to flush metrics.
	DefaultAggregationFlushInterval = 3 * time.Second
	// DefaultAggregation
	DefaultAggregation = false
)

// Options contains the configuration options for a client.
type Options struct {
	// Namespace to prepend to all metrics, events and service checks name.
	Namespace string
	// Tags are global tags to be applied to every metrics, events and service checks.
	Tags []string
	// MaxBytesPerPayload is the maximum number of bytes a single payload will contain.
	// The magic value 0 will set the option to the optimal size for the transport
	// protocol used when creating the client: 1432 for UDP and 8192 for UDS.
	MaxBytesPerPayload int
	// MaxMessagesPerPayload is the maximum number of metrics, events and/or service checks a single payload will contain.
	// This option can be set to `1` to create an unbuffered client.
	MaxMessagesPerPayload int
	// BufferPoolSize is the size of the pool of buffers in number of buffers.
	// The magic value 0 will set the option to the optimal size for the transport
	// protocol used when creating the client: 2048 for UDP and 512 for UDS.
	BufferPoolSize int
	// BufferFlushInterval is the interval after which the current buffer will get flushed.
	BufferFlushInterval time.Duration
	// BufferShardCount is the number of buffer "shards" that will be used.
	// Those shards allows the use of multiple buffers at the same time to reduce
	// lock contention.
	BufferShardCount int
	// SenderQueueSize is the size of the sender queue in number of buffers.
	// The magic value 0 will set the option to the optimal size for the transport
	// protocol used when creating the client: 2048 for UDP and 512 for UDS.
	SenderQueueSize int
	// WriteTimeoutUDS is the timeout after which a UDS packet is dropped.
	WriteTimeoutUDS time.Duration
	// Telemetry is a set of metrics automatically injected by the client in the
	// dogstatsd stream to be able to monitor the client itself.
	Telemetry bool
	// SendMode determins the behavior of the client when receiving to many
	// metrics. The client will either drop the metrics if its buffers are
	// full (DropOnSend mode) or block the caller until the metric can be
	// handled (BlockOnSend mode). By default the client will block. This
	// option should be set to DropOnSend only when use under very high
	// load.
	//
	// BlockOnSend uses a mutex internally which is much faster than
	// channel but causes some lock contention when used with a high number
	// of threads. Mutex are sharded based on the metrics name which
	// limit mutex contention when goroutines send different metrics.
	//
	// DropOnSend: uses channel (of DropSendingModeBufferSize size) to send
	// metrics and drop metrics if the channel is full. Sending metrics in
	// this mode is slower that BlockOnSend (because of the channel), but
	// will not block the application. This mode is made for application
	// using many goroutines, sending the same metrics at a very high
	// volume.
	SendMode SendingMode
	// DropSendingModeBufferSize is the size of the channel holding incoming metrics
	DropSendingModeBufferSize int
	// AggregationFlushInterval is the interval for the aggregator to flush metrics
	AggregationFlushInterval time.Duration
	// Aggregation enables/disables client side aggregation
	Aggregation bool
}

func resolveOptions(options []Option) (*Options, error) {
	o := &Options{
		Namespace:                 DefaultNamespace,
		Tags:                      DefaultTags,
		MaxBytesPerPayload:        DefaultMaxBytesPerPayload,
		MaxMessagesPerPayload:     DefaultMaxMessagesPerPayload,
		BufferPoolSize:            DefaultBufferPoolSize,
		BufferFlushInterval:       DefaultBufferFlushInterval,
		BufferShardCount:          DefaultBufferShardCount,
		SenderQueueSize:           DefaultSenderQueueSize,
		WriteTimeoutUDS:           DefaultWriteTimeoutUDS,
		Telemetry:                 DefaultTelemetry,
		SendMode:                  DefaultSendingMode,
		DropSendingModeBufferSize: DefaultDropSendingModeBufferSizer,
		AggregationFlushInterval:  DefaultAggregationFlushInterval,
		Aggregation:               DefaultAggregation,
	}

	for _, option := range options {
		err := option(o)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

// Option is a client option. Can return an error if validation fails.
type Option func(*Options) error

// WithNamespace sets the Namespace option.
func WithNamespace(namespace string) Option {
	return func(o *Options) error {
		o.Namespace = namespace
		return nil
	}
}

// WithTags sets the Tags option.
func WithTags(tags []string) Option {
	return func(o *Options) error {
		o.Tags = tags
		return nil
	}
}

// WithMaxMessagesPerPayload sets the MaxMessagesPerPayload option.
func WithMaxMessagesPerPayload(maxMessagesPerPayload int) Option {
	return func(o *Options) error {
		o.MaxMessagesPerPayload = maxMessagesPerPayload
		return nil
	}
}

// WithMaxBytesPerPayload sets the MaxBytesPerPayload option.
func WithMaxBytesPerPayload(MaxBytesPerPayload int) Option {
	return func(o *Options) error {
		o.MaxBytesPerPayload = MaxBytesPerPayload
		return nil
	}
}

// WithBufferPoolSize sets the BufferPoolSize option.
func WithBufferPoolSize(bufferPoolSize int) Option {
	return func(o *Options) error {
		o.BufferPoolSize = bufferPoolSize
		return nil
	}
}

// WithBufferFlushInterval sets the BufferFlushInterval option.
func WithBufferFlushInterval(bufferFlushInterval time.Duration) Option {
	return func(o *Options) error {
		o.BufferFlushInterval = bufferFlushInterval
		return nil
	}
}

// WithBufferShardCount sets the BufferShardCount option.
func WithBufferShardCount(bufferShardCount int) Option {
	return func(o *Options) error {
		o.BufferShardCount = bufferShardCount
		return nil
	}
}

// WithSenderQueueSize sets the SenderQueueSize option.
func WithSenderQueueSize(senderQueueSize int) Option {
	return func(o *Options) error {
		o.SenderQueueSize = senderQueueSize
		return nil
	}
}

// WithWriteTimeoutUDS sets the WriteTimeoutUDS option.
func WithWriteTimeoutUDS(writeTimeoutUDS time.Duration) Option {
	return func(o *Options) error {
		o.WriteTimeoutUDS = writeTimeoutUDS
		return nil
	}
}

// WithoutTelemetry disables the telemetry
func WithoutTelemetry() Option {
	return func(o *Options) error {
		o.Telemetry = false
		return nil
	}
}

// WithDropOnSendMode enables "drop mode"
func WithDropOnSendMode() Option {
	return func(o *Options) error {
		o.SendMode = DropOnSend
		return nil
	}
}

// WithBlockOnSendMode enables "bloc on send" mode (default)
func WithBlockOnSendMode() Option {
	return func(o *Options) error {
		o.SendMode = BlockOnSend
		return nil
	}
}

// WithDropSendingModeBufferSize the channel buffer size when using "drop mode"
func WithDropSendingModeBufferSize(bufferSize int) Option {
	return func(o *Options) error {
		o.DropSendingModeBufferSize = bufferSize
		return nil
	}
}

// WithoutAggregationInterval set the aggregation interval
func WithoutAggregationInterval(interval time.Duration) Option {
	return func(o *Options) error {
		o.AggregationFlushInterval = interval
		return nil
	}
}

// WithClientSideAggregation enables client side aggregation
func WithClientSideAggregation() Option {
	return func(o *Options) error {
		o.Aggregation = true
		return nil
	}
}

// WithoutClientSideAggregation disables client side aggregation
func WithoutClientSideAggregation() Option {
	return func(o *Options) error {
		o.Aggregation = false
		return nil
	}
}
