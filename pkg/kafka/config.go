package kafka

import (
	"crypto/tls"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ProducerConfig holds configurations for initializing a Kafka producer.
type ProducerConfig struct {
	Brokers                 []string
	ClientID                string
	SASLUser                string
	SASLPassword            string
	RequiredAcknowledgement kgo.Acks
	ProduceBatchMaxBytes    int32
	DialTimeout             time.Duration
	ReadTimeout             time.Duration
}

// ConsumerConfig holds configurations for initializing a Kafka consumer.
type ConsumerConfig struct {
	Brokers            []string
	ClientID           string
	SASLUser           string
	SASLPassword       string
	AutoCommitInterval time.Duration
	DialTimeout        time.Duration
	RebalancedTimeout  time.Duration
	ConsumerTopic      string
	ConsumerGroup      string
	FetchMaxBytes      int32
	SessionTimeout     time.Duration // Timeout used to detect consumer failures when using Kafka's group management facility.
	HeartbeatInterval  time.Duration // Expected frequency of heartbeats to the consumer coordinator.
	EnableAutoCommit   bool          // If true, the consumer's offset will be periodically committed in the background.
	AutoOffsetReset    kgo.Offset    // What to do when there is no initial offset in Kafka.
	CommitInterval     time.Duration // The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if enable.auto.commit is set to true.
	FetchMinBytes      int32         // Minimum amount of data the server should return for a fetch request.
}

// NewKafkaProducerClient make producer client
func NewKafkaProducerClient(cfg *ProducerConfig) (*kgo.Client, error) {
	opts := basicProducerOptions(cfg)

	// Reuse the appendSASLOption function for SASL Authentication and TLS configuration
	opts = appendSASLOption(opts, cfg.SASLUser, cfg.SASLPassword)

	return kgo.NewClient(opts...)
}

// basicProducerOptions returns the basic configuration options for a Kafka producer.
func basicProducerOptions(cfg *ProducerConfig) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(cfg.RequiredAcknowledgement),
		//kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchMaxBytes(cfg.ProduceBatchMaxBytes),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.RetryTimeout(cfg.ReadTimeout),
	}
}

func NewKafkaConsumerClient(cfg *ConsumerConfig) (*kgo.Client, error) {
	opts := basicConsumerOptions(cfg)

	opts = appendSASLOption(opts, cfg.SASLUser, cfg.SASLPassword)

	return kgo.NewClient(opts...)
}

// basicConsumerOptions returns the basic configuration options for a Kafka consumer.
func basicConsumerOptions(cfg *ConsumerConfig) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.RebalanceTimeout(cfg.RebalancedTimeout),
		kgo.FetchMaxBytes(cfg.FetchMaxBytes),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.HeartbeatInterval(cfg.HeartbeatInterval),
		kgo.FetchMinBytes(cfg.FetchMinBytes),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ConsumeTopics(cfg.ConsumerTopic),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
	}
	if cfg.EnableAutoCommit {
		opts = append(opts, kgo.AutoCommitInterval(cfg.AutoCommitInterval))
	}

	// Filter out any potential nil or default values to avoid setting unwanted options
	var filteredOpts []kgo.Opt
	for _, opt := range opts {
		if opt != nil {
			filteredOpts = append(filteredOpts, opt)
		}
	}

	return filteredOpts
}

// appendSASLOption appends the SASL authentication and TLS options to the given options slice.
func appendSASLOption(opts []kgo.Opt, user, password string) []kgo.Opt {
	// Add TLS configuration only if user and password are provided
	if user != "" && password != "" {
		tlsOpt := kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
		opts = append(opts, tlsOpt)

		saslMechanism := scram.Auth{
			User: user,
			Pass: password,
		}.AsSha512Mechanism()
		opts = append(opts, kgo.SASL(saslMechanism))
	}

	return opts
}
