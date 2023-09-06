package main

import (
	"context"
	"encoding/json"
	"event-validator/pkg/codec"
	"event-validator/pkg/kafka"
	"event-validator/pkg/kafka/consumer"
	"log"
	"time"
)

// Define topic to consume from
const topic = "event-tracking_track-events-approved"

func main() {
	// Define consumer configuration
	consumerConfig := &kafka.ConsumerConfig{
		Brokers:            []string{"localhost:9092"}, // replace with your broker(s)
		ClientID:           "my-machine",               // consumer member id
		SASLUser:           "",
		SASLPassword:       "",
		AutoCommitInterval: 5 * time.Second,
		DialTimeout:        10 * time.Second,
		RebalancedTimeout:  5 * time.Second,
		ConsumerTopic:      topic,
		ConsumerGroup:      "local-consumption",
		FetchMaxBytes:      1000000,
		SessionTimeout:     10 * time.Second,
		HeartbeatInterval:  3 * time.Second,
		EnableAutoCommit:   false,
		CommitInterval:     0,
		FetchMinBytes:      0,
	}

	// Create a new Kafka consumer client
	kafkaClient, err := kafka.NewKafkaConsumerClient(consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}

	// Use the JSON codec for deserialization
	jsonCodec := codec.NewCodecJson()

	// Create a Kafka consumer using the client and the codec
	kafkaConsumer := consumer.NewKafkaConsumer(kafkaClient, jsonCodec)

	// Start consuming from the topic
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(message map[string]interface{}) error {
		prettyJSON, err := json.MarshalIndent(message, "", "    ")
		if err != nil {
			log.Printf("Failed to marshal the message into pretty JSON: %v", err)
			return err
		}
		log.Printf("Received message: %s\n", string(prettyJSON))
		return nil
	}

	err = kafkaConsumer.StartConsuming(ctx, topic, handler)
	if err != nil {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	// Keep the application running (for example purposes)
	select {}
}
