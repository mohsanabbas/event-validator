package consumer

import (
	"context"
	"event-validator/pkg/codec"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
)

type kafkaConsumer struct {
	client *kgo.Client
	codec  codec.Codec // Injecting the Codec interface for flexibility
}

func (kc *kafkaConsumer) Close() error {
	kc.client.Close()
	return nil
}

func (kc *kafkaConsumer) StartConsuming(ctx context.Context, topic string, handler MessageHandler) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				fetches := kc.client.PollFetches(ctx)
				err := fetches.Err()
				if err != nil {
					// Handle error or log it
					continue
				}

				// Process fetched records
				fetches.EachTopic(func(fetchedTopic kgo.FetchTopic) {
					if strings.EqualFold(fetchedTopic.Topic, topic) { // Case-insensitive comparison
						fetchedTopic.EachPartition(func(part kgo.FetchPartition) {
							for _, record := range part.Records {
								// Deserialize record using the Codec
								message, err := kc.codec.Deserialize(record.Value)
								if err != nil {
									// Handle deserialization error or log it
									continue
								}
								err = handler(message)
								if err != nil {
									// Handle message processing error or log it
									continue
								}
							}
						})
					}
				})
			}
		}
	}()
	return nil
}

func NewKafkaConsumer(client *kgo.Client, c codec.Codec) Consumer {
	return &kafkaConsumer{
		client: client,
		codec:  c,
	}
}
