package consumer

import "context"

type Consumer interface {
	StartConsuming(ctx context.Context, topic string, handler MessageHandler) error
	Close() error
}

type MessageHandler func(message map[string]any) error
