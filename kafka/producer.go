package kafka

import (
	"context"
	"exchange-ws/config"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(c config.Config) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(c.Broker),
			Topic: c.Topic,
			//Balancer:     &kafka.Hash{},
			Balancer: &kafka.LeastBytes{},
			Async:    true,

			BatchSize:    1000,
			BatchBytes:   1 << 20,
			BatchTimeout: 5 * time.Millisecond,

			RequiredAcks:           kafka.RequireAll,
			Compression:            kafka.Snappy,
			ReadTimeout:            10 * time.Second,
			WriteTimeout:           10 * time.Second,
			AllowAutoTopicCreation: true,
			MaxAttempts:            30,
			WriteBackoffMin:        100 * time.Millisecond,
			WriteBackoffMax:        30 * time.Second,

			Completion: func(messages []kafka.Message, err error) {
				if err != nil {
					log.Printf("kafka async delivery failed for %d message(s): %v", len(messages), err)
				}
			},
		},
	}
}

func (p *Producer) Send(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka send (key=%s): %w", key, err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
