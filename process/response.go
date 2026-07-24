package process

import (
	"context"
	"exchange-ws/config"
	"exchange-ws/kafka"
	"time"
)

const publishTimeout = 20 * time.Second

type publisher interface {
	SendBatch(ctx context.Context, msgs []kafka.Message) (int, error)
	Close() error
}

var newPublisher = func(cfg config.Config) publisher { return kafka.NewSyncProducer(cfg) }

type infoResponse struct {
	Config    string `json:"config"`
	Category  string `json:"category"`
	Published int    `json:"published"`
	Time      int64  `json:"time"`
}

type infoErrorResponse struct {
	Config string `json:"config"`
	Error  string `json:"error"`
}
