package bybit

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"exchange-ws/client/wsconn"
	"exchange-ws/config"

	bybitapi "github.com/bybit-exchange/bybit.go.api"
)

var _ wsconn.Connection = (*conn)(nil)

type conn struct {
	ws     *bybitapi.WebSocket
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *conn) Disconnect() error {
	c.cancel()
	return c.ws.Disconnect()
}

func (c *conn) Done() <-chan struct{} {
	return c.ctx.Done()
}

func NewFutures(ctx context.Context, cfg config.Config, msgs chan map[string]interface{}) (wsconn.Connection, error) {
	pingInterval, err := strconv.Atoi(cfg.PingInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid ping interval %q: %w", cfg.PingInterval, err)
	}

	ws := bybitapi.NewBybitPrivateWebSocket(
		cfg.WssApiTick,
		cfg.YourApiKey,
		cfg.YourApiSecret,
		handleMessage(ctx, cfg, msgs),
		bybitapi.WithPingInterval(pingInterval),
		bybitapi.WithMaxAliveTime(cfg.WithMaxAliveTime),
	)

	str := strings.Trim(cfg.Symbol, "[]")
	symbols := strings.Split(str, ",")

	topics := make([]string, len(symbols))
	for i, s := range symbols {
		topics[i] = cfg.SymbolTopic + "." + strings.TrimSpace(s)
	}

	c, err := ws.Connect().SendSubscription(topics)
	if err != nil {
		log.Printf("bybit subscribe %v: %v", topics, err)
		return nil, err
	}

	cctx, cancel := context.WithCancel(ctx)
	return &conn{ws: c, ctx: cctx, cancel: cancel}, nil
}
