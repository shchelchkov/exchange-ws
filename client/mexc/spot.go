package mexc

import (
	"context"
	"errors"
	"exchange-ws/client/wsconn"
	"exchange-ws/config"
	"exchange-ws/value"
	"fmt"
	"strconv"
	"strings"
)

const PartialBookDepthStreams string = "PartialBookDepthStreams"

func NewSpot(ctx context.Context, cfg config.Config, msgs chan map[string]interface{}) (wsconn.Connection, error) {
	pingInterval, err := strconv.Atoi(cfg.PingInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid ping interval %q: %w", cfg.PingInterval, err)
	}

	symbols := splitSymbols(cfg.Symbol)
	topics := make([]string, len(symbols))
	for i, s := range symbols {
		topics[i] = fmt.Sprintf(cfg.SymbolTopic, s, cfg.Level)
	}

	c := newClient(cfg.WssApiTick, cfg.Proxy, topics, pingInterval, spotHandler(ctx, cfg, msgs), spotProtocol())
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func spotProtocol() protocol {
	return protocol{
		pingMsg: map[string]string{"method": "PING"},
		isPong:  func(raw []byte) bool { return strings.Contains(string(raw), "PONG") },
		subscribe: func(c *client, topics []string) error {
			return c.writeJSON(map[string]any{"method": "SUBSCRIPTION", "params": topics})
		},
		unsubscribe: func(c *client, topics []string) error {
			return c.writeJSON(map[string]any{"method": "UNSUBSCRIPTION", "params": topics})
		},
	}
}

func spotHandler(ctx context.Context, cfg config.Config, msgs chan<- map[string]interface{}) MessageHandler {
	return func(rawMsg string) error {
		select {
		case <-ctx.Done():
			return errors.New("stream is closing")
		default:
		}

		switch cfg.SymbolParser {
		case PartialBookDepthStreams:
			data, ok := value.ParsePartialBookDepthStreams(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			return wsconn.Send(ctx, msgs, *data)
		default:
			return nil
		}
	}
}
