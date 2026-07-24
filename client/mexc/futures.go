package mexc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"exchange-ws/client/wsconn"
	"exchange-ws/config"

	"github.com/bytedance/sonic"
)

const FuturesDepth string = "push.depth"

func NewFutures(ctx context.Context, cfg config.Config, msgs chan map[string]interface{}) (wsconn.Connection, error) {
	pingInterval, err := strconv.Atoi(cfg.PingInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid ping interval %q: %w", cfg.PingInterval, err)
	}

	symbols := splitSymbols(cfg.Symbol)

	c := newClient(cfg.WssApiTick, cfg.Proxy, symbols, pingInterval, futuresHandler(ctx, cfg, msgs), futuresProtocol(cfg.Level))
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func futuresProtocol(level string) protocol {
	limit := 0
	if n, err := strconv.Atoi(level); err == nil {
		limit = n
	}

	return protocol{
		pingMsg: map[string]string{"method": "ping"},
		isPong:  func(raw []byte) bool { return strings.Contains(string(raw), `"pong"`) },

		subscribe: func(c *client, symbols []string) error {
			for _, s := range symbols {
				param := map[string]any{"symbol": s}
				if limit > 0 {
					param["limit"] = limit
				}
				if err := c.writeJSON(map[string]any{"method": "sub.depth", "param": param}); err != nil {
					return err
				}
			}
			return nil
		},
		unsubscribe: func(c *client, symbols []string) error {
			for _, s := range symbols {
				_ = c.writeJSON(map[string]any{"method": "unsub.depth", "param": map[string]any{"symbol": s}})
			}
			return nil
		},
	}
}

func futuresHandler(ctx context.Context, cfg config.Config, msgs chan<- map[string]interface{}) MessageHandler {
	return func(rawMsg string) error {
		select {
		case <-ctx.Done():
			return errors.New("stream is closing")
		default:
		}

		data, ok := ParseFutures(rawMsg, cfg.SettingCode, cfg.ConfigCode)
		if !ok {
			return nil
		}
		return wsconn.Send(ctx, msgs, *data)
	}
}

func ParseFutures(msg, settingCode, configCode string) (*map[string]interface{}, bool) {
	var m map[string]interface{}
	if err := sonic.Unmarshal([]byte(msg), &m); err != nil {
		return nil, false
	}

	channel, _ := m["channel"].(string)
	if !strings.HasPrefix(channel, "push.") {
		return nil, false
	}
	symbol, _ := m["symbol"].(string)
	if symbol == "" {
		return nil, false
	}

	m["topic"] = channel + "." + symbol
	m["setting_code"] = settingCode
	m["config_code"] = configCode
	m["instant"] = time.Now().UnixNano()
	m["date_time"] = time.Now().UTC().Format(time.RFC3339Nano)

	return &m, true
}
