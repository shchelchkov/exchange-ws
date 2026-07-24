package bybit

import (
	"context"
	"errors"

	"exchange-ws/client/wsconn"
	"exchange-ws/config"
)

const (
	TopicTickers        string = "tickers"
	TopicPublicTrade    string = "publicTrade"
	TopicPublicSnapshot string = "publicTradeSnapshot"
	TopicOrderBook      string = "orderbook"
)

func handleMessage(ctx context.Context, cfg config.Config, msgs chan<- map[string]interface{}) func(string) error {
	return func(rawMsg string) error {
		select {
		case <-ctx.Done():
			return errors.New("stream is closing")
		default:
		}

		switch cfg.SymbolParser {

		case TopicTickers:
			data, ok := ParseTickers(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			return wsconn.Send(ctx, msgs, *data)

		case TopicPublicTrade:
			items, ok := ParsePublicTrade(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			for _, item := range *items {
				if err := wsconn.Send(ctx, msgs, item); err != nil {
					return err
				}
			}
			return nil

		case TopicPublicSnapshot:
			data, ok := ParsePublicTradeSnapshot(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			return wsconn.Send(ctx, msgs, *data)

		case TopicOrderBook:
			data, ok := ParseTopicOrderBook(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			return wsconn.Send(ctx, msgs, *data)

		default:
			return nil
		}
	}
}
