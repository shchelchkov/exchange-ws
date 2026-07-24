package bybit

import (
	"context"
	"errors"
	"exchange-ws/value"

	"exchange-ws/client/wsconn"
	"exchange-ws/config"
)

const (
	TopicTickers        string = "tickers"
	TopicKline          string = "kline"
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
			data, ok := value.ParseTickers(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			return wsconn.Send(ctx, msgs, *data)

		case TopicPublicTrade:
			items, ok := value.ParsePublicTrade(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			for _, item := range *items {
				if err := wsconn.Send(ctx, msgs, item); err != nil {
					return err
				}
			}
			return nil

		case TopicKline:
			items, ok := value.ParseTopicKline(rawMsg, cfg.SettingCode, cfg.ConfigCode)
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
			data, ok := value.ParsePublicTradeSnapshot(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			return wsconn.Send(ctx, msgs, *data)

		case TopicOrderBook:
			data, ok := value.ParseTopicOrderBook(rawMsg, cfg.SettingCode, cfg.ConfigCode)
			if !ok {
				return nil
			}
			return wsconn.Send(ctx, msgs, *data)

		default:
			return nil
		}
	}
}
