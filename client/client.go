package client

import (
	"context"
	"exchange-ws/client/bybit"
	"exchange-ws/client/mexc"
	"exchange-ws/client/wsconn"
	"exchange-ws/config"
)

func StreamWebSocket(ctx context.Context, cfg config.Config, msgs chan map[string]interface{}) (wsconn.Connection, error) {
	switch cfg.Platform {
	case "mexc_spot":
		return mexc.NewSpot(ctx, cfg, msgs)
	case "mexc_futures":
		return mexc.NewFutures(ctx, cfg, msgs)
	case "bybit_futures":
		return bybit.NewFutures(ctx, cfg, msgs)
	default:
		return bybit.NewFutures(ctx, cfg, msgs)
	}
}
