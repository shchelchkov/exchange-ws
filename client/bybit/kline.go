package bybit

import (
	"context"
	"exchange-ws/config"
	"exchange-ws/value"
	"fmt"

	bybitapi "github.com/bybit-exchange/bybit.go.api"
)

func FetchKline(ctx context.Context, cfg config.Config) (*map[string]interface{}, error) {

	client := bybitapi.NewBybitHttpClient("", "", bybitapi.WithBaseURL(bybitapi.MAINNET), bybitapi.WithProxyURL(cfg.Proxy))
	params := map[string]interface{}{
		"category": value.Category(cfg),
		"symbol":   value.Symbol(cfg),
		"interval": value.Interval(cfg),
		"limit":    value.Limit(cfg),
	}

	result, err := client.NewUtaBybitServiceWithParams(params).GetMarketKline(ctx)
	if err != nil {
		return nil, fmt.Errorf("bybit: read body from %s: %w", params, err)
	}
	if result.RetCode != 0 {
		return nil, fmt.Errorf("API error: %s", result.RetMsg)
	}

	resultData, ok := result.Result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("return format error")
	}

	return &resultData, nil

}
