package value

import (
	"exchange-ws/config"
	"strings"
)

const (
	DefaultCategory = "inverse"
	DefaultInterval = "1"
	DefaultLimit    = "200"
	DefaultSymbol   = "BTCUSDT"
	TypeSnapshot    = "snapshot"
	TypeKline       = "kline"
)

func Category(cfg config.Config) string {
	if c := strings.TrimSpace(cfg.Category); c != "" {
		return c
	}
	return DefaultCategory
}

func Symbol(cfg config.Config) string {
	if c := strings.TrimSpace(cfg.Symbol); c != "" {
		return c
	}
	return DefaultSymbol
}

func Interval(cfg config.Config) string {
	if c := strings.TrimSpace(cfg.Interval); c != "" {
		return c
	}
	return DefaultInterval
}

func Limit(cfg config.Config) string {
	if c := strings.TrimSpace(cfg.Limit); c != "" {
		return c
	}
	return DefaultLimit
}
