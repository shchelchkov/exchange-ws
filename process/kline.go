package process

import (
	"context"
	"exchange-ws/client/bybit"
	"exchange-ws/config"
	"exchange-ws/kafka"
	"exchange-ws/value"
	"fmt"
	"net/http"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/go-chi/render"
)

func KlineHandler(w http.ResponseWriter, r *http.Request) {
	configKey, cfg, done := conf(w, r)
	if done {
		return
	}

	if err := validateKlineConfig(cfg); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, infoErrorResponse{Config: configKey, Error: err.Error()})
		return
	}

	resp, err := bybit.FetchKline(r.Context(), cfg)
	if err != nil {
		render.Status(r, http.StatusBadGateway)
		render.JSON(w, r, infoErrorResponse{Config: configKey, Error: err.Error()})
		return
	}

	msgs, err := buildKlineMessages(resp, cfg)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, infoErrorResponse{Config: configKey, Error: err.Error()})
		return
	}

	producer := newPublisher(cfg)
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Printf("Kline %s: producer close: %v\n", configKey, err)
		}
	}()

	published, err := publishKline(r.Context(), producer, msgs)
	time, _ := (*resp)["time"].(int64)
	if err != nil {
		fmt.Printf("Kline %s: publish failed: %v\n", configKey, err)
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, map[string]interface{}{
			"config":    configKey,
			"category":  value.Category(cfg),
			"published": published,
			"failed":    len(msgs) - published,
			"time":      time,
			"error":     err.Error(),
		})
		return
	}

	render.Status(r, http.StatusOK)
	render.JSON(w, r, infoResponse{
		Config:    configKey,
		Category:  value.Category(cfg),
		Published: published,
		Time:      time,
	})
}

func conf(w http.ResponseWriter, r *http.Request) (string, config.Config, bool) {
	configKey := r.PathValue("config")
	fmt.Println("Kline configKey:", configKey)

	cfg, ok := config.GetConfig(configKey)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, infoErrorResponse{Config: configKey, Error: "config not found"})
		return "", config.Config{}, true
	}
	return configKey, cfg, false
}

func validateKlineConfig(cfg config.Config) error {
	var missing []string
	if strings.TrimSpace(cfg.HttpApi) == "" {
		missing = append(missing, "http_api")
	}
	if strings.TrimSpace(cfg.Broker) == "" {
		missing = append(missing, "broker")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		missing = append(missing, "topic")
	}
	if len(missing) > 0 {
		return fmt.Errorf("config incomplete, missing: %s", strings.Join(missing, ", "))
	}
	return nil
}

func buildKlineMessages(resp *map[string]interface{}, cfg config.Config) ([]kafka.Message, error) {
	items := value.BuildKlineMessages(resp, cfg)

	msgs := make([]kafka.Message, 0, len(items))
	for _, m := range items {
		topic, ok := m["topic"].(string)
		if !ok || topic == "" {
			fmt.Printf("Kline: missing or non-string topic, dropping message: %v\n", m["topic"])
			continue
		}
		payload, err := sonic.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("marshal message %s: %w", topic, err)
		}
		msgs = append(msgs, kafka.Message{Key: topic, Value: payload})
	}

	return msgs, nil
}

func publishKline(ctx context.Context, p publisher, msgs []kafka.Message) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	sendCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()

	return p.SendBatch(sendCtx, msgs)
}
