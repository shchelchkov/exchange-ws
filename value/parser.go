package value

import (
	"exchange-ws/client/mexc/mexcpb"
	"exchange-ws/config"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/proto"
)

func ParseTickers(msg string, settingCode string, configCode string) (*map[string]interface{}, bool) {
	var mess WSMessage
	if err := sonic.Unmarshal([]byte(msg), &mess); err != nil {
		return nil, false
	}

	data := convertData(mess)
	setCodeTickers(data, mess, settingCode, configCode)

	data["instant"] = time.Now().UnixNano()
	data["date_time"] = time.Now().UTC().Format(time.RFC3339Nano)

	return &data, true
}

func ParsePublicTrade(msg string, settingCode string, configCode string) (*[]map[string]interface{}, bool) {
	var mess WSMessageTrade
	if err := sonic.Unmarshal([]byte(msg), &mess); err != nil {
		return nil, false
	}

	data := convertDataPublicTrade(mess, settingCode, configCode)

	return &data, true
}

func ParseTopicKline(msg string, settingCode string, configCode string) (*[]map[string]interface{}, bool) {
	var mess WSMessageKline
	if err := sonic.Unmarshal([]byte(msg), &mess); err != nil {
		return nil, false
	}

	data := convertDataTopicKline(mess, settingCode, configCode)

	return &data, true
}

func ParsePublicTradeSnapshot(msg string, settingCode string, configCode string) (*map[string]interface{}, bool) {
	var mess WSMessageTrade
	if err := sonic.Unmarshal([]byte(msg), &mess); err != nil {
		return nil, false
	}

	data := make(map[string]interface{})
	setCodeTrade(data, mess, settingCode, configCode)
	data["items"] = mess.Data

	data["instant"] = time.Now().UnixNano()
	data["date_time"] = time.Now().UTC().Format(time.RFC3339Nano)

	return &data, true
}

func ParseTopicOrderBook(msg string, settingCode string, configCode string) (*map[string]interface{}, bool) {
	var mess WSMessageOrderBook
	if err := sonic.Unmarshal([]byte(msg), &mess); err != nil {
		return nil, false
	}

	data := make(map[string]interface{})
	setCodeOrderBook(data, mess, settingCode, configCode)
	data["data"] = mess.Data

	data["instant"] = time.Now().UnixNano()
	data["date_time"] = time.Now().UTC().Format(time.RFC3339Nano)

	return &data, true
}

func convertData(mess WSMessage) map[string]interface{} {
	data, ok := mess.Data.(map[string]interface{})
	if !ok || data == nil {
		return map[string]interface{}{}
	}
	return data
}

func convertDataPublicTrade(mess WSMessageTrade, settingCode string, configCode string) []map[string]interface{} {
	data := make([]map[string]interface{}, 0, len(mess.Data))
	s := time.Now().UTC().Format(time.RFC3339Nano)
	now := time.Now().UnixNano()

	for _, i := range mess.Data {
		if m, ok := i.(map[string]interface{}); ok {

			setCodeTrade(m, mess, settingCode, configCode)
			m["instant"] = now
			m["date_time"] = s

			data = append(data, m)
		}
	}
	return data
}

func convertDataTopicKline(mess WSMessageKline, settingCode string, configCode string) []map[string]interface{} {
	data := make([]map[string]interface{}, 0, len(mess.Data))
	now := time.Now().UnixNano()

	for _, i := range mess.Data {
		if m, ok := i.(map[string]interface{}); ok {

			setCodeKline(m, mess, settingCode, configCode)
			m["instant"] = now

			data = append(data, m)
		}

	}
	return data
}

func setCodeOrderBook(data map[string]interface{}, mess WSMessageOrderBook, settingCode string, configCode string) {
	data["topic"] = mess.Topic
	data["type"] = mess.Type
	data["cts"] = mess.Cts
	data["setting_code"] = settingCode
	data["config_code"] = configCode
}

func setCodeTickers(data map[string]interface{}, mess WSMessage, settingCode string, configCode string) {
	data["topic"] = mess.Topic
	data["type"] = mess.Type
	data["cs"] = mess.CS
	data["ts"] = mess.TS
	data["setting_code"] = settingCode
	data["config_code"] = configCode
}

func setCodeTrade(data map[string]interface{}, mess WSMessageTrade, settingCode string, configCode string) {
	data["topic"] = mess.Topic
	data["type"] = mess.Type
	data["cs"] = mess.CS
	data["ts"] = mess.TS
	data["setting_code"] = settingCode
	data["config_code"] = configCode
}

func setCodeKline(data map[string]interface{}, mess WSMessageKline, settingCode string, configCode string) {
	interval := data["interval"].(string)
	symbol := strings.TrimPrefix(mess.Topic, "kline."+interval+".")

	startTime := int64(data["start"].(float64))
	dateTime := time.UnixMilli(startTime).UTC().Format(time.RFC3339Nano)

	data["date_time"] = dateTime
	data["symbol"] = symbol

	data["topic"] = mess.Topic
	data["type"] = mess.Type
	data["ts"] = mess.TS
	data["setting_code"] = settingCode
	data["config_code"] = configCode
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

func ParsePartialBookDepthStreams(msg string, settingCode string, configCode string) (*map[string]interface{}, bool) {
	var w mexcpb.PushDataV3ApiWrapper
	if err := proto.Unmarshal([]byte(msg), &w); err != nil {
		return nil, false
	}

	depths := w.GetPublicLimitDepths()
	if depths == nil {
		return nil, false
	}

	data := map[string]interface{}{
		"topic":        w.GetChannel(),
		"symbol":       w.GetSymbol(),
		"version":      depths.GetVersion(),
		"asks":         depthLevels(depths.GetAsks()),
		"bids":         depthLevels(depths.GetBids()),
		"send_time":    w.GetSendTime(),
		"setting_code": settingCode,
		"config_code":  configCode,
		"instant":      time.Now().UnixNano(),
		"date_time":    time.Now().UTC().Format(time.RFC3339Nano),
	}
	return &data, true
}

func depthLevels(items []*mexcpb.PublicLimitDepthV3ApiItem) [][2]string {
	out := make([][2]string, len(items))
	for i, it := range items {
		out[i] = [2]string{it.GetPrice(), it.GetQuantity()}
	}
	return out
}

func ParseFuturesAlfa(msg, settingCode, configCode string) (*map[string]interface{}, bool) {
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

	data := make(map[string]interface{})

	data["topic"] = channel + "." + symbol
	data["setting_code"] = settingCode
	data["config_code"] = configCode
	data["instant"] = time.Now().UnixNano()
	data["date_time"] = time.Now().UTC().Format(time.RFC3339Nano)

	return &data, true
}

type AssetInfoEntity struct {
	Command string                 `json:"Command"`
	Channel bool                   `json:"Channel"`
	Id      string                 `json:"Id"`
	Payload map[string]interface{} `json:"Payload"`
}

func ParseInfoAlfa(msg, settingCode, configCode string) (*[]map[string]interface{}, bool) {
	var mess map[string]interface{}
	if err := sonic.Unmarshal([]byte(msg), &mess); err != nil {
		return nil, false
	}
	s, _ := mess["Payload"].(string)
	var payload map[string]interface{}
	if err := sonic.Unmarshal([]byte(s), &payload); err != nil {
		return nil, false
	}

	now := time.Now().UnixNano()
	ts := time.Now().UTC().Format(time.RFC3339Nano)

	data_arr, _ := payload["Data"].([]interface{})
	r_arr := make([]map[string]interface{}, 0, len(data_arr))

	for _, v := range data_arr {
		r_arr = append(r_arr, map[string]interface{}{
			"topic":        "AssetInfoEntity",
			"data":         v,
			"setting_code": settingCode,
			"config_code":  configCode,
			"instant":      now,
			"date_time":    ts,
		})
	}

	return &r_arr, true
}

const (
	TopicTickersPrefix        string = "tickers"
	TopicKlinePrefix          string = "kline"
	TopicPublicTradePrefix    string = "publicTrade"
	TopicPublicSnapshotPrefix string = "publicTradeSnapshot"
	TopicOrderBookPrefix      string = "orderbook"
)

func BuildTickerMessages(resp *map[string]interface{}, cfg config.Config) []map[string]interface{} {
	if resp == nil {
		return nil
	}
	category, _ := (*resp)["category"].(string)
	if category == "" {
		category = Category(cfg)
	}

	prefix := strings.TrimSpace(cfg.SymbolTopic)
	if prefix == "" {
		prefix = TopicTickersPrefix
	}

	instant := time.Now()
	instantNs := instant.UnixNano()
	dateTime := instant.UTC().Format(time.RFC3339Nano)

	time, _ := (*resp)["time"].(int64)
	list, _ := (*resp)["list"].([]interface{})
	out := make([]map[string]interface{}, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			log.Printf("bybit: tickers item without symbol, skipping: %v", item)
			continue
		}
		symbol, _ := m["symbol"].(string)
		data := make(map[string]interface{}, len(m)+8)
		for k, v := range m {
			data[k] = v
		}

		data["topic"] = prefix + "." + symbol
		data["symbol"] = symbol
		data["type"] = TypeSnapshot
		data["category"] = category
		data["ts"] = time
		data["setting_code"] = cfg.SettingCode
		data["caption"] = cfg.SettingCode
		data["config_code"] = cfg.ConfigCode
		data["instant"] = instantNs
		data["date_time"] = dateTime

		out = append(out, data)
	}

	return out
}

func BuildKlineMessages(resp *map[string]interface{}, cfg config.Config) []map[string]interface{} {
	if resp == nil {
		return nil
	}

	prefix := strings.TrimSpace(cfg.SymbolTopic)
	if prefix == "" {
		prefix = TopicKlinePrefix
	}
	interval := Interval(cfg)

	category, _ := (*resp)["category"].(string)
	if category == "" {
		category = Category(cfg)
	}

	instant := time.Now()
	instantNs := instant.UnixNano()

	list, _ := (*resp)["list"].([]interface{})
	symbol, _ := (*resp)["symbol"].(string)
	out := make([]map[string]interface{}, 0, len(list))
	for _, item := range list {
		listData, ok := item.([]interface{})
		if !ok {
			continue
		}
		data := make(map[string]interface{}, len(listData)+8)
		startTimeStr := listData[0].(string)

		startTime, err := strconv.ParseInt(startTimeStr, 10, 64)
		if err != nil {
			return nil
		}

		dateTime := time.UnixMilli(startTime).UTC().Format(time.RFC3339Nano)

		data["start"] = listData[0]
		data["open"] = listData[1]
		data["high"] = listData[2]
		data["low"] = listData[3]
		data["close"] = listData[4]
		data["volume"] = listData[5]
		data["turnover"] = listData[6]

		data["confirm"] = "true"

		data["topic"] = prefix + "." + symbol
		data["symbol"] = symbol
		data["type"] = TypeKline
		data["category"] = category
		data["interval"] = interval
		data["ts"] = startTime
		data["setting_code"] = cfg.SettingCode
		data["caption"] = cfg.SettingCode
		data["config_code"] = cfg.ConfigCode
		data["instant"] = instantNs
		data["date_time"] = dateTime

		out = append(out, data)
	}

	return out
}
