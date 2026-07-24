package bybit

import (
	"time"

	"github.com/bytedance/sonic"
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

	data := convertDataPublicTrade(mess)
	s := time.Now().UTC().Format(time.RFC3339Nano)
	now := time.Now().UnixNano()

	for i := range data {
		setCodeTrade(data[i], mess, settingCode, configCode)
		data[i]["instant"] = now
		data[i]["date_time"] = s
	}

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

func convertDataPublicTrade(mess WSMessageTrade) []map[string]interface{} {
	data := make([]map[string]interface{}, 0, len(mess.Data))
	for _, i := range mess.Data {
		if m, ok := i.(map[string]interface{}); ok {
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
