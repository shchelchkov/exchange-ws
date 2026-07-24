package bybit

type WSMessage struct {
	SettingCode string      `json:"settingCode"`
	Success     bool        `json:"success"`
	RetMsg      string      `json:"ret_msg"`
	ConnId      string      `json:"conn_id"`
	ReqId       string      `json:"req_id"`
	Topic       string      `json:"topic"`
	Type        string      `json:"type"`
	Data        interface{} `json:"data"`
	Op          string      `json:"op,omitempty"`
	CS          int64       `json:"cs,omitempty"`
	TS          int64       `json:"ts,omitempty"`
}

type WSMessageTrade struct {
	SettingCode string        `json:"settingCode"`
	Success     bool          `json:"success"`
	RetMsg      string        `json:"ret_msg"`
	ConnId      string        `json:"conn_id"`
	ReqId       string        `json:"req_id"`
	Topic       string        `json:"topic"`
	Type        string        `json:"type"`
	Data        []interface{} `json:"data"`
	Op          string        `json:"op,omitempty"`
	CS          int64         `json:"cs,omitempty"`
	TS          int64         `json:"ts,omitempty"`
}

type WSMessageOrderBook struct {
	SettingCode string      `json:"settingCode"`
	Topic       string      `json:"topic"`
	Type        string      `json:"type"`
	Data        interface{} `json:"data"`
	Cts         int         `json:"cts,omitempty"`
}
