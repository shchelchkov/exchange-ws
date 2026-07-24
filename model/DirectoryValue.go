package model

import "encoding/json"

type DirectoryValue struct {
	data map[string]interface{}
}

func NewModel() *DirectoryValue {
	return &DirectoryValue{data: make(map[string]interface{})}
}

func (m *DirectoryValue) AddAttribute(key string, value interface{}) *DirectoryValue {
	m.data[key] = value
	return m
}

func (m *DirectoryValue) GetAttribute(key string) interface{} {
	return m.data[key]
}

func (m *DirectoryValue) Data() map[string]interface{} {
	return m.data
}

func (m *DirectoryValue) ToJSON() ([]byte, error) {
	return json.Marshal(m.data)
}
