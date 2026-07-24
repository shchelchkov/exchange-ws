package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/go-chi/render"
	"gopkg.in/yaml.v3"
)

var (
	configCache = make(map[string]Config)
	mu          sync.RWMutex
)

const (
	DirectoryFunctionUrl = "http://ts-directory-function"
	DefaultCacheKey      = "config"
)

type Config struct {
	SettingUrl                   string `json:"setting_url"                     yaml:"ts-directory-function"`
	SettingCode                  string `json:"setting_code"                    yaml:"SettingCode"`
	ConfigCode                   string `json:"config_code"                     yaml:"ConfigCode"`
	Platform                     string `json:"platform"                        yaml:"platform"`
	YourApiKey                   string `json:"your_api_key"                    yaml:"YOUR_API_KEY"`
	YourApiSecret                string `json:"your_api_secret"                 yaml:"YOUR_API_SECRET"`
	WithMaxAliveTime             string `json:"with_max_alive_time"             yaml:"WithMaxAliveTime"`
	PingInterval                 string `json:"ping_interval"                   yaml:"pingInterval"`
	Proxy                        string `json:"Proxy"                           yaml:"Proxy"`
	HttpApi                      string `json:"http_api"                        yaml:"HttpApi"`
	HttpApiTick                  string `json:"http_api_tick"                   yaml:"HttpApiTick"`
	WssApi                       string `json:"wss_api"                         yaml:"WssApi"`
	WssApiTick                   string `json:"wss_api_tick"                    yaml:"WssApiTick"`
	SymbolTopic                  string `json:"symbol_topic"                    yaml:"SymbolTopic"`
	SymbolParser                 string `json:"symbol_parser"                   yaml:"SymbolParser"`
	Symbol                       string `json:"symbol"                          yaml:"Symbol"`
	ExchengeWsUrl                string `json:"exchenge_ws_url"                 yaml:"ExchengeWsUrl"`
	LagNs                        int64  `json:"lag_ns"                          yaml:"LagNs"`
	Level                        string `json:"level"                           yaml:"level"`
	Prod                         string `json:"prod"                            yaml:"Prod"`
	Broker                       string `json:"broker"                          yaml:"Broker"`
	Topic                        string `json:"topic"                           yaml:"Topic"`
	InstanceID                   string `json:"instance_id"                     yaml:"InstanceID"`
	DefaultZone                  string `json:"default_zone"                    yaml:"DefaultZone"`
	App                          string `json:"app"                             yaml:"App"`
	Port                         string `json:"port"                            yaml:"Port"`
	RenewalIntervalInSecs        string `json:"renewal_interval_in_secs"        yaml:"RenewalIntervalInSecs"`
	RegistryFetchIntervalSeconds string `json:"registry_fetch_interval_seconds" yaml:"RegistryFetchIntervalSeconds"`
	DurationInSecs               string `json:"duration_in_secs"                yaml:"DurationInSecs"`
}

func LoadConfig() error {
	configs, err := Get()
	if err != nil {
		return err
	}

	mu.Lock()
	for _, cfg := range configs {
		configCache[cfg.ConfigCode] = cfg
	}
	mu.Unlock()

	return nil
}

func Get() ([]Config, error) {
	configPath := flag.String("config", "", "path to yaml config")
	settingUrl := flag.String("settingUrl", DirectoryFunctionUrl, "ts-directory-function url")
	flag.Parse()

	r, err := getYaml(*configPath)
	if err != nil {
		fmt.Printf("configuration error: %v\n", err)
	} else {
		return r, err
	}

	return []Config{
		{
			SettingUrl: *settingUrl,
		},
	}, nil
}

func GetConfig(key string) (Config, bool) {
	mu.RLock()
	defer mu.RUnlock()

	data, ok := configCache[key]
	if !ok {
		fmt.Printf("configuration not found for key: %s\n", key)
		return Config{}, false
	}
	return data, true
}

func SetConfig(key string, config *Config) (Config, bool) {
	mu.Lock()
	defer mu.Unlock()

	if len(config.SymbolTopic) == 0 {
		config.SymbolTopic = "tickers"
	}

	configCache[key] = *config

	return configCache[key], true
}

func getYaml(path string) ([]Config, error) {
	var configs []Config

	f, err := ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(f, &configs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal yaml: %w", err)
	}

	return configs, nil
}

func ReadFile(path string) ([]byte, error) {
	if path == "" {
		return os.ReadFile("config.yaml")
	}
	return os.ReadFile(path)
}

func GetConfigHandler(w http.ResponseWriter, r *http.Request) {
	configKey := r.PathValue("config")
	fmt.Println("get configKey:", configKey)

	data, b := GetConfig(configKey)
	if b != true {
		w.WriteHeader(http.StatusNotFound)
		render.JSON(w, r, Config{})
	} else {
		w.WriteHeader(http.StatusOK)
		render.JSON(w, r, data)
	}
}

func SetConfigHandler(w http.ResponseWriter, r *http.Request) {
	configKey := r.PathValue("config")
	fmt.Println("set configKey:", configKey)

	var config Config
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	data, ok := SetConfig(configKey, &config)
	if !ok {
		http.Error(w, "failed to set config", http.StatusBadRequest)
		return
	}
	render.JSON(w, r, data)
}
