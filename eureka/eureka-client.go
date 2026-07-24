package eureka

import (
	"encoding/json"
	"exchange-ws/config"
	"fmt"
	"log"
	"net/http"
	"strconv"

	eurekaClient "github.com/xuanbo/eureka-client"
)

func CreateEurekaClient(configKey string) {
	cfg, ok := config.GetConfig(configKey)
	if !ok {
		return
	}
	if cfg.DefaultZone == "" {
		fmt.Println("No default eureka client configured")
		return
	}
	if err := NewEurekaClient(cfg); err != nil {
		log.Printf("eureka client: %v", err)
	}
}

func NewEurekaClient(cfg config.Config) error {
	port, err := strconv.Atoi(cfg.Port)
	if err != nil {
		return fmt.Errorf("invalid Port %q: %w", cfg.Port, err)
	}

	renewalIntervalInSecs, err := strconv.Atoi(cfg.RenewalIntervalInSecs)
	if err != nil {
		return fmt.Errorf("invalid RenewalIntervalInSecs %q: %w", cfg.RenewalIntervalInSecs, err)
	}

	registryFetchIntervalSeconds, err := strconv.Atoi(cfg.RegistryFetchIntervalSeconds)
	if err != nil {
		return fmt.Errorf("invalid RegistryFetchIntervalSeconds %q: %w", cfg.RegistryFetchIntervalSeconds, err)
	}

	durationInSecs, err := strconv.Atoi(cfg.DurationInSecs)
	if err != nil {
		return fmt.Errorf("invalid DurationInSecs %q: %w", cfg.DurationInSecs, err)
	}

	client := eurekaClient.NewClient(
		&eurekaClient.Config{
			DefaultZone:                  cfg.DefaultZone,
			App:                          cfg.App,
			Port:                         port,
			RenewalIntervalInSecs:        renewalIntervalInSecs,
			RegistryFetchIntervalSeconds: registryFetchIntervalSeconds,
			DurationInSecs:               durationInSecs,
			Metadata: map[string]interface{}{
				"VERSION":              "0.1.0",
				"NODE_GROUP_ID":        0,
				"PRODUCT_CODE":         "DEFAULT",
				"PRODUCT_VERSION_CODE": "DEFAULT",
				"PRODUCT_ENV_CODE":     "DEFAULT",
				"SERVICE_VERSION_CODE": "DEFAULT",
			},
		}, func(instance *eurekaClient.Instance) {
			instance.InstanceID = cfg.InstanceID
		})

	client.Start()

	http.HandleFunc("/v1/services", func(writer http.ResponseWriter, request *http.Request) {
		apps := client.Applications

		b, _ := json.Marshal(apps)
		_, _ = writer.Write(b)
	})

	if err := http.ListenAndServe(":10000", nil); err != nil {
		return fmt.Errorf("eureka http server: %w", err)
	}
	return nil
}
