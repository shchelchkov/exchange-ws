package process

import (
	"exchange-ws/config"
	"fmt"
	"net/http"

	"github.com/go-chi/render"
)

type streamResponse struct {
	Config string      `json:"config"`
	Status StreamState `json:"status"`
}

func BbtHandler(w http.ResponseWriter, r *http.Request, manager *Manager) {
	configKey := r.PathValue("config")
	fmt.Println("start configKey:", configKey)

	cfg, ok := config.GetConfig(configKey)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, map[string]string{"config": configKey, "error": "config not found"})
		return
	}

	manager.Start(cfg.ConfigCode, cfg)
	state, _ := manager.State(cfg.ConfigCode)
	render.JSON(w, r, streamResponse{Config: configKey, Status: state})
}

func BbtHandlerStop(w http.ResponseWriter, r *http.Request, manager *Manager) {
	configKey := r.PathValue("config")
	fmt.Println("stop configKey:", configKey)

	cfg, ok := config.GetConfig(configKey)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, map[string]string{"config": configKey, "error": "config not found"})
		return
	}

	manager.StopStream(cfg.ConfigCode)
	render.JSON(w, r, streamResponse{Config: configKey, Status: StateStopped})
}
