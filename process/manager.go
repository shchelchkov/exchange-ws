package process

import (
	"context"
	"exchange-ws/config"
	"fmt"
	"sync"
)

type Manager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	streams map[string]*Stream
	mu      sync.Mutex
}

func NewManager(parent context.Context) *Manager {
	ctx, cancel := context.WithCancel(parent)
	return &Manager{
		ctx:     ctx,
		cancel:  cancel,
		streams: make(map[string]*Stream),
	}
}

func (m *Manager) Start(key string, cfg config.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.streams[key]; ok {
		if s.State() != StateStopped {
			fmt.Printf("stream %s already running\n", key)
			return
		}
		delete(m.streams, key)
	}

	s := NewStream(m.ctx, cfg)
	m.streams[key] = s
	s.Start()
}

func (m *Manager) State(key string) (StreamState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.streams[key]
	if !ok {
		return StateStopped, false
	}
	return s.State(), true
}

func (m *Manager) StopStream(key string) {
	m.mu.Lock()
	s, ok := m.streams[key]
	if ok {
		delete(m.streams, key)
	}
	m.mu.Unlock()
	if ok {
		s.Stop()
	}
}

func (m *Manager) Shutdown() {
	wg := sync.WaitGroup{}

	m.mu.Lock()
	for _, s := range m.streams {
		wg.Add(1)
		go func(s *Stream) {
			defer wg.Done()
			s.Stop()
		}(s)
	}
	m.mu.Unlock()
	wg.Wait()

	m.mu.Lock()
	m.streams = map[string]*Stream{}
	m.mu.Unlock()
	m.cancel()
}
