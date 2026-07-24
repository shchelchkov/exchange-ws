package mexc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"exchange-ws/client/wsconn"

	"github.com/gorilla/websocket"
)

var _ wsconn.Connection = (*client)(nil)

type MessageHandler func(message string) error

type protocol struct {
	pingMsg     any
	isPong      func(raw []byte) bool
	subscribe   func(c *client, topics []string) error
	unsubscribe func(c *client, topics []string) error
}

type client struct {
	url          string
	proxy        string
	pingInterval int
	onMessage    MessageHandler
	topics       []string
	proto        protocol

	dialer   *websocket.Dialer
	ctx      context.Context
	cancel   context.CancelFunc
	pongChan chan struct{}

	mutex       sync.Mutex
	conn        *websocket.Conn
	isConnected bool
	missedPings int
}

func newClient(wsURL, proxyAddr string, topics []string, pingInterval int, handler MessageHandler, proto protocol) *client {
	dialer := &websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	if proxyAddr != "" {
		if u, err := url.Parse(proxyAddr); err == nil {
			dialer.Proxy = http.ProxyURL(u)
		} else {
			log.Printf("mexc: invalid proxy %q, ignoring: %v", proxyAddr, err)
		}
	}

	return &client{
		url:          wsURL,
		proxy:        proxyAddr,
		dialer:       dialer,
		onMessage:    handler,
		topics:       topics,
		pingInterval: pingInterval,
		pongChan:     make(chan struct{}, 1),
		proto:        proto,
	}
}

func (c *client) Connect(parent context.Context) error {
	conn, _, err := c.dialer.Dial(c.url, nil)
	if err != nil {
		return fmt.Errorf("mexc dial %s: %w", c.url, err)
	}

	c.mutex.Lock()
	c.conn = conn
	c.isConnected = true
	c.mutex.Unlock()

	c.ctx, c.cancel = context.WithCancel(parent)

	go c.readLoop(conn)
	go c.ping(c.pingInterval)

	if err := c.proto.subscribe(c, c.topics); err != nil {
		_ = c.Disconnect()
		return fmt.Errorf("mexc subscribe: %w", err)
	}
	return nil
}

func (c *client) readLoop(conn *websocket.Conn) {
	defer c.cancel()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-c.ctx.Done():
			default:
				log.Printf("mexc read error: %v", err)
			}
			return
		}

		if c.proto.isPong(raw) {
			select {
			case c.pongChan <- struct{}{}:
			default:
			}
			continue
		}

		if c.onMessage != nil {
			if err := c.onMessage(string(raw)); err != nil {
				log.Printf("mexc handler error: %v", err)
			}
		}
	}
}

func (c *client) ping(seconds int) {
	interval := time.Duration(seconds) * time.Second
	pongTimeout := time.Duration(seconds/2) * time.Second
	const maxMissed = 2

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-ticker.C:
			if err := c.writeJSON(c.proto.pingMsg); err != nil {
				log.Printf("mexc ping failed: %v; closing for reconnect", err)
				c.closeConn()
				return
			}

			select {
			case <-c.ctx.Done():
				return
			case <-c.pongChan:
				c.mutex.Lock()
				c.missedPings = 0
				c.mutex.Unlock()
			case <-time.After(pongTimeout):
				c.mutex.Lock()
				c.missedPings++
				missed := c.missedPings
				c.mutex.Unlock()
				log.Printf("mexc PONG timeout (missed %d)", missed)
				if missed > maxMissed {
					log.Printf("mexc missed pongs > %d; closing for reconnect", maxMissed)
					c.closeConn()
					return
				}
			}
		}
	}
}

func (c *client) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *client) Disconnect() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.proto.unsubscribe != nil {
		_ = c.proto.unsubscribe(c, c.topics)
	}
	c.closeConn()
	return nil
}

func (c *client) writeJSON(v interface{}) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.conn == nil {
		return errors.New("mexc: connection closed")
	}
	return c.conn.WriteJSON(v)
}

func (c *client) closeConn() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.isConnected = false
}

func splitSymbols(raw string) []string {
	parts := strings.Split(strings.Trim(raw, "[]"), ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
