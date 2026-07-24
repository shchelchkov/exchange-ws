package process

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"exchange-ws/client"
	"exchange-ws/config"
	"exchange-ws/kafka"

	"github.com/bytedance/sonic"
)

type Stream struct {
	cfg             config.Config
	producer        kafka.Producer
	msgs            chan map[string]interface{}
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	backoffMin      time.Duration
	backoffMax      time.Duration
	maxRetries      int
	reconnectJitter float64
	state           atomic.Int32
}

func NewStream(parent context.Context, cfg config.Config) *Stream {
	ctx, cancel := context.WithCancel(parent)
	producer := kafka.NewProducer(cfg)
	return &Stream{
		cfg:             cfg,
		producer:        *producer,
		msgs:            make(chan map[string]interface{}, 1024),
		ctx:             ctx,
		cancel:          cancel,
		backoffMin:      500 * time.Millisecond,
		backoffMax:      30 * time.Second,
		maxRetries:      0,
		reconnectJitter: 0.2,
	}
}

func (stream *Stream) Start() {
	stream.setState(StateConnecting)

	stream.wg.Add(1)
	go stream.worker()

	stream.wg.Add(1)
	go stream.connectLoop()
}

func (stream *Stream) Stop() {
	stream.cancel()
	stream.wg.Wait()
	if err := stream.producer.Close(); err != nil {
		log.Printf("stream %s: producer close: %v", stream.cfg.ConfigCode, err)
	}
}

func (stream *Stream) State() StreamState {
	return StreamState(stream.state.Load())
}

func (stream *Stream) setState(s StreamState) {
	stream.state.Store(int32(s))
}

func (stream *Stream) connectLoop() {
	defer stream.wg.Done()
	defer stream.setState(StateStopped)

	backoff := stream.backoffMin
	reconnecting := false
	for {
		select {
		case <-stream.ctx.Done():
			return
		default:
		}

		if reconnecting {
			stream.setState(StateReconnecting)
		} else {
			stream.setState(StateConnecting)
		}

		conn, err := client.StreamWebSocket(stream.ctx, stream.cfg, stream.msgs)
		if err != nil {
			log.Printf("connect error: %v (retry in %s)", err, backoff)
			sleep := backoff + time.Duration(rand.Float64()*stream.reconnectJitter*float64(backoff))
			select {
			case <-time.After(sleep):
			case <-stream.ctx.Done():
				return
			}
			backoff = min(backoff*2, stream.backoffMax)
			reconnecting = true
			continue
		}
		backoff = stream.backoffMin

		stream.setState(StateConnected)
		select {
		case <-stream.ctx.Done():
			_ = conn.Disconnect()
			return
		case <-conn.Done():
			_ = conn.Disconnect()
			select {
			case <-stream.ctx.Done():
				return
			default:
				log.Printf("connection lost, reconnecting")
				reconnecting = true
			}
		}
	}
}

func (stream *Stream) worker() {
	defer stream.wg.Done()
	for {
		select {
		case <-stream.ctx.Done():

			for {
				select {
				case m := <-stream.msgs:
					stream.publish(m)
				default:
					return
				}
			}
		case m := <-stream.msgs:
			stream.publish(m)
		}
	}
}

func (stream *Stream) publish(m map[string]interface{}) {
	payload, err := sonic.Marshal(m)
	if err != nil {
		fmt.Printf("failed to marshal message: %v\n", err)
		return
	}
	topic, ok := m["topic"].(string)
	if !ok {
		fmt.Printf("publish: missing or non-string topic, dropping message: %v\n", m["topic"])
		return
	}
	if err := stream.producer.Send(topic, payload); err != nil {
		fmt.Printf("kafka send failed: %v\n", err)
	}
}
