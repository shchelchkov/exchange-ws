package wsconn

import "context"

type Connection interface {
	Disconnect() error
	Done() <-chan struct{}
}

func Send(ctx context.Context, msgs chan<- map[string]interface{}, msg map[string]interface{}) error {
	select {
	case msgs <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
