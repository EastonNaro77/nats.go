package nats

import (
	"context"
	"io"
)

type objectResult struct {
	sub *Subscription
	// ... other fields
}

func (o *objectResult) Read(p []byte) (n int, err error) {
	select {
	case <-o.sub.conn.ctx.Done():
		return 0, ErrConnectionClosed
	default:
	}

	// Implementation of chunk reading with context and connection monitoring
	msg, err := o.sub.NextMsgWithContext(o.ctx)
	if err != nil {
		return 0, err
	}
	// ... process msg
	return n, nil
}

func (o *objectResult) Close() error {
	// Ensure cleanup of ephemeral consumer
	return o.sub.Unsubscribe()
}