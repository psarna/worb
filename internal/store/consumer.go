package store

import (
	"context"
	"time"
)

func consumeBatches[T any](
	ctx context.Context,
	in <-chan T,
	batchSize int,
	flushEvery time.Duration,
	process func([]T) error,
) error {
	buf := make([]T, 0, batchSize)
	timer := time.NewTimer(flushEvery)
	defer timer.Stop()

	for {
		select {
		case item, ok := <-in:
			if !ok {
				if len(buf) > 0 {
					process(buf)
				}
				return nil
			}
			buf = append(buf, item)
			if len(buf) >= batchSize {
				process(buf)
				buf = buf[:0]
				timer.Reset(flushEvery)
			}
		case <-timer.C:
			if len(buf) > 0 {
				process(buf)
				buf = buf[:0]
			}
			timer.Reset(flushEvery)
		case <-ctx.Done():
			if len(buf) > 0 {
				process(buf)
			}
			return ctx.Err()
		}
	}
}
