package worker

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"taskforge/internal/job"
)

type Handler func(ctx context.Context, j job.Job) error

// sleep waits for d, but returns early if ctx is cancelled/timed out.
func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func EmailHandler(ctx context.Context, j job.Job) error {
	if j.Payload["to"] == "" {
		return errors.New("missing 'to' in payload")
	}
	return sleep(ctx, 800*time.Millisecond)
}

func ReportHandler(ctx context.Context, j job.Job) error {
	if err := sleep(ctx, 2*time.Second); err != nil {
		return err
	}
	if rand.Float64() < 0.4 {
		return errors.New("report service unavailable")
	}
	return nil
}