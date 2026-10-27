package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"golang.org/x/sync/errgroup"
)

// Consumer is the bus side the worker needs: a batch of records and a way to
// say a record is finished. When and how progress is committed is the bus's
// business; offsets never reach the worker.
type Consumer interface {
	Poll(ctx context.Context) (core.Batch, error)
	Done(rec core.Record)
}

// Executor runs one job end to end. Returns false when the job was already
// claimed by someone else.
type Executor interface {
	Execute(ctx context.Context, jobID core.JobID) (bool, error)
}

// Worker consumes job messages from the bus and runs them through the
// executor. It owns the consume loop and nothing else: claiming, leases and
// result writing all live in the executor.
type Worker struct {
	consumer Consumer
	exec     Executor
	cfg      config.WorkerConfig
	log      *slog.Logger
}

func New(c Consumer, e Executor, cfg config.WorkerConfig) *Worker {
	return &Worker{
		consumer: c,
		exec:     e,
		cfg:      cfg,
		log:      slog.Default().With("worker", cfg.WorkerId),
	}
}

// Reader keeps polling while a job is in flight, so the fetch loop never
// waits on simulation work. The processor reports each finished record back
// to the consumer, which commits how far processing got — not how far
// reading got.
func (w *Worker) Run(ctx context.Context) error {
	const chBufferCoef int = 10 // multiplies
	records := make(chan core.Record, w.cfg.BatchSize*chBufferCoef)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return w.readLoop(ctx, records) })
	g.Go(func() error { return w.processLoop(ctx, records) })

	return g.Wait()
}

func (w *Worker) readLoop(ctx context.Context, out chan<- core.Record) error {
	defer close(out) // tells processLoop to drain and exit
	// сообщает processLoop, что пора дочитать и выйти
	for {
		batch, err := w.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("poll: %w", err)
		}
		if batch.Closed {
			return nil
		}
		for _, rec := range batch.Records {
			select {
			case out <- rec:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (w *Worker) processLoop(ctx context.Context, in <-chan core.Record) error {
	for rec := range in {
		if ctx.Err() != nil {
			return nil
		}
		w.handle(ctx, rec) // errors are logged, not propagated
		// ошибки журналируются, а не распространяются

		// A record interrupted by shutdown is not done: the job may still be
		// queued with no other message pointing at it. Leaving it uncommitted
		// gets it redelivered to another worker.
		if ctx.Err() != nil {
			return nil
		}
		w.consumer.Done(rec)
	}
	return nil
}

// handle processes one record. A record that cannot be parsed is skipped: it
// will never parse, and blocking the partition forever is worse than losing
// one message.
func (w *Worker) handle(ctx context.Context, rec core.Record) (bool, error) {
	var msg core.JobMessage
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		w.log.Error("malformed message, skipping",
			"partition", rec.Partition, "offset", rec.Offset, "error", err)
		return false, nil
	}

	log := w.log.With("job_id", msg.JobID, "experiment_id", msg.ExperimentID)

	claimed, err := w.exec.Execute(ctx, msg.JobID)
	if err != nil {
		log.Error("job failed", "error", err)
		return false, err
	}
	if !claimed {
		log.Debug("job already claimed elsewhere")
		return false, nil
	}
	log.Debug("job done")
	return true, nil
}
