package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
)

// Consumer is the bus side the worker needs: a batch of records and a way to
// commit progress.
type Consumer interface {
	Poll(ctx context.Context) (core.Batch, error)
	Commit(ctx context.Context) error
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
	log      *slog.Logger
}

func New(c Consumer, e Executor) *Worker {
	return &Worker{
		consumer: c,
		exec:     e,
		log:      slog.Default().With("worker", config.WorkerID()),
	}
}

// Run consumes until the context is cancelled. It does not stop on job
// failures: a failed job is already recorded in Postgres, and the controller
// decides whether to retry it.
// Потребляет до отмены контекста. Не останавливается на сбоях заданий:
// отказавшее задание уже записано в Postgres, и решение о повторе принимает
// контроллер.
func (w *Worker) Run(ctx context.Context) error {
	var done, skipped, failed int

	defer func() {
		w.log.Info("worker stopped", "done", done, "skipped", skipped, "failed", failed)
	}()

	for {
		batch, err := w.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // shutdown, not a failure
			}
			return fmt.Errorf("poll: %w", err)
		}
		if batch.Closed {
			return nil
		}

		for _, rec := range batch.Records {
			// Stop between records rather than inside a job: the executor
			// handles its own cancellation.
			// Останавливаемся между заданиями, а не внутри: отменой самого
			// задания занимается executor.
			if ctx.Err() != nil {
				return nil
			}

			claimed, err := w.handle(ctx, rec)
			switch {
			case err != nil:
				failed++
			case claimed:
				done++
			default:
				skipped++
			}
		}

		// Commit only after the whole batch is processed. A crash before this
		// means redelivery, which Claim absorbs; committing earlier would
		// silently drop work.
		if len(batch.Records) > 0 {
			if err := w.consumer.Commit(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				w.log.Error("commit offsets", "error", err)
			}
		}
	}
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
