package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/scoring"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/jackc/pgx/v5"
)

type Store interface {
	ReclaimExpired(ctx context.Context, limit int) ([]store.ExpiredJob, error)
	FailExhausted(ctx context.Context, limit int) (int, error)
	FinalizeCompleted(ctx context.Context) (int, error)
	UnscoredResults(ctx context.Context, limit int) ([]core.PendingEvaluation, error)
	SaveEvaluation(ctx context.Context, jobID core.JobID, ev core.Evaluation, scoringHash string) error
	FetchUnpublished(ctx context.Context, limit int) ([]store.OutboxRow, pgx.Tx, error)
	MarkPublished(ctx context.Context, tx pgx.Tx, ids []int64) error
	PurgePublished(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

// Publisher is all the controller needs from the bus: a way to send a batch.
type Publisher interface {
	PublishBatch(ctx context.Context, rows []store.OutboxRow) error
}

type Controller struct {
	store     Store
	publisher Publisher
	interval  time.Duration
	batch     int
	lastPurge time.Time
}

func New(st Store, pub Publisher, interval time.Duration, batch int) *Controller {
	return &Controller{
		store:     st,
		publisher: pub,
		interval:  interval,
		batch:     batch,
		lastPurge: time.Now(),
	}
}

func (c *Controller) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.publishLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		c.reconcileLoop(ctx)
	}()

	wg.Wait()
	return nil
}

// publishLoop moves outbox rows to the bus. It runs on a tight tick because
// every delay here is idle worker time.
func (c *Controller) publishLoop(ctx context.Context) {
	published, err := c.PublishOutbox(ctx)
	if err != nil && ctx.Err() == nil {
		slog.Error("publish outbox", "error", err)
	}
	if published > 0 {
		slog.Debug("Jobs published", "published", published)
	}

	t := time.NewTicker(50 * time.Millisecond) // 500ms
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			published, err = c.PublishOutbox(ctx)
			if err != nil && ctx.Err() == nil {
				slog.Error("publish outbox", "error", err)
			}
			if published > 0 {
				slog.Debug("Jobs published", "published", published)
			}
		}
	}
}

// reconcileLoop brings the stored state back in line: expired leases,
// exhausted jobs, unscored results, finished experiments.
func (c *Controller) reconcileLoop(ctx context.Context) {
	t := time.NewTicker(c.interval) // 5s
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Reconcile(ctx); err != nil && ctx.Err() == nil {
				slog.Error("reconcile", "error", err)
			}
		}
	}
}

func (c *Controller) Reconcile(ctx context.Context) error {
	expired, err := c.store.ReclaimExpired(ctx, c.batch)
	if err != nil {
		return fmt.Errorf("reclaim: %w", err)
	}
	if len(expired) > 0 {
		slog.Info("claims expired / jobs reset", "count", len(expired))
	}

	failed, err := c.store.FailExhausted(ctx, c.batch)
	if err != nil {
		return fmt.Errorf("fail exhausted: %w", err)
	}
	if failed > 0 {
		slog.Warn("no more attempts", "count", failed)
	}

	_, err = c.scoreResults(ctx)
	if err != nil {
		return fmt.Errorf("scoring has failed: %w", err)
	}

	done, err := c.store.FinalizeCompleted(ctx)
	if err != nil {
		return fmt.Errorf("finalize: %w", err)
	}
	if done > 0 {
		slog.Info("эксперименты завершены", "count", done)
	}

	// Housekeeping runs on its own, much slower cadence.
	if time.Since(c.lastPurge) > time.Hour {
		n, err := c.store.PurgePublished(ctx, 24*time.Hour, 10_000)
		if err != nil {
			slog.Error("purge outbox", "error", err)
		} else if n > 0 {
			slog.Info("purged outbox rows", "count", n)
		}
		c.lastPurge = time.Now()
	}
	return nil
}

func (c *Controller) scoreResults(ctx context.Context) (int, error) {
	pending, err := c.store.UnscoredResults(ctx, c.batch)
	if err != nil {
		return 0, fmt.Errorf("unscored results: %w", err)
	}

	n := 0
	for _, p := range pending {
		ev, err := scoring.Evaluate(p.Scoring, p.Zones, p.Metrics, p.Flags)
		if err != nil {
			slog.Warn("scoring failed", "job_id", p.JobID, "error", err)
			continue
		}
		if err := c.store.SaveEvaluation(ctx, p.JobID, ev, p.ScoringHash); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (c *Controller) PublishOutbox(ctx context.Context) (int, error) {
	rows, tx, err := c.store.FetchUnpublished(ctx, c.batch)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) // no-op after a successful commit
	// ничего не делает после успешной фиксации
	if len(rows) == 0 {
		return 0, nil
	}

	if err := c.publisher.PublishBatch(ctx, rows); err != nil {
		return 0, fmt.Errorf("publish: %w", err)
	}

	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	if err := c.store.MarkPublished(ctx, tx, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(rows), nil
}
