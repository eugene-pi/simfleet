package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/scoring"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Store interface {
	ReclaimExpired(ctx context.Context, limit int) ([]store.ExpiredJob, error)
	FailExhausted(ctx context.Context, limit int) (int, error)
	FinalizeCompleted(ctx context.Context) (int, error)
	UnscoredResults(ctx context.Context, limit int) ([]core.PendingEvaluation, error)
	SaveEvaluation(ctx context.Context, jobID core.JobID, ev core.Evaluation, scoringHash string) error
}

type Controller struct {
	store    Store
	interval time.Duration
	batch    int
}

func New(st Store, interval time.Duration, batch int) *Controller {
	return &Controller{store: st, interval: interval, batch: batch}
}

func (c *Controller) Run(ctx context.Context) error {
	t := time.NewTicker(c.interval)
	defer t.Stop()

	for {
		start := time.Now()
		if err := c.Reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return nil // отмена, а не сбой
			}
			slog.Error("проход не удался", "error", err)
		} else {
			slog.Debug("проход завершён", "duration", time.Since(start))
		}

		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (c *Controller) Reconcile(ctx context.Context) error {
	expired, err := c.store.ReclaimExpired(ctx, c.batch)
	if err != nil {
		return fmt.Errorf("reclaim: %w", err)
	}
	if len(expired) > 0 {
		slog.Info("аренды истекли, задания возвращены", "count", len(expired))
	}

	failed, err := c.store.FailExhausted(ctx, c.batch)
	if err != nil {
		return fmt.Errorf("fail exhausted: %w", err)
	}
	if failed > 0 {
		slog.Warn("попытки исчерпаны", "count", failed)
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
