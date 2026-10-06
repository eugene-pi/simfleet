package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Executor struct {
	store    Store
	blob     Blob
	runners  Registry
	workerID string
	doom     int
	wc       config.WorkerConfig
}

func NewExecutor(st Store, b Blob, reg Registry, workerID string, wc config.WorkerConfig) *Executor {
	return &Executor{store: st, blob: b, runners: reg, workerID: workerID, wc: wc, doom: rand.N(100) + 50}
}

func (e *Executor) Execute(ctx context.Context, jobID core.JobID) (bool, error) {
	attempt, err := e.store.Claim(ctx, jobID, e.workerID, e.wc.LeaseDuration)
	if errors.Is(err, store.ErrNotClaimable) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim %s: %w", jobID, err)
	}

	task, err := e.store.LoadTask(ctx, jobID)
	if err != nil {
		return false, fmt.Errorf("load task %s: %w", jobID, err)
	}
	runner, ok := e.runners.Get(task.Runner)
	if !ok {
		return false, e.store.MarkFailed(ctx, jobID, attempt, "unknown runner: "+task.Runner)
	}

	if e.workerID == "w1" {
		e.doom -= 1
		if e.doom == 0 {
			os.Exit(137)
		}
	}

	// Контекст, привязанный к аренде: как только её не удаётся продлить,
	// прогон отменяется.
	runCtx, cancel := context.WithTimeout(ctx, runner.Profile().Timeout)
	defer cancel()

	go e.keepLease(runCtx, cancel, jobID, attempt)

	res, err := runner.Run(runCtx, task)
	if err != nil {
		return false, e.store.ReleaseForRetry(ctx, jobID, attempt, err.Error())
	}
	return true, e.saveSuccess(ctx, task, attempt, res)
}

func (e *Executor) saveSuccess(ctx context.Context, t core.Task, attempt int, res core.Result) error {
	var key string
	if len(res.Artifact) > 0 {
		key = fmt.Sprintf("exp/%s/job/%s/attempt/%d/trace.json",
			t.ExperimentID, t.JobID, attempt)
		if err := e.blob.Put(ctx, key, bytes.NewReader(res.Artifact)); err != nil {
			return fmt.Errorf("put artifact: %w", err)
		}
	} else {
		key = ""
	}

	err := e.store.SaveResult(ctx, store.ResultRecord{
		JobID:       t.JobID,
		Attempt:     attempt,
		ArtifactKey: key,
		Result:      res,
	})
	if errors.Is(err, store.ErrFenced) {
		return nil
	}
	return err
}

func (e *Executor) keepLease(ctx context.Context, cancel context.CancelFunc,
	jobID core.JobID, attempt int) {

	t := time.NewTicker(e.wc.RenewEvery)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := e.store.RenewLease(ctx, jobID, attempt, e.wc.RenewEvery); err != nil {
				slog.Warn("job lease has been lost", "job", jobID, "error", err)
				cancel()
				return
			}
		}
	}
}
