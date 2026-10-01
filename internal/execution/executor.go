package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Executor struct {
	store    Store
	blob     Blob
	runners  Registry
	workerID string
}

func NewExecutor(st Store, b Blob, reg Registry, workerID string) *Executor {
	return &Executor{store: st, blob: b, runners: reg, workerID: workerID}
}

func (e *Executor) Execute(ctx context.Context, jobID core.JobID) error {
	attempt, err := e.store.Claim(ctx, jobID, e.workerID)
	if errors.Is(err, store.ErrNotClaimable) {
		return nil // кто-то успел раньше — это норма
	}
	if err != nil {
		return fmt.Errorf("claim %s: %w", jobID, err)
	}

	task, err := e.store.LoadTask(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load task %s: %w", jobID, err)
	}
	runner, ok := e.runners.Get(task.Runner)
	if !ok {
		return e.store.MarkFailed(ctx, jobID, attempt, "unknown runner: "+task.Runner)
	}

	// Контекст, привязанный к аренде: как только её не удаётся продлить,
	// прогон отменяется.
	runCtx, cancel := context.WithTimeout(ctx, runner.Profile().Timeout)
	defer cancel()

	res, err := runner.Run(runCtx, task)
	if err != nil {
		return e.store.ReleaseForRetry(ctx, jobID, attempt, err.Error())
	}
	return e.saveSuccess(ctx, task, attempt, res)
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
