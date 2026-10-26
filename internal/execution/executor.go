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
	"github.com/eugene-pi/simfleet/internal/runner"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Executor struct {
	store Store
	blob  Blob
	desc  runner.Descriptor
	r     runner.Runner
	doom  int // test only
	wc    config.WorkerConfig
}

// NewExecutor fails on a non-positive Profile.Timeout: context.WithTimeout
// would return an already-expired context, and every job would cancel instantly.
func NewExecutor(st Store, b Blob, desc runner.Descriptor, r runner.Runner, wc config.WorkerConfig) (*Executor, error) {
	if desc.Profile.Timeout <= 0 {
		return nil, fmt.Errorf("runner %q: profile timeout must be positive, got %v",
			desc.Name, desc.Profile.Timeout)
	}
	return &Executor{
		store: st,
		blob:  b,
		r:     r,
		desc:  desc,
		wc:    wc,
		doom:  rand.N(100) + 50}, nil
}

func (e *Executor) Execute(ctx context.Context, jobID core.JobID) (bool, error) {
	attempt, err := e.store.Claim(ctx, jobID, e.wc.WorkerId, e.wc.LeaseDuration)
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
	if task.Runner != e.desc.Name {
		return false, fmt.Errorf("unexpected task runner '%s'", task.Runner)
	}
	//TODO: exclude from prod
	if e.wc.WorkerId == "w1" {
		e.doom -= 1
		if e.doom == 0 {
			os.Exit(137)
		}
	}

	// context tied with lease duration.
	runCtx, cancel := context.WithTimeout(ctx, e.desc.Profile.Timeout)
	defer cancel()

	go e.keepLease(runCtx, cancel, jobID, attempt)

	res, err := e.r.Run(runCtx, task)
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
