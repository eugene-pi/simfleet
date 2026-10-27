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
	"github.com/eugene-pi/simfleet/internal/job"
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

// writeTimeout bounds the state writes made after a run. They use a context
// detached from shutdown, so it must stay well inside the pod's
// terminationGracePeriodSeconds.
const writeTimeout = 10 * time.Second

// writeCtx is for state writes once the job is claimed. It ignores shutdown —
// the job is ours and must be left in a definite state — but is bounded by
// writeTimeout, counted from the moment of the write.
func writeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
}

func (e *Executor) Execute(ctx context.Context, jobID core.JobID) (bool, error) {
	attempt, err := e.store.Claim(ctx, jobID, e.wc.WorkerId, e.wc.LeaseDuration)
	if errors.Is(err, store.ErrNotClaimable) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim %s: %w", jobID, err)
	}

	// From here on the job is ours: every exit path must leave it in a
	// definite state, even when shutdown has already cancelled ctx.
	task, err := e.store.LoadTask(ctx, jobID)
	if err != nil {
		err = fmt.Errorf("load task %s: %w", jobID, err)
		return true, errors.Join(err, e.giveBack(ctx, jobID, attempt, err))
	}
	if task.Runner != e.desc.Name {
		// A routing or configuration error; no other attempt on this worker
		// type will do better, so fail it now instead of burning attempts.
		err = fmt.Errorf("%w: task runner %q, worker runs %q",
			runner.ErrPermanent, task.Runner, e.desc.Name)
		return true, errors.Join(err, e.fail(ctx, jobID, attempt, err))
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
		return true, e.giveBack(ctx, jobID, attempt, err)
	}
	// Saved even if shutdown began after the run finished: the work is done.
	return true, e.saveSuccess(ctx, task, attempt, res)
}

// giveBack handles an attempt that did not produce a result. Interrupted by
// shutdown: hand the job straight back so another worker can take it now,
// rather than after the lease expires. Otherwise apply the retry policy.
func (e *Executor) giveBack(ctx context.Context, jobID core.JobID, attempt int, cause error) error {
	if ctx.Err() == nil {
		return e.fail(ctx, jobID, attempt, cause)
	}
	wctx, cancel := writeCtx(ctx)
	defer cancel()
	return ignoreFenced(e.store.ReleaseForRetry(wctx, jobID, attempt, "worker shutdown"))
}

// fail retries the job or retires it, per job.OnFailure: permanent errors and
// exhausted attempts go to failed, everything else back to the queue.
func (e *Executor) fail(ctx context.Context, jobID core.JobID, attempt int, cause error) error {
	kind := job.FailureInfra
	if errors.Is(cause, runner.ErrPermanent) {
		kind = job.FailurePermanent
	}

	wctx, cancel := writeCtx(ctx)
	defer cancel()

	var err error
	if job.OnFailure(attempt, kind) == job.EventExhaust {
		slog.Warn("job failed", "job", jobID, "attempt", attempt, "error", cause)
		err = e.store.MarkFailed(wctx, jobID, attempt, cause.Error())
	} else {
		slog.Info("job attempt failed, retrying", "job", jobID, "attempt", attempt, "error", cause)
		err = e.store.ReleaseForRetry(wctx, jobID, attempt, cause.Error())
	}
	return ignoreFenced(err)
}

// ignoreFenced treats a fenced write as success: the job already moved on
// (lease reclaimed, another attempt running), so there is nothing to undo.
func ignoreFenced(err error) error {
	if errors.Is(err, store.ErrFenced) {
		return nil
	}
	return err
}

func (e *Executor) saveSuccess(ctx context.Context, t core.Task, attempt int, res core.Result) error {
	ctx, cancel := writeCtx(ctx)
	defer cancel()

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
	return ignoreFenced(err)
}

// keepLease extends the lease by a full LeaseDuration every RenewEvery
// (LeaseDuration/3). Only a definite loss stops the job; a failed renewal
// (network, DB hiccup) is retried on the next tick, and the lease still has
// time left — that slack is the reason for renewing at a third.
func (e *Executor) keepLease(ctx context.Context, cancel context.CancelFunc,
	jobID core.JobID, attempt int) {

	t := time.NewTicker(e.wc.RenewEvery)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := e.store.RenewLease(ctx, jobID, attempt, e.wc.LeaseDuration)
			switch {
			case err == nil:
			case errors.Is(err, store.ErrLeaseLost):
				slog.Warn("job lease has been lost", "job", jobID, "attempt", attempt)
				cancel()
				return
			case ctx.Err() != nil:
				return
			default:
				slog.Warn("lease renewal failed, will retry", "job", jobID, "error", err)
			}
		}
	}
}
