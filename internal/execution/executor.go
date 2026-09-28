package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Executor struct {
	store    Store
	blob     Blob
	runners  map[string]Runner
	workerID string

	leaseTTL   time.Duration // 60s
	renewEvery time.Duration // 20s — треть от TTL
}

func (e *Executor) Execute(ctx context.Context, jobID uuid.UUID) error {
	attempt, err := e.store.Claim(ctx, jobID, e.workerID)
	if errors.Is(err, store.ErrNotClaimable) {
		return nil // кто-то успел раньше — это норма
	}
	if err != nil {
		return err // сбой базы: сообщение не подтверждаем
	}

	task, err := e.store.LoadTask(ctx, jobID)
	if err != nil {
		return err
	}
	runner, ok := e.runners[task.Runner]
	if !ok {
		return e.store.MarkFailed(ctx, jobID, attempt, "unknown runner: "+task.Runner)
	}

	// Контекст, привязанный к аренде: как только её не удаётся продлить,
	// прогон отменяется.
	runCtx, cancel := context.WithTimeout(ctx, runner.Profile().Timeout)
	defer cancel()
	go e.keepLease(runCtx, cancel, jobID, attempt)

	// Метка горутины: в Go 1.27 она попадает в аварийную трассировку,
	// поэтому при панике сразу видно, какое задание её вызвало.
	var res Result
	var runErr error
	pprof.Do(runCtx, pprof.Labels("job_id", jobID.String(), "runner", runner.Name()),
		func(c context.Context) {
			res, runErr = runSafely(c, runner, task)
		})

	if runErr != nil {
		return e.handleFailure(ctx, jobID, attempt, runErr)
	}
	return e.saveSuccess(ctx, task, attempt, res)
}

func (e *Executor) saveSuccess(ctx context.Context, t core.Task, attempt int, res core.Result) error {
	key := fmt.Sprintf("exp/%s/job/%s/attempt/%d/trace.parquet",
		t.ExperimentID, t.JobID, attempt)

	if len(res.Artifact) > 0 {
		if err := e.blob.Put(ctx, key, bytes.NewReader(res.Artifact)); err != nil {
			return err
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
