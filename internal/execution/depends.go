package execution

import (
	"context"
	"io"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/runner"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Store interface {
	Claim(ctx context.Context, jobID core.JobID, workerID string, ld time.Duration) (int, error)
	LoadTask(ctx context.Context, jobID core.JobID) (core.Task, error)
	SaveResult(ctx context.Context, r store.ResultRecord) error
	ReleaseForRetry(ctx context.Context, jobID core.JobID, attempt int, cause string) error
	MarkFailed(ctx context.Context, jobID core.JobID, attempt int, cause string) error
	RenewLease(ctx context.Context, jobID core.JobID, attempt int, ld time.Duration) error
}

type Blob interface {
	Put(ctx context.Context, key string, body io.Reader) error
}

type Registry interface {
	Get(name string) (runner.Runner, bool)
}
