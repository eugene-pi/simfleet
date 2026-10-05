package execution

import (
	"context"
	"io"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/runner"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Store interface {
	Claim(ctx context.Context, jobID core.JobID, workerID string) (int, error)
	LoadTask(ctx context.Context, jobID core.JobID) (core.Task, error)
	SaveResult(ctx context.Context, r store.ResultRecord) error
	ReleaseForRetry(ctx context.Context, jobID core.JobID, attempt int, cause string) error
	MarkFailed(ctx context.Context, jobID core.JobID, attempt int, cause string) error
	RenewLease(ctx context.Context, jobID core.JobID, attempt int) error
}

type Blob interface {
	Put(ctx context.Context, key string, body io.Reader) error
}

type Registry interface {
	Get(name string) (runner.Runner, bool)
}
