package execution

import (
	"context"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"

	"io"
	"uuid"
)

type Store interface {
	Claim(ctx context.Context, jobID uuid.UUID, workerID string) (int, error)
	RenewLease(ctx context.Context, jobID uuid.UUID, attempt int) error
	SaveResult(ctx context.Context, r store.ResultRecord) error
	ReleaseForRetry(ctx context.Context, jobID uuid.UUID, attempt int, cause string) error
	MarkFailed(ctx context.Context, jobID uuid.UUID, attempt int, cause string) error
	LoadTask(ctx context.Context, jobID uuid.UUID) (core.Task, error)
}

type Blob interface {
	Put(ctx context.Context, key string, body io.Reader) error
}

type Runner interface {
	Name() string
	Profile() core.Profile
	Run(ctx context.Context, t core.Task) (core.Result, error)
}
