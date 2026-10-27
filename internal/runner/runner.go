// internal/runner/runner.go
package runner

import (
	"context"
	"errors"

	"github.com/eugene-pi/simfleet/internal/core"
)

// ErrPermanent marks a failure that retrying cannot fix: bad parameters,
// unsupported input. Wrap it (fmt.Errorf("%w: ...", runner.ErrPermanent)) and
// the job fails at once instead of burning its remaining attempts. Any other
// error is treated as transient and retried.
var ErrPermanent = errors.New("permanent failure")

type Runner interface {
	// Run must be safe for concurrent use
	Run(ctx context.Context, t core.Task) (core.Result, error)
}
