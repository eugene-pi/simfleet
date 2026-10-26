// internal/runner/runner.go
package runner

import (
	"context"

	"github.com/eugene-pi/simfleet/internal/core"
)

type Runner interface {
	Run(ctx context.Context, t core.Task) (core.Result, error)
}
