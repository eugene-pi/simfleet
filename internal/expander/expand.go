package expander

import (
	"context"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

// internal/expander/expand.go
func Create(ctx context.Context, st Store, spec core.Spec) (core.ExperimentID, int, error) {
	expID := core.ExperimentID(uuid.NewV7())

	combos := Cartesian(spec.Sweep) // чистая функция, тестируется отдельно
	if len(combos) == 0 {
		return expID, 0, ErrEmptySweep
	}

	jobs := make([]store.NewJob, len(combos))
	for i, params := range combos {
		jobs[i] = store.NewJob{
			ID:     core.JobID(uuid.NewV7()),
			Idx:    i,
			Params: params,
			Seed:   DeriveSeed(expID, i),
		}
	}
	return expID, len(jobs), st.CreateExperiment(ctx, expID, spec, jobs)
}
