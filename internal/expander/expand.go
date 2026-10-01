package expander

import (
	"context"
	"fmt"
	"strings"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/runner"
	"github.com/eugene-pi/simfleet/internal/store"
)

type Store interface {
	CreateExperiment(ctx context.Context, exp store.NewExperiment) error
}

type SchemaProvider interface {
	ParamSchema() map[string]core.ParamKind
}

type Registry interface {
	Get(name string) (runner.Runner, bool)
	// Names нужен только для сообщения об ошибке
	Names() []string
}

func Create(ctx context.Context, st Store, reg Registry, spec core.Spec) (core.ExperimentID, int, error) {
	r, ok := reg.Get(spec.Runner)
	if !ok {
		return core.ExperimentID{}, 0, fmt.Errorf("%w: %q (доступны: %s)",
			runner.ErrUnknownRunner, spec.Runner, strings.Join(reg.Names(), ", "))
	}
	if err := ValidateSweep(r.ParamSchema(), spec.Sweep); err != nil {
		return core.ExperimentID{}, 0, err
	}

	combos, err := Cartesian(spec.Sweep)
	if err != nil {
		return core.ExperimentID{}, 0, err
	}

	expID := core.ExperimentID(uuid.NewV7())

	jobs := make([]store.NewJob, len(combos))
	for i, params := range combos {
		jobs[i] = store.NewJob{
			ID:     core.JobID(uuid.NewV7()),
			Idx:    i,
			Params: params,
			Seed:   DeriveSeed(expID, i),
		}
	}

	err = st.CreateExperiment(ctx, store.NewExperiment{
		ID:     expID,
		Spec:   spec,
		Runner: spec.Runner,
		Jobs:   jobs,
	})
	if err != nil {
		return core.ExperimentID{}, 0, err
	}
	return expID, len(jobs), nil
}

func ValidateSweep(schema map[string]core.ParamKind, sweep core.Sweep) error {
	for name, pv := range sweep {
		kind, known := schema[name]
		if !known {
			return fmt.Errorf("%w: %q", ErrUnknownParam, name)
		}
		vals, err := expandParam(pv)
		if err != nil {
			return fmt.Errorf("параметр %q: %w", name, err)
		}
		for _, v := range vals {
			if v.Kind() != kind {
				return fmt.Errorf("%w: %q ожидает %s, получено %s",
					ErrWrongParamKind, name, kind, v.Kind())
			}
		}
	}
	for name := range schema {
		if _, ok := sweep[name]; !ok {
			return fmt.Errorf("%w: %q", ErrMissingParam, name)
		}
	}
	return nil
}
