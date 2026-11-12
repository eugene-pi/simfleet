// Package submit turns a spec file into an experiment: parse, resolve the
// runner, expand the sweep and store it with its outbox rows. Shared by the
// simfleet CLI and the runlocal dev harness, so both submit the same way.
//
// Importing this package does not register any runner: the binary that
// submits imports the runner packages it supports, as cmd/worker does.
package submit

import (
	"context"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/expander"
	"github.com/eugene-pi/simfleet/internal/runner"
)

// LoadSpec reads and parses a YAML (or JSON) spec file.
func LoadSpec(path string) (core.Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return core.Spec{}, fmt.Errorf("read spec: %w", err)
	}
	return ParseSpec(data)
}

func ParseSpec(data []byte) (core.Spec, error) {
	var spec core.Spec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return core.Spec{}, fmt.Errorf("parse spec: %w", err)
	}
	return spec, nil
}

// Submit creates the experiment and its jobs. It returns the experiment id
// and the number of jobs.
func Submit(ctx context.Context, st expander.Store, spec core.Spec) (core.ExperimentID, int, error) {
	desc, ok := runner.Get(spec.Runner)
	if !ok {
		return core.ExperimentID{}, 0, fmt.Errorf("runner %q unavailable, enabled runners are %v",
			spec.Runner, runner.Names())
	}
	return expander.Create(ctx, st, desc, spec)
}
