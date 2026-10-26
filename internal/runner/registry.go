package runner

import (
	"errors"
	"fmt"

	"github.com/eugene-pi/simfleet/internal/core"
)

var (
	ErrUnknownRunner = errors.New("unknown runner")
)

// Descriptor is everything the platform needs to know about a runner without
// knowing what it does: its name, how to deploy it, and what parameters it
// accepts. Deployment-level values (topic, group, partitions) are derived
// from the name, so adding a runner never touches platform code.
type Descriptor struct {
	Name       string
	Partitions int32
	Profile    core.Profile
	Params     map[string]core.ParamKind
}

// Config carries runner-specific settings from the environment. The platform
// passes it through without interpreting it.
type Environment struct {
	WorkDir string
	Vars    map[string]string
}

// Factory builds the implementation. Registration cannot fail; building can,
// so the error surfaces in main where it can be reported properly.
type Factory func(env Environment) (Runner, error)

type entry struct {
	desc    Descriptor
	factory Factory
}

var registry = map[string]entry{}

// Register adds a workload. Panics on a duplicate name: that is a programming
// error that must surface at startup, not a condition to handle.
func Register(d Descriptor, f Factory) {
	if d.Name == "" {
		panic("runner: empty name")
	}
	if _, dup := registry[d.Name]; dup {
		panic("runner: already registered: " + d.Name)
	}
	registry[d.Name] = entry{desc: d, factory: f}
}

func Get(name string) (Descriptor, bool) {
	e, ok := registry[name]
	return e.desc, ok
}

func Names() []string {
	out := make([]string, 0, len(registry))
	for name := range registry {
		out = append(out, name)
	}
	return out
}

// Build instantiates one workload. Only the worker calls this, and only for
// the workload it was configured with.
// Создаёт одну нагрузку. Вызывает только исполнитель, и только ту, которая
// ему задана.
func Build(name string, env Environment) (Runner, error) {
	e, ok := registry[name]

	if !ok {
		return nil, fmt.Errorf("%w: %q (available: %v)", ErrUnknownRunner, name, Names())
	}
	r, err := e.factory(env)
	if err != nil {
		return nil, fmt.Errorf("build runner %q: %w", name, err)
	}
	return r, nil
}
