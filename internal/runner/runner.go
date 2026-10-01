// internal/runner/runner.go
package runner

import (
	"context"
	"sort"
	"sync"

	"github.com/eugene-pi/simfleet/internal/core"
)

type Runner interface {
	Name() string
	Profile() core.Profile
	ParamSchema() map[string]core.ParamKind
	Run(ctx context.Context, t core.Task) (core.Result, error)
}

// Factory создаёт реализацию. Конфигурация передаётся при сборке,
// а не при регистрации: в init никакой конфигурации ещё нет.
type Factory func(cfg Config) (Runner, error)

type Config struct {
	WorkDir string
	Env     map[string]string // то, что нужно конкретной реализации
}

var (
	mu        sync.Mutex
	factories = map[string]Factory{}
)

func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := factories[name]; dup {
		panic("runner already registered: " + name)
	}
	factories[name] = f
}

func Names() []string {
	mu.Lock()
	defer mu.Unlock()
	out := make([]string, 0, len(factories))
	for n := range factories {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
