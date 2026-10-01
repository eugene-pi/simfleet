package runner

import (
	"errors"
	"fmt"
)

var (
	ErrUnknownRunner = errors.New("unknown runner")
)

type Registry map[string]Runner

// Build создаёт реализации по именам. Пустой список — создать все.
func Build(cfg Config, names ...string) (Registry, error) {
	mu.Lock()
	defer mu.Unlock()

	if len(names) == 0 {
		names = make([]string, 0, len(factories))
		for n := range factories {
			names = append(names, n)
		}
	}

	reg := make(Registry, len(names))
	for _, n := range names {
		f, ok := factories[n]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownRunner, n)
		}
		r, err := f(cfg)
		if err != nil {
			return nil, fmt.Errorf("build runner %q: %w", n, err)
		}
		reg[n] = r
	}
	return reg, nil
}

func (r Registry) Get(name string) (Runner, bool) { v, ok := r[name]; return v, ok }

func (r Registry) Names() []string {
	rv := make([]string, 0, len(r))
	idx := 0
	for k, _ := range r {
		rv[idx] = k
		idx++
	}
	return rv
}
