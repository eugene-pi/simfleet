package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"sigs.k8s.io/yaml"

	"github.com/eugene-pi/simfleet/internal/blob"
	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/envfile"
	"github.com/eugene-pi/simfleet/internal/execution"
	"github.com/eugene-pi/simfleet/internal/expander"
	"github.com/eugene-pi/simfleet/internal/runner"
	_ "github.com/eugene-pi/simfleet/internal/runner/crossing"
	"github.com/eugene-pi/simfleet/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: runlocal <spec.yaml>")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		envfile.LoadEnv()
		dsn = os.Getenv("DATABASE_URL")
		if dsn == "" {
			log.Fatal("DATABASE_URL is not set")
		}
	}

	if err := run(dsn); err != nil {
		log.Fatal(err)
	}
}

func run(dsn string) error {
	ctx := context.Background()
	st, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	spec, err := loadSpec(os.Args[1])
	if err != nil {
		return err
	}

	registry, err := runner.Build(runner.Config{WorkDir: os.TempDir()})
	if err != nil {
		return err
	}

	// 1. Создать эксперимент и развернуть перебор
	expID, n, err := expander.Create(ctx, st, registry, spec)
	if err != nil {
		return err
	}
	log.Printf("эксперимент %s: %d заданий", expID, n)

	exec := execution.NewExecutor(st, blob.NewLocalFS("./out"), registry, config.WorkerID())

	for {
		ids, err := st.NextQueued(ctx, expID, 100)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if err := exec.Execute(ctx, id); err != nil {
				log.Printf("задание %s: %v", id, err)
			}
		}
	}

	// 3. Сводка
	sum, err := st.Summary(ctx, expID)
	if err != nil {
		return err
	}
	fmt.Printf("выполнено %d, отказов %d\n", sum.Completed, sum.Failed)
	return nil
}

func loadSpec(path string) (core.Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return core.Spec{}, fmt.Errorf("read spec: %w", err)
	}
	var spec core.Spec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return core.Spec{}, fmt.Errorf("parse spec: %w", err)
	}
	return spec, nil
}
