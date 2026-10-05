package main

import (
	"context"
	"errors"
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
		log.Fatal("usage: runlocal <cmd> <spec.yaml>")
	}
	envfile.LoadEnv()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
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
	registry, err := runner.Build(runner.Config{WorkDir: os.TempDir()})
	if err != nil {
		return err
	}

	switch os.Args[1] {
	case "submit":
		if len(os.Args) > 2 {
			_, err := submit(ctx, st, registry, os.Args[2])
			return err
		} else {
			return errors.New("usage: runlocal submit <spec.yaml>")
		}
	case "work":
		return work(ctx, st, registry)
	case "spawn":
		return spawn(ctx, 2)
	default:
		if len(os.Args) > 2 {
			return fmt.Errorf("unknown command %v", os.Args[1])
		}
	}
	return runSingle(ctx, st, registry, os.Args[1])
}

func submit(ctx context.Context, st *store.Postgres, reg runner.Registry, path string) (core.ExperimentID, error) {
	spec, err := loadSpec(path)
	if err != nil {
		return core.ExperimentID{}, err
	}
	expID, n, err := expander.Create(ctx, st, reg, spec)
	if err != nil {
		return core.ExperimentID{}, err
	}
	fmt.Println(expID)
	log.Printf("создано заданий: %d", n)
	return expID, nil
}

func work(ctx context.Context, st *store.Postgres, reg runner.Registry) error {
	workerID := config.WorkerID()
	wc := config.LoadConfig()
	exec := execution.NewExecutor(st, blob.NewLocalFS("./out"), reg, workerID, wc)

	log.Printf("исполнитель %s начал работу", workerID)
	failed, done, skipped := 0, 0, 0
	for {
		ids, err := st.NextQueued(ctx, 20)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, jobID := range ids {
			claimed, err := exec.Execute(ctx, jobID)
			if err != nil {
				log.Printf("Job %s: %v", jobID, err)
				failed++
			} else if claimed {
				done++
			} else {
				skipped++
			}
		}
	}
	log.Printf("Executor %s has finished: done %d, failed %d, skipped %d", workerID, done, failed, skipped)
	return nil
}

func runSingle(ctx context.Context, st *store.Postgres, registry runner.Registry, path string) error {
	expID, err := submit(ctx, st, registry, path)
	if err != nil {
		return err
	}
	wc := config.LoadConfig()

	exec := execution.NewExecutor(st, blob.NewLocalFS("./out"), registry, config.WorkerID(), wc)

	for {
		ids, err := st.NextQueuedForExperiment(ctx, expID, 100)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if _, err := exec.Execute(ctx, id); err != nil {
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
