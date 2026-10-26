package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/eugene-pi/simfleet/internal/blob"
	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/envfile"
	"github.com/eugene-pi/simfleet/internal/execution"
	"github.com/eugene-pi/simfleet/internal/expander"
	"github.com/eugene-pi/simfleet/internal/runner"
	_ "github.com/eugene-pi/simfleet/internal/runner/crossing"
	_ "github.com/eugene-pi/simfleet/internal/runner/slowcrossing"
	"github.com/eugene-pi/simfleet/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		slog.Error("usage: runlocal <cmd> <spec.yaml>")
		os.Exit(1)
	}
	envfile.LoadEnv()
	core.SetupLogger()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		slog.Error("DATABASE_URL is not set")
		os.Exit(1)
	}
	if err := run(dsn); err != nil {
		slog.Any("error", err)
	}
}

func run(dsn string) error {
	ctx := context.Background()
	st, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	switch os.Args[1] {
	case "submit":
		if len(os.Args) > 2 {
			_, err := submit(ctx, st, os.Args[2])
			return err
		} else {
			return errors.New("usage: runlocal submit <spec.yaml>")
		}
	case "work":
		return work(ctx, st)
	case "spawnOld":
		var runnerName string
		if len(os.Args) > 2 {
			if spec, err := loadSpec(os.Args[2]); err == nil {
				runnerName = spec.Runner
			}
		}
		return spawnLocal(ctx, runnerName, 5)
	case "spawn":
		var runnerName string
		if len(os.Args) > 2 {
			if spec, err := loadSpec(os.Args[2]); err == nil {
				runnerName = spec.Runner
			}
		}
		ctxw, cancel := context.WithCancel(ctx)
		defer cancel()
		cmds, err := spawn(ctxw, "./bin/worker", runnerName, 5)
		var wg sync.WaitGroup
		for _, cmd := range cmds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := cmd.Wait(); err != nil {
					slog.Warn("process exited", "error", err)
				}
			}()
		}
		if err == nil {
			err = waitForCompletion(ctxw, st)
		}
		cancel()
		wg.Wait()
		return err
	default:
		if len(os.Args) > 2 {
			return fmt.Errorf("unknown command %v", os.Args[1])
		}
	}
	return runSingle(ctx, st, os.Args[1])
}

func submit(ctx context.Context, st *store.Postgres, path string) (core.ExperimentID, error) {
	spec, err := loadSpec(path)
	if err != nil {
		return core.ExperimentID{}, err
	}
	desc, ok := runner.Get(spec.Runner)
	if !ok {
		return core.ExperimentID{}, fmt.Errorf("runner %s unavailable. Enabled runners are %v", spec.Runner, runner.Names())
	}
	expID, n, err := expander.Create(ctx, st, desc, spec)
	if err != nil {
		return core.ExperimentID{}, err
	}
	fmt.Println(expID)
	slog.Info("jobs created", "job_count", n)
	return expID, nil
}

func work(ctx context.Context, st *store.Postgres) error {
	wc := config.LoadWorkerConfig()
	slog.SetDefault(slog.With("worker_id", wc.WorkerId))
	desc, ok := runner.Get(wc.Runner)
	if !ok {
		return fmt.Errorf("runner %s unavailable. Enabled runners are %v", wc.Runner, runner.Names())
	}
	r, err := runner.Build(wc.Runner, runner.Environment{WorkDir: os.TempDir()})
	if err != nil {
		return err
	}
	exec := execution.NewExecutor(st, blob.NewLocalFS("./out"), desc, r, wc)

	slog.Info("executor started")
	failed, done, skipped := 0, 0, 0
	for {
		ids, err := st.NextQueued(ctx, 20)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			if wc.ExitWhenIdle {
				active, err := st.HasActiveExperiments(ctx)
				if err != nil {
					return err
				}
				if !active {
					slog.Info("No active experiments, exiting")
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for _, jobID := range ids {
			claimed, err := exec.Execute(ctx, jobID)
			if err != nil {
				slog.Error("unable to execute", "job", jobID, "error", err)
				failed++
			} else if claimed {
				done++
			} else {
				skipped++
			}
		}
	}
	slog.Info("Executor has finished: ", "done", done, "failed", failed, "skipped", skipped)
	return nil
}

func runSingle(ctx context.Context, st *store.Postgres, path string) error {
	expID, err := submit(ctx, st, path)
	if err != nil {
		return err
	}
	wc := config.LoadWorkerConfig()
	if spec, err := loadSpec(path); err == nil {
		wc.Runner = spec.Runner
	}
	desc, ok := runner.Get(wc.Runner)
	if !ok {
		return fmt.Errorf("runner %s unavailable. Enabled runners are %v", wc.Runner, runner.Names())
	}
	r, err := runner.Build(wc.Runner, runner.Environment{WorkDir: os.TempDir()})
	if err != nil {
		return err
	}
	exec := execution.NewExecutor(st, blob.NewLocalFS("./out"), desc, r, wc)

	for {
		ids, err := st.NextQueuedForExperiment(ctx, expID, 100)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			if wc.ExitWhenIdle {
				active, err := st.HasActiveExperiments(ctx)
				if err != nil {
					return err
				}
				if !active {
					slog.Info("No active experiments, exiting")
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for _, id := range ids {
			if _, err := exec.Execute(ctx, id); err != nil {
				log.Printf("задание %s: %v", id, err)
			}
		}
	}

	// 3. Summary
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
