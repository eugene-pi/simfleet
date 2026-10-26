package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/eugene-pi/simfleet/internal/blob"
	"github.com/eugene-pi/simfleet/internal/bus"
	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/envfile"
	"github.com/eugene-pi/simfleet/internal/execution"
	"github.com/eugene-pi/simfleet/internal/runner"
	_ "github.com/eugene-pi/simfleet/internal/runner/crossing"
	_ "github.com/eugene-pi/simfleet/internal/runner/slowcrossing"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/eugene-pi/simfleet/internal/worker"
)

func main() {
	core.RunService("worker", func(ctx context.Context) error {
		envfile.LoadEnv()
		core.SetupLogger()
		cfg := config.LoadWorkerConfig()
		if cfg.DatabaseURL == "" {
			slog.Error("DATABASE_URL is not set")
			os.Exit(1)
		}

		st, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer st.Close()
		desc, ok := runner.Get(cfg.Runner)
		if !ok {
			return fmt.Errorf("runner %s unavailable. Enabled runners are %v", cfg.Runner, runner.Names())
		}
		r, err := runner.Build(cfg.Runner, runner.Environment{WorkDir: os.TempDir()})
		if err != nil {
			return err
		}
		exec, err := execution.NewExecutor(st, blob.NewLocalFS("./out"), desc, r, cfg)
		if err != nil {
			return err
		}
		cons, err := bus.NewKafkaConsumer(config.LoadKafkaConfig())
		if err != nil {
			return err
		}
		defer cons.Close()

		return worker.New(cons, exec, cfg).Run(ctx)
	})
}
