package main

import (
	"context"
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

		reg, err := runner.Build(runner.Config{WorkDir: os.TempDir()})
		if err != nil {
			return err
		}
		kcfg := config.LoadKafkaConfig()
		cons, err := bus.NewKafkaConsumer(kcfg.Brokers, kcfg.Group, reg.Names())
		if err != nil {
			return err
		}
		defer cons.Close()

		exec := execution.NewExecutor(st, blob.NewLocalFS("./out"), reg, config.WorkerID(), cfg)
		return worker.New(cons, exec).Run(ctx)
	})
}
