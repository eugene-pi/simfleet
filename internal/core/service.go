package core

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/eugene-pi/simfleet/internal/envfile"
)

func RunService(name string, f func(ctx context.Context) error) {
	envfile.LoadEnv()
	SetupLogger()
	slog.SetDefault(slog.With("service", name))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("starting")
	if err := f(ctx); err != nil {
		slog.Error("exited with error", "error", err)
		os.Exit(1)
	}
	slog.Info("stopped")
}
