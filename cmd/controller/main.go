// cmd/controller/main.go
package main

import (
	"context"

	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/controller"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

func main() {
	core.RunService("controller", func(ctx context.Context) error {
		cfg, err := config.LoadControllerConfig()
		if err != nil {
			return err
		}
		store, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer store.Close()

		return controller.New(store, cfg.Interval, cfg.ReclaimBatch).Run(ctx)
	})
}
