// cmd/controller/main.go
package main

import (
	"context"

	"github.com/eugene-pi/simfleet/internal/app"
	"github.com/eugene-pi/simfleet/internal/bus"
	"github.com/eugene-pi/simfleet/internal/config"
	"github.com/eugene-pi/simfleet/internal/controller"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/metrics"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
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
		bus, err := bus.NewKafka(config.LoadKafkaConfig())
		if err != nil {
			return err
		}
		defer bus.Close()

		prometheus.MustRegister(metrics.NewDBCollector(store))

		cont := controller.New(store, bus, cfg.Interval, cfg.ReclaimBatch)

		g, ctx := errgroup.WithContext(ctx)
		g.Go(app.StartHTTP(ctx, cfg.MetricsAddr, store))
		g.Go(func() error { return cont.Run(ctx) })
		return g.Wait()
	})
}
