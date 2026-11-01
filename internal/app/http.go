package app

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// readiness check: whether this process can reach what it needs.
// Ping must be cheap and fast: a scrape every few seconds that runs an aggregate would add real load,
// and a slow check times out and reports a healthy process as unready.

type Ready interface {
	Ping(ctx context.Context) error
}

// StartHTTP runs the observability endpoints alongside the main work.
// /healthz answers "is this process alive" and must never check dependencies:
// a liveness failure restarts the pod, so checking remote here turns a
// brief into a restart storm across every pod at once.
// /readyz answers "can this process do useful work" and does local and remote checks.
func StartHTTP(ctx context.Context, addr string, ready Ready) func() error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		rctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := ready.Ping(rctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1) // buffered: nobody may be reading on shutdown

	go func() {
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	// Returns a function the caller runs in its errgroup: it waits for either
	// the server to fail on its own (port taken) or for shutdown.
	return func() error {
		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
			// A fresh context: the original is already cancelled, and passing
			// it would make Shutdown return instantly without draining.
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return srv.Shutdown(sctx)
		}
	}
}
