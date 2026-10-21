package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/eugene-pi/simfleet/internal/store"
)

// run workers as runLocal executable with param, reads jobs from db
func spawnLocal(ctx context.Context, n int) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for i := range n {
		slog.Info("starting worker", "worker", i+1)
		cmd := exec.CommandContext(ctx, self, "work")
		cmd.Env = append(os.Environ(), fmt.Sprintf("WORKER_ID=w%d", i+1))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start worker %d: %w", i+1, err)
		}
		slog.Info("worker started", "worker", i+1, "pid", cmd.Process.Pid)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cmd.Wait(); err != nil {
				slog.Warn("worker exited", "error", err)
			}
		}()
	}
	wg.Wait()
	return nil
}

// spawn starts n copies of a binary, each with its own WORKER_ID, and waits
// for all of them. The binaries are the real ones that ship in the images,
// so what runlocal exercises is what production runs.
func spawn(ctx context.Context, bin string, n int) ([]*exec.Cmd, error) {
	path, err := exec.LookPath(bin)
	if err != nil {
		return nil, fmt.Errorf("%s not found, run make build: %w", bin, err)
	}

	cmds := make([]*exec.Cmd, 0, n)
	for i := range n {
		cmd := exec.CommandContext(ctx, path)
		cmd.Env = append(os.Environ(), fmt.Sprintf("WORKER_ID=w%d", i+1))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// SIGTERM instead of the default Kill, so the process can finish its
		// current job — the same shutdown path Kubernetes uses.
		cmd.Cancel = func() error {
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		cmd.WaitDelay = 10 * time.Second

		if err := cmd.Start(); err != nil {
			return cmds, fmt.Errorf("start %s %d: %w", bin, i+1, err)
		}
		slog.Info("started", "bin", bin, "n", i+1, "pid", cmd.Process.Pid)
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

func waitForCompletion(ctx context.Context, st *store.Postgres) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			active, err := st.HasActiveExperiments(ctx)
			if err != nil {
				return err
			}
			if !active {
				slog.Info("no active experiments, shutting down")
				return nil
			}
		}
	}
}
