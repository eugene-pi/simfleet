package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
)

// cmd/runlocal/main.go, подкоманда spawn
func spawn(ctx context.Context, n int) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for i := range n {
		log.Printf("starting worker %d\r\n", i+1)
		cmd := exec.CommandContext(ctx, self, "work")
		cmd.Env = append(os.Environ(), fmt.Sprintf("WORKER_ID=w%d", i+1))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start worker %d: %w", i+1, err)
		}
		log.Printf("исполнитель w%d запущен, pid %d", i+1, cmd.Process.Pid)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cmd.Wait(); err != nil {
				log.Printf("исполнитель завершился: %v", err)
			}
		}()
	}
	wg.Wait()
	return nil
}
