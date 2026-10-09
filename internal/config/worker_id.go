package config

import (
	"fmt"
	"os"
)

func WorkerID() string {
	if id := os.Getenv(ENV_WORKER_ID); id != "" {
		return id
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
