package config

import (
	"fmt"
	"os"
	"time"
)

type WorkerConfig struct {
	DatabaseURL   string
	Runner        string
	WorkerId      string
	LeaseDuration time.Duration
	RenewEvery    time.Duration
	BatchSize     int
	ExitWhenIdle  bool
}

func loadId() string {
	if id := os.Getenv(ENV_WORKER_ID); id != "" {
		return id
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func LoadWorkerConfig() WorkerConfig {
	lds := loadInt(ENV_SIMFLEET_LEASE_DURATION, lease_duration_sec_default)
	batchSize := loadInt(ENV_SIMFLEET_BATCH_SIZE, batch_size_default)
	worker_stays := loadInt(ENV_SIMFLEET_WORKER_STAYS, -1)
	lease := time.Second * time.Duration(lds)
	return WorkerConfig{
		DatabaseURL:   loadString(ENV_DATABASE_URL, ""),
		Runner:        loadString(ENV_SIMFLEET_RUNNER, ""),
		WorkerId:      loadId(),
		LeaseDuration: lease,
		// Divide the Duration, not the seconds: 1s/3 must not round down to 0,
		// which would make time.NewTicker panic.
		RenewEvery:   lease / lease_renew_divisor,
		BatchSize:    batchSize,
		ExitWhenIdle: worker_stays <= 0,
	}
}
