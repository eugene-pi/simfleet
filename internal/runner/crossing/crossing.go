package crossing

import (
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/runner"
)

func init() {
	runner.Register("crossing", func(cfg runner.Config) (runner.Runner, error) {
		return &Crossing{workDir: cfg.WorkDir}, nil
	})
}

type Crossing struct{ workDir string }

func (c *Crossing) Name() string { return "crossing" }

func (c *Crossing) Profile() core.Profile {
	return core.Profile{
		Timeout:       30 * time.Second,
		Deterministic: true,
		MemoryMB:      64,
	}
}

func (c *Crossing) ParamSchema() map[string]core.ParamKind {
	return map[string]core.ParamKind{
		"vehicle_speed_mps":  core.KindNum,
		"pedestrian_delay_s": core.KindNum,
		"reaction_time_s":    core.KindNum,
		"max_decel_mps2":     core.KindNum,
		"sensor_noise_std":   core.KindNum,
	}
}
