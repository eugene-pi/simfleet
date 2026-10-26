package slowcrossing

import (
	"context"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/runner"
	"github.com/eugene-pi/simfleet/internal/runner/crossing"
)

func init() {
	runner.Register(runner.Descriptor{
		Name:       "slowcrossing",
		Partitions: 4,
		Profile:    core.Profile{Timeout: 30 * time.Second, Deterministic: true},
		Params: map[string]core.ParamKind{
			"vehicle_speed_mps":       core.KindNum, // initial vehicle's speed
			"pedestrian_appears_at_m": core.KindNum, // initital distance between the vehicle and the ped
			"reaction_time_s":         core.KindNum, // delay between object recognition and braking
			"max_decel_mps2":          core.KindNum, // how fast can we deccelerate (friction, road condition, tires - combined)
			"sensor_noise_std":        core.KindNum, // this introduces inaccuracy into distance measuring
		},
	}, func(cfg runner.Environment) (runner.Runner, error) {
		return &SlowCrossing{
			inner: crossing.Crossing{},
			delay: time.Second * 5,
		}, nil
	})
}

// slowCrossing wraps the crossing runner with an artificial delay. Its only
// purpose is to exercise the platform under long-running jobs: consumer group
// rebalancing mid-job, head-of-line blocking, lease renewal over many cycles.
type SlowCrossing struct {
	inner crossing.Crossing
	delay time.Duration
}

func (s *SlowCrossing) Run(ctx context.Context, t core.Task) (core.Result, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return core.Result{}, ctx.Err()
	}
	return s.inner.Run(ctx, t)
}
