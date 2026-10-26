package crossing

import (
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/runner"
)

func init() {
	runner.Register(runner.Descriptor{
		Name:    "crossing",
		Profile: core.Profile{Timeout: 30 * time.Second, Deterministic: true},
		Params: map[string]core.ParamKind{
			"vehicle_speed_mps":       core.KindNum, // initial vehicle's speed
			"pedestrian_appears_at_m": core.KindNum, // initital distance between the vehicle and the ped
			"reaction_time_s":         core.KindNum, // delay between object recognition and braking
			"max_decel_mps2":          core.KindNum, // how fast can we deccelerate (friction, road condition, tires - combined)
			"sensor_noise_std":        core.KindNum, // this introduces inaccuracy into distance measuring
		},
	}, func(cfg runner.Environment) (runner.Runner, error) {
		return &Crossing{}, nil
	})
}

type Crossing struct {
	workDir string
}
