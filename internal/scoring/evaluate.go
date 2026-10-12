// internal/scoring/scoring.go
package scoring

import (
	"fmt"
	"math"

	"github.com/eugene-pi/simfleet/internal/core"
)

// Evaluate computes a score from result metrics.
func Evaluate(s core.Scoring, zones []core.Zone,
	metrics map[string]float64, flags map[string]bool) (core.Evaluation, error) {

	for _, g := range s.Gates {
		if flags[g.When] {
			z, err := zoneFor(zones, g.Score)
			if err != nil {
				return core.Evaluation{}, err
			}
			return core.Evaluation{
				Score:      g.Score,
				Zone:       z,
				Components: map[string]float64{},
				GatedBy:    g.When,
			}, nil
		}
	}

	comps := make(map[string]float64, len(s.Components))
	for _, c := range s.Components {
		v, ok := metrics[c.Metric]
		if !ok {
			return core.Evaluation{}, fmt.Errorf("%w: %q", ErrMissingMetric, c.Metric)
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return core.Evaluation{}, fmt.Errorf("metric %q is not finite", c.Metric)
		}
		comps[c.Metric] = normalize(v, c.Good, c.Bad)
	}

	score, err := combine(s.Combine, s.Components, comps)
	if err != nil {
		return core.Evaluation{}, err
	}
	score = math.Max(0, math.Min(100, score))

	z, err := zoneFor(zones, score)
	if err != nil {
		return core.Evaluation{}, err
	}
	return core.Evaluation{Score: score, Zone: z, Components: comps}, nil
}
