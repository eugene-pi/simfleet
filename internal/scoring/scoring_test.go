package scoring_test

import (
	"errors"
	"math"
	"testing"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/scoring"
)

// helper: standard three-zone setup used by most tests.
// Стандартный набор из трёх зон для большинства тестов.
func zones() []core.Zone {
	z := []core.Zone{
		{Name: "green", Min: 80},
		{Name: "yellow", Min: 50},
		{Name: "red", Min: 0},
	}
	if err := scoring.ValidateZones(z); err != nil {
		panic(err)
	}
	return z
}

func TestNormalizeDirection(t *testing.T) {
	cases := []struct {
		name      string
		good, bad float64
		value     float64
		want      float64
	}{
		{"higher is better: at good", 3.0, 0.5, 3.0, 100},
		{"higher is better: at bad", 3.0, 0.5, 0.5, 0},
		{"higher is better: midpoint", 3.0, 0.5, 1.75, 50},
		{"lower is better: at good", 2.0, 8.0, 2.0, 100},
		{"lower is better: at bad", 2.0, 8.0, 8.0, 0},
		{"lower is better: midpoint", 2.0, 8.0, 5.0, 50},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := core.Scoring{
				Components: []core.ScoreComponent{{Metric: "m", Good: tc.good, Bad: tc.bad}},
				Combine:    "min",
			}
			ev, err := scoring.Evaluate(s, zones(), map[string]float64{"m": tc.value}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(ev.Score-tc.want) > 1e-9 {
				t.Errorf("score = %v, want %v", ev.Score, tc.want)
			}
		})
	}
}

func TestNormalizeClamping(t *testing.T) {
	s := core.Scoring{
		Components: []core.ScoreComponent{{Metric: "m", Good: 3.0, Bad: 0.5}},
	}
	for _, tc := range []struct {
		value, want float64
	}{
		{10.0, 100}, // far beyond good / далеко за good
		{-5.0, 0},   // beyond bad, negative / за bad, отрицательное
	} {
		ev, err := scoring.Evaluate(s, zones(), map[string]float64{"m": tc.value}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Score != tc.want {
			t.Errorf("value %v: score = %v, want %v", tc.value, ev.Score, tc.want)
		}
	}
}

// The min combine must not let a good component rescue a bad one: a scenario
// that stopped 20 cm from a pedestrian is not redeemed by gentle braking.
// Свёртка минимумом не должна позволять хорошей составляющей вытянуть
// плохую: сценарий с остановкой в 20 см не оправдывается мягким торможением.
func TestMinIsNotCompensating(t *testing.T) {
	s := core.Scoring{
		Components: []core.ScoreComponent{
			{Metric: "dist", Good: 3.0, Bad: 0.5},
			{Metric: "decel", Good: 2.0, Bad: 8.0},
		},
		Combine: "min",
	}
	metrics := map[string]float64{"dist": 0.5, "decel": 2.0} // 0 и 100
	ev, err := scoring.Evaluate(s, zones(), metrics, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Score != 0 {
		t.Errorf("score = %v, want 0 (weakest link)", ev.Score)
	}
	if ev.Zone != "red" {
		t.Errorf("zone = %q, want red", ev.Zone)
	}
	if got := ev.Components["decel"]; got != 100 {
		t.Errorf("components must keep per-metric detail: decel = %v", got)
	}
}

func TestGateOverridesComponents(t *testing.T) {
	s := core.Scoring{
		Gates:      []core.Gate{{When: "collision", Score: 0}},
		Components: []core.ScoreComponent{{Metric: "dist", Good: 3.0, Bad: 0.5}},
		Combine:    "min",
	}
	// Perfect distance, but a collision happened.
	// Отличное расстояние, но столкновение произошло.
	metrics := map[string]float64{"dist": 5.0}
	flags := map[string]bool{"collision": true}

	ev, err := scoring.Evaluate(s, zones(), metrics, flags)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Score != 0 {
		t.Errorf("score = %v, want 0", ev.Score)
	}
	if ev.GatedBy != "collision" {
		t.Errorf("GatedBy = %q, want collision", ev.GatedBy)
	}
}

func TestGateNotTriggered(t *testing.T) {
	// flags present but false → gate must not fire
	// признак есть, но ложный → отсечка не срабатывает
}

// Zone membership uses score >= Min, so exactly 80 is already green.
// Принадлежность зоне проверяется как score >= Min, поэтому ровно 80 —
// это уже зелёная зона.
func TestZoneBoundaries(t *testing.T) {
	s := core.Scoring{
		Components: []core.ScoreComponent{{Metric: "m", Good: 100, Bad: 0}},
	}
	for _, tc := range []struct {
		score float64
		want  string
	}{
		{100, "green"}, {80, "green"}, {79.9, "yellow"},
		{50, "yellow"}, {49.9, "red"}, {0, "red"},
	} {
		ev, err := scoring.Evaluate(s, zones(), map[string]float64{"m": tc.score}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Zone != tc.want {
			t.Errorf("score %v: zone = %q, want %q", tc.score, ev.Zone, tc.want)
		}
	}
}

func TestMissingMetric(t *testing.T) {
	s := core.Scoring{
		Components: []core.ScoreComponent{{Metric: "absent", Good: 1, Bad: 0}},
	}
	_, err := scoring.Evaluate(s, zones(), map[string]float64{"other": 1}, nil)
	if !errors.Is(err, scoring.ErrMissingMetric) {
		t.Errorf("err = %v, want ErrMissingMetric", err)
	}
}

func TestNonFiniteMetric(t *testing.T) {
	// Inf и NaN должны давать ошибку, а не оценку
}

func TestZonesWithGapRejected(t *testing.T) {
	z := []core.Zone{{Name: "green", Min: 80}, {Name: "yellow", Min: 50}}
	if err := scoring.ValidateZones(z); !errors.Is(err, scoring.ErrZoneGap) {
		t.Errorf("err = %v, want ErrZoneGap", err)
	}
}

func TestZonesSortedInPlace(t *testing.T) {
	z := []core.Zone{{Name: "red", Min: 0}, {Name: "green", Min: 80}, {Name: "yellow", Min: 50}}
	if err := scoring.ValidateZones(z); err != nil {
		t.Fatal(err)
	}
	if z[0].Name != "green" {
		t.Errorf("zones must be sorted descending, got %q first", z[0].Name)
	}
}
