// Package report turns what the store knows about an experiment into a
// report: outcome shares, per-parameter breakdown, extreme cases, and a plain
// statement of what the numbers do not cover. Rendering (text, HTML, live
// view) lives next to it; the store only counts.
package report

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/store"
)

// SegmentKind tells a zone apart from the two ways a job can lack a score.
type SegmentKind int

const (
	KindZone       SegmentKind = iota
	KindFailed                 // job failed, no result
	KindUnfinished             // queued or running
	KindPending                // finished but not scored (yet) with the current spec
)

// Segment is one slice of an outcome bar. Shares are of all jobs in the bar,
// not of scored ones: a bar of 3,400 scored out of 3,510 must look shorter
// on green, not identical to a complete run.
type Segment struct {
	Label string
	Kind  SegmentKind
	Rank  int // zone position, best first; colours and glyphs key off it
	Count int
	Share float64 // 0..1 of Bar total
}

// Bar is an outcome distribution over a set of jobs.
type Bar struct {
	Total    int
	Segments []Segment
}

func (b Bar) Count(label string) int {
	for _, s := range b.Segments {
		if s.Label == label {
			return s.Count
		}
	}
	return 0
}

// Axis is the outcome breakdown by the values of one swept parameter.
type Axis struct {
	Param string
	// Spread is max − min of the mean score over this parameter's values.
	// A crude but honest "how much does this parameter decide" measure:
	// marginal, so it does not see interactions between parameters.
	Spread float64
	Values []AxisValue
}

type AxisValue struct {
	Value     core.ParamValue
	Outcomes  Bar
	Scored    int
	MeanScore float64 // NaN when nothing scored yet
}

// FixedParam is a swept parameter that took a single value: nothing to
// compare, so it is reported once instead of as a one-row table.
type FixedParam struct {
	Param string
	Value core.ParamValue
}

type Report struct {
	ID        core.ExperimentID
	Name      string
	Runner    string
	State     string
	CreatedAt time.Time
	Finished  bool
	// GeneratedAt is when the report was built.
	GeneratedAt time.Time
	// WallTime runs from submit to completion; for an unfinished experiment,
	// to the moment the report was built.
	WallTime time.Duration

	Jobs    store.Summary
	Results store.ResultStats
	Overall Bar

	Axes  []Axis
	Fixed []FixedParam

	N           int
	Worst, Best []store.CaseRow
	// Columns of the case tables: swept parameters that vary, and every
	// metric seen in the listed cases.
	CaseParams  []string
	CaseMetrics []string

	// Warnings say why the numbers above are not the whole experiment.
	// Empty means complete: every job finished and was scored.
	Warnings []string
}

func (r Report) Complete() bool { return len(r.Warnings) == 0 }

// CoarseDurations is true when runs are mostly shorter than the 1 ms
// resolution they are recorded at (runners report whole, truncated
// milliseconds). The sum and mean are then lower bounds, not measurements.
func (r Report) CoarseDurations() bool {
	return r.Results.Count > 0 && r.Results.MeanDuration < time.Millisecond
}

// Columns are the outcome buckets worth a column in per-value tables: every
// zone, plus failed / unfinished / not scored only when the experiment has
// any — a complete run should not carry three columns of dots.
func (r Report) Columns() []Segment {
	var out []Segment
	for _, s := range r.Overall.Segments {
		if s.Kind == KindZone || s.Count > 0 {
			out = append(out, s)
		}
	}
	return out
}

// Build assembles a report. now is used only for unfinished experiments.
func Build(d store.ReportData, n int, now time.Time) Report {
	r := fromProgress(d.ProgressData, now)
	r.N = n
	r.Worst, r.Best = d.Worst, d.Best

	zones := zoneOrder(d.Experiment.Spec.Zones, d.Zones)
	r.Axes, r.Fixed = buildAxes(d.Axes, zones)

	// Fixed parameters are the same in every row, so they are left out of
	// the case tables; they are listed once under the axes instead.
	fixed := map[string]bool{}
	for _, f := range r.Fixed {
		fixed[f.Param] = true
	}
	params, metrics := map[string]bool{}, map[string]bool{}
	for _, c := range slices.Concat(d.Worst, d.Best) {
		for p := range c.Params {
			if !fixed[p] {
				params[p] = true
			}
		}
		for m := range c.Metrics {
			metrics[m] = true
		}
	}
	r.CaseParams = slices.Sorted(maps.Keys(params))
	r.CaseMetrics = slices.Sorted(maps.Keys(metrics))
	return r
}

// BuildProgress assembles the reduced report a live view shows: overall
// outcomes and completeness, no axes or cases.
func BuildProgress(d store.ProgressData, now time.Time) Report {
	return fromProgress(d, now)
}

func fromProgress(d store.ProgressData, now time.Time) Report {
	e := d.Experiment
	r := Report{
		ID:        e.ID,
		Name:      e.Name,
		Runner:    e.Runner,
		State:     e.State,
		CreatedAt: e.CreatedAt,
		Finished:  finished(e.State),
		Jobs:      d.Jobs,
		Results:   d.Results,

		GeneratedAt: now,
	}
	end := now
	if e.CompletedAt != nil {
		end = *e.CompletedAt
	}
	r.WallTime = max(end.Sub(e.CreatedAt), 0)

	zones := zoneOrder(e.Spec.Zones, d.Zones)
	r.Overall = makeBar(zones, d.Zones, d.Jobs.Failed, d.Jobs.Queued+d.Jobs.Running, d.Jobs.Total)
	r.Warnings = warnings(d)
	return r
}

func finished(state string) bool {
	return state == "completed" || state == "cancelled"
}

// warnings lists every reason the statistics cover less than the whole
// experiment. Order: the most fundamental first.
func warnings(d store.ProgressData) []string {
	var w []string
	j, res := d.Jobs, d.Results
	scored := res.Scored

	if !finished(d.Experiment.State) {
		w = append(w, fmt.Sprintf("experiment is still %s: %d queued, %d running",
			d.Experiment.State, j.Queued, j.Running))
	}
	if j.Failed > 0 {
		w = append(w, fmt.Sprintf("%d %s failed and have no result",
			j.Failed, plural(j.Failed, "job", "jobs")))
	}
	if j.Cancelled > 0 {
		w = append(w, fmt.Sprintf("%d %s cancelled", j.Cancelled, plural(j.Cancelled, "job was", "jobs were")))
	}
	if res.Unscored > 0 {
		w = append(w, fmt.Sprintf("%d %s not scored yet with the current scoring spec",
			res.Unscored, plural(res.Unscored, "result is", "results are")))
	}
	if res.ScoringErrors > 0 {
		w = append(w, fmt.Sprintf("%d %s could not be scored (scoring_error)",
			res.ScoringErrors, plural(res.ScoringErrors, "result", "results")))
	}
	if len(w) > 0 || scored != j.Total {
		w = append([]string{fmt.Sprintf("scores cover %d of %d jobs (%s)",
			scored, j.Total, pct(ratio(scored, j.Total)))}, w...)
	}
	return w
}

// zoneOrder returns zone names best first: the spec's zones by descending
// Min, then any zone present in the data but unknown to the spec (a spec
// edited after scoring), so no counted result disappears from the report.
func zoneOrder(spec []core.Zone, counts map[string]int) []string {
	zs := slices.Clone(spec)
	slices.SortStableFunc(zs, func(a, b core.Zone) int { return cmp.Compare(b.Min, a.Min) })
	var out []string
	seen := map[string]bool{}
	for _, z := range zs {
		if !seen[z.Name] {
			out = append(out, z.Name)
			seen[z.Name] = true
		}
	}
	for _, name := range slices.Sorted(maps.Keys(counts)) {
		if !seen[name] {
			out = append(out, name)
			seen[name] = true
		}
	}
	return out
}

func makeBar(zones []string, counts map[string]int, failed, unfinished, total int) Bar {
	b := Bar{Total: total}
	known := 0
	for i, z := range zones {
		b.Segments = append(b.Segments, Segment{Label: z, Kind: KindZone, Rank: i, Count: counts[z]})
		known += counts[z]
	}
	b.Segments = append(b.Segments,
		Segment{Label: "failed", Kind: KindFailed, Count: failed},
		Segment{Label: "unfinished", Kind: KindUnfinished, Count: unfinished},
		Segment{Label: "not scored", Kind: KindPending, Count: max(total-known-failed-unfinished, 0)},
	)
	for i := range b.Segments {
		b.Segments[i].Share = ratio(b.Segments[i].Count, total)
	}
	return b
}

func buildAxes(cells []store.AxisCell, zones []string) ([]Axis, []FixedParam) {
	type acc struct {
		value      core.ParamValue
		total      int
		failed     int
		unfinished int
		scored     int
		scoreSum   float64
		zones      map[string]int
	}
	byParam := map[string]map[string]*acc{} // param → value key → acc
	for _, c := range cells {
		vals := byParam[c.Param]
		if vals == nil {
			vals = map[string]*acc{}
			byParam[c.Param] = vals
		}
		key := valueKey(c.Value)
		a := vals[key]
		if a == nil {
			a = &acc{value: c.Value, zones: map[string]int{}}
			vals[key] = a
		}
		a.total += c.Count
		switch {
		case c.Zone != "":
			a.zones[c.Zone] += c.Count
			a.scored += c.Count
			a.scoreSum += c.ScoreSum
		case c.State == "failed":
			a.failed += c.Count
		case c.State == "queued" || c.State == "running":
			a.unfinished += c.Count
		}
	}

	var axes []Axis
	var fixed []FixedParam
	for _, param := range slices.Sorted(maps.Keys(byParam)) {
		vals := slices.Collect(maps.Values(byParam[param]))
		if len(vals) == 1 {
			fixed = append(fixed, FixedParam{Param: param, Value: vals[0].value})
			continue
		}
		slices.SortFunc(vals, func(a, b *acc) int { return compareValues(a.value, b.value) })

		ax := Axis{Param: param}
		lo, hi := math.Inf(1), math.Inf(-1)
		for _, a := range vals {
			v := AxisValue{
				Value:     a.value,
				Outcomes:  makeBar(zones, a.zones, a.failed, a.unfinished, a.total),
				Scored:    a.scored,
				MeanScore: math.NaN(),
			}
			if a.scored > 0 {
				v.MeanScore = a.scoreSum / float64(a.scored)
				lo, hi = min(lo, v.MeanScore), max(hi, v.MeanScore)
			}
			ax.Values = append(ax.Values, v)
		}
		if hi >= lo {
			ax.Spread = hi - lo
		}
		axes = append(axes, ax)
	}
	// Most decisive parameter first; name breaks ties for a stable report.
	slices.SortStableFunc(axes, func(a, b Axis) int { return cmp.Compare(b.Spread, a.Spread) })
	return axes, fixed
}

func valueKey(v core.ParamValue) string {
	return fmt.Sprintf("%d:%s", v.Kind(), v.String())
}

// compareValues orders numbers numerically (so 8 < 10), then strings, then
// booleans.
func compareValues(a, b core.ParamValue) int {
	if c := cmp.Compare(a.Kind(), b.Kind()); c != 0 {
		return c
	}
	if x, ok := a.AsFloat(); ok {
		y, _ := b.AsFloat()
		return cmp.Compare(x, y)
	}
	if x, ok := a.AsBool(); ok {
		y, _ := b.AsBool()
		return cmp.Compare(boolInt(x), boolInt(y))
	}
	return cmp.Compare(a.String(), b.String())
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func ratio(n, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(n) / float64(total)
}

func pct(f float64) string {
	return fmt.Sprintf("%.1f%%", f*100)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// CaseNote explains a case score beyond the number: which gate fired, or
// which flags were raised.
func CaseNote(c store.CaseRow) string {
	if c.GatedBy != "" {
		return c.GatedBy
	}
	var raised []string
	for _, f := range slices.Sorted(maps.Keys(c.Flags)) {
		if c.Flags[f] {
			raised = append(raised, f)
		}
	}
	if len(raised) == 0 {
		return ""
	}
	return strings.Join(raised, ", ")
}
