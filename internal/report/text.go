package report

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/eugene-pi/simfleet/internal/store"
)

// TextStyle controls terminal output.
type TextStyle struct {
	Color bool // ANSI colours; without them each segment gets its own glyph
	Width int  // bar width in cells
}

func (s TextStyle) width() int {
	if s.Width <= 0 {
		return 40
	}
	return s.Width
}

// WriteText renders the full report for a terminal or a plain-text file.
func WriteText(w io.Writer, r Report, st TextStyle) error {
	p := &printer{w: w}

	p.f("Experiment  %s  (%s)\n", r.Name, r.ID)
	p.f("Runner      %s    State  %s\n", r.Runner, r.State)
	p.f("Created     %s    Wall time  %s\n",
		r.CreatedAt.Local().Format("2006-01-02 15:04:05"), FormatDuration(r.WallTime))
	p.f("\n")

	writeWarnings(p, r)

	p.f("Jobs        %d   %s\n", r.Jobs.Total, jobStates(r))
	res := r.Results
	if res.Count > 0 {
		p.f("Run time    total %s (sum of %d runs)   mean %s/job   min %s   max %s\n",
			RunDuration(res.TotalDuration), res.Count, RunDuration(res.MeanDuration),
			RunDuration(res.MinDuration), RunDuration(res.MaxDuration))
		if r.CoarseDurations() {
			p.f("            runs are recorded in whole ms and most are shorter: the total is a lower bound\n")
		}
	} else {
		p.f("Run time    no results yet\n")
	}
	p.f("\n")

	p.f("Outcomes of all %d jobs\n", r.Overall.Total)
	p.f("  %s\n", renderBar(r.Overall, st))
	writeLegend(p, r.Overall, st)
	p.f("\n")

	writeAxes(p, r, st)
	writeCases(p, r, "Worst", r.Worst, r.Results.MinScore, r.Results.MinScoreTies)
	writeCases(p, r, "Best", r.Best, r.Results.MaxScore, r.Results.MaxScoreTies)
	return p.err
}

// writeLegend lists a bar's segments with counts and shares. Formatted by
// hand, not with tabwriter: the ANSI codes in the swatch would count towards
// column widths.
func writeLegend(p *printer, b Bar, st TextStyle) {
	labelW, countW := 0, len(strconv.Itoa(b.Total))
	for _, s := range b.Segments {
		labelW = max(labelW, len(s.Label))
	}
	for _, s := range b.Segments {
		if s.Count == 0 && s.Kind != KindZone {
			continue
		}
		p.f("  %s %-*s  %*d  %6s\n", swatch(s, st), labelW, s.Label, countW, s.Count, pct(s.Share))
	}
}

func writeWarnings(p *printer, r Report) {
	if r.Complete() {
		p.f("Complete: every job finished and was scored.\n\n")
		return
	}
	p.f("INCOMPLETE — the statistics below do not cover the whole experiment:\n")
	for _, w := range r.Warnings {
		p.f("  ! %s\n", w)
	}
	p.f("\n")
}

func jobStates(r Report) string {
	j := r.Jobs
	parts := []string{}
	for _, s := range []struct {
		name string
		n    int
	}{
		{"completed", j.Completed}, {"failed", j.Failed}, {"running", j.Running},
		{"queued", j.Queued}, {"cancelled", j.Cancelled},
	} {
		if s.n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", s.name, s.n))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func writeAxes(p *printer, r Report, st TextStyle) {
	if len(r.Axes) == 0 && len(r.Fixed) == 0 {
		return
	}
	p.f("By parameter — most decisive first (spread of mean score across values)\n")
	var cols []string
	for _, s := range r.Columns() {
		cols = append(cols, s.Label)
	}
	for _, a := range r.Axes {
		p.f("\n  %s   spread %.1f\n", a.Param, a.Spread)
		tw := tabwriter.NewWriter(p, 0, 0, 2, ' ', 0)
		fmt.Fprintf(tw, "    value\tjobs\tmean\t%s\t\n", strings.Join(cols, "\t"))
		for _, v := range a.Values {
			mean := "—"
			if !math.IsNaN(v.MeanScore) {
				mean = strconv.FormatFloat(v.MeanScore, 'f', 1, 64)
			}
			cells := []string{}
			for _, c := range cols {
				cells = append(cells, ShareCell(v.Outcomes.Count(c), v.Outcomes.Total))
			}
			fmt.Fprintf(tw, "    %s\t%d\t%s\t%s\t%s\n",
				v.Value, v.Outcomes.Total, mean, strings.Join(cells, "\t"),
				renderBar(v.Outcomes, TextStyle{Color: st.Color, Width: 20}))
		}
		tw.Flush()
	}
	if len(r.Fixed) > 0 {
		fixed := make([]string, len(r.Fixed))
		for i, f := range r.Fixed {
			fixed[i] = fmt.Sprintf("%s = %s", f.Param, f.Value)
		}
		p.f("\n  Fixed: %s\n", strings.Join(fixed, ", "))
	}
	p.f("\n")
}

func writeCases(p *printer, r Report, title string, cases []store.CaseRow, extreme float64, ties int) {
	if r.N <= 0 {
		return
	}
	if len(cases) == 0 {
		p.f("%s cases: none scored yet\n\n", title)
		return
	}
	p.f("%s %d", title, len(cases))
	if ties > len(cases) {
		p.f(" — %d jobs share score %.1f; these are the first by index", ties, extreme)
	}
	p.f("\n")

	tw := tabwriter.NewWriter(p, 0, 0, 2, ' ', 0)
	head := slices.Concat([]string{"  idx", "score", "zone"}, r.CaseParams, r.CaseMetrics, []string{"note"})
	fmt.Fprintln(tw, strings.Join(head, "\t"))
	for _, c := range cases {
		row := []string{"  " + strconv.Itoa(c.Idx), strconv.FormatFloat(c.Score, 'f', 1, 64), c.Zone}
		for _, name := range r.CaseParams {
			if v, ok := c.Params[name]; ok {
				row = append(row, v.String())
			} else {
				row = append(row, "")
			}
		}
		for _, name := range r.CaseMetrics {
			if v, ok := c.Metrics[name]; ok {
				row = append(row, strconv.FormatFloat(v, 'g', 4, 64))
			} else {
				row = append(row, "")
			}
		}
		row = append(row, CaseNote(c))
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	tw.Flush()
	p.f("\n")
}

// ShareCell prints a share as a whole percentage. A non-zero count never
// rounds to "0%" (one red job in 702 is not none), nor a partial one to "100%".
func ShareCell(n, total int) string {
	f := ratio(n, total) * 100
	switch {
	case n == 0:
		return "·"
	case f < 0.5:
		return "<1%"
	case f >= 99.5 && n < total:
		return ">99%"
	default:
		return fmt.Sprintf("%.0f%%", f)
	}
}

// --- bars ---

const (
	ansiReset = "\x1b[0m"
	ansiDim   = "\x1b[2m"
)

// renderBar draws a stacked bar. Every non-empty segment gets at least one
// cell: three failed jobs out of 3,510 must still be visible.
func renderBar(b Bar, st TextStyle) string {
	cells := allocate(b, st.width())
	var sb strings.Builder
	sb.WriteByte('[')
	for i, s := range b.Segments {
		if cells[i] == 0 {
			continue
		}
		g := strings.Repeat(glyph(s, st.Color), cells[i])
		if st.Color {
			sb.WriteString(ansiColor(s) + g + ansiReset)
		} else {
			sb.WriteString(g)
		}
	}
	if b.Total == 0 {
		sb.WriteString(strings.Repeat(" ", st.width()))
	}
	sb.WriteByte(']')
	return sb.String()
}

// allocate splits width cells between segments by largest remainder, with
// a minimum of one cell for any non-empty segment.
func allocate(b Bar, width int) []int {
	cells := make([]int, len(b.Segments))
	if b.Total == 0 {
		return cells
	}
	type rem struct {
		i int
		r float64
	}
	var rems []rem
	used := 0
	for i, s := range b.Segments {
		if s.Count == 0 {
			continue
		}
		exact := s.Share * float64(width)
		cells[i] = max(int(exact), 1)
		used += cells[i]
		rems = append(rems, rem{i, exact - math.Floor(exact)})
	}
	slices.SortStableFunc(rems, func(a, b rem) int {
		switch {
		case a.r > b.r:
			return -1
		case a.r < b.r:
			return 1
		}
		return 0
	})
	for k := 0; used < width && len(rems) > 0; k = (k + 1) % len(rems) {
		cells[rems[k].i]++
		used++
	}
	// The one-cell minimum may overshoot: take back from the widest.
	for used > width {
		widest := 0
		for i := range cells {
			if cells[i] > cells[widest] {
				widest = i
			}
		}
		if cells[widest] <= 1 {
			break
		}
		cells[widest]--
		used--
	}
	return cells
}

func glyph(s Segment, color bool) string {
	switch s.Kind {
	case KindFailed:
		if color {
			return "█"
		}
		return "x"
	case KindUnfinished:
		if color {
			return "·"
		}
		return " "
	case KindPending:
		return "░"
	}
	if color {
		return "█"
	}
	return [...]string{"█", "▓", "▒", "░", "#", "="}[min(s.Rank, 5)]
}

func swatch(s Segment, st TextStyle) string {
	g := glyph(s, st.Color)
	if st.Color {
		return ansiColor(s) + g + ansiReset
	}
	return g
}

func ansiColor(s Segment) string {
	switch s.Kind {
	case KindFailed:
		return "\x1b[35m" // magenta
	case KindPending, KindUnfinished:
		return ansiDim
	}
	switch strings.ToLower(s.Label) {
	case "green", "pass", "good", "ok":
		return "\x1b[32m"
	case "yellow", "amber", "orange", "warn", "warning":
		return "\x1b[33m"
	case "red", "fail", "bad":
		return "\x1b[31m"
	}
	return [...]string{"\x1b[32m", "\x1b[33m", "\x1b[31m", "\x1b[36m", "\x1b[34m"}[min(s.Rank, 4)]
}

// RunDuration formats a measured run time. Runs are stored in whole
// milliseconds, so anything under one is shown as "<1ms" rather than as the
// microseconds an average of truncated values would suggest.
func RunDuration(d time.Duration) string {
	if d < time.Millisecond {
		return "<1ms"
	}
	return FormatDuration(d)
}

// FormatDuration prints a duration at a precision that suits its size.
func FormatDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return d.String()
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	default:
		return d.Round(time.Second).String()
	}
}

// printer remembers the first write error so the renderers stay readable.
type printer struct {
	w   io.Writer
	err error
}

func (p *printer) Write(b []byte) (int, error) {
	if p.err != nil {
		return 0, p.err
	}
	n, err := p.w.Write(b)
	p.err = err
	return n, err
}

func (p *printer) f(format string, args ...any) {
	fmt.Fprintf(p, format, args...)
}
