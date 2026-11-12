package report

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/eugene-pi/simfleet/internal/store"
)

//go:embed report.html.tmpl
var htmlFS embed.FS

var htmlTmpl = template.Must(template.New("report.html.tmpl").Funcs(template.FuncMap{
	"dur":   FormatDuration,
	"pct":   pct,
	"score": func(f float64) string { return strconv.FormatFloat(f, 'f', 1, 64) },
	"mean": func(f float64) string {
		if math.IsNaN(f) {
			return "—"
		}
		return strconv.FormatFloat(f, 'f', 1, 64)
	},
	"metric": func(c store.CaseRow, name string) string {
		if v, ok := c.Metrics[name]; ok {
			return strconv.FormatFloat(v, 'g', 4, 64)
		}
		return ""
	},
	"param": func(c store.CaseRow, name string) string {
		if v, ok := c.Params[name]; ok {
			return v.String()
		}
		return ""
	},
	"note":  CaseNote,
	"class": segmentClass,
	// width keeps a non-empty segment visible however small its share.
	"width": func(s Segment) string {
		return strconv.FormatFloat(max(s.Share*100, 0.6), 'f', 2, 64) + "%"
	},
	"ts":      func(t time.Time) string { return t.Local().Format("2006-01-02 15:04:05") },
	"nonzero": func(b Bar) []Segment { return nonEmpty(b) },
	"ties":    func(ties, shown int) bool { return ties > shown },
	"cut":     func(s string, n int) string { return s[:min(n, len(s))] },
	"jobs":    jobStates,
	"dict": func(kv ...any) (map[string]any, error) {
		if len(kv)%2 != 0 {
			return nil, fmt.Errorf("dict: odd number of arguments")
		}
		m := make(map[string]any, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			k, ok := kv[i].(string)
			if !ok {
				return nil, fmt.Errorf("dict: key %v is not a string", kv[i])
			}
			m[k] = kv[i+1]
		}
		return m, nil
	},
	"run":   RunDuration,
	"share": func(b Bar, label string) string { return ShareCell(b.Count(label), b.Total) },
}).ParseFS(htmlFS, "report.html.tmpl"))

// WriteHTML renders the report as one self-contained HTML page: no scripts,
// no external assets, so it can be attached, archived or served as is.
func WriteHTML(w io.Writer, r Report) error {
	return htmlTmpl.Execute(w, r)
}

func nonEmpty(b Bar) []Segment {
	var out []Segment
	for _, s := range b.Segments {
		if s.Count > 0 {
			out = append(out, s)
		}
	}
	return out
}

// segmentClass maps a segment to a CSS class. Zones use the status palette
// by name when the name is a conventional one, otherwise by rank.
func segmentClass(s Segment) string {
	switch s.Kind {
	case KindFailed:
		return "seg-failed"
	case KindUnfinished:
		return "seg-unfinished"
	case KindPending:
		return "seg-pending"
	}
	switch strings.ToLower(s.Label) {
	case "green", "pass", "good", "ok":
		return "seg-good"
	case "yellow", "amber", "orange", "warn", "warning":
		return "seg-warn"
	case "red", "fail", "bad":
		return "seg-crit"
	}
	return [...]string{"seg-good", "seg-warn", "seg-crit", "seg-z3", "seg-z4"}[min(s.Rank, 4)]
}
