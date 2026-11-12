// Command simfleet is the user-facing CLI: submit experiments and report on
// them. It is short-lived and talks only to Postgres (DATABASE_URL), so the
// same binary works from a laptop against a port-forwarded database or as a
// one-off pod inside the cluster.
//
//	simfleet submit [-watch] [report flags] spec.yaml
//	simfleet report [-watch] [-n 5] [-o report.html] [experiment-id]
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/envfile"
	"github.com/eugene-pi/simfleet/internal/report"
	_ "github.com/eugene-pi/simfleet/internal/runner/crossing"
	_ "github.com/eugene-pi/simfleet/internal/runner/slowcrossing"
	"github.com/eugene-pi/simfleet/internal/store"
	"github.com/eugene-pi/simfleet/internal/submit"
)

const usage = `usage:
  simfleet submit [-watch] [report flags] <spec.yaml>
      create an experiment; with -watch, follow it live and print the report
  simfleet report [flags] [experiment-id]
      report on an experiment (default: the most recent one)

Run "simfleet <command> -h" for the flags.`

func main() {
	envfile.LoadEnv()
	core.SetupLogger()

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "submit":
		err = runSubmit(ctx, os.Args[2:])
	case "report":
		err = runReport(ctx, os.Args[2:])
	case "-h", "-help", "--help", "help":
		fmt.Println(usage)
		return
	default:
		err = fmt.Errorf("unknown command %q\n\n%s", os.Args[1], usage)
	}
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(2)
	}
	if err != nil {
		slog.Error("simfleet failed", "error", err)
		os.Exit(1)
	}
}

// reportFlags are shared by "report" and "submit -watch".
type reportFlags struct {
	n         int
	format    string
	out       string
	watch     bool
	interval  time.Duration
	scoreWait time.Duration
	color     string
	width     int
}

func (f *reportFlags) register(fs *flag.FlagSet) {
	fs.IntVar(&f.n, "n", 5, "number of worst and best cases to list")
	fs.StringVar(&f.format, "format", "", "text or html (default: from -o extension, else text)")
	fs.StringVar(&f.out, "o", "", "write the report to this file instead of stdout")
	fs.BoolVar(&f.watch, "watch", false, "show live statistics until the experiment finishes, then the report")
	fs.DurationVar(&f.interval, "interval", 2*time.Second, "live update interval")
	fs.DurationVar(&f.scoreWait, "score-wait", 2*time.Minute,
		"after the last job, how long to wait for scoring before reporting anyway")
	fs.StringVar(&f.color, "color", "auto", "auto, always or never (NO_COLOR is honoured)")
	fs.IntVar(&f.width, "width", 40, "bar width in characters")
}

func runSubmit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	var rf reportFlags
	rf.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: simfleet submit [flags] <spec.yaml>")
	}
	spec, err := submit.LoadSpec(fs.Arg(0))
	if err != nil {
		return err
	}
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	id, n, err := submit.Submit(ctx, st, spec)
	if err != nil {
		return err
	}
	// The id goes to stdout alone, so scripts can capture it.
	fmt.Println(id)
	slog.Info("experiment submitted", "experiment", id, "jobs", n)

	if !rf.watch {
		return nil
	}
	return produce(ctx, st, id, rf)
}

func runReport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	var rf reportFlags
	rf.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: simfleet report [flags] [experiment-id]")
	}
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	var id core.ExperimentID
	if fs.NArg() == 1 {
		u, err := uuid.Parse(fs.Arg(0))
		if err != nil {
			return fmt.Errorf("experiment id %q: %w", fs.Arg(0), err)
		}
		id = core.ExperimentID(u)
	} else if id, err = st.LatestExperimentID(ctx); err != nil {
		return err
	}
	return produce(ctx, st, id, rf)
}

// produce builds the report — after a live watch if asked — and writes it.
func produce(ctx context.Context, st *store.Postgres, id core.ExperimentID, rf reportFlags) error {
	format, err := rf.resolveFormat()
	if err != nil {
		return err
	}
	style := report.TextStyle{Color: useColor(rf.color, os.Stdout), Width: rf.width}

	var r report.Report
	if rf.watch {
		r, err = report.Watch(ctx, st, id, report.WatchOptions{
			Interval:  rf.interval,
			ScoreWait: rf.scoreWait,
			N:         rf.n,
			OnTick:    liveRenderer(os.Stdout, style),
		})
	} else {
		var d store.ReportData
		if d, err = st.ReportData(ctx, id, rf.n); err == nil {
			r = report.Build(d, rf.n, time.Now())
		}
	}
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if format == "html" {
		err = report.WriteHTML(&buf, r)
	} else {
		if rf.out != "" {
			style.Color = false // a file gets no escape codes
		}
		err = report.WriteText(&buf, r, style)
	}
	if err != nil {
		return err
	}

	if rf.out == "" {
		_, err = buf.WriteTo(os.Stdout)
		return err
	}
	if err := os.WriteFile(rf.out, buf.Bytes(), 0o644); err != nil {
		return err
	}
	slog.Info("report written", "path", rf.out, "complete", r.Complete())
	return nil
}

func (rf reportFlags) resolveFormat() (string, error) {
	f := strings.ToLower(rf.format)
	if f == "" {
		switch strings.ToLower(filepath.Ext(rf.out)) {
		case ".html", ".htm":
			f = "html"
		default:
			f = "text"
		}
	}
	if f != "text" && f != "html" {
		return "", fmt.Errorf("unknown format %q, want text or html", rf.format)
	}
	if f == "html" && rf.out == "" && isTerminal(os.Stdout) {
		return "", errors.New("html to a terminal: pass -o report.html or redirect stdout")
	}
	return f, nil
}

// liveRenderer redraws the screen in place on a terminal, and prints one
// line per update anywhere else (a log file, kubectl logs, a pipe).
func liveRenderer(w *os.File, style report.TextStyle) func(report.LiveView) error {
	if !isTerminal(w) || !enableVT(w) {
		return func(v report.LiveView) error {
			_, err := fmt.Fprintln(w, report.LiveLine(v))
			return err
		}
	}
	return func(v report.LiveView) error {
		var buf bytes.Buffer
		buf.WriteString("\x1b[H\x1b[2J") // home + clear: one write per frame, no flicker
		if err := report.WriteLive(&buf, v, style); err != nil {
			return err
		}
		if v.Finished && !v.Scoring {
			buf.WriteString("\n")
		}
		_, err := buf.WriteTo(w)
		return err
	}
}

func openStore(ctx context.Context) (*store.Postgres, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, errors.New("DATABASE_URL is not set")
	}
	return store.NewPostgresStore(ctx, dsn)
}

func useColor(mode string, f *os.File) bool {
	switch mode {
	case "always":
		return enableVT(f)
	case "never":
		return false
	}
	if os.Getenv("NO_COLOR") != "" || !isTerminal(f) {
		return false
	}
	return enableVT(f)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
