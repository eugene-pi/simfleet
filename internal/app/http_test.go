package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type pinger struct{ err error }

func (p pinger) Ping(context.Context) error { return p.err }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get(url)
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("get %s: %v", url, err)
			}
			time.Sleep(5 * time.Millisecond) // the server starts asynchronously
			continue
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(body)
	}
}

func TestEndpoints(t *testing.T) {
	addr := freeAddr(t)
	ready := pinger{err: errors.New("connect to host=db.internal user=simfleet failed")}

	ctx, cancel := context.WithCancel(context.Background())
	wait := StartHTTP(ctx, addr, ready)

	if code, _ := get(t, fmt.Sprintf("http://%s/healthz", addr)); code != http.StatusOK {
		t.Errorf("healthz = %d, want 200: liveness must ignore dependencies", code)
	}
	if code, body := get(t, fmt.Sprintf("http://%s/metrics", addr)); code != http.StatusOK ||
		!strings.Contains(body, "go_goroutines") {
		t.Errorf("metrics = %d, body has no standard series", code)
	}

	code, body := get(t, fmt.Sprintf("http://%s/readyz", addr))
	if code != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d, want 503", code)
	}
	if strings.Contains(body, "db.internal") {
		t.Errorf("readyz leaks the cause: %q", body)
	}

	cancel()
	if err := wait(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestReadyWhenPingSucceeds(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	wait := StartHTTP(ctx, addr, pinger{})

	if code, _ := get(t, fmt.Sprintf("http://%s/readyz", addr)); code != http.StatusOK {
		t.Errorf("readyz = %d, want 200", code)
	}
	cancel()
	if err := wait(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestTakenPortIsReported(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	wait := StartHTTP(context.Background(), l.Addr().String(), nil)
	if err := wait(); err == nil {
		t.Error("want an error for a taken port")
	}
}
