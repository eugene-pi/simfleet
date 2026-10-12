package controller_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"reflect"
	"testing"
	"time"
	"uuid"

	"github.com/eugene-pi/simfleet/internal/controller"
	"github.com/eugene-pi/simfleet/internal/core"
	"github.com/eugene-pi/simfleet/internal/dbtest"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

type scoreRow struct {
	Score float64
	Zone  string
}

func TestScoringPipeline(t *testing.T) {
	st, pool := dbtest.NewPostgresAndPool(t)
	ctx := context.Background()

	expID := dbtest.SeedExperiment(t, pool, core.Spec{
		Name:   "scoring-test",
		Runner: "fake",
		Scoring: core.Scoring{
			Gates:      []core.Gate{{When: "collision", Score: 0}},
			Components: []core.ScoreComponent{{Metric: "dist", Good: 3.0, Bad: 0.5}},
			Combine:    "min",
		},
		Zones: []core.Zone{{Name: "green", Min: 80}, {Name: "yellow", Min: 50}, {Name: "red", Min: 0}},
	})
	// три результата: хороший, средний, со столкновением
	dbtest.SeedResult(t, pool, expID, 0, map[string]float64{"dist": 3.0}, nil)
	dbtest.SeedResult(t, pool, expID, 1, map[string]float64{"dist": 1.75}, nil)
	dbtest.SeedResult(t, pool, expID, 2, map[string]float64{"dist": 5.0},
		map[string]bool{"collision": true})

	c := controller.New(st, time.Second, 100)
	if err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	// 1. всё оценено
	got := fetchScores(t, pool, expID)
	want := map[int]string{0: "green", 1: "yellow", 2: "red"}
	for k, v := range got {
		if v2, ok := want[k]; ok {
			assert.Equal(t, v.Zone, v2)
		} else {
			t.Errorf("unexpected result %v", k)
		}
	}

	// 2. повторный проход ничего не меняет (идемпотентность)
	before := got
	if err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, fetchScores(t, pool, expID)) {
		t.Error("second pass changed results")
	}

	// 3. смена порогов приводит к пересчёту
	updateScoring(t, pool, expID, core.Scoring{
		Components: []core.ScoreComponent{{Metric: "dist", Good: 2.0, Bad: 0.5}},
		Combine:    "min",
	})
	if err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	// задание 1 должно сместиться в green
}

// fetchScores returns zone and score per job index, so assertions can refer to
// the seeding order rather than to generated ids.
func fetchScores(t *testing.T, pool *pgxpool.Pool, expID core.ExperimentID) map[int]scoreRow {
	t.Helper()
	ctx := context.Background()

	const q = `
		SELECT j.idx, r.score, r.zone
		  FROM job_results r
		  JOIN jobs j ON j.id = r.job_id
		 WHERE j.experiment_id = $1 AND r.score IS NOT NULL
		 ORDER BY j.idx`

	rows, err := pool.Query(ctx, q, uuid.UUID(expID))
	if err != nil {
		t.Fatalf("fetch scores: %v", err)
	}
	defer rows.Close()

	out := map[int]scoreRow{}
	for rows.Next() {
		var idx int
		var sr scoreRow
		if err := rows.Scan(&idx, &sr.Score, &sr.Zone); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[idx] = sr
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// updateScoring rewrites the experiment's scoring spec and bumps its hash so
// the controller picks the results up for rescoring on the next pass.
func updateScoring(t *testing.T, pool *pgxpool.Pool, expID core.ExperimentID, s core.Scoring) {
	t.Helper()

	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal scoring: %v", err)
	}

	const q = `UPDATE experiments SET scoring = $2, scoring_hash = $3 WHERE id = $1`
	if _, err := pool.Exec(context.Background(), q,
		uuid.UUID(expID), raw, hashOf(raw)); err != nil {
		t.Fatalf("update scoring: %v", err)
	}
}

// hashOf gives a stable fingerprint of the scoring spec. Any change to the
// spec changes the hash, which is what triggers rescoring.
func hashOf(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
