-- +goose Up
CREATE TYPE job_state AS ENUM (
    'queued', 'running', 'completed', 'failed', 'cancelled'
);

CREATE TYPE experiment_state AS ENUM (
    'pending', 'expanding', 'running', 'completed', 'cancelled'
);

CREATE TABLE experiments (
    id            uuid PRIMARY KEY,
    name          text NOT NULL,
    runner        text NOT NULL,
    spec          jsonb NOT NULL,
    scoring       jsonb NOT NULL,
    scoring_hash  text NOT NULL,
    state         experiment_state NOT NULL DEFAULT 'pending',
    total_jobs    int,
    created_by    text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz
);

CREATE TABLE jobs (
    id            uuid PRIMARY KEY,
    experiment_id uuid NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    idx           int NOT NULL,
    params        jsonb NOT NULL,
    seed          bigint NOT NULL,
    state         job_state NOT NULL DEFAULT 'queued',
    attempt       int NOT NULL DEFAULT 0,
    lease_until   timestamptz,
    worker_id     text,
    last_error    text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (experiment_id, idx)
);

CREATE INDEX jobs_experiment_state_idx ON jobs (experiment_id, state);
CREATE INDEX jobs_expired_leases_idx   ON jobs (lease_until)
    WHERE state = 'running';

CREATE TABLE job_results (
    job_id       uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    attempt      int NOT NULL,
    metrics      jsonb NOT NULL,
    flags        jsonb NOT NULL DEFAULT '{}',
    labels       jsonb NOT NULL DEFAULT '{}',
    artifact_key text,
    duration_ms  int NOT NULL,
    score        real,
    zone         text,
    scoring_hash text,
    components   jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX job_results_score_idx ON job_results (score);

CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    msg_key      text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE job_results;
DROP TABLE jobs;
DROP TABLE experiments;
DROP TYPE experiment_state;
DROP TYPE job_state;
