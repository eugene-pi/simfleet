-- +goose Up
-- The outbox records what kind of job a message is, not where it goes:
-- mapping a runner to a broker destination is the bus's job.
ALTER TABLE outbox RENAME COLUMN topic TO runner;

-- +goose Down
ALTER TABLE outbox RENAME COLUMN runner TO topic;
