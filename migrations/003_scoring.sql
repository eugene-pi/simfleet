-- +goose Up
ALTER TABLE job_results ADD COLUMN gated_by      text;
ALTER TABLE job_results ADD COLUMN scoring_error text;

-- Partial index for the scoring queue: only rows that still need scoring.
-- Частичный индекс для очереди оценивания: только строки, ждущие оценки.
CREATE INDEX job_results_unscored_idx ON job_results (created_at)
    WHERE scoring_error IS NULL;

CREATE INDEX job_results_zone_idx ON job_results (zone);

-- +goose Down
DROP INDEX job_results_zone_idx;
DROP INDEX job_results_unscored_idx;
ALTER TABLE job_results DROP COLUMN scoring_error;
ALTER TABLE job_results DROP COLUMN gated_by;