-- +goose Up
-- Who actually produced this result. jobs.worker_id is the current lease
-- holder and gets cleared on completion, so attribution lives here.
-- Кто фактически выполнил задание. jobs.worker_id — это текущий арендатор,
-- он обнуляется при завершении, поэтому авторство хранится здесь.
ALTER TABLE job_results ADD COLUMN worker_id text;

-- +goose Down
ALTER TABLE job_results DROP COLUMN worker_id;