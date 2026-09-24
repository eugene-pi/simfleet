SHELL := /bin/bash
export PATH := $(HOME)/go/bin:/usr/local/go/bin:$(PATH)

# подхватить .env, если он есть
ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: build test lint migrate db-up db-down run-api tidy

build:
	go build -o bin/ ./cmd/...

test:
	go test ./... -race -count=1

lint:
	golangci-lint run

tidy:
	go mod tidy

db-up:
	docker run -d --name pg -p 5432:5432 \
	  -e POSTGRES_PASSWORD=dev -e POSTGRES_DB=simfleet postgres:17

db-down:
	docker rm -f pg

migrate:
	go run ./cmd/migrate

run-api:
	go run ./cmd/api