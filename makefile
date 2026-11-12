SHELL := /bin/bash
export PATH := $(HOME)/go/bin:/usr/local/go/bin:$(PATH)

# подхватить .env, если он есть
ifneq (,$(wildcard .env))
include .env
export
endif

SPEC ?= examples/crossing-small.yaml
VERSION ?= dev
IMAGES  := controller worker migrate

.PHONY: build images test lint migrate db-up db-down run-api run-local tidy

build:
	go build -o bin/ ./cmd/...

images:
	@for c in $(IMAGES); do \
	  docker build -f build/Dockerfile --build-arg CMD=$$c --build-arg VERSION=$(VERSION) \
	    -t simfleet/$$c:$(VERSION) -t simfleet/$$c:latest . ; \
	done

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

run-local:
	go run ./cmd/runlocal submit $(SPEC)
