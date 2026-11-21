SHELL := /bin/bash
export PATH := $(HOME)/go/bin:/usr/local/go/bin:$(PATH)

# подхватить .env, если он есть
ifneq (,$(wildcard .env))
include .env
export
endif

SPEC ?= examples/crossing-small.yaml
DOCKER_NETWORK ?= simfleet_default
VERSION ?= dev
IMAGES  := controller worker migrate simfleet
# experiment id for "make report"; empty means the most recent one
EXP ?=

include make/go.mk
include make/docker.mk
include make/k8s.mk

.DEFAULT_GOAL := help

# Self-documenting help: every target with a ## comment shows up here.
.PHONY: dev help

.PHONY: dev
dev: ## Full local deploy
	compose-up k8s-load k8s-secrets k8s-apply 

help:
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

