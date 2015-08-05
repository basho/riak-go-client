.PHONY: all get-deps lint test unit-test integration-test fmt help

all: lint test

get-deps:
	go get -t ./...

lint: get-deps
	go vet

unit-test: lint
	go test -v ./...

integration-test: lint
	go test -v -tags=integration ./...

# TODO: add integration-test
test: lint unit-test

fmt:
	gofmt -s -w .

help:
	@echo ''
	@echo ' Targets:'
	@echo '--------------------------------------------------'
	@echo ' all              - Run everything                '
	@echo ' fmt              - Format code                   '
	@echo ' lint             - Run jshint                    '
	@echo ' test             - Run unit & integration tests  '
	@echo ' unit-test        - Run unit tests                '
	@echo ' integration-test - Run integration tests         '
	@echo '--------------------------------------------------'
	@echo ''
