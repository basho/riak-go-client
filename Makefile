.PHONY: all install-deps lint
.PHONY: unit-test integration-test security-test test fmt help

PROJDIR = $(realpath $(CURDIR))

export RIAK_HOST = localhost
export RIAK_PORT = 8087

all: install-deps lint test

install-deps:
	cd $(PROJDIR) && go get -t github.com/basho/riak-go-client/...

lint: install-deps
	cd $(PROJDIR) && go tool vet -shadow=true -shadowstrict=true $(PROJDIR)
	cd $(PROJDIR) && go vet github.com/basho/riak-go-client/...

unit-test: lint
	cd $(PROJDIR) && go test -v

integration-test: lint
	cd $(PROJDIR) && go test -v -tags=integration

security-test: lint
	cd $(PROJDIR) && go test -v -tags=security

test: integration-test

fmt:
	cd $(PROJDIR) && gofmt -s -w .

protogen:
	$(PROJDIR)/build/protogen $(PROJDIR)

help:
	@echo ''
	@echo ' Targets:'
	@echo '----------------------------------------------------------'
	@echo ' all                  - Run everything                    '
	@echo ' fmt                  - Format code                       '
	@echo ' lint                 - Run "go vet"                      '
	@echo ' test                 - Run unit & integration tests      '
	@echo ' unit-test            - Run unit tests                    '
	@echo ' integration-test     - Run integration tests             '
	@echo ' security-test        - Run security tests                '
	@echo '----------------------------------------------------------'
	@echo ''
