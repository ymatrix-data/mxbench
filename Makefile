all: build

.PHONY: build linux_amd64 linux_arm64 clean lint race e2e test clean release

GINKGO:=go run github.com/onsi/ginkgo/ginkgo
REPO_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

GIT_VERSION:=$(shell git tag --sort=-creatordate --points-at HEAD "${TAG_FILTER}*" | tail -n 1)
ifeq ($(GIT_VERSION),)
GIT_VERSION:=$(shell git describe --abbrev=0 --tags)
ifeq ($(GIT_VERSION),)
GIT_VERSION:=Build-Dev
else
GIT_VERSION:=$(GIT_VERSION)+Dev
endif
endif
GIT_BRANCH:=$(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT:=$(shell git rev-parse --short=8 HEAD)
MAIN_VERSION_STR:=github.com/ymatrix-data/mxbench/internal/util.VersionStr=$(GIT_VERSION)
MAIN_BRANCH_STR:=github.com/ymatrix-data/mxbench/internal/util.BranchStr=$(GIT_BRANCH)
MAIN_COMMIT_STR:=github.com/ymatrix-data/mxbench/internal/util.CommitStr=$(GIT_COMMIT)
LDFLAGS:=-X $(MAIN_VERSION_STR) -X $(MAIN_BRANCH_STR) -X $(MAIN_COMMIT_STR)

build:
	go build -o ./bin/mxbench -ldflags "$(LDFLAGS)" ./cmd/mxbench/main.go

linux_amd64:
	env GOOS=linux GOARCH=amd64 go build -o ./bin/mxbench -ldflags "$(LDFLAGS)" ./cmd/mxbench/main.go

linux_arm64:
	env GOOS=linux GOARCH=arm go build -o ./bin/mxbench -ldflags "$(LDFLAGS)" ./cmd/mxbench/main.go

release:
	CGO_ENABLED=0 go build -o ./bin/mxbench -a -ldflags "$(LDFLAGS)" ./cmd/mxbench/main.go

clean:
	rm -rf ./bin/*

lint:
	golangci-lint -v run

test:
	$(GINKGO) -race ./internal/...

e2e:
	go build -tags e2e -ldflags "$(LDFLAGS)" -o mxbench_e2e ./cmd/mxbench
	unset PGDATABASE; PATH="$(REPO_DIR):${PATH}" $(GINKGO) -tags e2e ./test/e2e/cli


race:
	go build -race -ldflags "$(LDFLAGS)" -o mxbench_race ./cmd/mxbench
