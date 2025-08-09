#!Makefile

SHELL = /usr/bin/env bash
PROGRAM := $(shell echo $(notdir $(CURDIR)) | cut -f1 -d"-")
BINDIR := bin
GOOSARCHES = linux/amd64 linux/ppc64 linux/ppc64le linux/arm64
VERSION ?= $(shell /usr/bin/git describe --tags --abbrev=0)
GITREVISION ?= $(shell /usr/bin/git rev-parse HEAD)
BUILDERNAME ?= $(shell /usr/bin/hostname)
BUILDBRANCH ?= $(shell /usr/bin/git rev-parse --abbrev-ref HEAD)

default: build

build:
	@echo "Building $(PROGRAM)"
	@time go build -trimpath -ldflags '-s -w -X  check_kafka/version.Version=$(VERSION) -X check_kafka/version.GitRevision=$(GITREVISION) -X check_kafka/version.Builder=$(BUILDERNAME) -X check_kafka/version.GitBranch=$(BUILDBRANCH) -X check_kafka/version.Client=check_kafka' -o $(PROGRAM) $(PROGRAM).go

clean:
	@echo "Cleaning $(PROGRAM)"
	rm -rf bin/* $(PROGRAM)

gofmt:
	find . -type f -name '*.go' -exec gofmt -w {} \;
