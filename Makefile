.PHONY: build run-dev build-docker clean

GIT_BRANCH := $(subst heads/,,$(shell git rev-parse --abbrev-ref HEAD 2>/dev/null))
DEV_IMAGE := nomad-logzio-dev$(if $(GIT_BRANCH),:$(subst /,-,$(GIT_BRANCH)))
DEV_DOCKER_IMAGE := nomad-logzio-bin-dev$(if $(GIT_BRANCH),:$(subst /,-,$(GIT_BRANCH)))

default: clean install crossbinary dockerdeps

clean:
	rm -rf dist/
	rm -rf bin/

binary: install
	GOOS=linux CGO_ENABLED=0 GOGC=off GOARCH=amd64 go build -a -tags netgo -ldflags '-w' -o "$(CURDIR)/bin/nomad-logzio"

crossbinary: binary
	GOOS=linux GOARCH=amd64 go build -o "$(CURDIR)/dist/nomad-logzio-linux-amd64"
	GOOS=linux GOARCH=386 go build -o "$(CURDIR)/dist/nomad-logzio-linux-386"
	GOOS=darwin GOARCH=amd64 go build -o "$(CURDIR)/dist/nomad-logzio-darwin-amd64"
	GOOS=darwin GOARCH=386 go build -o "$(CURDIR)/dist/nomad-logzio-darwin-386"
	GOOS=windows GOARCH=amd64 go build -o "$(CURDIR)/dist/nomad-logzio-windows-amd64.exe"
	GOOS=windows GOARCH=386 go build -o "$(CURDIR)/dist/nomad-logzio-windows-386.exe"

install: clean
	go mod vendor
	go generate

test:
	go test ./...

dist:
	mkdir dist

run-dev:
	go generate
	go test ./...
	go build -o "nomad-logzio"
	./nomad-logzio

build-docker:
	docker build -t "$(DEV_DOCKER_IMAGE)" .
