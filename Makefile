.PHONY: build build-all clean lint test

BINARY=plugin
PLATFORMS=linux/amd64 linux/arm64 darwin/arm64
VERSION ?= $(shell git describe --tags --always 2>/dev/null | sed 's/^v//')
CLIENT_ID ?=
LDFLAGS=-s -w -X main.version=$(VERSION) -X github.com/wetrakr/wetrakr-silo/provider.DefaultClientID=$(CLIENT_ID)

build:
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BINARY) .

build-all:
	@mkdir -p dist
	@set -e; for platform in $(PLATFORMS); do \
		GOOS=$${platform%%/*} GOARCH=$${platform##*/} CGO_ENABLED=0 \
		go build -trimpath -ldflags="$(LDFLAGS)" -o dist/$(BINARY)-$${platform%%/*}-$${platform##*/} .; \
	done

test:
	go test ./...

lint:
	golangci-lint run ./...

clean:
	rm -f $(BINARY)
	rm -rf dist
