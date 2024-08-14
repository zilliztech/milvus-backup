# Variables
BINARY_NAME=milvus-backup
VERSION=$(shell git describe --tags --always --dirty)
COMMIT=$(shell git rev-parse --short HEAD)
DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

# Default target
all: gen build

# Build the binary
build:
	@echo "Building binary..."
	GO111MODULE=on CGO_ENABLED=0 go build -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)" -o $(BINARY_NAME)

gen:
	./scripts/gen_swag.sh
	./scripts/gen_proto.sh

.PHONY: all build gen