# Variables
BINARY_NAME=milvus-backup
PKG := github.com/zilliztech/milvus-backup
VERSION=$(shell git describe --tags --always)
COMMIT=$(shell git rev-parse --short HEAD)
DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

LDFLAGS += -X "$(PKG)/version.Version=$(VERSION)"
LDFLAGS += -X "$(PKG)/version.Commit=$(COMMIT)"
LDFLAGS += -X "$(PKG)/version.Date=$(DATE)"

# Default target
all: gen build

# Build the binary
build:
	@echo "Building binary..."
	GO111MODULE=on CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o $(BINARY_NAME)

gen:
	./scripts/gen_swag.sh
	./scripts/gen_proto.sh

fmt:
	@echo Formatting code...
	@goimports -w --local $(PKG) ./
	@echo Format code done

.PHONY: all build gen