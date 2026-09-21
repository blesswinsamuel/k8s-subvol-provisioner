BIN := bin/subvol-provisioner
PKG := github.com/blesswinsamuel/k8s-subvol-provisioner
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(DATE)

.PHONY: all build test vet clean docker-build

all: build

build:
	@mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/subvol-provisioner

test:
	go test -v -race ./...

vet:
	go vet ./...

docker-build:
	docker build -t k8s-subvol-provisioner:latest .

clean:
	rm -rf bin/
