.PHONY: build test version

# Scope the dirty check to this directory. `git describe --dirty` reports the whole
# containing repo, which would mark the stamp dirty for unrelated changes elsewhere.
VERSION ?= $(shell v="$$(git describe --tags --always 2>/dev/null || printf dev)"; if [ -n "$$(git status --porcelain -- . 2>/dev/null)" ]; then v="$$v-dirty"; fi; printf '%s' "$$v")
BINARY ?= ./bin/amq-keepalive
LDFLAGS := -X github.com/ohade/amq-keepalive/internal/app.Version=$(VERSION)

build:
	@mkdir -p "$(dir $(BINARY))"
	go build -ldflags "$(LDFLAGS)" -o "$(BINARY)" ./cmd/amq-keepalive

test:
	go vet ./...
	go test ./...

version: build
	"$(BINARY)" -v
