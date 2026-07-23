#!/bin/sh
set -eu

: "${AMQ_SOURCE:?set AMQ_SOURCE to an explicit agent-message-queue source checkout}"
[ -d "$AMQ_SOURCE" ] || { printf '%s\n' "AMQ_SOURCE is not a directory: $AMQ_SOURCE" >&2; exit 2; }
[ -f "$AMQ_SOURCE/go.mod" ] || { printf '%s\n' "AMQ_SOURCE lacks go.mod: $AMQ_SOURCE" >&2; exit 2; }

contract_tmp=$(mktemp -d "${TMPDIR:-/tmp}/amq-keepalive-contract.XXXXXX")
trap 'rm -r -- "$contract_tmp"' EXIT HUP INT TERM

(
  cd "$AMQ_SOURCE"
  go build -o "$contract_tmp/amq" ./cmd/amq
)
AMQ_BIN="$contract_tmp/amq" go test -count=1 ./internal/amq -run '^TestAMQBinaryProducerContracts$'
