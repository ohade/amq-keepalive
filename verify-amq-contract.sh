#!/bin/sh
set -eu

: "${AMQ_BIN:?set AMQ_BIN to the AMQ binary under test}"
AMQ_BIN="$AMQ_BIN" go test -count=1 ./internal/amq -run '^TestAMQBinaryProducerContracts$'
