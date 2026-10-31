#!/bin/sh
# The Go runner owns all controller/broker/consumer children and fault proxies.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
workdir=$(mktemp -d "${TMPDIR:-/tmp}/minikafka-demo-build.XXXXXX")
runner_pid=
cleanup() {
    if [ -n "$runner_pid" ]; then
        kill -TERM "$runner_pid" 2>/dev/null || :
        wait "$runner_pid" 2>/dev/null || :
        runner_pid=
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cd "$root"
go build -o "$workdir/minikafka" ./cmd/minikafka
go build -o "$workdir/minikafka-demo" ./cmd/minikafka-demo
"$workdir/minikafka-demo" --binary "$workdir/minikafka" "$@" &
runner_pid=$!
status=0
wait "$runner_pid" || status=$?
runner_pid=
exit "$status"
