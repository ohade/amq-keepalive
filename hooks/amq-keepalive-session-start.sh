#!/usr/bin/env bash
# SessionStart hook wrapper for amq-keepalive.
#
# This hook is intentionally non-blocking: it logs reattach failures and always
# returns an empty hook response so agent startup is not held hostage by AMQ.

set -u

INPUT="$(cat 2>/dev/null || true)"
BIN="${AMQ_KEEPALIVE_BIN:-amq-keepalive}"
ADAPTER="${AMQ_KEEPALIVE_ADAPTER:-ghostty}"
TARGET="${AMQ_KEEPALIVE_TARGET:-}"
REGISTRY="${AMQ_KEEPALIVE_REGISTRY:-}"
AMQ_BIN="${AMQ_KEEPALIVE_AMQ:-amq}"
SELF_BIN="${AMQ_KEEPALIVE_SELF:-$BIN}"
ROOT="${AMQ_KEEPALIVE_ROOT:-}"
BASE_ROOT="${AMQ_KEEPALIVE_BASE_ROOT:-}"
SESSION_NAME="${AMQ_KEEPALIVE_SESSION:-}"
ME="${AMQ_KEEPALIVE_ME:-}"
LOG_PATH="${AMQ_KEEPALIVE_LOG:-$HOME/.amq-keepalive/session-start.log}"
TIMEOUT_SECONDS="${AMQ_KEEPALIVE_TIMEOUT_SECONDS:-10}"

if [[ "${AMQ_KEEPALIVE_DISABLED:-0}" == "1" ]]; then
    printf '{}\n'
    exit 0
fi

mkdir -p "$(dirname "$LOG_PATH")" 2>/dev/null || true

log() {
    printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOG_PATH" 2>/dev/null || true
}

CWD=""
if command -v jq >/dev/null 2>&1 && [[ -n "$INPUT" ]]; then
    CWD="$(printf '%s' "$INPUT" | jq -r '.cwd // .workdir // .working_directory // empty' 2>/dev/null || true)"
fi
if [[ -n "$CWD" && -d "$CWD" ]]; then
    cd "$CWD" 2>/dev/null || true
fi

if ! command -v "$BIN" >/dev/null 2>&1; then
    log "skip: amq-keepalive binary not found: $BIN"
    printf '{}\n'
    exit 0
fi

args=(reattach --adapter "$ADAPTER" --amq "$AMQ_BIN")
[[ -n "${AMQ_KEEPALIVE_SELF:-}" ]] && args+=(--self "$SELF_BIN")
[[ -n "$TARGET" ]] && args+=(--target "$TARGET")
[[ -n "$REGISTRY" ]] && args+=(--registry "$REGISTRY")
[[ -n "$ROOT" ]] && args+=(--root "$ROOT")
[[ -n "$BASE_ROOT" ]] && args+=(--base-root "$BASE_ROOT")
[[ -n "$SESSION_NAME" ]] && args+=(--session "$SESSION_NAME")
[[ -n "$ME" ]] && args+=(--me "$ME")
[[ "${AMQ_KEEPALIVE_NO_START:-0}" == "1" ]] && args+=(--no-start)

run_reattach() {
    "$BIN" "${args[@]}" >> "$LOG_PATH" 2>&1
}

if [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ && "$TIMEOUT_SECONDS" -gt 0 ]]; then
    timeout_marker="${TMPDIR:-/tmp}/amq-keepalive-timeout.$$"
    rm -f "$timeout_marker" 2>/dev/null || true

    (
        trap 'exit 143' TERM
        run_reattach
    ) 2>> "$LOG_PATH" &
    reattach_pid=$!
    (
        sleep "$TIMEOUT_SECONDS"
        if kill -0 "$reattach_pid" 2>/dev/null; then
            : > "$timeout_marker" 2>/dev/null || true
            pkill -TERM -P "$reattach_pid" 2>/dev/null || true
            kill -TERM "$reattach_pid" 2>/dev/null || true
            sleep 1
            pkill -KILL -P "$reattach_pid" 2>/dev/null || true
            kill -KILL "$reattach_pid" 2>/dev/null || true
        fi
    ) &
    watchdog_pid=$!

    wait "$reattach_pid" 2>/dev/null
    status=$?
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true

    if [[ -f "$timeout_marker" ]]; then
        rm -f "$timeout_marker" 2>/dev/null || true
        log "reattach timed out after ${TIMEOUT_SECONDS}s adapter=$ADAPTER"
        printf '{}\n'
        exit 0
    fi
else
    run_reattach
    status=$?
fi

if [[ "$status" -eq 0 ]]; then
    log "reattach ok adapter=$ADAPTER"
else
    log "reattach failed status=$status adapter=$ADAPTER"
fi

printf '{}\n'
