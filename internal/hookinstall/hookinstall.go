package hookinstall

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	AgentBoth   = "both"
	AgentClaude = "claude"
	AgentCodex  = "codex"

	DefaultTimeout = 10 * time.Second
)

const SessionStartScript = `#!/usr/bin/env bash
# SessionStart hook wrapper for amq-keepalive.
#
# Agent startup always continues. Wake attachment, readiness verification, and
# the optional cmux notification all share one bounded deadline. A failed wake
# is surfaced to the TUI while queued AMQ messages remain untouched.

set -u

BIN="${AMQ_KEEPALIVE_BIN:-amq-keepalive}"
ADAPTER="${AMQ_KEEPALIVE_ADAPTER:-}"
TARGET="${AMQ_KEEPALIVE_TARGET:-}"
REGISTRY="${AMQ_KEEPALIVE_REGISTRY:-}"
AMQ_BIN="${AMQ_KEEPALIVE_AMQ:-amq}"
SELF_BIN="${AMQ_KEEPALIVE_SELF:-$BIN}"
CMUX_BIN="${AMQ_KEEPALIVE_CMUX:-${CMUX_BUNDLED_CLI_PATH:-cmux}}"
ROOT="${AMQ_KEEPALIVE_ROOT:-${AM_ROOT:-}}"
BASE_ROOT="${AMQ_KEEPALIVE_BASE_ROOT:-${AM_BASE_ROOT:-}}"
SESSION_NAME="${AMQ_KEEPALIVE_SESSION:-${AM_SESSION:-}}"
ME="${AMQ_KEEPALIVE_ME:-${AM_ME:-}}"
BASELINE_FILE="${AMQ_WAKE_BASELINE_FILE:-}"
BASELINE_ERROR="${AMQ_WAKE_BASELINE_ERROR:-}"
LOG_PATH="${AMQ_KEEPALIVE_LOG:-$HOME/.amq-keepalive/session-start.log}"
DEFAULT_TIMEOUT_SECONDS="${AMQ_KEEPALIVE_DEFAULT_TIMEOUT_SECONDS:-10}"
TIMEOUT_SECONDS="${AMQ_KEEPALIVE_TIMEOUT_SECONDS:-$DEFAULT_TIMEOUT_SECONDS}"
STDIN_TIMEOUT_SECONDS="${AMQ_KEEPALIVE_STDIN_TIMEOUT_SECONDS:-1}"
WAKE_TIMEOUT_MILLISECONDS="${AMQ_KEEPALIVE_WAKE_TIMEOUT_MILLISECONDS:-}"
RUN_TIMED_OUT=0

if [[ -z "$ADAPTER" ]]; then
    if [[ -n "${CMUX_SURFACE_ID:-}" ]]; then
        ADAPTER="cmux"
    else
        ADAPTER="ghostty"
    fi
fi
if [[ "$ADAPTER" == "cmux" && -z "$TARGET" && -n "${CMUX_SURFACE_ID:-}" ]]; then
    TARGET="cmux:surface:${CMUX_SURFACE_ID}"
fi
if [[ -z "$SESSION_NAME" && -n "$ROOT" ]]; then
    SESSION_NAME="${ROOT##*/}"
fi

if [[ "${AMQ_KEEPALIVE_DISABLED:-0}" == "1" ]]; then
    printf '{}\n'
    exit 0
fi

mkdir -p "$(dirname "$LOG_PATH")" 2>/dev/null || true

log() {
    printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOG_PATH" 2>/dev/null || true
}

if ! [[ "$DEFAULT_TIMEOUT_SECONDS" =~ ^[0-9]+$ && "$DEFAULT_TIMEOUT_SECONDS" -gt 0 ]]; then
    DEFAULT_TIMEOUT_SECONDS=10
fi
if ! [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ && "$TIMEOUT_SECONDS" -gt 0 ]]; then
    log "invalid timeout ${TIMEOUT_SECONDS}; using ${DEFAULT_TIMEOUT_SECONDS}s"
    TIMEOUT_SECONDS="$DEFAULT_TIMEOUT_SECONDS"
fi
if ! [[ "$STDIN_TIMEOUT_SECONDS" =~ ^[0-9]+$ && "$STDIN_TIMEOUT_SECONDS" -gt 0 ]]; then
    STDIN_TIMEOUT_SECONDS=1
fi

# Bash's SECONDS counter starts at assignment time and avoids the nearly-one-
# second phase loss of repeated date +%s calls. Reserved stdin and inner-work
# budgets fit TIMEOUT_SECONDS; only the combined process wrapper gets the
# documented one-second scheduler grace below the host hook's hard timeout.
SECONDS=0

remaining_seconds() {
	local remaining
	remaining=$((TIMEOUT_SECONDS - SECONDS))
    if [[ "$remaining" -lt 0 ]]; then
        remaining=0
    fi
    printf '%s' "$remaining"
}

run_bounded() {
    local seconds="$1"
    shift
    RUN_TIMED_OUT=0
    if [[ "$seconds" -le 0 ]]; then
        RUN_TIMED_OUT=1
        return 124
    fi

    local marker
    marker="$(mktemp "${TMPDIR:-/tmp}/amq-keepalive-timeout.XXXXXX")" || return 125
    printf 'pending\n' > "$marker"
	"$@" &
	local command_pid=$!
	(
		# Use elapsed time so scheduler-delayed polls cannot extend the deadline.
		SECONDS=0
		while [[ "$SECONDS" -lt "$seconds" ]] && kill -0 "$command_pid" 2>/dev/null; do
			sleep 0.05
		done
		if kill -0 "$command_pid" 2>/dev/null; then
            printf 'timeout\n' > "$marker" 2>/dev/null || true
            pkill -TERM -P "$command_pid" 2>/dev/null || true
            kill -TERM "$command_pid" 2>/dev/null || true
            sleep 0.1
            pkill -KILL -P "$command_pid" 2>/dev/null || true
            kill -KILL "$command_pid" 2>/dev/null || true
        fi
    ) >/dev/null 2>&1 &
    local watchdog_pid=$!

    wait "$command_pid" 2>/dev/null
    local status=$?
    pkill -TERM -P "$watchdog_pid" 2>/dev/null || true
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true
    if [[ "$(cat "$marker" 2>/dev/null || true)" == "timeout" ]]; then
        RUN_TIMED_OUT=1
        rm -f "$marker" 2>/dev/null || true
        return 124
    fi
    rm -f "$marker" 2>/dev/null || true
    return "$status"
}

emit_warning() {
    printf '%s\n' '{"systemMessage":"AMQ wake unavailable; messages remain queued.","hookSpecificOutput":{"hookEventName":"SessionStart","additionalContext":"AMQ wake is unavailable for this session. Messages remain queued; run amq doctor --ops after startup."}}'
}

notify_cmux() {
    local surface="${CMUX_SURFACE_ID:-}"
    if [[ -z "$surface" && "$TARGET" == cmux:surface:* ]]; then
        surface="${TARGET#cmux:surface:}"
    fi
    if [[ "$ADAPTER" != "cmux" || -z "$surface" ]]; then
        return 0
    fi
    if ! command -v "$CMUX_BIN" >/dev/null 2>&1; then
        log "cmux warning skipped: binary not found: $CMUX_BIN"
        return 0
    fi
    local remaining
    remaining="$(remaining_seconds)"
    if [[ "$remaining" -le 0 ]]; then
        log "cmux warning skipped: deadline exhausted"
        return 0
    fi
    if ! run_bounded "$remaining" "$CMUX_BIN" notify --surface "$surface" --title "AMQ wake unavailable" --body "Messages remain queued" >> "$LOG_PATH" 2>&1; then
        log "cmux warning notification failed"
    fi
}

fail_visible() {
    log "$1"
    notify_cmux || true
    emit_warning
    exit 0
}

if [[ -z "$BASELINE_FILE" && -z "$BASELINE_ERROR" && -z "$ROOT" && -z "$ME" ]]; then
    printf '{}\n'
    exit 0
fi
if [[ -n "$BASELINE_ERROR" ]]; then
    fail_visible "wake unavailable: deferred baseline capture failed code=$BASELINE_ERROR"
fi
if [[ -z "$BASELINE_FILE" ]]; then
    fail_visible "wake unavailable: AMQ_WAKE_BASELINE_FILE is missing"
fi
if [[ -z "$ROOT" || -z "$SESSION_NAME" || -z "$ME" ]]; then
    fail_visible "wake unavailable: exact AMQ root/session/agent identity is missing"
fi
if ! command -v "$BIN" >/dev/null 2>&1; then
    fail_visible "wake unavailable: amq-keepalive binary not found: $BIN"
fi
if ! command -v "$AMQ_BIN" >/dev/null 2>&1; then
    fail_visible "wake unavailable: amq binary not found: $AMQ_BIN"
fi
if ! command -v jq >/dev/null 2>&1; then
    fail_visible "wake unavailable: jq is required for notifier verification"
fi

max_stdin_budget=$((TIMEOUT_SECONDS - 2))
if [[ "$max_stdin_budget" -lt 0 ]]; then
	max_stdin_budget=0
fi
stdin_budget="$STDIN_TIMEOUT_SECONDS"
if [[ "$stdin_budget" -gt "$max_stdin_budget" ]]; then
	stdin_budget="$max_stdin_budget"
fi
work_budget=$((TIMEOUT_SECONDS - stdin_budget))
INPUT=""
if [[ "$stdin_budget" -gt 0 ]]; then
	IFS= read -r -t "$stdin_budget" INPUT || true
fi

CWD=""
if [[ -n "$INPUT" ]]; then
    CWD="$(printf '%s' "$INPUT" | jq -r '.cwd // .workdir // .working_directory // empty' 2>/dev/null || true)"
fi
if [[ -n "$CWD" && -d "$CWD" ]]; then
    cd "$CWD" 2>/dev/null || true
fi

work_milliseconds=$((work_budget * 1000))
default_wake_timeout_milliseconds=$((work_milliseconds - 500))
if [[ "$default_wake_timeout_milliseconds" -le 0 ]]; then
	default_wake_timeout_milliseconds=$((work_milliseconds / 2))
fi
if [[ "$default_wake_timeout_milliseconds" -le 0 ]]; then
    default_wake_timeout_milliseconds=100
fi
if ! [[ "$WAKE_TIMEOUT_MILLISECONDS" =~ ^[0-9]+$ && "$WAKE_TIMEOUT_MILLISECONDS" -gt 0 ]]; then
    [[ -n "$WAKE_TIMEOUT_MILLISECONDS" ]] && log "invalid wake timeout ${WAKE_TIMEOUT_MILLISECONDS}ms; using ${default_wake_timeout_milliseconds}ms"
    WAKE_TIMEOUT_MILLISECONDS="$default_wake_timeout_milliseconds"
fi
if [[ "$WAKE_TIMEOUT_MILLISECONDS" -ge "$work_milliseconds" ]]; then
	clamped_wake_timeout_milliseconds=$((work_milliseconds - 500))
    if [[ "$clamped_wake_timeout_milliseconds" -le 0 ]]; then
        clamped_wake_timeout_milliseconds=100
    fi
	log "wake timeout ${WAKE_TIMEOUT_MILLISECONDS}ms must be shorter than work budget ${work_milliseconds}ms; using ${clamped_wake_timeout_milliseconds}ms"
    WAKE_TIMEOUT_MILLISECONDS="$clamped_wake_timeout_milliseconds"
fi

args=(reattach --adapter "$ADAPTER" --amq "$AMQ_BIN" --wake-ready-timeout "${WAKE_TIMEOUT_MILLISECONDS}ms" --baseline-file "$BASELINE_FILE")
[[ -n "${AMQ_KEEPALIVE_SELF:-}" ]] && args+=(--self "$SELF_BIN")
[[ -n "$TARGET" ]] && args+=(--target "$TARGET")
[[ -n "$REGISTRY" ]] && args+=(--registry "$REGISTRY")
[[ -n "$ROOT" ]] && args+=(--root "$ROOT")
[[ -n "$BASE_ROOT" ]] && args+=(--base-root "$BASE_ROOT")
[[ -n "$SESSION_NAME" ]] && args+=(--session "$SESSION_NAME")
[[ -n "$ME" ]] && args+=(--me "$ME")
[[ "${AMQ_KEEPALIVE_NO_START:-0}" == "1" ]] && args+=(--no-start)

who_output_path="$(mktemp "${TMPDIR:-/tmp}/amq-keepalive-who.XXXXXX")" || fail_visible "wake unavailable: could not create verifier output"
work_result_path="$(mktemp "${TMPDIR:-/tmp}/amq-keepalive-result.XXXXXX")" || {
	rm -f "$who_output_path" 2>/dev/null || true
	fail_visible "wake unavailable: could not create work result"
}

# Invoked indirectly by run_bounded via its argv.
# shellcheck disable=SC2329
reattach_and_verify() {
	"$BIN" "${args[@]}" >> "$LOG_PATH" 2>&1
	local reattach_status=$?
	if [[ "$reattach_status" -ne 0 ]]; then
		printf 'reattach:%s\n' "$reattach_status" > "$work_result_path"
		return 20
	fi
	if ! "$AMQ_BIN" who --root "$ROOT" --json > "$who_output_path" 2>> "$LOG_PATH"; then
		printf 'who_failed\n' > "$work_result_path"
		return 21
	fi
	local who_output
	who_output="$(cat "$who_output_path" 2>/dev/null || true)"
	if ! printf '%s' "$who_output" | jq -e --arg session "$SESSION_NAME" --arg me "$ME" 'any(.[]; .name == $session and any(.agents[]; .handle == $me and .presence_source == "notifier_live"))' >/dev/null 2>&1; then
		printf 'ack_missing\n' > "$work_result_path"
		return 22
	fi
	printf 'ok\n' > "$work_result_path"
	return 0
}

# Process creation and scheduler latency are outside AMQ's inner readiness
# budget. Keep one fixed second of watchdog grace; the installed host hook has
# a further five-second hard timeout, so the wrapper remains bounded.
work_watchdog_budget=$((work_budget + 1))
run_bounded "$work_watchdog_budget" reattach_and_verify
work_status=$?
work_result="$(cat "$work_result_path" 2>/dev/null || true)"
rm -f "$who_output_path" "$work_result_path" 2>/dev/null || true
if [[ "$work_status" -ne 0 ]]; then
	if [[ "$RUN_TIMED_OUT" -eq 1 ]]; then
		fail_visible "wake unavailable: reattach and notifier verification exceeded the shared deadline"
	fi
	case "$work_result" in
		reattach:*)
			reattach_status="${work_result#reattach:}"
			fail_visible "wake unavailable: reattach failed status=$reattach_status adapter=$ADAPTER target=${TARGET:-auto}"
			;;
		who_failed)
			fail_visible "wake unavailable: amq who verification failed"
			;;
		ack_missing)
			fail_visible "wake unavailable: exact notifier_live acknowledgement missing session=$SESSION_NAME agent=$ME"
			;;
		*)
			fail_visible "wake unavailable: reattach and notifier verification failed status=$work_status"
			;;
	esac
fi

log "reattach verified adapter=$ADAPTER target=${TARGET:-auto} session=$SESSION_NAME agent=$ME"
printf '{}\n'
exit 0
`

type Options struct {
	Agent        string
	ScriptPath   string
	BinaryPath   string
	ClaudeConfig string
	CodexConfig  string
	Timeout      time.Duration
	DryRun       bool
}

type FileResult struct {
	Path    string `json:"path"`
	Changed bool   `json:"changed"`
	Backup  string `json:"backup,omitempty"`
}

type Result struct {
	Agent              string                 `json:"agent"`
	Script             FileResult             `json:"script"`
	Configs            map[string]FileResult  `json:"configs"`
	Commands           map[string]string      `json:"commands"`
	Snippets           map[string]interface{} `json:"snippets"`
	TimeoutSeconds     int                    `json:"timeout_seconds"`
	HookTimeoutSeconds int                    `json:"hook_timeout_seconds"`
	DryRun             bool                   `json:"dry_run"`
}

func DefaultScriptPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".amq-keepalive", "hooks", "amq-keepalive-session-start.sh"), nil
}

func DefaultClaudeConfig() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".claude", "settings.json"), nil
}

func DefaultCodexConfig() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".codex", "hooks.json"), nil
}

func Install(opts Options) (Result, error) {
	normalized, err := NormalizeOptions(opts)
	if err != nil {
		return Result{}, err
	}
	command := buildHookCommand(normalized.BinaryPath, normalized.ScriptPath, normalized.Timeout)
	hookTimeoutSeconds := int(normalized.Timeout.Seconds()) + 5
	result := Result{
		Agent:              normalized.Agent,
		Configs:            map[string]FileResult{},
		Commands:           map[string]string{},
		Snippets:           map[string]interface{}{},
		TimeoutSeconds:     int(normalized.Timeout.Seconds()),
		HookTimeoutSeconds: hookTimeoutSeconds,
		DryRun:             normalized.DryRun,
	}

	scriptResult := FileResult{Path: normalized.ScriptPath}
	if !normalized.DryRun {
		changed, err := writeExecutableIfChanged(normalized.ScriptPath, []byte(SessionStartScript))
		if err != nil {
			return Result{}, err
		}
		scriptResult.Changed = changed
	}
	result.Script = scriptResult

	if normalized.Agent == AgentClaude || normalized.Agent == AgentBoth {
		snippet := claudeSessionStartEntry(command, hookTimeoutSeconds)
		result.Commands[AgentClaude] = command
		result.Snippets[AgentClaude] = snippet
		if !normalized.DryRun {
			fileResult, err := installClaudeHook(normalized.ClaudeConfig, command, hookTimeoutSeconds)
			if err != nil {
				return Result{}, err
			}
			result.Configs[AgentClaude] = fileResult
		}
	}
	if normalized.Agent == AgentCodex || normalized.Agent == AgentBoth {
		snippet := codexSessionStartEntry(command, hookTimeoutSeconds)
		result.Commands[AgentCodex] = command
		result.Snippets[AgentCodex] = snippet
		if !normalized.DryRun {
			fileResult, err := installCodexHook(normalized.CodexConfig, command, hookTimeoutSeconds)
			if err != nil {
				return Result{}, err
			}
			result.Configs[AgentCodex] = fileResult
		}
	}
	return result, nil
}

func NormalizeOptions(opts Options) (Options, error) {
	switch opts.Agent {
	case "", AgentBoth:
		opts.Agent = AgentBoth
	case AgentClaude, AgentCodex:
	default:
		return Options{}, fmt.Errorf("--agent must be one of %s, %s, %s", AgentClaude, AgentCodex, AgentBoth)
	}
	if opts.ScriptPath == "" {
		path, err := DefaultScriptPath()
		if err != nil {
			return Options{}, err
		}
		opts.ScriptPath = path
	}
	scriptPath, err := absPath(opts.ScriptPath)
	if err != nil {
		return Options{}, fmt.Errorf("resolve script path: %w", err)
	}
	opts.ScriptPath = scriptPath

	if opts.BinaryPath == "" {
		return Options{}, errors.New("binary path is required")
	}
	binaryPath, err := resolveExecutable(opts.BinaryPath)
	if err != nil {
		return Options{}, fmt.Errorf("resolve binary path: %w", err)
	}
	opts.BinaryPath = binaryPath

	if opts.ClaudeConfig == "" {
		path, err := DefaultClaudeConfig()
		if err != nil {
			return Options{}, err
		}
		opts.ClaudeConfig = path
	}
	opts.ClaudeConfig, err = absPath(opts.ClaudeConfig)
	if err != nil {
		return Options{}, fmt.Errorf("resolve Claude config: %w", err)
	}

	if opts.CodexConfig == "" {
		path, err := DefaultCodexConfig()
		if err != nil {
			return Options{}, err
		}
		opts.CodexConfig = path
	}
	opts.CodexConfig, err = absPath(opts.CodexConfig)
	if err != nil {
		return Options{}, fmt.Errorf("resolve Codex config: %w", err)
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}
	if opts.Timeout < time.Second {
		return Options{}, errors.New("--timeout must be at least 1s")
	}
	return opts, nil
}

func installClaudeHook(path, command string, hookTimeoutSeconds int) (FileResult, error) {
	doc, err := loadJSONObject(path)
	if err != nil {
		return FileResult{}, err
	}
	hooks, err := objectField(doc, "hooks")
	if err != nil {
		return FileResult{}, err
	}
	sessionStart, err := arrayField(hooks, "SessionStart")
	if err != nil {
		return FileResult{}, err
	}
	changed := false
	if !claudeHasCommand(sessionStart, command) {
		sessionStart = append(sessionStart, claudeSessionStartEntry(command, hookTimeoutSeconds))
		hooks["SessionStart"] = sessionStart
		doc["hooks"] = hooks
		changed = true
	}
	return saveJSONIfChanged(path, doc, changed)
}

func installCodexHook(path, command string, hookTimeoutSeconds int) (FileResult, error) {
	doc, err := loadJSONObject(path)
	if err != nil {
		return FileResult{}, err
	}
	hooks, err := objectField(doc, "hooks")
	if err != nil {
		return FileResult{}, err
	}
	sessionStart, err := arrayField(hooks, "SessionStart")
	if err != nil {
		return FileResult{}, err
	}
	changed := false
	if !codexHasCommand(sessionStart, command) {
		hook := codexHook(command, hookTimeoutSeconds)
		if len(sessionStart) == 0 {
			sessionStart = append(sessionStart, map[string]interface{}{"hooks": []interface{}{hook}})
		} else {
			first, ok := sessionStart[0].(map[string]interface{})
			if !ok {
				sessionStart = append(sessionStart, map[string]interface{}{"hooks": []interface{}{hook}})
				hooks["SessionStart"] = sessionStart
				doc["hooks"] = hooks
				return saveJSONIfChanged(path, doc, true)
			}
			hookList := interfaceArray(first["hooks"])
			hookList = append(hookList, hook)
			first["hooks"] = hookList
		}
		hooks["SessionStart"] = sessionStart
		doc["hooks"] = hooks
		changed = true
	}
	return saveJSONIfChanged(path, doc, changed)
}

func claudeSessionStartEntry(command string, hookTimeoutSeconds int) map[string]interface{} {
	return map[string]interface{}{
		"matcher": "*",
		"hooks": []interface{}{
			map[string]interface{}{
				"type":          "command",
				"command":       command,
				"timeout":       hookTimeoutSeconds,
				"statusMessage": "Reattaching AMQ wake...",
			},
		},
	}
}

func codexSessionStartEntry(command string, hookTimeoutSeconds int) map[string]interface{} {
	return map[string]interface{}{
		"hooks": []interface{}{
			codexHook(command, hookTimeoutSeconds),
		},
	}
}

func codexHook(command string, hookTimeoutSeconds int) map[string]interface{} {
	return map[string]interface{}{
		"type":    "command",
		"command": command,
		"timeout": hookTimeoutSeconds * 1000,
	}
}

func claudeHasCommand(entries []interface{}, command string) bool {
	for _, entry := range entries {
		obj, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		for _, hook := range interfaceArray(obj["hooks"]) {
			hookObj, ok := hook.(map[string]interface{})
			if ok && strings.TrimSpace(fmt.Sprint(hookObj["command"])) == command {
				return true
			}
		}
	}
	return false
}

func codexHasCommand(entries []interface{}, command string) bool {
	return claudeHasCommand(entries, command)
}

func loadJSONObject(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]interface{}{}, nil
		}
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return map[string]interface{}{}, nil
	}
	var doc map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if doc == nil {
		doc = map[string]interface{}{}
	}
	return doc, nil
}

func saveJSONIfChanged(path string, doc map[string]interface{}, changed bool) (FileResult, error) {
	result := FileResult{Path: path, Changed: changed}
	if !changed {
		return result, nil
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return FileResult{}, err
	}
	data = append(data, '\n')
	backup, err := backupIfExists(path)
	if err != nil {
		return FileResult{}, err
	}
	if err := writeFileAtomic(path, data, 0o600); err != nil {
		return FileResult{}, err
	}
	result.Backup = backup
	return result, nil
}

func backupIfExists(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	backup := fmt.Sprintf("%s.bak-%s", path, time.Now().UTC().Format("20060102T150405Z"))
	if err := os.WriteFile(backup, data, 0o600); err != nil {
		return "", err
	}
	return backup, nil
}

func writeExecutableIfChanged(path string, data []byte) (bool, error) {
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, data) {
		return false, os.Chmod(path, 0o755)
	} else if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	return true, writeFileAtomic(path, data, 0o755)
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".amq-keepalive-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

func objectField(doc map[string]interface{}, key string) (map[string]interface{}, error) {
	if raw, ok := doc[key]; ok {
		if obj, ok := raw.(map[string]interface{}); ok {
			return obj, nil
		}
		return nil, fmt.Errorf("%q must be a JSON object", key)
	}
	obj := map[string]interface{}{}
	doc[key] = obj
	return obj, nil
}

func arrayField(doc map[string]interface{}, key string) ([]interface{}, error) {
	if raw, ok := doc[key]; ok {
		values, ok := raw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("%q must be a JSON array", key)
		}
		return values, nil
	}
	values := []interface{}{}
	doc[key] = values
	return values, nil
}

func interfaceArray(raw interface{}) []interface{} {
	if values, ok := raw.([]interface{}); ok {
		return values
	}
	return []interface{}{}
}

func buildHookCommand(binaryPath, scriptPath string, timeout time.Duration) string {
	return fmt.Sprintf("AMQ_KEEPALIVE_BIN=%s AMQ_KEEPALIVE_TIMEOUT_SECONDS=%s %s",
		shellQuote(binaryPath),
		shellQuote(fmt.Sprintf("%d", int(timeout.Seconds()))),
		shellQuote(scriptPath),
	)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func resolveExecutable(path string) (string, error) {
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	resolved, err := exec.LookPath(path)
	if err != nil {
		return "", err
	}
	return absPath(resolved)
}

func absPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	return filepath.Abs(path)
}
