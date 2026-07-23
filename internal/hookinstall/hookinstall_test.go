package hookinstall

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestInstallBothWritesScriptAndMergesConfigs(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "hooks", "amq-keepalive-session-start.sh")
	binaryPath := writeExecutable(t, filepath.Join(dir, "amq-keepalive"))
	claudeConfig := filepath.Join(dir, "claude", "settings.json")
	codexConfig := filepath.Join(dir, "codex", "hooks.json")
	mustWrite(t, claudeConfig, []byte(`{"hooks":{"SessionStart":[{"matcher":"resume","hooks":[{"type":"command","command":"existing"}]}]}}`))
	mustWrite(t, codexConfig, []byte(`{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"existing","timeout":5000}]}]}}`))

	result, err := Install(Options{
		Agent:        AgentBoth,
		ScriptPath:   scriptPath,
		BinaryPath:   binaryPath,
		ClaudeConfig: claudeConfig,
		CodexConfig:  codexConfig,
		Timeout:      time.Second,
	})
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Script.Changed {
		t.Fatal("script changed = false, want true")
	}
	if result.Configs[AgentClaude].Backup == "" || result.Configs[AgentCodex].Backup == "" {
		t.Fatalf("expected config backups, got %#v", result.Configs)
	}
	info, err := os.Stat(scriptPath)
	if err != nil {
		t.Fatalf("stat script: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("script mode = %o, want 755", info.Mode().Perm())
	}
	scriptData, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read script: %v", err)
	}
	if string(scriptData) != SessionStartScript {
		t.Fatal("installed script does not match embedded script")
	}

	claude := readJSON(t, claudeConfig)
	if countCommand(claude, result.Commands[AgentClaude]) != 1 {
		t.Fatalf("Claude command not installed exactly once:\n%s", mustMarshal(t, claude))
	}
	if !strings.Contains(result.Commands[AgentClaude], "AMQ_KEEPALIVE_TIMEOUT_SECONDS='1'") {
		t.Fatalf("Claude command missing timeout env: %q", result.Commands[AgentClaude])
	}

	codex := readJSON(t, codexConfig)
	if countCommand(codex, result.Commands[AgentCodex]) != 1 {
		t.Fatalf("Codex command not installed exactly once:\n%s", mustMarshal(t, codex))
	}
	if !strings.Contains(mustMarshal(t, codex), `"timeout": 6`) {
		t.Fatalf("Codex hook timeout missing or wrong:\n%s", mustMarshal(t, codex))
	}
}

func TestInstallIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	binaryPath := writeExecutable(t, filepath.Join(dir, "amq-keepalive"))
	claudeConfig := filepath.Join(dir, "settings.json")

	opts := Options{
		Agent:        AgentClaude,
		ScriptPath:   filepath.Join(dir, "hook.sh"),
		BinaryPath:   binaryPath,
		ClaudeConfig: claudeConfig,
		Timeout:      time.Second,
	}
	first, err := Install(opts)
	if err != nil {
		t.Fatalf("first Install() error = %v", err)
	}
	second, err := Install(opts)
	if err != nil {
		t.Fatalf("second Install() error = %v", err)
	}
	if second.Configs[AgentClaude].Changed {
		t.Fatal("second config install changed file, want idempotent no-op")
	}
	doc := readJSON(t, claudeConfig)
	if countCommand(doc, first.Commands[AgentClaude]) != 1 {
		t.Fatalf("command count != 1 after repeat install:\n%s", mustMarshal(t, doc))
	}
}

func TestInstallRecognizesWrappedExistingCodexScript(t *testing.T) {
	dir := t.TempDir()
	binaryPath := writeExecutable(t, filepath.Join(dir, "amq-keepalive"))
	scriptPath := filepath.Join(dir, "hooks", "amq-keepalive-session-start.sh")
	codexConfig := filepath.Join(dir, "hooks.json")
	wrappedCommand := "HOME='/Users/test' AMQ_KEEPALIVE_AMQ='/opt/homebrew/bin/amq' " +
		"AMQ_KEEPALIVE_BIN=" + shellQuote(binaryPath) + " '/bin/bash' " + shellQuote(scriptPath)
	doc := map[string]interface{}{
		"hooks": map[string]interface{}{
			"SessionStart": []interface{}{
				map[string]interface{}{
					"hooks": []interface{}{
						map[string]interface{}{
							"type":    "command",
							"command": wrappedCommand,
							"timeout": 15,
						},
					},
				},
			},
		},
	}
	original := []byte(mustMarshal(t, doc) + "\n")
	mustWrite(t, codexConfig, original)

	result, err := Install(Options{
		Agent:       AgentCodex,
		ScriptPath:  scriptPath,
		BinaryPath:  binaryPath,
		CodexConfig: codexConfig,
		Timeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if result.Configs[AgentCodex].Changed {
		t.Fatal("wrapped existing Codex hook was rewritten or duplicated")
	}
	after, err := os.ReadFile(codexConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, original) {
		t.Fatalf("existing Codex config changed:\n%s", after)
	}
	installed := readJSON(t, codexConfig)
	if countCommand(installed, wrappedCommand) != 1 {
		t.Fatalf("wrapped hook count != 1:\n%s", mustMarshal(t, installed))
	}
}

func TestDryRunDoesNotWriteFiles(t *testing.T) {
	dir := t.TempDir()
	binaryPath := writeExecutable(t, filepath.Join(dir, "amq-keepalive"))
	scriptPath := filepath.Join(dir, "hook.sh")
	configPath := filepath.Join(dir, "settings.json")

	result, err := Install(Options{
		Agent:        AgentClaude,
		ScriptPath:   scriptPath,
		BinaryPath:   binaryPath,
		ClaudeConfig: configPath,
		Timeout:      time.Second,
		DryRun:       true,
	})
	if err != nil {
		t.Fatalf("Install(dry-run) error = %v", err)
	}
	if !result.DryRun || result.Commands[AgentClaude] == "" {
		t.Fatalf("bad dry-run result: %#v", result)
	}
	if _, err := os.Stat(scriptPath); !os.IsNotExist(err) {
		t.Fatalf("script stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config stat err = %v, want not exist", err)
	}
}

func TestEmbeddedScriptMatchesRepositoryHook(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "hooks", "amq-keepalive-session-start.sh"))
	if err != nil {
		t.Fatalf("read repository hook: %v", err)
	}
	if string(data) != SessionStartScript {
		t.Fatal("embedded SessionStart script drifted from hooks/amq-keepalive-session-start.sh")
	}
	info, err := os.Stat(filepath.Join("..", "..", "hooks", "amq-keepalive-session-start.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("repository hook mode = %o, want 0755", info.Mode().Perm())
	}
}

const sessionStartWarning = "{\"systemMessage\":\"AMQ wake unavailable; messages remain queued.\",\"hookSpecificOutput\":{\"hookEventName\":\"SessionStart\",\"additionalContext\":\"AMQ wake is unavailable for this session. Messages remain queued; run amq doctor --ops after startup.\"}}\n"

func TestSessionStartScriptUsesElapsedWatchdogDeadline(t *testing.T) {
	if !strings.Contains(SessionStartScript, "SECONDS=0\n\t\twhile [[ \"$SECONDS\" -lt \"$seconds\" ]]") {
		t.Fatal("session-start watchdog must use an elapsed deadline")
	}
	if strings.Contains(SessionStartScript, "watchdog_ticks") {
		t.Fatal("session-start watchdog must not extend deadlines by counting polls")
	}
}

func TestSessionStartScriptNormalizesInvalidTimeoutAndReturns(t *testing.T) {
	dir := t.TempDir()
	scriptPath := writeSessionStartScript(t, dir)
	binaryPath := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), "#!/bin/sh\nsleep 5\n")
	logPath := filepath.Join(dir, "session-start.log")

	cmd := exec.Command("bash", scriptPath)
	cmd.Env = append(sessionStartTestEnv(t, dir, "notifier_live"),
		"AMQ_KEEPALIVE_BIN="+binaryPath,
		"AMQ_KEEPALIVE_LOG="+logPath,
		"AMQ_KEEPALIVE_TIMEOUT_SECONDS=0",
		"AMQ_KEEPALIVE_DEFAULT_TIMEOUT_SECONDS=1",
		"AMQ_KEEPALIVE_STDIN_TIMEOUT_SECONDS=1",
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	if err := cmd.Run(); err != nil {
		t.Fatalf("hook run error = %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("hook took %s, want bounded return", elapsed)
	}
	if got := stdout.String(); got != sessionStartWarning {
		t.Fatalf("stdout = %q, want wake warning", got)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "invalid timeout 0; using 1s") {
		t.Fatalf("log missing invalid timeout normalization:\n%s", logText)
	}
	if !strings.Contains(logText, "reattach and notifier verification exceeded the shared deadline") {
		t.Fatalf("log missing timeout:\n%s", logText)
	}
}

func TestSessionStartScriptDoesNotBlockOnOpenStdin(t *testing.T) {
	dir := t.TempDir()
	scriptPath := writeSessionStartScript(t, dir)
	binaryPath := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), "#!/bin/sh\nexit 0\n")
	logPath := filepath.Join(dir, "session-start.log")

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", scriptPath)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() error = %v", err)
	}
	defer reader.Close()
	defer writer.Close()
	cmd.Stdin = reader
	tmpDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(sessionStartTestEnv(t, dir, "notifier_live"),
		"AMQ_KEEPALIVE_BIN="+binaryPath,
		"AMQ_KEEPALIVE_LOG="+logPath,
		"AMQ_KEEPALIVE_TIMEOUT_SECONDS=2",
		"AMQ_KEEPALIVE_STDIN_TIMEOUT_SECONDS=1",
		"TMPDIR="+tmpDir,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	if err := cmd.Run(); err != nil {
		t.Fatalf("hook run error = %v", err)
	}
	// The wrapper deadline is 3s (2s inner budget + 1s scheduler grace).
	// Allow only teardown/measurement jitter here; the separate source guard
	// rejects poll-count watchdogs, including the prior 3.56s overshoot bug.
	if elapsed := time.Since(start); elapsed > 3500*time.Millisecond {
		t.Fatalf("hook took %s, want stdin read bounded", elapsed)
	}
	if got := stdout.String(); got != "{}\n" {
		t.Fatalf("stdout = %q, want empty hook response", got)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("stderr = %q, want silent quick success", got)
	}
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("bounded hook leaked temp files: %v", entries)
	}
}

func TestSessionStartTimeoutDoesNotSignalDetachedWakeGrandchild(t *testing.T) {
	dir := t.TempDir()
	scriptPath := writeSessionStartScript(t, dir)
	childPath := writeExecutableBody(t, filepath.Join(dir, "wake-child"), "#!/bin/sh\nsleep 30\n")
	pidPath := filepath.Join(dir, "wake.pid")
	keepalivePath := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), `#!/bin/sh
"$AMQ_KEEPALIVE_TEST_WAKE_CHILD" &
printf '%s\n' "$!" > "$AMQ_KEEPALIVE_TEST_WAKE_PID"
sleep 30
`)
	cmd := exec.Command("bash", scriptPath)
	cmd.Stdin = strings.NewReader("{}\n")
	cmd.Env = append(sessionStartTestEnv(t, dir, "notifier_live"),
		"AMQ_KEEPALIVE_BIN="+keepalivePath,
		"AMQ_KEEPALIVE_TEST_WAKE_CHILD="+childPath,
		"AMQ_KEEPALIVE_TEST_WAKE_PID="+pidPath,
		"AMQ_KEEPALIVE_LOG="+filepath.Join(dir, "session-start.log"),
		"AMQ_KEEPALIVE_TIMEOUT_SECONDS=1",
	)
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != sessionStartWarning {
		t.Fatalf("timeout output = %q", output)
	}
	pidData, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("detached wake grandchild was signaled by hook timeout: %v", err)
	}
}

func TestSessionStartScriptAutoSelectsExactCmuxSurfaceAndLogsFailure(t *testing.T) {
	dir := t.TempDir()
	scriptPath := writeSessionStartScript(t, dir)
	argsPath := filepath.Join(dir, "args.log")
	ownerPath := filepath.Join(dir, "owner.log")
	binaryPath := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), `#!/bin/sh
printf '%s\n' "$@" > "$AMQ_KEEPALIVE_CAPTURE"
printf '%s' "$AMQ_WAKE_OWNER" > "$AMQ_KEEPALIVE_OWNER_CAPTURE"
echo 'existing wake target differs' >&2
exit 7
`)
	logPath := filepath.Join(dir, "session-start.log")
	cmuxArgsPath := filepath.Join(dir, "cmux-args.log")
	cmuxPath := writeExecutableBody(t, filepath.Join(dir, "cmux"), "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$AMQ_KEEPALIVE_CMUX_CAPTURE\"\n")

	cmd := exec.Command("bash", scriptPath)
	cmd.Stdin = strings.NewReader("{}\n")
	cmd.Env = append(withoutEnv(sessionStartTestEnv(t, dir, "notifier_live"),
		"AMQ_KEEPALIVE_ADAPTER",
		"AMQ_KEEPALIVE_TARGET",
		"CMUX_SURFACE_ID",
	),
		"AMQ_KEEPALIVE_BIN="+binaryPath,
		"AMQ_KEEPALIVE_CAPTURE="+argsPath,
		"AMQ_KEEPALIVE_OWNER_CAPTURE="+ownerPath,
		"AMQ_KEEPALIVE_LOG="+logPath,
		"AMQ_KEEPALIVE_TIMEOUT_SECONDS=2",
		"AMQ_KEEPALIVE_CMUX="+cmuxPath,
		"AMQ_KEEPALIVE_CMUX_CAPTURE="+cmuxArgsPath,
		"CMUX_SURFACE_ID=F901D722-6789-4BBB-9818-C4E97F20BEB3",
	)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("hook run error = %v", err)
	}
	if got := stdout.String(); got != sessionStartWarning {
		t.Fatalf("stdout = %q, want wake warning", got)
	}
	argsData, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	argsText := string(argsData)
	for _, want := range []string{
		"reattach\n",
		"--adapter\ncmux\n",
		"--wake-ready-timeout\n1500ms\n",
		"--target\ncmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3\n",
		"--baseline-file\n",
	} {
		if !strings.Contains(argsText, want) {
			t.Fatalf("args missing %q:\n%s", want, argsText)
		}
	}
	ownerData, err := os.ReadFile(ownerPath)
	if err != nil {
		t.Fatalf("read owner capture: %v", err)
	}
	if string(ownerData) != `{"pid":4242,"process_start":"owner-start","boot_id":"boot-1"}` {
		t.Fatalf("SessionStart owner = %q, want deferred owner", ownerData)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "existing wake target differs") ||
		!strings.Contains(logText, "reattach failed status=7 adapter=cmux target=cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3") {
		t.Fatalf("log does not expose target mismatch:\n%s", logText)
	}
	cmuxArgs, err := os.ReadFile(cmuxArgsPath)
	if err != nil {
		t.Fatal(err)
	}
	wantCmux := "notify\n--surface\nF901D722-6789-4BBB-9818-C4E97F20BEB3\n--title\nAMQ wake unavailable\n--body\nMessages remain queued\n"
	if string(cmuxArgs) != wantCmux {
		t.Fatalf("cmux notification args = %q, want %q", cmuxArgs, wantCmux)
	}
}

func TestSessionStartScriptFallsBackToGhosttyOutsideCmux(t *testing.T) {
	dir := t.TempDir()
	scriptPath := writeSessionStartScript(t, dir)
	argsPath := filepath.Join(dir, "args.log")
	binaryPath := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), `#!/bin/sh
printf '%s\n' "$@" > "$AMQ_KEEPALIVE_CAPTURE"
`)

	cmd := exec.Command("bash", scriptPath)
	cmd.Stdin = strings.NewReader("{}\n")
	cmd.Env = append(withoutEnv(sessionStartTestEnv(t, dir, "notifier_live"),
		"AMQ_KEEPALIVE_ADAPTER",
		"AMQ_KEEPALIVE_TARGET",
		"CMUX_SURFACE_ID",
	),
		"AMQ_KEEPALIVE_BIN="+binaryPath,
		"AMQ_KEEPALIVE_CAPTURE="+argsPath,
		"AMQ_KEEPALIVE_LOG="+filepath.Join(dir, "session-start.log"),
		"AMQ_KEEPALIVE_TIMEOUT_SECONDS=2",
	)
	if err := cmd.Run(); err != nil {
		t.Fatalf("hook run error = %v", err)
	}
	argsData, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	argsText := string(argsData)
	if !strings.Contains(argsText, "--adapter\nghostty\n") {
		t.Fatalf("args do not fall back to Ghostty:\n%s", argsText)
	}
	if strings.Contains(argsText, "--target\n") {
		t.Fatalf("fallback unexpectedly supplied a target:\n%s", argsText)
	}
	if !strings.Contains(argsText, "--baseline-file\n") {
		t.Fatalf("fallback omitted deferred baseline:\n%s", argsText)
	}
}

func TestSessionStartScriptClampsInnerWakeTimeoutBelowOuterWatchdog(t *testing.T) {
	dir := t.TempDir()
	scriptPath := writeSessionStartScript(t, dir)
	argsPath := filepath.Join(dir, "args.log")
	logPath := filepath.Join(dir, "session-start.log")
	binaryPath := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), `#!/bin/sh
printf '%s\n' "$@" > "$AMQ_KEEPALIVE_CAPTURE"
`)

	cmd := exec.Command("bash", scriptPath)
	cmd.Stdin = strings.NewReader("{}\n")
	cmd.Env = append(sessionStartTestEnv(t, dir, "notifier_live"),
		"AMQ_KEEPALIVE_BIN="+binaryPath,
		"AMQ_KEEPALIVE_CAPTURE="+argsPath,
		"AMQ_KEEPALIVE_LOG="+logPath,
		"AMQ_KEEPALIVE_TIMEOUT_SECONDS=2",
		"AMQ_KEEPALIVE_WAKE_TIMEOUT_MILLISECONDS=2000",
	)
	if err := cmd.Run(); err != nil {
		t.Fatalf("hook run error = %v", err)
	}
	argsData, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read args: %v", err)
	}
	if !strings.Contains(string(argsData), "--wake-ready-timeout\n1500ms\n") {
		t.Fatalf("inner timeout was not clamped below outer watchdog:\n%s", argsData)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(logData), "wake timeout 2000ms must be shorter than work budget 2000ms; using 1500ms") {
		t.Fatalf("clamp was not logged:\n%s", logData)
	}
}

func TestSessionStartScriptIgnoresOrdinaryNonAMQLaunch(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command("bash", writeSessionStartScript(t, dir))
	cmd.Stdin = strings.NewReader("{}\n")
	cmd.Env = append(sessionStartCleanEnv(), "AMQ_KEEPALIVE_LOG="+filepath.Join(dir, "session-start.log"))
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "{}\n" {
		t.Fatalf("ordinary launch output = %q, want quiet success", output)
	}
}

func TestSessionStartScriptWarnsForIntendedAMQLaunchMissingFloor(t *testing.T) {
	dir := t.TempDir()
	called := filepath.Join(dir, "called")
	binary := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), "#!/bin/sh\n: > \"$AMQ_KEEPALIVE_CALLED\"\n")
	cmd := exec.Command("bash", writeSessionStartScript(t, dir))
	cmd.Stdin = strings.NewReader("{}\n")
	cmd.Env = append(sessionStartCleanEnv(),
		"AM_ROOT="+filepath.Join(dir, "mail", "collab"),
		"AM_SESSION=collab",
		"AM_ME=codex",
		"AMQ_KEEPALIVE_BIN="+binary,
		"AMQ_KEEPALIVE_CALLED="+called,
		"AMQ_KEEPALIVE_LOG="+filepath.Join(dir, "session-start.log"),
	)
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != sessionStartWarning {
		t.Fatalf("missing-floor output = %q, want warning", output)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(output, &payload); err != nil {
		t.Fatalf("warning is not valid JSON: %v", err)
	}
	if _, err := os.Stat(called); !os.IsNotExist(err) {
		t.Fatalf("missing floor invoked keepalive: %v", err)
	}
}

func TestSessionStartScriptRejectsRecentActivityWithoutLiveNotifier(t *testing.T) {
	dir := t.TempDir()
	binary := writeExecutableBody(t, filepath.Join(dir, "amq-keepalive"), "#!/bin/sh\nexit 0\n")
	logPath := filepath.Join(dir, "session-start.log")
	cmd := exec.Command("bash", writeSessionStartScript(t, dir))
	cmd.Stdin = strings.NewReader("{}\n")
	cmd.Env = append(sessionStartTestEnv(t, dir, "recent_activity"),
		"AMQ_KEEPALIVE_BIN="+binary,
		"AMQ_KEEPALIVE_LOG="+logPath,
	)
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != sessionStartWarning {
		t.Fatalf("recent-only output = %q, want warning", output)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logData), "exact notifier_live acknowledgement missing session=collab agent=codex") {
		t.Fatalf("recent-only verifier failure missing from log:\n%s", logData)
	}
}

func writeExecutable(t *testing.T, path string) string {
	t.Helper()
	return writeExecutableBody(t, path, "#!/bin/sh\nexit 0\n")
}

func withoutEnv(env []string, keys ...string) []string {
	blocked := map[string]bool{}
	for _, key := range keys {
		blocked[key] = true
	}
	out := make([]string, 0, len(env))
	for _, item := range env {
		key, _, _ := strings.Cut(item, "=")
		if !blocked[key] {
			out = append(out, item)
		}
	}
	return out
}

func sessionStartCleanEnv() []string {
	return withoutEnv(os.Environ(),
		"AM_ROOT", "AM_BASE_ROOT", "AM_SESSION", "AM_ME",
		"AMQ_WAKE_BASELINE_FILE", "AMQ_WAKE_BASELINE_ERROR", "AMQ_WAKE_OWNER",
		"AMQ_KEEPALIVE_ROOT", "AMQ_KEEPALIVE_BASE_ROOT", "AMQ_KEEPALIVE_SESSION", "AMQ_KEEPALIVE_ME",
		"AMQ_KEEPALIVE_BIN", "AMQ_KEEPALIVE_AMQ", "AMQ_KEEPALIVE_ADAPTER", "AMQ_KEEPALIVE_TARGET",
		"AMQ_KEEPALIVE_CMUX", "AMQ_KEEPALIVE_LOG", "AMQ_KEEPALIVE_TIMEOUT_SECONDS",
		"AMQ_KEEPALIVE_DEFAULT_TIMEOUT_SECONDS", "AMQ_KEEPALIVE_STDIN_TIMEOUT_SECONDS",
		"AMQ_KEEPALIVE_WAKE_TIMEOUT_MILLISECONDS", "CMUX_SURFACE_ID", "CMUX_BUNDLED_CLI_PATH",
	)
}

func sessionStartTestEnv(t *testing.T, dir, presenceSource string) []string {
	t.Helper()
	root := filepath.Join(dir, "mail", "collab")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	baseline := filepath.Join(root, "wake-baseline.json")
	mustWrite(t, baseline, []byte("test baseline\n"))
	whoJSON := `[{"name":"collab","agents":[{"handle":"codex","presence_source":"` + presenceSource + `"}]}]`
	amqPath := writeExecutableBody(t, filepath.Join(dir, "amq-who"), "#!/bin/sh\nif [ \"$1\" = who ]; then\n  printf '%s\\n' '"+whoJSON+"'\n  exit 0\nfi\nexit 12\n")
	return append(sessionStartCleanEnv(),
		"AM_ROOT="+root,
		"AM_BASE_ROOT="+filepath.Dir(root),
		"AM_SESSION=collab",
		"AM_ME=codex",
		"AMQ_WAKE_BASELINE_FILE="+baseline,
		`AMQ_WAKE_OWNER={"pid":4242,"process_start":"owner-start","boot_id":"boot-1"}`,
		"AMQ_KEEPALIVE_AMQ="+amqPath,
	)
}

func writeSessionStartScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "hook.sh")
	return writeExecutableBody(t, path, SessionStartScript)
}

func writeExecutableBody(t *testing.T, path string, body string) string {
	t.Helper()
	mustWrite(t, path, []byte(body))
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("chmod executable: %v", err)
	}
	return path
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readJSON(t *testing.T, path string) map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var doc map[string]interface{}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal json: %v\n%s", err, data)
	}
	return doc
}

func countCommand(doc map[string]interface{}, command string) int {
	count := 0
	hooks, _ := doc["hooks"].(map[string]interface{})
	for _, entry := range interfaceArray(hooks["SessionStart"]) {
		entryObj, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		for _, hook := range interfaceArray(entryObj["hooks"]) {
			hookObj, ok := hook.(map[string]interface{})
			if ok && hookObj["command"] == command {
				count++
			}
		}
	}
	return count
}

func mustMarshal(t *testing.T, v interface{}) string {
	t.Helper()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}
