package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
)

const testWakeOwner = `{"pid":4242,"process_start":"owner-start","boot_id":"boot-1"}`

func TestMain(m *testing.M) {
	if err := os.Setenv(amq.WakeOwnerEnvironment, testWakeOwner); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestHelpWritesUsageToStdoutAndExitsZero(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), []string{"--help"})
	if code != 0 {
		t.Fatalf("help code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "usage: amq-keepalive") {
		t.Fatalf("stdout does not contain usage: %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	for _, removed := range []string{"gc", "retire-session"} {
		if strings.Contains(stdout.String(), removed) {
			t.Fatalf("help still advertises removed command %q: %s", removed, stdout.String())
		}
	}
}

func TestRemovedRetirementCommandsAndFlagAreRejected(t *testing.T) {
	for _, command := range []string{"gc", "retire-session"} {
		var stderr bytes.Buffer
		code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{command})
		if code != 2 || !strings.Contains(stderr.String(), "unknown command") {
			t.Fatalf("%s code=%d stderr=%q, want removed command", command, code, stderr.String())
		}
	}
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"reattach", "--retire-detached",
	})
	if code != 1 || !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("retire-detached code=%d stderr=%q, want removed flag", code, stderr.String())
	}
}

func TestReattachReplacesCurrentSessionAdapterEntry(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := filepath.Join(dir, "old-inbox.txt")
	newTarget := filepath.Join(dir, "new-inbox.txt")
	otherTarget := filepath.Join(dir, "other-inbox.txt")

	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", oldTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--no-start",
	)
	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", otherTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "claude",
		"--no-start",
	)

	runApp(t, "reattach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", newTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--no-start",
	)

	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Entries) != 2 {
		t.Fatalf("entries = %d, want 2: %#v", len(loaded.Entries), loaded.Entries)
	}
	targets := map[string]string{}
	for _, entry := range loaded.Entries {
		targets[entry.Agent] = entry.Target
	}
	if targets["codex"] != newTarget {
		t.Fatalf("codex target = %q, want %q", targets["codex"], newTarget)
	}
	if targets["claude"] != otherTarget {
		t.Fatalf("claude target = %q, want %q", targets["claude"], otherTarget)
	}
}

func TestAttachPersistsValidatedBaselineBinding(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	baseline := filepath.Join(dir, "wake-baseline.json")
	if err := os.WriteFile(baseline, []byte("manifest\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", filepath.Join(dir, "inbox.txt"),
		"--root", filepath.Join(dir, "mail", "collab"),
		"--base-root", filepath.Join(dir, "mail"),
		"--session", "collab",
		"--me", "codex",
		"--baseline-file", baseline,
		"--no-start",
	)
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.Entries) != 1 {
		t.Fatalf("registry entries=%#v err=%v", loaded.Entries, err)
	}
	digest, err := amq.BaselineDigest(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Entries[0].BaselineFile != baseline ||
		loaded.Entries[0].BaselineDigest != digest ||
		loaded.Entries[0].WakeOwner != testWakeOwner {
		t.Fatalf("persisted baseline binding = %+v", loaded.Entries[0])
	}
}

func TestAttachNoStartRefusesMissingWakeOwner(t *testing.T) {
	t.Setenv(amq.WakeOwnerEnvironment, "")
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"attach", "--registry", filepath.Join(t.TempDir(), "registry.json"),
		"--adapter", "file", "--target", filepath.Join(t.TempDir(), "inbox.txt"),
		"--root", "/tmp/amq-root", "--base-root", "/tmp", "--session", "amq-root",
		"--me", "codex", "--no-start",
	})
	if code != 1 || !strings.Contains(stderr.String(), "AMQ_WAKE_OWNER is required") {
		t.Fatalf("code=%d stderr=%q, want owner-bound refusal", code, stderr.String())
	}
}

func TestAttachIsIdempotentForSamePhysicalCmuxOwner(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\nprintf '%s\\n' '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"F901D722-6789-4BBB-9818-C4E97F20BEB3\",\"tty\":\"ttys101\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	args := []string{
		"attach", "--registry", registryPath, "--adapter", "cmux", "--target", target,
		"--root", "/tmp/idempotent", "--base-root", "/tmp", "--session", "idempotent", "--me", "codex", "--no-start",
	}
	runApp(t, args...)
	runApp(t, args...)
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != target {
		t.Fatalf("idempotent attach entries=%#v err=%v", loaded.Entries, err)
	}
}

func TestReattachRejectsDifferentSurfaceAliasOnOwnedPhysicalTTY(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	firstTarget := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	secondTarget := "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2"
	store := registry.New(registryPath)
	if _, err := store.Upsert(registry.Entry{Root: "/tmp/first", Agent: "codex", Adapter: "cmux", Target: firstTarget, WakeOwner: testWakeOwner}); err != nil {
		t.Fatalf("Upsert first owner: %v", err)
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\nprintf '%s\\n' '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"F901D722-6789-4BBB-9818-C4E97F20BEB3\",\"tty\":\"/dev/ttys011\"},{\"id\":\"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2\",\"tty\":\"ttys011\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"reattach", "--registry", registryPath, "--adapter", "cmux", "--target", secondTarget,
		"--root", "/tmp/second", "--base-root", "/tmp", "--session", "second", "--me", "claude", "--no-start",
	})
	if code != 1 || !strings.Contains(stderr.String(), "2 live surface aliases") {
		t.Fatalf("code=%d stderr=%s, want physical ownership refusal", code, stderr.String())
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != firstTarget {
		t.Fatalf("physical collision changed registry: entries=%#v err=%v", loaded.Entries, err)
	}
}

func TestConcurrentReattachClaimStartsOnlyWinningWake(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := filepath.Join(dir, "inbox.txt")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	calls := filepath.Join(dir, "amq-calls.log")
	t.Setenv("AMQ_KEEPALIVE_AMQ_CALLS", calls)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
printf 'wake\n' >> "$AMQ_KEEPALIVE_AMQ_CALLS"
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
printf ready > "$ready"
sleep 0.1
`), 0o700); err != nil {
		t.Fatalf("write fake amq: %v", err)
	}
	start := make(chan struct{})
	codes := make(chan int, 2)
	for index := 0; index < 2; index++ {
		index := index
		go func() {
			<-start
			codes <- (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}).Run(context.Background(), []string{
				"reattach", "--registry", registryPath, "--adapter", "file", "--target", target,
				"--root", fmt.Sprintf("/tmp/race-%d", index), "--base-root", "/tmp",
				"--session", fmt.Sprintf("race-%d", index), "--me", fmt.Sprintf("agent-%d", index),
				"--amq", fakeAMQ, "--self", "/bin/amq-keepalive",
			})
		}()
	}
	close(start)
	firstCode, secondCode := <-codes, <-codes
	if firstCode+secondCode != 1 {
		t.Fatalf("concurrent codes=(%d,%d), want one success and one ownership refusal", firstCode, secondCode)
	}
	data, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(data), "wake\n") != 1 {
		t.Fatalf("wake calls=%q err=%v, want exactly one start", data, err)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != target {
		t.Fatalf("winning registry entries=%#v err=%v", loaded.Entries, err)
	}
}

func TestReattachPreservesRegistryWhenWakeTargetCannotChange(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := filepath.Join(dir, "old-inbox.txt")
	newTarget := filepath.Join(dir, "new-inbox.txt")
	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", oldTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--no-start",
	)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte("#!/bin/sh\necho 'existing wake target differs' >&2\nexit 7\n"), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), []string{
		"reattach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", newTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--amq", fakeAMQ,
		"--wake-ready-timeout", "5s",
	})
	if code != 1 {
		t.Fatalf("reattach code = %d, want 1\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "amq wake exited before becoming ready") {
		t.Fatalf("stderr does not expose wake failure:\n%s", stderr.String())
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Target != oldTarget {
		t.Fatalf("entries = %#v, want old target restored", loaded.Entries)
	}
}

func TestReattachPersistsRecoverableReservationBeforeWakeReadiness(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := filepath.Join(dir, "old-inbox.txt")
	newTarget := filepath.Join(dir, "new-inbox.txt")
	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", oldTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--no-start",
	)
	startedPath := filepath.Join(dir, "wake-started")
	releasePath := filepath.Join(dir, "wake-release")
	t.Setenv("AMQ_KEEPALIVE_TEST_STARTED", startedPath)
	t.Setenv("AMQ_KEEPALIVE_TEST_RELEASE", releasePath)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
: > "$AMQ_KEEPALIVE_TEST_STARTED"
while [ ! -f "$AMQ_KEEPALIVE_TEST_RELEASE" ]; do sleep 0.01; done
exit 7
`), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	defer func() { _ = os.WriteFile(releasePath, []byte("release"), 0o600) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan int, 1)
	go func() {
		done <- (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}).Run(ctx, []string{
			"reattach",
			"--registry", registryPath,
			"--adapter", "file",
			"--target", newTarget,
			"--root", "/tmp/amq-root",
			"--base-root", "/tmp",
			"--session", "amq-root",
			"--me", "codex",
			"--amq", fakeAMQ,
			"--wake-ready-timeout", "5s",
		})
	}()
	waitForPath(t, startedPath, 2*time.Second)

	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatalf("Load(in-flight) error = %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Target != newTarget || loaded.Entries[0].State != registry.StateAttached {
		t.Fatalf("in-flight entries = %#v, want inactive candidate reservation", loaded.Entries)
	}
	if err := os.WriteFile(releasePath, []byte("release"), 0o600); err != nil {
		t.Fatalf("release fake AMQ: %v", err)
	}
	if code := <-done; code != 1 {
		t.Fatalf("reattach code = %d, want failure", code)
	}
	loaded, err = registry.New(registryPath).Load()
	if err != nil {
		t.Fatalf("Load(final) error = %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Target != oldTarget {
		t.Fatalf("final entries = %#v, want old target preserved", loaded.Entries)
	}
}

func TestReattachPersistsCandidateOnlyAfterWakeReady(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := filepath.Join(dir, "old-inbox.txt")
	newTarget := filepath.Join(dir, "new-inbox.txt")
	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", oldTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--no-start",
	)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
printf ready > "$ready"
`), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	runApp(t, "reattach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", newTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--amq", fakeAMQ,
		"--wake-ready-timeout", "2s",
	)
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Target != newTarget {
		t.Fatalf("entries = %#v, want ready candidate persisted", loaded.Entries)
	}
	if loaded.Entries[0].State != registry.StateActive || loaded.Entries[0].LastSupervisorDecision != "ensured" {
		t.Fatalf("candidate was not persisted active after readiness: %#v", loaded.Entries[0])
	}
}

func TestReattachCancellationPreservesReservationForLateReadyWake(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := filepath.Join(dir, "old-inbox.txt")
	newTarget := filepath.Join(dir, "new-inbox.txt")
	for _, path := range []string{oldTarget, newTarget} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write target %s: %v", path, err)
		}
	}
	runApp(t, "attach", "--registry", registryPath, "--adapter", "file", "--target", oldTarget,
		"--root", "/tmp/amq-root", "--base-root", "/tmp", "--session", "amq-root", "--me", "codex", "--no-start")
	started := filepath.Join(dir, "started")
	allowReady := filepath.Join(dir, "allow-ready")
	lateReady := filepath.Join(dir, "late-ready")
	release := filepath.Join(dir, "release")
	t.Setenv("AMQ_KEEPALIVE_STARTED", started)
	t.Setenv("AMQ_KEEPALIVE_ALLOW_READY", allowReady)
	t.Setenv("AMQ_KEEPALIVE_LATE_READY", lateReady)
	t.Setenv("AMQ_KEEPALIVE_RELEASE", release)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
: > "$AMQ_KEEPALIVE_STARTED"
while [ ! -f "$AMQ_KEEPALIVE_ALLOW_READY" ]; do sleep 0.01; done
printf ready > "$ready"
: > "$AMQ_KEEPALIVE_LATE_READY"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do sleep 0.01; done
`), 0o700); err != nil {
		t.Fatalf("write fake amq: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(ctx, []string{
			"reattach", "--registry", registryPath, "--adapter", "file", "--target", newTarget,
			"--root", "/tmp/amq-root", "--base-root", "/tmp", "--session", "amq-root", "--me", "codex",
			"--amq", fakeAMQ, "--self", "/bin/amq-keepalive", "--wake-ready-timeout", "5s",
		})
	}()
	waitForPath(t, started, 2*time.Second)
	cancel()
	if code := <-done; code != 1 || !strings.Contains(stderr.String(), "reservation was preserved") {
		t.Fatalf("code=%d stderr=%s, want uncertain readiness with preserved reservation", code, stderr.String())
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != newTarget || loaded.Entries[0].State != registry.StateAttached {
		t.Fatalf("late-ready reservation entries=%#v err=%v", loaded.Entries, err)
	}
	if err := os.WriteFile(allowReady, nil, 0o600); err != nil {
		t.Fatalf("allow late ready: %v", err)
	}
	waitForPath(t, lateReady, 2*time.Second)
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatalf("release late wake: %v", err)
	}

	wake := &appCountingWake{}
	if _, err := (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}).superviseOnce(
		context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second,
	); err != nil {
		t.Fatalf("supervise recoverable reservation: %v", err)
	}
	if len(wake.starts) != 1 {
		t.Fatalf("supervisor starts=%d, want reservation convergence", len(wake.starts))
	}
}

func TestNormalizeAMQPathsUsesAbsoluteBaseSessionRoot(t *testing.T) {
	root, base := normalizeAMQPaths(".agent-mail/team-upgrader_v3", "/Users/test/git/.agent-mail", "team-upgrader_v3")
	if root != "/Users/test/git/.agent-mail/team-upgrader_v3" {
		t.Fatalf("root = %q, want absolute session root", root)
	}
	if base != "/Users/test/git/.agent-mail" {
		t.Fatalf("base = %q, want absolute base root", base)
	}
}

func TestInstallHookCommandWritesRequestedConfig(t *testing.T) {
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "amq-keepalive")
	if err := os.WriteFile(binaryPath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	runApp(t, "install-hook",
		"--agent", "codex",
		"--script", filepath.Join(dir, "hook.sh"),
		"--bin", binaryPath,
		"--codex-config", filepath.Join(dir, "hooks.json"),
		"--timeout", "1s",
	)
	if _, err := os.Stat(filepath.Join(dir, "hook.sh")); err != nil {
		t.Fatalf("hook not installed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "hooks.json"))
	if err != nil {
		t.Fatalf("read hooks config: %v", err)
	}
	if !bytes.Contains(data, []byte("AMQ_KEEPALIVE_TIMEOUT_SECONDS='1'")) {
		t.Fatalf("hooks config missing timeout command:\n%s", data)
	}
}

type appFailingWake struct{}

func (appFailingWake) RepairWake(context.Context, string, string) (amq.WakeRepairResult, error) {
	return amq.WakeRepairResult{Status: "refused", Reason: "unverified wake lock; refusing repair"}, errors.New("exit status 1")
}

func (appFailingWake) StartWake(context.Context, amq.StartWakeRequest) error {
	return errors.New("unverified wake lock; refusing second injector")
}

func TestSuperviseWarnsOncePerPersistentFailureTransition(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	_, err := registry.New(registryPath).Upsert(registry.Entry{
		Root: "/tmp/amq-root", Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "inbox.txt"),
		WakeOwner: testWakeOwner,
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := App{Stdout: &stdout, Stderr: &stderr}
	if _, err := app.superviseOnce(context.Background(), registryPath, appFailingWake{}, "/bin/amq-keepalive", time.Second); err != nil {
		t.Fatalf("first superviseOnce() error = %v", err)
	}
	warning := stderr.String()
	for _, want := range []string{
		"action=start_failed",
		`root="/tmp/amq-root"`,
		`agent="codex"`,
		`adapter="file"`,
		`target="` + filepath.Join(dir, "inbox.txt") + `"`,
		"failure_count=1",
		"unverified wake lock",
	} {
		if !strings.Contains(warning, want) {
			t.Fatalf("warning missing %q:\n%s", want, warning)
		}
	}

	stdout.Reset()
	stderr.Reset()
	if _, err := app.superviseOnce(context.Background(), registryPath, appFailingWake{}, "/bin/amq-keepalive", time.Second); err != nil {
		t.Fatalf("second superviseOnce() error = %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("backoff recheck repeated warning:\n%s", stderr.String())
	}
}

type appCountingWake struct {
	starts []amq.StartWakeRequest
}

func (w *appCountingWake) StartWake(_ context.Context, req amq.StartWakeRequest) error {
	w.starts = append(w.starts, req)
	return nil
}

type appCancelingWake struct {
	cancel context.CancelFunc
	starts int
}

func (w *appCancelingWake) StartWake(ctx context.Context, _ amq.StartWakeRequest) error {
	w.starts++
	w.cancel()
	return ctx.Err()
}

func TestSuperviseCancellationStopsLaterStartsAndLeavesRegistryUnchanged(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	for index := 0; index < 2; index++ {
		target := filepath.Join(dir, fmt.Sprintf("target-%d", index))
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatalf("write target: %v", err)
		}
		if _, err := store.Upsert(registry.Entry{
			Root: fmt.Sprintf("/tmp/cancel-%d", index), Agent: "codex", Adapter: "file", Target: target, WakeOwner: testWakeOwner,
		}); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	before, err := store.Load()
	if err != nil {
		t.Fatalf("Load before: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	wake := &appCancelingWake{cancel: cancel}
	results, err := (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}).superviseOnce(
		ctx, registryPath, wake, "/bin/amq-keepalive", time.Second,
	)
	if err != nil || len(results) != 2 {
		t.Fatalf("results=%#v err=%v", results, err)
	}
	if wake.starts != 1 {
		t.Fatalf("wake starts=%d, want first only", wake.starts)
	}
	after, err := store.Load()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("registry mutated on cancellation: before=%#v after=%#v err=%v", before, after, err)
	}
}

func TestSuperviseBatchesCmuxInventoryAndDefersHealthyEntries(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	targets := []string{
		"cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3",
		"cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2",
	}
	for i, target := range targets {
		if _, err := store.Upsert(registry.Entry{Root: filepath.Join(dir, string(rune('a'+i))), Agent: "codex", Adapter: "cmux", Target: target, WakeOwner: testWakeOwner}); err != nil {
			t.Fatalf("Upsert(%s): %v", target, err)
		}
	}
	calls := filepath.Join(dir, "cmux-calls.log")
	t.Setenv("AMQ_KEEPALIVE_CMUX_CALLS", calls)
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$AMQ_KEEPALIVE_CMUX_CALLS"
printf '%s\n' '{"windows":[{"workspaces":[{"panes":[{"surfaces":[{"id":"F901D722-6789-4BBB-9818-C4E97F20BEB3","tty":"ttys101"},{"id":"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2","tty":"ttys102"}]}]}]}]}'
`), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	wake := &appCountingWake{}
	app := App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}
	results, err := app.superviseOnce(context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second)
	if err != nil || len(results) != 2 || len(wake.starts) != 2 {
		t.Fatalf("first pass results=%#v starts=%d err=%v", results, len(wake.starts), err)
	}
	results, err = app.superviseOnce(context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second)
	if err != nil || len(results) != 2 || len(wake.starts) != 2 {
		t.Fatalf("deferred pass results=%#v starts=%d err=%v", results, len(wake.starts), err)
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatalf("read cmux calls: %v", err)
	}
	if got := strings.Count(strings.TrimSpace(string(data)), "system.tree"); got != 1 {
		t.Fatalf("system.tree calls = %d, want one across two entries and one deferred pass:\n%s", got, data)
	}
}

func TestSuperviseFailsClosedWhenOneRegisteredSurfaceHasLiveTTYAlias(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	if _, err := store.Upsert(registry.Entry{
		Root: "/tmp/tty-owner", Agent: "codex", Adapter: "cmux",
		Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", WakeOwner: testWakeOwner, State: registry.StateActive,
	}); err != nil {
		t.Fatalf("Upsert registered owner: %v", err)
	}
	calls := filepath.Join(dir, "cmux-calls.log")
	t.Setenv("AMQ_KEEPALIVE_CMUX_CALLS", calls)
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$AMQ_KEEPALIVE_CMUX_CALLS"
printf '%s\n' '{"windows":[{"workspaces":[{"panes":[{"surfaces":[{"id":"F901D722-6789-4BBB-9818-C4E97F20BEB3","tty":"/dev/ttys011"},{"id":"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2","tty":"ttys011"}]}]}]}]}'
`), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	wake := &appCountingWake{}
	var stderr bytes.Buffer
	results, err := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).superviseOnce(
		context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second,
	)
	if err != nil || len(results) != 1 {
		t.Fatalf("supervise results=%#v err=%v", results, err)
	}
	if len(wake.starts) != 0 {
		t.Fatalf("physical collision touched AMQ %d times, want 0", len(wake.starts))
	}
	result := results[0]
	if result.Action != "backoff" || result.Error == nil || !strings.Contains(result.Error.Error(), "2 live surface aliases") {
		t.Fatalf("alias ambiguity result=%+v, want fail-closed backoff", result)
	}
	if !strings.Contains(stderr.String(), "inspect cmux aliases and existing wakes manually") {
		t.Fatalf("operator warning missing manual-action guidance:\n%s", stderr.String())
	}
	data, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(data), "system.tree") != 1 {
		t.Fatalf("system.tree calls=%q err=%v, want one shared inventory", data, err)
	}
}

func TestSuperviseFailsClosedOnNormalizedTargetOwnershipCollision(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	upper := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	lower := strings.ToLower(upper)
	entries := []registry.Entry{
		{ID: registry.EntryID("/tmp/one", "codex", "cmux", upper), Root: "/tmp/one", Agent: "codex", Adapter: "cmux", Target: upper, State: registry.StateActive},
		{ID: registry.EntryID("/tmp/two", "claude", "cmux", lower), Root: "/tmp/two", Agent: "claude", Adapter: "cmux", Target: lower, State: registry.StateActive},
	}
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatalf("Save legacy collision: %v", err)
	}
	wake := &appCountingWake{}
	results, err := (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}).superviseOnce(
		context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second,
	)
	if err != nil {
		t.Fatalf("superviseOnce: %v", err)
	}
	if len(wake.starts) != 0 {
		t.Fatalf("collision touched AMQ %d times, want 0", len(wake.starts))
	}
	for _, result := range results {
		if result.Action != "backoff" || result.Error == nil || !strings.Contains(result.Error.Error(), "ownership collision") {
			t.Fatalf("collision result = %+v, want fail-closed backoff", result)
		}
	}
}

func TestContinuousSuperviseDoesNotEmitPerPassJSON(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(ctx, []string{
		"supervise", "--registry", registryPath, "--interval", "5ms",
	})
	if code != 0 || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("code=%d ctx=%v stderr=%s", code, ctx.Err(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("continuous supervisor emitted per-pass stdout:\n%s", stdout.String())
	}
}

func runApp(t *testing.T, args ...string) {
	t.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), args)
	if code != 0 {
		t.Fatalf("Run(%v) = %d\nstdout:\n%s\nstderr:\n%s", args, code, stdout.String(), stderr.String())
	}
}

func waitForPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
