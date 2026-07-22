package amq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStartWakeWaitsForReadyFileAndPassesTarget(t *testing.T) {
	dir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '%s\n' "$@" > "$AMQ_KEEPALIVE_ARGS_LOG"
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then
    ready="$arg"
  fi
  previous="$arg"
done
if [ -z "$ready" ]; then
  exit 11
fi
printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$ready"
`)

	binding, err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		Timeout:   5 * time.Second,
		Owner:     testWakeOwner(),
	})
	if err != nil {
		t.Fatalf("StartWake() error = %v", err)
	}
	if !binding.Complete() {
		t.Fatalf("binding = %#v", binding)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	args := string(data)
	for _, want := range []string{
		"wake\n",
		"-root\n/tmp/amq-root\n",
		"-me\ncodex\n",
		"--baseline-existing\n",
		"-inject-via\n/tmp/amq-keepalive\n",
		"-inject-arg\ninject\n",
		"-inject-arg\nghostty\n",
		"-inject-arg\nghostty:terminal:abc\n",
		"--accept-existing-wake\n",
		"--require-owner\n",
		"-ready-file\n",
		"--result-file\n",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("args log missing %q:\n%s", want, args)
		}
	}
}

func TestStartWakePassesExactRegisteredBaseline(t *testing.T) {
	dir := t.TempDir()
	baseline := filepath.Join(dir, "wake-baseline.json")
	if err := os.WriteFile(baseline, []byte("manifest\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := BaselineDigest(baseline)
	if err != nil {
		t.Fatal(err)
	}
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '%s\n' "$@" > "$AMQ_KEEPALIVE_ARGS_LOG"
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$arg"; fi
  previous="$arg"
done
`)
	if _, err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "cmux", Target: "cmux:surface:abc", BaselineFile: baseline,
		BaselineDigest: digest, Timeout: 5 * time.Second, Owner: testWakeOwner(),
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatal(err)
	}
	args := string(data)
	if !strings.Contains(args, "--baseline-file\n"+baseline+"\n") ||
		!strings.Contains(args, "--replace-existing-baseline\n") ||
		strings.Contains(args, "--baseline-existing\n") {
		t.Fatalf("wake args do not carry exact baseline:\n%s", args)
	}
}

func TestStartWakeRejectsChangedOrUnsafeBaselineBeforeExec(t *testing.T) {
	dir := t.TempDir()
	baseline := filepath.Join(dir, "wake-baseline.json")
	if err := os.WriteFile(baseline, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := BaselineDigest(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(baseline, []byte("second\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = NewCLI(filepath.Join(dir, "must-not-run")).StartWake(context.Background(), StartWakeRequest{
		InjectVia: "/tmp/amq-keepalive", Adapter: "cmux", Target: "cmux:surface:abc",
		BaselineFile: baseline, BaselineDigest: digest, Owner: testWakeOwner(),
	})
	if err == nil || !strings.Contains(err.Error(), "digest changed") {
		t.Fatalf("changed baseline error = %v", err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(baseline, link); err != nil {
		t.Fatal(err)
	}
	if _, err := BaselineDigest(link); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink baseline error = %v", err)
	}
}

func TestStartWakeFailsWhenProcessExitsBeforeReady(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
exit 7
`)

	_, err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		Timeout:   5 * time.Second,
		Owner:     testWakeOwner(),
	})
	if err == nil {
		t.Fatal("StartWake() error = nil, want readiness failure")
	}
	if !strings.Contains(err.Error(), "amq wake exited before becoming ready") {
		t.Fatalf("error = %v, want readiness failure", err)
	}
}

func TestStartWakeKeepsSharedReadyDirectoryForPostReadyWork(t *testing.T) {
	dir := t.TempDir()
	readyPathLog := filepath.Join(dir, "ready-path.log")
	postReady := filepath.Join(dir, "post-ready")
	release := filepath.Join(dir, "release")
	t.Setenv("AMQ_KEEPALIVE_READY_PATH_LOG", readyPathLog)
	t.Setenv("AMQ_KEEPALIVE_POST_READY", postReady)
	t.Setenv("AMQ_KEEPALIVE_RELEASE", release)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
printf '%s' "$ready" > "$AMQ_KEEPALIVE_READY_PATH_LOG"
printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$ready"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do sleep 0.01; done
[ -d "${ready%/*}" ] || exit 12
: > "$AMQ_KEEPALIVE_POST_READY"
`)

	_, err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", Timeout: 5 * time.Second, Owner: testWakeOwner(),
	})
	if err != nil {
		t.Fatalf("StartWake() error = %v", err)
	}
	data, err := os.ReadFile(readyPathLog)
	if err != nil {
		t.Fatalf("read ready path log: %v", err)
	}
	readyPath := string(data)
	readyDir := filepath.Dir(readyPath)
	if info, err := os.Stat(readyDir); err != nil || !info.IsDir() {
		t.Fatalf("ready directory was removed before wake post-ready work: dir=%q err=%v", readyDir, err)
	}
	if _, err := os.Stat(readyPath); !os.IsNotExist(err) {
		t.Fatalf("acknowledged ready marker still exists: path=%q err=%v", readyPath, err)
	}
	if err := os.WriteFile(release, []byte("release"), 0o600); err != nil {
		t.Fatalf("release wake: %v", err)
	}
	waitForFile(t, postReady, 2*time.Second)
}

func TestStartWakeCancelAfterReadyDoesNotKillEstablishedWake(t *testing.T) {
	dir := t.TempDir()
	readyPathLog := filepath.Join(dir, "ready-path.log")
	check := filepath.Join(dir, "check")
	alive := filepath.Join(dir, "alive")
	release := filepath.Join(dir, "release")
	exited := filepath.Join(dir, "exited")
	t.Setenv("AMQ_KEEPALIVE_READY_PATH_LOG", readyPathLog)
	t.Setenv("AMQ_KEEPALIVE_CHECK", check)
	t.Setenv("AMQ_KEEPALIVE_ALIVE", alive)
	t.Setenv("AMQ_KEEPALIVE_RELEASE", release)
	t.Setenv("AMQ_KEEPALIVE_EXITED", exited)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
printf '%s' "$ready" > "$AMQ_KEEPALIVE_READY_PATH_LOG"
printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$ready"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do
  if [ -f "$AMQ_KEEPALIVE_CHECK" ]; then : > "$AMQ_KEEPALIVE_ALIVE"; fi
  sleep 0.01
done
: > "$AMQ_KEEPALIVE_EXITED"
`)

	ctx, cancel := context.WithCancel(context.Background())
	_, err := NewCLI(fakeAMQ).StartWake(ctx, StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", Timeout: 5 * time.Second, Owner: testWakeOwner(),
	})
	if err != nil {
		t.Fatalf("StartWake() error = %v", err)
	}
	cancel()
	if err := os.WriteFile(check, []byte("check"), 0o600); err != nil {
		t.Fatalf("request liveness check: %v", err)
	}
	waitForFile(t, alive, 2*time.Second)
	if err := os.WriteFile(release, []byte("release"), 0o600); err != nil {
		t.Fatalf("release wake: %v", err)
	}
	waitForFile(t, exited, 2*time.Second)
	data, err := os.ReadFile(readyPathLog)
	if err != nil {
		t.Fatalf("read ready path log: %v", err)
	}
	waitForMissingFile(t, string(data), 2*time.Second)
}

func TestStartWakeCancelBeforeReadyLeavesChildUnsignaled(t *testing.T) {
	dir := t.TempDir()
	started := filepath.Join(dir, "started")
	allowReady := filepath.Join(dir, "allow-ready")
	lateReady := filepath.Join(dir, "late-ready")
	release := filepath.Join(dir, "release")
	exited := filepath.Join(dir, "exited")
	t.Setenv("AMQ_KEEPALIVE_STARTED", started)
	t.Setenv("AMQ_KEEPALIVE_ALLOW_READY", allowReady)
	t.Setenv("AMQ_KEEPALIVE_LATE_READY", lateReady)
	t.Setenv("AMQ_KEEPALIVE_RELEASE", release)
	t.Setenv("AMQ_KEEPALIVE_EXITED", exited)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
: > "$AMQ_KEEPALIVE_STARTED"
while [ ! -f "$AMQ_KEEPALIVE_ALLOW_READY" ]; do sleep 0.01; done
printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$ready"
: > "$AMQ_KEEPALIVE_LATE_READY"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do sleep 0.01; done
: > "$AMQ_KEEPALIVE_EXITED"
`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := NewCLI(fakeAMQ).StartWake(ctx, StartWakeRequest{
			Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
			Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", Timeout: 5 * time.Second, Owner: testWakeOwner(),
		})
		done <- err
	}()
	waitForFile(t, started, 2*time.Second)
	cancel()
	err := <-done
	if !errors.Is(err, ErrWakeReadinessUncertain) || !errors.Is(err, context.Canceled) {
		t.Fatalf("StartWake() error = %v, want canceled uncertain readiness", err)
	}
	if err := os.WriteFile(allowReady, nil, 0o600); err != nil {
		t.Fatalf("allow late readiness: %v", err)
	}
	waitForFile(t, lateReady, 2*time.Second)
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatalf("release child: %v", err)
	}
	waitForFile(t, exited, 2*time.Second)
}

func TestStartWakeTimesOutWhenReadyFileNeverAppears(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
sleep 0.2
`)

	start := time.Now()
	_, err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		Timeout:   50 * time.Millisecond,
		Owner:     testWakeOwner(),
	})
	if err == nil {
		t.Fatal("StartWake() error = nil, want readiness timeout")
	}
	if !strings.Contains(err.Error(), "timed out") || !errors.Is(err, ErrWakeReadinessUncertain) {
		t.Fatalf("error = %v, want uncertain timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("StartWake took %s, want timeout branch to return promptly", elapsed)
	}
}

func TestWakeOwnerParsingRequiresStrongIdentityAndSurfacesCaptureError(t *testing.T) {
	owner, err := ParseWakeOwner(`{"pid":42,"process_start":"start-1","boot_id":"boot-1","session_id":42}`)
	if err != nil || !owner.Strong() {
		t.Fatalf("owner=%#v err=%v", owner, err)
	}
	for _, raw := range []string{"", `{"pid":42}`, `{"pid":42,"process_start":"start-1","boot_id":"boot-1","extra":true}`, `{"pid":42,"process_start":"start-1","boot_id":"boot-1"} {}`} {
		if _, err := ParseWakeOwner(raw); err == nil {
			t.Fatalf("ParseWakeOwner(%q) succeeded", raw)
		}
	}
	t.Setenv("AMQ_WAKE_OWNER", `{"pid":42,"process_start":"start-1","boot_id":"boot-1"}`)
	t.Setenv("AMQ_WAKE_OWNER_ERROR", "owner capture unavailable")
	if _, err := WakeOwnerFromEnvironment(); err == nil || !strings.Contains(err.Error(), "owner capture unavailable") {
		t.Fatalf("capture error=%v", err)
	}
}

func TestStartWakePassesOnlyRequestedOwnerEnvironment(t *testing.T) {
	dir := t.TempDir()
	ownerLog := filepath.Join(dir, "owner.log")
	t.Setenv("AMQ_KEEPALIVE_OWNER_LOG", ownerLog)
	t.Setenv("AMQ_WAKE_OWNER", `{"pid":999,"process_start":"stale","boot_id":"stale"}`)
	t.Setenv("AMQ_WAKE_OWNER_ERROR", "stale daemon error")
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '%s\n%s\n' "$AMQ_WAKE_OWNER" "${AMQ_WAKE_OWNER_ERROR-unset}" > "$AMQ_KEEPALIVE_OWNER_LOG"
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$arg"; fi
  previous="$arg"
done
`)
	if _, err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: dir, Me: "codex", InjectVia: "/bin/sh", Adapter: "file", Target: filepath.Join(dir, "target"),
		Owner: testWakeOwner(), Timeout: time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(ownerLog)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"pid":42,"process_start":"start-1","boot_id":"boot-1","session_id":42}` + "\nunset\n"
	if string(data) != want {
		t.Fatalf("owner env=%q want=%q", data, want)
	}
}

func TestRetireWakeParsesStructuredNonzeroAndRequiresExactEcho(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '{"schema":1,"status":"refused","reason_code":"owner_live","root":"%s","agent":"codex","lock":"%s/agents/codex/.wake.lock","target":"%s/agents/codex/.wake.target","generation":"generation-1","target_digest":"sha256:target-1"}\n' "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT"
exit 1
`)
	t.Setenv("AMQ_KEEPALIVE_TEST_ROOT", dir)
	request := RetireWakeRequest{
		Root: dir, Me: "codex", InjectVia: "/bin/sh", Adapter: "file", Target: filepath.Join(dir, "target"),
		Generation: "generation-1", TargetDigest: "sha256:target-1", RequireOwnerGone: true, Check: true,
	}
	result, err := NewCLI(fakeAMQ).RetireWake(context.Background(), request)
	if err == nil || result.Status != "refused" || result.ReasonCode != "owner_live" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	request.Generation = "different"
	if _, err := NewCLI(fakeAMQ).RetireWake(context.Background(), request); err == nil || !strings.Contains(err.Error(), "generation/digest mismatch") {
		t.Fatalf("mismatch error=%v", err)
	}
}

func TestRetireWakeManualUsesAtomicBindingFlagsAndStrictEcho(t *testing.T) {
	dir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("AMQ_KEEPALIVE_MANUAL_ARGS", argsLog)
	t.Setenv("AMQ_KEEPALIVE_TEST_ROOT", dir)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf 'CALL' >> "$AMQ_KEEPALIVE_MANUAL_ARGS"
for arg in "$@"; do printf '|%s' "$arg" >> "$AMQ_KEEPALIVE_MANUAL_ARGS"; done
printf '\n' >> "$AMQ_KEEPALIVE_MANUAL_ARGS"
check=false
for arg in "$@"; do [ "$arg" = "--check" ] && check=true; done
if [ "$check" = true ]; then
  status=eligible
  reason=manual_eligible
else
  case "${AMQ_KEEPALIVE_MANUAL_MODE-}" in
    receipt) status=already_retired; reason=tombstone_match ;;
    unexpected) status=superseded; reason=generation_superseded ;;
    *) status=retired; reason=manual_retired ;;
  esac
fi
generation=generation-1
[ "${AMQ_KEEPALIVE_WRONG_ECHO-}" = 1 ] && generation=generation-2
printf '{"schema":1,"status":"%s","reason_code":"%s","root":"%s","agent":"codex","lock":"%s/agents/codex/.wake.lock","target":"%s/agents/codex/.wake.target","generation":"%s","target_digest":"sha256:target-1","future_field":true}\n' "$status" "$reason" "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT" "$generation"
`)
	cli := NewCLI(fakeAMQ)
	base := RetireWakeRequest{
		Root: dir, Me: "codex", InjectVia: "/opt/amq-keepalive", Adapter: "synthetic", Target: "surface-alpha",
		Manual: true, Check: true, Timeout: time.Second,
	}
	checked, err := cli.RetireWake(context.Background(), base)
	if err != nil || checked.Status != "eligible" || checked.ReasonCode != "manual_eligible" || checked.Generation == "" || checked.TargetDigest == "" {
		t.Fatalf("manual check result=%#v err=%v", checked, err)
	}
	mutation := base
	mutation.Check = false
	mutation.Generation = checked.Generation
	mutation.TargetDigest = checked.TargetDigest
	retired, err := cli.RetireWake(context.Background(), mutation)
	if err != nil || retired.Status != "retired" || retired.ReasonCode != "manual_retired" {
		t.Fatalf("manual mutation result=%#v err=%v", retired, err)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "|--manual|--check") || strings.Contains(lines[0], "--if-generation") {
		t.Fatalf("manual preflight argv=%q", lines)
	}
	if !strings.Contains(lines[1], "|--manual|--if-generation|generation-1|--if-target-digest|sha256:target-1") || strings.Contains(lines[1], "|--check") {
		t.Fatalf("manual mutation argv=%q", lines[1])
	}

	t.Setenv("AMQ_KEEPALIVE_WRONG_ECHO", "1")
	if _, err := cli.RetireWake(context.Background(), mutation); err == nil || !strings.Contains(err.Error(), "exact preflight generation/digest") {
		t.Fatalf("wrong manual echo error=%v", err)
	}
	invalid := mutation
	invalid.TargetDigest = ""
	if _, err := cli.RetireWake(context.Background(), invalid); err == nil || !strings.Contains(err.Error(), "supplied together") {
		t.Fatalf("half-bound manual request error=%v", err)
	}
	t.Setenv("AMQ_KEEPALIVE_WRONG_ECHO", "")
	t.Setenv("AMQ_KEEPALIVE_MANUAL_MODE", "receipt")
	receipt, err := cli.RetireWake(context.Background(), mutation)
	if err != nil || receipt.Status != "already_retired" || receipt.ReasonCode != "tombstone_match" {
		t.Fatalf("manual receipt result=%#v err=%v", receipt, err)
	}
	t.Setenv("AMQ_KEEPALIVE_MANUAL_MODE", "unexpected")
	if _, err := cli.RetireWake(context.Background(), mutation); err == nil || !strings.Contains(err.Error(), "unexpected status/reason") {
		t.Fatalf("unexpected manual completion error=%v", err)
	}
}

func TestRetireWakeAcceptsOnlyIdentityConfirmedSupersededBinding(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AMQ_KEEPALIVE_TEST_ROOT", dir)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '{"schema":1,"status":"superseded","reason_code":"generation_superseded","root":"%s","agent":"codex","lock":"%s/agents/codex/.wake.lock","target":"%s/agents/codex/.wake.target","generation":"generation-1","target_digest":"sha256:target-1","current_generation":"generation-2","current_target_digest":"sha256:target-2","current_wake_mode":"owner_bound"}\n' "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT"
`)
	result, err := NewCLI(fakeAMQ).RetireWake(context.Background(), RetireWakeRequest{
		Root: dir, Me: "codex", InjectVia: "/bin/sh", Adapter: "file", Target: filepath.Join(dir, "target"),
		Generation: "generation-1", TargetDigest: "sha256:target-1", RequireOwnerGone: true,
	})
	if err != nil || result.Status != "superseded" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestRetireWakeAcceptsExplicitRawSupersededBinding(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AMQ_KEEPALIVE_TEST_ROOT", dir)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '{"schema":1,"status":"superseded","reason_code":"generation_superseded","root":"%s","agent":"codex","lock":"%s/agents/codex/.wake.lock","target":"%s/agents/codex/.wake.target","generation":"generation-1","target_digest":"sha256:target-1","current_generation":"generation-2","current_wake_mode":"raw"}\n' "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT"
`)
	result, err := NewCLI(fakeAMQ).RetireWake(context.Background(), RetireWakeRequest{
		Root: dir, Me: "codex", InjectVia: "/bin/sh", Adapter: "file", Target: filepath.Join(dir, "target"),
		Generation: "generation-1", TargetDigest: "sha256:target-1", RequireOwnerGone: true,
	})
	if err != nil || result.Status != "superseded" || result.CurrentWakeMode != "raw" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestSupersededReplacementProofRejectsAmbiguousModes(t *testing.T) {
	request := RetireWakeRequest{Generation: "generation-1", TargetDigest: "sha256:target-1"}
	base := RetireWakeResult{
		Status: "superseded", ReasonCode: "generation_superseded",
		CurrentGeneration: "generation-2", CurrentTargetDigest: "sha256:target-2",
	}
	for _, result := range []RetireWakeResult{
		base,
		func() RetireWakeResult { value := base; value.CurrentWakeMode = "unknown"; return value }(),
		func() RetireWakeResult { value := base; value.CurrentWakeMode = "raw"; return value }(),
		func() RetireWakeResult {
			value := base
			value.CurrentWakeMode = "owner_bound"
			value.CurrentGeneration = request.Generation
			return value
		}(),
	} {
		if SupersededProvesReplacement(request, result) {
			t.Fatalf("ambiguous result accepted: %#v", result)
		}
	}
}

func TestReadWakeBindingRejectsTrailingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready.json")
	if err := os.WriteFile(path, []byte(`{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"} {}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readWakeBinding(path); err == nil || !strings.Contains(err.Error(), "trailing JSON") {
		t.Fatalf("readWakeBinding() error=%v", err)
	}
}

func TestLifecycleJSONAllowsAdditiveFieldsButRejectsWrongSchemaAndTrailingData(t *testing.T) {
	additive := `{"schema":1,"status":"retired","reason_code":"retired_exact","root":"/tmp/root","agent":"codex","lock":"/tmp/root/agents/codex/.wake.lock","target":"/tmp/target","generation":"g","target_digest":"d","extra":true}`
	if result, err := parseRetireResult([]byte(additive)); err != nil || result.Status != "retired" {
		t.Fatalf("additive retire field was not tolerated: result=%#v err=%v", result, err)
	}
	for name, body := range map[string]string{
		"schema":   `{"schema":2,"status":"retired","reason_code":"retired_exact","root":"/tmp/root","agent":"codex","lock":"/tmp/root/agents/codex/.wake.lock","target":"/tmp/target","generation":"g","target_digest":"d"}`,
		"trailing": `{"schema":1,"status":"retired","reason_code":"retired_exact","root":"/tmp/root","agent":"codex","lock":"/tmp/root/agents/codex/.wake.lock","target":"/tmp/target","generation":"g","target_digest":"d"} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRetireResult([]byte(body)); err == nil {
				t.Fatalf("parseRetireResult(%s) succeeded", body)
			}
		})
	}
	ready := filepath.Join(t.TempDir(), "ready.json")
	if err := os.WriteFile(ready, []byte(`{"schema":1,"generation":"g","target_digest":"d","extra":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readWakeBinding(ready); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("readWakeBinding unknown-field error=%v", err)
	}
}

func TestEnvAllowsAdditiveFieldsButRequiresSchemaFieldsAndEnums(t *testing.T) {
	valid := `{"schema_version":1,"amq_version":"test","root":"/tmp/root","base_root":"/tmp","session_name":"root","in_session":true,"me":"worker","project":"test","root_source":"flag","peers":{},"capabilities":["wake_gc_v1"],"extra":true}`
	if environment, err := parseEnv([]byte(valid)); err != nil || environment.Root != "/tmp/root" {
		t.Fatalf("additive env field was not tolerated: environment=%#v err=%v", environment, err)
	}
	for name, payload := range map[string]string{
		"wrong-schema":        strings.Replace(valid, `"schema_version":1`, `"schema_version":2`, 1),
		"missing-capability":  strings.Replace(valid, `,"capabilities":["wake_gc_v1"]`, ``, 1),
		"unknown-root-source": strings.Replace(valid, `"root_source":"flag"`, `"root_source":"surprise"`, 1),
		"trailing":            valid + ` {}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("AMQ_KEEPALIVE_ENV_PAYLOAD", payload)
			fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), "#!/bin/sh\nprintf '%s\\n' \"$AMQ_KEEPALIVE_ENV_PAYLOAD\"\n")
			if _, err := NewCLI(fakeAMQ).Env(context.Background()); err == nil {
				t.Fatalf("Env accepted %s", payload)
			}
		})
	}
}

func TestAMQProducerGoldenFixtures(t *testing.T) {
	envData, err := os.ReadFile(filepath.Join("testdata", "amq-env-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var environment Env
	environment, err = parseEnv(envData)
	if err != nil {
		t.Fatalf("decode env producer fixture: %v", err)
	}
	if environment.SchemaVersion != 1 || environment.RootID == "" || environment.BaseRootID == "" ||
		!environment.Wake || !environment.HasCapability(CapabilityWakeGCV1) {
		t.Fatalf("env fixture lost producer fields: %#v", environment)
	}

	retireData, err := os.ReadFile(filepath.Join("testdata", "wake-retire-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	retired, err := parseRetireResult(retireData)
	if err != nil {
		t.Fatalf("strict-decode retire producer fixture: %v", err)
	}
	if retired.Schema != 1 || retired.Lock == "" || retired.Target == "" || retired.PID == 0 || retired.Status != "eligible" {
		t.Fatalf("retire fixture lost producer fields: %#v", retired)
	}

	startFailureData, err := os.ReadFile(filepath.Join("testdata", "wake-start-failure-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	startFailurePath := filepath.Join(t.TempDir(), "wake-start-result.json")
	if err := os.WriteFile(startFailurePath, startFailureData, 0o600); err != nil {
		t.Fatal(err)
	}
	startFailure, err := readWakeCommandResult(startFailurePath)
	if err != nil {
		t.Fatalf("strict-decode wake start failure producer fixture: %v", err)
	}
	if err := ValidateExistingWakeBlocker("/tmp/amq-contract", "worker", WakeBinding{
		Generation: "generation-1", TargetDigest: "sha256:target-1",
	}, startFailure); err != nil {
		t.Fatalf("wake start failure fixture lost blocker proof: %v", err)
	}
}

func TestAMQBinaryProducerContracts(t *testing.T) {
	bin := os.Getenv("AMQ_BIN")
	if bin == "" {
		t.Skip("set AMQ_BIN to run the cross-repository producer contract")
	}
	root := filepath.Join(t.TempDir(), "amq-root")
	if err := os.MkdirAll(filepath.Join(root, "agents", "worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(bin, "--no-update-check", "init", "--root", root, "--agents", "worker").CombinedOutput(); err != nil {
		t.Fatalf("initialize AMQ contract root: %v\n%s", err, output)
	}
	envCommand := exec.Command(bin, "env", "--root", root, "--me", "worker", "--json")
	envData, err := envCommand.Output()
	if err != nil {
		t.Fatalf("AMQ env producer failed: %v", err)
	}
	environment, err := parseEnv(envData)
	if err != nil {
		t.Fatalf("AMQ env producer drifted from keepalive consumer: %v\n%s", err, envData)
	}
	if environment.SchemaVersion != 1 {
		t.Fatalf("AMQ env schema=%d", environment.SchemaVersion)
	}
	injector := filepath.Join(root, "injector")
	if err := os.WriteFile(injector, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	retireCommand := exec.Command(bin, "wake", "retire",
		"--root", root, "--me", "worker", "--inject-via", injector,
		"--if-generation", "generation-1", "--if-target-digest", "sha256:target-1",
		"--require-owner-gone", "--check", "--json",
	)
	retireData, retireErr := retireCommand.Output()
	if retireErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(retireErr, &exitErr) {
			t.Fatalf("AMQ retire producer did not execute: %v", retireErr)
		}
	}
	if _, err := parseRetireResult(retireData); err != nil {
		t.Fatalf("AMQ retire producer drifted from keepalive consumer: %v\n%s", err, retireData)
	}

	// Start one real producer and use its published, identity-complete lock to
	// force a second real producer through the structured already-running path.
	// This prevents a hand-written lock fixture from drifting away from AMQ's
	// process-identity requirements.
	canonicalRoot, err := canonicalPath(root)
	if err != nil {
		t.Fatal(err)
	}
	contractBin := filepath.Join(root, "amq")
	binData, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(contractBin, binData, 0o700); err != nil {
		t.Fatal(err)
	}
	firstReadyPath := filepath.Join(root, "first-ready.json")
	firstResultPath := filepath.Join(root, "first-result.json")
	firstWake := exec.Command(contractBin, "--no-update-check", "wake",
		"--root", canonicalRoot, "--me", "worker", "--inject-via", injector,
		"--inject-arg", "contract-first", "--ready-file", firstReadyPath,
		"--result-file", firstResultPath,
	)
	var firstOutput bytes.Buffer
	firstWake.Stdout = &firstOutput
	firstWake.Stderr = &firstOutput
	if err := firstWake.Start(); err != nil {
		t.Fatalf("start first AMQ wake producer: %v", err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstWake.Wait() }()
	t.Cleanup(func() {
		if firstWake.Process != nil {
			_ = firstWake.Process.Signal(os.Interrupt)
		}
		select {
		case <-firstDone:
		case <-time.After(2 * time.Second):
			if firstWake.Process != nil {
				_ = firstWake.Process.Kill()
			}
			<-firstDone
		}
	})
	waitForFile(t, firstReadyPath, 5*time.Second)
	binding, err := readWakeBinding(firstReadyPath)
	if err != nil {
		t.Fatalf("read first AMQ wake binding: %v; output=%s", err, firstOutput.String())
	}
	secondResultPath := filepath.Join(root, "second-result.json")
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSecond()
	secondWake := exec.CommandContext(secondCtx, contractBin, "--no-update-check", "wake",
		"--root", canonicalRoot, "--me", "worker", "--inject-via", injector,
		"--inject-arg", "contract-second", "--accept-existing-wake",
		"--ready-file", filepath.Join(root, "second-ready.json"),
		"--result-file", secondResultPath,
	)
	secondOutput, secondErr := secondWake.CombinedOutput()
	if secondErr == nil {
		t.Fatal("second AMQ wake unexpectedly accepted a different target")
	}
	startFailure, err := readWakeCommandResult(secondResultPath)
	if err != nil {
		t.Fatalf("AMQ wake start failure producer drifted from keepalive consumer: %v; command=%v output=%s", err, secondErr, secondOutput)
	}
	if startFailure.CurrentWakeMode != "inject-via" {
		t.Fatalf("AMQ ownerless blocker mode=%q want inject-via; result=%#v", startFailure.CurrentWakeMode, startFailure)
	}
	if err := ValidateExistingWakeBlocker(root, "worker", binding, startFailure); err == nil {
		t.Fatalf("AMQ ownerless blocker was accepted as owner-bound retirement proof: result=%#v", startFailure)
	}
}

func TestValidateExistingWakeBlockerRequiresExactIdentity(t *testing.T) {
	root := t.TempDir()
	binding := WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"}
	valid := WakeCommandResult{
		Schema: 1, Status: "failed", ReasonCode: "existing_wake_blocking",
		Root: root, Agent: "codex", CurrentWakeMode: "owner_bound",
		CurrentGeneration: binding.Generation, CurrentTargetDigest: binding.TargetDigest,
	}
	if err := ValidateExistingWakeBlocker(root, "codex", binding, valid); err != nil {
		t.Fatalf("valid blocker rejected: %v", err)
	}
	mutations := []func(*WakeCommandResult){
		func(v *WakeCommandResult) { v.Root = "" },
		func(v *WakeCommandResult) { v.Agent = "claude" },
		func(v *WakeCommandResult) { v.CurrentWakeMode = "raw" },
		func(v *WakeCommandResult) { v.CurrentGeneration = "generation-2" },
		func(v *WakeCommandResult) { v.CurrentTargetDigest = "sha256:target-2" },
	}
	for index, mutate := range mutations {
		candidate := valid
		mutate(&candidate)
		if err := ValidateExistingWakeBlocker(root, "codex", binding, candidate); err == nil {
			t.Fatalf("mutation %d accepted: %#v", index, candidate)
		}
	}
}

func TestParseRetireResultStatusContract(t *testing.T) {
	valid := map[string]string{
		"eligible":        "owner_gone",
		"retired":         "retired_exact",
		"already_retired": "tombstone_match",
		"superseded":      "generation_superseded",
		"error":           "internal_error",
		"refused":         "owner_live",
	}
	for status, reason := range valid {
		t.Run("valid "+status, func(t *testing.T) {
			body := fmt.Sprintf(`{"schema":1,"status":%q,"reason_code":%q,"agent":"worker","root":"/tmp/root","lock":"/tmp/root/agents/worker/.wake.lock","target":"/tmp/inbox","generation":"generation-1","target_digest":"sha256:target-1"}`, status, reason)
			result, err := parseRetireResult([]byte(body))
			if err != nil || result.Status != status || result.ReasonCode != reason {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
	invalid := map[string]string{
		"eligible":        "not_owner_gone",
		"retired":         "not_retired_exact",
		"already_retired": "not_tombstone_match",
		"superseded":      "not_generation_superseded",
		"error":           "not_internal_error",
		"unknown":         "unknown_reason",
	}
	for status, reason := range invalid {
		t.Run("invalid "+status, func(t *testing.T) {
			body := fmt.Sprintf(`{"schema":1,"status":%q,"reason_code":%q,"agent":"worker","root":"/tmp/root","lock":"/tmp/root/agents/worker/.wake.lock","target":"/tmp/inbox"}`, status, reason)
			if _, err := parseRetireResult([]byte(body)); err == nil {
				t.Fatalf("parseRetireResult(%s) succeeded", body)
			}
		})
	}
	for name, body := range map[string]string{
		"empty":          " ",
		"malformed":      "{",
		"missing status": `{"schema":1,"reason_code":"owner_live"}`,
		"missing reason": `{"schema":1,"status":"refused"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRetireResult([]byte(body)); err == nil {
				t.Fatalf("parseRetireResult(%q) succeeded", body)
			}
		})
	}
}

func TestReadWakeCommandResultContract(t *testing.T) {
	for _, reason := range []string{"existing_wake_blocking", "invalid_owner", "unverified_wake", "invalid_baseline", "internal_failure"} {
		t.Run("valid "+reason, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "result.json")
			body := fmt.Sprintf(`{"schema":1,"status":"failed","reason_code":%q}`, reason)
			if reason == "existing_wake_blocking" {
				body = `{"schema":1,"status":"failed","reason_code":"existing_wake_blocking","root":"/tmp/root","agent":"worker","current_generation":"generation-1","current_target_digest":"sha256:target-1","current_wake_mode":"owner_bound"}`
			}
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			result, err := readWakeCommandResult(path)
			if err != nil || result.ReasonCode != reason {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
	for _, mode := range []string{"owner_bound", "inject-via", "raw", "paste", "none", "unverified"} {
		t.Run("valid blocker mode "+mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "result.json")
			body := fmt.Sprintf(`{"schema":1,"status":"failed","reason_code":"existing_wake_blocking","root":"/tmp/root","agent":"worker","current_generation":"generation-1","current_target_digest":"sha256:target-1","current_wake_mode":%q}`, mode)
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readWakeCommandResult(path); err != nil {
				t.Fatalf("known blocker mode %q rejected: %v", mode, err)
			}
		})
	}
	for name, body := range map[string]string{
		"wrong schema": `{"schema":2,"status":"failed","reason_code":"invalid_owner"}`,
		"wrong status": `{"schema":1,"status":"ok","reason_code":"invalid_owner"}`,
		"missing code": `{"schema":1,"status":"failed"}`,
		"unknown code": `{"schema":1,"status":"failed","reason_code":"surprise"}`,
		"unknown mode": `{"schema":1,"status":"failed","reason_code":"existing_wake_blocking","root":"/tmp/root","agent":"worker","current_generation":"generation-1","current_target_digest":"sha256:target-1","current_wake_mode":"future-mode"}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "result.json")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readWakeCommandResult(path); err == nil {
				t.Fatalf("readWakeCommandResult(%s) succeeded", body)
			}
		})
	}
	t.Run("additive field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "result.json")
		body := `{"schema":1,"status":"failed","reason_code":"invalid_owner","future_detail":{"safe":true}}`
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readWakeCommandResult(path); err != nil {
			t.Fatalf("additive start-result field was not tolerated: %v", err)
		}
	})
}

func TestSecureLifecycleFileGuards(t *testing.T) {
	var destination map[string]any
	if err := readSecureJSONFile(filepath.Join(t.TempDir(), "missing.json"), &destination); err == nil {
		t.Fatal("missing lifecycle file accepted")
	}

	dir := t.TempDir()
	insecure := filepath.Join(dir, "insecure.json")
	if err := os.WriteFile(insecure, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := readSecureJSONFile(insecure, &destination); err == nil {
		t.Fatal("insecure lifecycle file accepted")
	}

	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := readSecureJSONFile(link, &destination); err == nil {
		t.Fatal("symlink lifecycle file accepted")
	}

	oversized := filepath.Join(dir, "oversized.json")
	if err := os.WriteFile(oversized, bytes.Repeat([]byte("x"), maxLifecycleResultBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := readSecureJSONFile(oversized, &destination); err == nil {
		t.Fatal("oversized lifecycle file accepted")
	}
}

func TestWakeBindingAndBlockerContractBranches(t *testing.T) {
	for name, body := range map[string]string{
		"wrong schema":       `{"schema":2,"generation":"generation-1","target_digest":"sha256:target-1"}`,
		"missing generation": `{"schema":1,"target_digest":"sha256:target-1"}`,
		"missing digest":     `{"schema":1,"generation":"generation-1"}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ready.json")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readWakeBinding(path); err == nil {
				t.Fatalf("readWakeBinding(%s) succeeded", body)
			}
		})
	}

	root := t.TempDir()
	binding := WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"}
	valid := WakeCommandResult{
		Schema: 1, Status: "failed", ReasonCode: "existing_wake_blocking",
		Root: root, Agent: "codex", CurrentWakeMode: "owner_bound",
		CurrentGeneration: binding.Generation, CurrentTargetDigest: binding.TargetDigest,
	}
	for name, check := range map[string]func() error{
		"wrong result contract": func() error {
			candidate := valid
			candidate.Schema = 2
			return ValidateExistingWakeBlocker(root, "codex", binding, candidate)
		},
		"incomplete binding": func() error {
			return ValidateExistingWakeBlocker(root, "codex", WakeBinding{}, valid)
		},
		"empty expected root": func() error {
			return ValidateExistingWakeBlocker("", "codex", binding, valid)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(); err == nil {
				t.Fatal("invalid blocker accepted")
			}
		})
	}
}

func TestValidateRetireEchoContract(t *testing.T) {
	root := t.TempDir()
	request := RetireWakeRequest{Root: root, Me: "codex", Generation: "generation-1", TargetDigest: "sha256:target-1"}
	valid := RetireWakeResult{
		Status: "retired", ReasonCode: "retired_exact", Root: root, Agent: request.Me,
		Generation: request.Generation, TargetDigest: request.TargetDigest,
	}
	if err := validateRetireEcho(request, valid); err != nil {
		t.Fatalf("valid echo rejected: %v", err)
	}
	tests := []struct {
		name   string
		req    RetireWakeRequest
		result RetireWakeResult
	}{
		{name: "empty request root", req: func() RetireWakeRequest { value := request; value.Root = ""; return value }(), result: valid},
		{name: "wrong response root", req: request, result: func() RetireWakeResult { value := valid; value.Root = ""; return value }()},
		{name: "wrong agent", req: request, result: func() RetireWakeResult { value := valid; value.Agent = "claude"; return value }()},
		{name: "wrong generation", req: request, result: func() RetireWakeResult { value := valid; value.Generation = "generation-2"; return value }()},
		{name: "unproven superseded", req: request, result: func() RetireWakeResult {
			value := valid
			value.Status = "superseded"
			value.ReasonCode = "generation_superseded"
			return value
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateRetireEcho(test.req, test.result); err == nil {
				t.Fatal("invalid echo accepted")
			}
		})
	}
	if _, err := canonicalPath(""); err == nil {
		t.Fatal("empty canonical path accepted")
	}
}

func TestWakeOwnerLengthNULAndEnvironmentBranches(t *testing.T) {
	if _, err := ParseWakeOwner(strings.Repeat("x", 4097)); err == nil {
		t.Fatal("oversized wake owner accepted")
	}
	if _, err := ParseWakeOwner(`{"pid":42,"process_start":"start\u0000bad","boot_id":"boot"}`); err == nil {
		t.Fatal("wake owner containing NUL accepted")
	}
	t.Setenv("AMQ_WAKE_OWNER_ERROR", "")
	t.Setenv("AMQ_WAKE_OWNER", `{"pid":42,"process_start":"start-1","boot_id":"boot-1","session_id":42}`)
	if owner, err := WakeOwnerFromEnvironment(); err != nil || !owner.Strong() {
		t.Fatalf("owner=%#v err=%v", owner, err)
	}
}

func TestBoundedBufferDiscardsExcessWithoutBlockingChild(t *testing.T) {
	buffer := newBoundedBuffer(3)
	if written, err := buffer.Write([]byte("hello")); err != nil || written != 5 {
		t.Fatalf("Write() written=%d err=%v", written, err)
	}
	if got := buffer.String(); got != "hel" || !buffer.Exceeded() {
		t.Fatalf("buffer=%q exceeded=%v", got, buffer.Exceeded())
	}
}

func TestNewWakeReadyPathScavengesOnlyStaleMarkers(t *testing.T) {
	cache := t.TempDir()
	t.Setenv("AMQ_KEEPALIVE_CACHE_DIR", cache)
	dir := filepath.Join(cache, "amq-keepalive", "readiness")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create readiness dir: %v", err)
	}
	oldMarker := filepath.Join(dir, "wake-old")
	recentMarker := filepath.Join(dir, "wake-recent")
	for _, path := range []string{oldMarker, recentMarker} {
		if err := os.WriteFile(path, []byte("ready"), 0o600); err != nil {
			t.Fatalf("write marker: %v", err)
		}
	}
	oldTime := time.Now().Add(-staleWakeReadyMarkerAge - time.Hour)
	if err := os.Chtimes(oldMarker, oldTime, oldTime); err != nil {
		t.Fatalf("age old marker: %v", err)
	}
	_, reserved, err := newWakeReadyPath()
	if err != nil {
		t.Fatalf("newWakeReadyPath: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(reserved); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("remove reserved readiness path: %v", err)
		}
	})
	if _, err := os.Stat(oldMarker); !os.IsNotExist(err) {
		t.Fatalf("old marker was not scavenged: %v", err)
	}
	if _, err := os.Stat(recentMarker); err != nil {
		t.Fatalf("recent marker was scavenged: %v", err)
	}
}

func TestRetireWakeJoinsProcessParseAndSanitizedStderrErrors(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), "#!/bin/sh\nprintf '{'\nprintf '\\033[31moperator detail\\007\\n' >&2\nexit 7\n")
	_, err := NewCLI(fakeAMQ).RetireWake(context.Background(), RetireWakeRequest{
		Root: dir, Me: "worker", InjectVia: fakeAMQ, Adapter: "file", Target: filepath.Join(dir, "target"),
		Generation: "generation-1", TargetDigest: "sha256:target-1", RequireOwnerGone: true, Check: true,
	})
	if err == nil {
		t.Fatal("malformed failed retire unexpectedly succeeded")
	}
	text := err.Error()
	for _, want := range []string{"process failed", "parse amq wake retire JSON", "amq stderr:", "operator detail"} {
		if !strings.Contains(text, want) {
			t.Fatalf("joined error %q lacks %q", text, want)
		}
	}
	if strings.ContainsAny(text, "\x1b\a") {
		t.Fatalf("stderr control characters were not sanitized: %q", text)
	}
}

func TestStartWakeJoinsMalformedResultWithProcessReadinessFailure(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
result=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "--result-file" ]; then result="$arg"; fi
  previous="$arg"
done
[ -n "$result" ] || exit 11
printf '{' > "$result"
exit 7
`)
	_, err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "worker", InjectVia: "/tmp/amq-keepalive", Adapter: "file", Target: filepath.Join(dir, "target"),
		Timeout: 5 * time.Second, Owner: testWakeOwner(),
	})
	if err == nil {
		t.Fatal("malformed wake start result unexpectedly succeeded")
	}
	for _, want := range []string{"amq wake exited before becoming ready", "parse amq wake start result"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("joined error %q lacks %q", err, want)
		}
	}
}

func writeExecutable(t *testing.T, path string, body string) string {
	t.Helper()
	t.Setenv("AMQ_KEEPALIVE_CACHE_DIR", filepath.Join(filepath.Dir(path), "cache"))
	body = strings.Replace(body, "#!/bin/sh\n", "#!/bin/sh\numask 077\n", 1)
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	return path
}

func testWakeOwner() WakeOwner {
	return WakeOwner{PID: 42, ProcessStart: "start-1", BootID: "boot-1", SessionID: 42}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("file %q did not appear within %s", path, timeout)
}

func waitForMissingFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("file %q still exists after %s", path, timeout)
}
