package amq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testWakeOwner = `{"pid":4242,"process_start":"owner-start","boot_id":"boot-1"}`

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
printf ready > "$ready"
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		WakeOwner: testWakeOwner,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartWake() error = %v", err)
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
		"-ready-file\n",
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
  if [ "$previous" = "-ready-file" ]; then printf ready > "$arg"; fi
  previous="$arg"
done
`)
	if err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "cmux", Target: "cmux:surface:abc", BaselineFile: baseline,
		WakeOwner:      testWakeOwner,
		BaselineDigest: digest, Timeout: 5 * time.Second,
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
	err = NewCLI(filepath.Join(dir, "must-not-run")).StartWake(context.Background(), StartWakeRequest{
		InjectVia: "/tmp/amq-keepalive", Adapter: "cmux", Target: "cmux:surface:abc",
		WakeOwner:    testWakeOwner,
		BaselineFile: baseline, BaselineDigest: digest,
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
printf 'invalid wake owner for this session\n' >&2
exit 7
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		WakeOwner: testWakeOwner,
		Timeout:   5 * time.Second,
	})
	if err == nil {
		t.Fatal("StartWake() error = nil, want readiness failure")
	}
	if !strings.Contains(err.Error(), "amq wake exited before becoming ready") {
		t.Fatalf("error = %v, want readiness failure", err)
	}
	if !strings.Contains(err.Error(), "invalid wake owner for this session") {
		t.Fatalf("error = %v, want actionable child stderr", err)
	}
}

func TestStartWakeDetectsAlreadyOwnedFromCapturedStderr(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf 'wake is already owned by another process\n' >&2
exit 7
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "ghostty", Target: "ghostty:terminal:abc", WakeOwner: testWakeOwner,
		Timeout: 5 * time.Second,
	})
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("StartWake() error = %v, want ErrAlreadyRunning", err)
	}
	if !strings.Contains(err.Error(), "already owned by another process") {
		t.Fatalf("error = %v, want captured ownership detail", err)
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
printf ready > "$ready"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do sleep 0.01; done
[ -d "${ready%/*}" ] || exit 12
: > "$AMQ_KEEPALIVE_POST_READY"
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", Timeout: 5 * time.Second,
		WakeOwner: testWakeOwner,
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
	t.Setenv("AMQ_KEEPALIVE_READY_PATH_LOG", readyPathLog)
	t.Setenv("AMQ_KEEPALIVE_CHECK", check)
	t.Setenv("AMQ_KEEPALIVE_ALIVE", alive)
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
printf ready > "$ready"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do
  if [ -f "$AMQ_KEEPALIVE_CHECK" ]; then : > "$AMQ_KEEPALIVE_ALIVE"; fi
  sleep 0.01
done
`)

	ctx, cancel := context.WithCancel(context.Background())
	err := NewCLI(fakeAMQ).StartWake(ctx, StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", Timeout: 5 * time.Second,
		WakeOwner: testWakeOwner,
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
	t.Setenv("AMQ_KEEPALIVE_STARTED", started)
	t.Setenv("AMQ_KEEPALIVE_ALLOW_READY", allowReady)
	t.Setenv("AMQ_KEEPALIVE_LATE_READY", lateReady)
	t.Setenv("AMQ_KEEPALIVE_RELEASE", release)
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
printf ready > "$ready"
: > "$AMQ_KEEPALIVE_LATE_READY"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do sleep 0.01; done
`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NewCLI(fakeAMQ).StartWake(ctx, StartWakeRequest{
			Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
			Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", Timeout: 5 * time.Second,
			WakeOwner: testWakeOwner,
		})
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
}

func TestStartWakeTimesOutWhenReadyFileNeverAppears(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
sleep 0.2
`)

	start := time.Now()
	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		WakeOwner: testWakeOwner,
		Timeout:   50 * time.Millisecond,
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

func TestWaitForWakeReadyPrefersExitedChildOverCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan wakeProcessResult, 1)
	done <- wakeProcessResult{Err: errors.New("exit status 9"), Stderr: "specific wake refusal"}

	processDone, err := waitForWakeReady(ctx, done, filepath.Join(t.TempDir(), "missing"), time.Second)
	if !processDone {
		t.Fatal("waitForWakeReady() processDone = false, want true")
	}
	if err == nil || !strings.Contains(err.Error(), "exit status 9") || !strings.Contains(err.Error(), "specific wake refusal") {
		t.Fatalf("waitForWakeReady() error = %v, want actual child result", err)
	}
}

func TestWaitForWakeReadyPrefersExitedChildOverTimeout(t *testing.T) {
	done := make(chan wakeProcessResult, 1)
	done <- wakeProcessResult{Err: errors.New("exit status 8"), Stderr: "timeout-edge refusal"}

	processDone, err := waitForWakeReady(context.Background(), done, filepath.Join(t.TempDir(), "missing"), time.Nanosecond)
	if !processDone {
		t.Fatal("waitForWakeReady() processDone = false, want true")
	}
	if err == nil || !strings.Contains(err.Error(), "exit status 8") || !strings.Contains(err.Error(), "timeout-edge refusal") {
		t.Fatalf("waitForWakeReady() error = %v, want actual child result", err)
	}
}

func TestBoundedCaptureReportsTruncationAndKeepsDraining(t *testing.T) {
	capture := newBoundedCapture(8)
	data := []byte("0123456789abcdef")
	written, err := capture.Write(data)
	if err != nil || written != len(data) {
		t.Fatalf("Write() = (%d, %v), want (%d, nil)", written, err, len(data))
	}
	got := capture.String()
	if !strings.HasPrefix(got, "01234567") || !strings.Contains(got, "stderr truncated after 8 bytes") {
		t.Fatalf("String() = %q, want bounded prefix and truncation marker", got)
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

func writeExecutable(t *testing.T, path string, body string) string {
	t.Helper()
	t.Setenv("AMQ_KEEPALIVE_CACHE_DIR", filepath.Join(filepath.Dir(path), "cache"))
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	return path
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
