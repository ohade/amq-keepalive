package amq

import (
	"context"
	"os"
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
printf ready > "$ready"
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
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

func TestStartWakeDoesNotInheritCoopOwnerToken(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AMQ_WAKE_OWNER", "owner-for-the-calling-session")
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
if [ "${AMQ_WAKE_OWNER+x}" = x ]; then
  printf 'inherited AMQ_WAKE_OWNER\n' >&2
  exit 12
fi
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
printf ready > "$ready"
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "claude",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "cmux",
		Target:    "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2",
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartWake() error = %v", err)
	}
}

func TestCheckWakeParsesSchemaTwoOwnerState(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '%s\n' '{"schema":2,"agent":"codex","root":"/tmp/amq-root","wake":{"status":"valid","live":true,"owner_bound":true}}'
`)

	result, err := NewCLI(fakeAMQ).CheckWake(context.Background(), "/tmp/amq-root", "codex")
	if err != nil {
		t.Fatalf("CheckWake() error = %v", err)
	}
	if result.Schema != 2 || !result.Wake.Live || !result.Wake.OwnerBound || result.Wake.Status != "valid" {
		t.Fatalf("result = %#v", result)
	}
}

func TestRecoverOwnerWakeParsesRecoveredResult(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '%s\n' '{"status":"recovered","agent":"codex","root":"/tmp/amq-root","pid":4242,"owner_pid":4000,"owner_session":3999}'
`)

	result, err := NewCLI(fakeAMQ).RecoverOwnerWake(context.Background(), "/tmp/amq-root", "codex")
	if err != nil {
		t.Fatalf("RecoverOwnerWake() error = %v", err)
	}
	if result.Status != "recovered" || result.PID != 4242 || result.OwnerPID != 4000 {
		t.Fatalf("result = %#v", result)
	}
}

func TestStartWakeFailsWhenProcessExitsBeforeReady(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
exit 7
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		Timeout:   5 * time.Second,
	})
	if err == nil {
		t.Fatal("StartWake() error = nil, want readiness failure")
	}
	if !strings.Contains(err.Error(), "amq wake exited before becoming ready") {
		t.Fatalf("error = %v, want readiness failure", err)
	}
}

func TestRetireWakePassesExactSavedTargetIdentity(t *testing.T) {
	dir := t.TempDir()
	argsLog := filepath.Join(dir, "args.log")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '%s\n' "$@" > "$AMQ_KEEPALIVE_ARGS_LOG"
printf '%s\n' '{"status":"retired","agent":"codex","root":"/tmp/amq-root","pid":4242}'
`)

	result, err := NewCLI(fakeAMQ).RetireWake(context.Background(), RetireWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "cmux",
		Target:    "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3",
	})
	if err != nil {
		t.Fatalf("RetireWake() error = %v", err)
	}
	if result.Status != "retired" || result.PID != 4242 {
		t.Fatalf("result = %#v", result)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	args := string(data)
	for _, want := range []string{
		"wake\nretire\n-json\n",
		"-root\n/tmp/amq-root\n",
		"-me\ncodex\n",
		"-inject-via\n/tmp/amq-keepalive\n",
		"-inject-arg\ninject\n",
		"-inject-arg\ncmux\n",
		"-inject-arg\ncmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3\n",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("args log missing %q:\n%s", want, args)
		}
	}
}

func TestStartWakeTimesOutWhenReadyFileNeverAppears(t *testing.T) {
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
sleep 10
`)

	start := time.Now()
	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root:      "/tmp/amq-root",
		Me:        "codex",
		InjectVia: "/tmp/amq-keepalive",
		Adapter:   "ghostty",
		Target:    "ghostty:terminal:abc",
		Timeout:   500 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("StartWake() error = nil, want readiness timeout")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %v, want timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("StartWake took %s, want timeout branch to return promptly", elapsed)
	}
}

func writeExecutable(t *testing.T, path string, body string) string {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write executable: %v", err)
	}
	return path
}
