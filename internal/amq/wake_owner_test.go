package amq

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWakeOwnerFromEnvironmentRequiresExactIdentity(t *testing.T) {
	t.Setenv(WakeOwnerEnvironment, testWakeOwner)
	got, err := WakeOwnerFromEnvironment()
	if err != nil {
		t.Fatalf("WakeOwnerFromEnvironment() error = %v", err)
	}
	if got != testWakeOwner {
		t.Fatalf("owner = %q, want %q", got, testWakeOwner)
	}

	t.Setenv(WakeOwnerEnvironment, `{"pid":4242}`)
	if _, err := WakeOwnerFromEnvironment(); err == nil || !strings.Contains(err.Error(), "process start is required") {
		t.Fatalf("partial owner error = %v, want exact-identity refusal", err)
	}
}

func TestStartWakeStripsAmbientOwnerAndPassesStoredOwner(t *testing.T) {
	dir := t.TempDir()
	ownerLog := filepath.Join(dir, "owner.log")
	t.Setenv("AMQ_KEEPALIVE_OWNER_LOG", ownerLog)
	t.Setenv(WakeOwnerEnvironment, `{"pid":9999,"process_start":"ambient","boot_id":"wrong"}`)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
printf '%s' "$AMQ_WAKE_OWNER" > "$AMQ_KEEPALIVE_OWNER_LOG"
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
printf ready > "$ready"
`)

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "file", Target: "/tmp/inbox", WakeOwner: testWakeOwner,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("StartWake() error = %v", err)
	}
	data, err := os.ReadFile(ownerLog)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != testWakeOwner {
		t.Fatalf("child owner = %q, want stored owner %q", data, testWakeOwner)
	}
}

func TestStartWakeRefusesOwnerlessLegacyRequestBeforeExec(t *testing.T) {
	dir := t.TempDir()
	called := filepath.Join(dir, "called")
	t.Setenv("AMQ_KEEPALIVE_CALLED", called)
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), "#!/bin/sh\n: > \"$AMQ_KEEPALIVE_CALLED\"\n")

	err := NewCLI(fakeAMQ).StartWake(context.Background(), StartWakeRequest{
		InjectVia: "/tmp/amq-keepalive", Adapter: "file", Target: "/tmp/inbox",
	})
	if err == nil || !strings.Contains(err.Error(), "wake owner is required") {
		t.Fatalf("StartWake() error = %v, want owner refusal", err)
	}
	if _, err := os.Stat(called); !os.IsNotExist(err) {
		t.Fatalf("ownerless request executed AMQ: stat=%v", err)
	}
}
