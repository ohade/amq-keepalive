package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
	"github.com/ohade/amq-keepalive/internal/supervisor"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("AMQ_WAKE_OWNER", `{"pid":42,"process_start":"start-1","boot_id":"boot-1","session_id":42}`)
	_ = os.Unsetenv("AMQ_WAKE_OWNER_ERROR")
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
}

func TestGCAbandonRequiresExactDoubleConfirmation(t *testing.T) {
	var stderr bytes.Buffer
	code := (App{Stdout: io.Discard, Stderr: &stderr}).Run(context.Background(), []string{
		"gc", "--registry", filepath.Join(t.TempDir(), "registry.json"),
		"--abandon-batch", "batch-a", "--confirm-abandon-batch", "batch-b",
	})
	if code != 1 || !strings.Contains(stderr.String(), "same exact non-empty batch id") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestDoctorReportsActiveBatchWithoutMutatingRegistry(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := canonicalGCRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	entry := registry.Entry{
		ID: "entry", Root: root, Agent: "worker", Adapter: "file", Target: filepath.Join(root, "target"), State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: registry.WakeOwner{PID: 42, ProcessStart: "start", BootID: "boot", SessionID: 42},
		WakeBinding: registry.WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"},
	}
	member := registry.GCRootBatchMember{
		EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
		WakeOwnerPresent: entry.WakeOwnerPresent, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
	}
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	file := registry.File{SchemaVersion: registry.SchemaVersion, Entries: []registry.Entry{entry},
		GCRootBatches:  []registry.GCRootBatch{{ID: "batch-1", CanonicalRoot: root, StartedAt: now, Phase: registry.GCRootBatchRetiring, Members: []registry.GCRootBatchMember{member}}},
		GCRootAttempts: []registry.GCRootAttempt{{CanonicalRoot: root, StartedAt: now}},
	}
	data, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	if err := os.WriteFile(registryPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	fakeAMQ := filepath.Join(dir, "amq")
	env := `{"schema_version":1,"amq_version":"test","root":"/tmp/root","base_root":"/tmp","session_name":"root","in_session":true,"me":"worker","project":"test","root_source":"flag","peers":{},"capabilities":["wake_gc_v1"]}`
	if err := os.WriteFile(fakeAMQ, []byte("#!/bin/sh\nprintf '%s\\n' '"+env+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), []string{"doctor", "--registry", registryPath, "--amq", fakeAMQ})
	if code != 0 || !strings.Contains(stdout.String(), `"active_gc_batch_id": "batch-1"`) || !strings.Contains(stdout.String(), `"active_gc_batch_phase": "retiring"`) {
		t.Fatalf("doctor code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	for _, forbidden := range []string{registryPath + ".lock", registryPath + ".registration.lock", registryPath + ".v1.bak"} {
		if _, err := os.Lstat(forbidden); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("doctor created %q: %v", forbidden, err)
		}
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
	if loaded.Entries[0].BaselineFile != baseline || loaded.Entries[0].BaselineDigest != digest {
		t.Fatalf("persisted baseline binding = %+v", loaded.Entries[0])
	}
}

func TestAttachWithoutStrongOwnerPersistsInactiveReservationAndWarns(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := filepath.Join(dir, "inbox.txt")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AMQ_WAKE_OWNER", "")
	t.Setenv("AMQ_WAKE_OWNER_ERROR", "owner capture failed")
	called := filepath.Join(dir, "amq-called")
	t.Setenv("AMQ_KEEPALIVE_CALLED", called)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte("#!/bin/sh\nif [ \"$1\" = env ]; then printf '%s\\n' '{\"schema_version\":1,\"amq_version\":\"test\",\"root\":\"/tmp/root\",\"base_root\":\"/tmp\",\"session_name\":\"root\",\"in_session\":true,\"me\":\"worker\",\"project\":\"test\",\"root_source\":\"flag\",\"peers\":{},\"capabilities\":[\"wake_gc_v1\"]}'; exit 0; fi\n: > \"$AMQ_KEEPALIVE_CALLED\"\nexit 99\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"attach", "--registry", registryPath, "--adapter", "file", "--target", target,
		"--root", dir, "--base-root", filepath.Dir(dir), "--session", filepath.Base(dir), "--me", "codex",
		"--amq", fakeAMQ, "--self", "/bin/sh",
	})
	if code != 1 || !strings.Contains(stderr.String(), "AMQ wake unavailable; messages remain queued") ||
		!strings.Contains(stderr.String(), "owner capture failed") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].WakeOwnerPresent || loaded.Entries[0].State != registry.StateAttached {
		t.Fatalf("inactive reservation=%#v err=%v", loaded.Entries, err)
	}
	if _, err := os.Stat(called); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ownerless attach invoked AMQ: %v", err)
	}
}

func TestManagedAttachCapabilityGatePrecedesRegistryMutation(t *testing.T) {
	for _, command := range []string{"attach", "reattach"} {
		t.Run(command, func(t *testing.T) {
			dir := t.TempDir()
			registryPath := filepath.Join(dir, "registry.json")
			oldTarget := filepath.Join(dir, "old.txt")
			newTarget := filepath.Join(dir, "new.txt")
			for _, target := range []string{oldTarget, newTarget} {
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if command == "reattach" {
				runApp(t, "attach", "--registry", registryPath, "--adapter", "file", "--target", oldTarget,
					"--root", dir, "--base-root", filepath.Dir(dir), "--session", filepath.Base(dir), "--me", "codex", "--no-start")
			}
			before, beforeErr := os.ReadFile(registryPath)
			touched := filepath.Join(dir, "wake-touched")
			t.Setenv("AMQ_KEEPALIVE_WAKE_TOUCHED", touched)
			fakeAMQ := filepath.Join(dir, "amq")
			script := "#!/bin/sh\nif [ \"$1\" = env ]; then printf '%s\\n' '{\"schema_version\":1,\"amq_version\":\"test\",\"root\":\"/tmp/root\",\"base_root\":\"/tmp\",\"session_name\":\"root\",\"in_session\":true,\"me\":\"worker\",\"project\":\"test\",\"root_source\":\"flag\",\"peers\":{},\"capabilities\":[]}'; exit 0; fi\n: > \"$AMQ_KEEPALIVE_WAKE_TOUCHED\"\n"
			if err := os.WriteFile(fakeAMQ, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
				command, "--registry", registryPath, "--adapter", "file", "--target", newTarget,
				"--root", dir, "--base-root", filepath.Dir(dir), "--session", filepath.Base(dir), "--me", "codex", "--amq", fakeAMQ,
			})
			if code != 1 || !strings.Contains(stderr.String(), "messages remain queued") || !strings.Contains(stderr.String(), "wake_gc_v1") {
				t.Fatalf("code=%d stderr=%s", code, stderr.String())
			}
			if _, err := os.Stat(touched); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("capability failure touched wake: %v", err)
			}
			after, afterErr := os.ReadFile(registryPath)
			if command == "attach" {
				if !errors.Is(beforeErr, os.ErrNotExist) || !errors.Is(afterErr, os.ErrNotExist) {
					t.Fatalf("attach capability failure created registry: before=%v after=%v", beforeErr, afterErr)
				}
			} else if beforeErr != nil || afterErr != nil || !bytes.Equal(before, after) {
				t.Fatalf("reattach capability failure mutated registry: beforeErr=%v afterErr=%v", beforeErr, afterErr)
			}
		})
	}
}

func TestManagedAttachEnvFailureLeavesRegistryAbsentButNoStartStillRegisters(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte("#!/bin/sh\nexit 42\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	args := []string{"attach", "--registry", registryPath, "--adapter", "file", "--target", target,
		"--root", dir, "--base-root", filepath.Dir(dir), "--session", filepath.Base(dir), "--me", "codex", "--amq", fakeAMQ}
	var stderr bytes.Buffer
	if code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), args); code != 1 || !strings.Contains(stderr.String(), "capability check failed") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(registryPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("env failure created registry: %v", err)
	}
	args = append(args, "--no-start")
	if code := (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}).Run(context.Background(), args); code != 0 {
		t.Fatalf("--no-start code=%d", code)
	}
	if _, err := os.Stat(registryPath); err != nil {
		t.Fatalf("--no-start did not register: %v", err)
	}
}

type appWakeReply struct {
	binding amq.WakeBinding
	err     error
}

type appSequenceWake struct {
	replies []appWakeReply
	starts  []amq.StartWakeRequest
}

func (w *appSequenceWake) StartWake(_ context.Context, request amq.StartWakeRequest) (amq.WakeBinding, error) {
	w.starts = append(w.starts, request)
	if len(w.replies) == 0 {
		return amq.WakeBinding{}, errors.New("unexpected wake start")
	}
	reply := w.replies[0]
	w.replies = w.replies[1:]
	return reply.binding, reply.err
}

type appLifecycleReply struct {
	result amq.RetireWakeResult
	err    error
}

type appSequenceLifecycle struct {
	replies  []appLifecycleReply
	requests []amq.RetireWakeRequest
}

func (*appSequenceLifecycle) Env(context.Context) (amq.Env, error) {
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (l *appSequenceLifecycle) RetireWake(_ context.Context, request amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	l.requests = append(l.requests, request)
	if len(l.replies) == 0 {
		return amq.RetireWakeResult{}, errors.New("unexpected lifecycle request")
	}
	reply := l.replies[0]
	l.replies = l.replies[1:]
	return reply.result, reply.err
}

type appProbeOK struct{}

func (appProbeOK) Name() string                        { return "file" }
func (appProbeOK) Probe(context.Context, string) error { return nil }
func (appProbeOK) Inject(context.Context, string, string) error {
	return nil
}

type appOwnershipInventory map[string]string

func (i appOwnershipInventory) Probe(target string) error {
	_, err := i.OwnershipKey(target)
	return err
}

func (i appOwnershipInventory) OwnershipKey(target string) (string, error) {
	key, ok := i[target]
	if !ok {
		return "", fmt.Errorf("unknown target %q", target)
	}
	return key, nil
}

func TestPhysicalTargetPreflightIgnoresRetiredRows(t *testing.T) {
	file := registry.File{Entries: []registry.Entry{{
		ID: "retired", Root: "/tmp/old", Agent: "codex", Adapter: "file", Target: "/tmp/old-target", State: registry.StateRetired,
	}}}
	candidate := registry.Entry{ID: "candidate", Root: "/tmp/new", Agent: "claude", Adapter: "file", Target: "/tmp/new-target"}
	inventory := appOwnershipInventory{"/tmp/old-target": "same-physical-target", "/tmp/new-target": "same-physical-target"}
	if err := checkPhysicalTargetAvailable(file, appProbeOK{}, inventory, candidate, false); err != nil {
		t.Fatalf("retired row blocked target: %v", err)
	}
	file.Entries[0].State = registry.StateActive
	if err := checkPhysicalTargetAvailable(file, appProbeOK{}, inventory, candidate, false); !errors.Is(err, registry.ErrTargetOwned) {
		t.Fatalf("active row did not block target: %v", err)
	}
}

func TestRecoverDetachedRegistrationRetiresOnlyExactOwnerGoneConflict(t *testing.T) {
	dir := t.TempDir()
	store := registry.New(filepath.Join(dir, "registry.json"))
	old := registry.Entry{
		ID:   registry.EntryID(dir, "codex", "file", filepath.Join(dir, "old")),
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "old"), State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
	}
	next, err := store.Upsert(registry.Entry{
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "new"), State: registry.StateAttached,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(),
		Transition: registry.ReattachTransition{
			Phase: registry.TransitionReserved, OldID: old.ID, OldRoot: old.Root, OldAgent: old.Agent,
			OldAdapter: old.Adapter, OldTarget: old.Target, OldOwnerSet: true,
			OldOwner: old.WakeOwner, OldBinding: old.WakeBinding,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	blocking := appExactBlockingWake(old)
	wake := &appSequenceWake{replies: []appWakeReply{
		{err: blocking},
		{binding: amq.WakeBinding{Generation: "generation-2", TargetDigest: "sha256:target-2"}},
	}}
	lifecycle := &appSequenceLifecycle{replies: []appLifecycleReply{
		{result: appRetireResult(old, "eligible", "owner_gone")},
		{result: appRetireResult(old, "retired", "retired_exact")},
	}}
	reconciler := supervisor.Reconciler{Wake: wake, Adapter: appProbeOK{}, InjectVia: "/bin/sh", WakeTimeout: time.Second}
	updated, ready, err := recoverDetachedRegistration(context.Background(), store, []registry.Entry{old}, lifecycle, reconciler, next)
	if err != nil || !ready || updated.State != registry.StateActive || updated.WakeBinding.Generation != "generation-2" {
		t.Fatalf("updated=%#v ready=%v err=%v", updated, ready, err)
	}
	if len(wake.starts) != 2 || len(lifecycle.requests) != 2 || !lifecycle.requests[0].Check || lifecycle.requests[1].Check {
		t.Fatalf("starts=%#v lifecycle=%#v", wake.starts, lifecycle.requests)
	}
	for _, request := range lifecycle.requests {
		if request.Generation != old.WakeBinding.Generation || request.TargetDigest != old.WakeBinding.TargetDigest ||
			request.Target != old.Target || !request.RequireOwnerGone {
			t.Fatalf("non-exact retirement request: %#v", request)
		}
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Transition.Phase != registry.TransitionOldRetired || loaded.Entries[0].Target != next.Target {
		t.Fatalf("durable transition=%#v err=%v", loaded.Entries, err)
	}
}

func TestRecoverDetachedRegistrationNeverRetiresAnUnclassifiedStartFailure(t *testing.T) {
	dir := t.TempDir()
	store := registry.New(filepath.Join(dir, "registry.json"))
	old := registry.Entry{
		ID:   registry.EntryID(dir, "codex", "file", filepath.Join(dir, "old")),
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "old"), State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
	}
	next, err := store.Upsert(registry.Entry{
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "new"), State: registry.StateAttached,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(),
		Transition: registry.ReattachTransition{Phase: registry.TransitionReserved},
	})
	if err != nil {
		t.Fatal(err)
	}
	wake := &appSequenceWake{replies: []appWakeReply{{err: &amq.WakeStartError{
		Result: amq.WakeCommandResult{Schema: 1, Status: "failed", ReasonCode: "existing_wake_blocking"},
		Cause:  errors.New("unbound blocker result"),
	}}}}
	lifecycle := &appSequenceLifecycle{}
	reconciler := supervisor.Reconciler{Wake: wake, Adapter: appProbeOK{}, InjectVia: "/bin/sh", WakeTimeout: time.Second}
	_, ready, err := recoverDetachedRegistration(context.Background(), store, []registry.Entry{old}, lifecycle, reconciler, next)
	if err == nil || ready || !strings.Contains(err.Error(), "did not prove the persisted old wake") || len(lifecycle.requests) != 0 {
		t.Fatalf("ready=%v err=%v lifecycle=%#v", ready, err, lifecycle.requests)
	}
}

func TestRecoverDetachedRegistrationPreservesOldRetiredPhaseWhenRetryFails(t *testing.T) {
	dir := t.TempDir()
	store := registry.New(filepath.Join(dir, "registry.json"))
	old := registry.Entry{
		ID:   registry.EntryID(dir, "codex", "file", filepath.Join(dir, "old")),
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "old"), State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
	}
	next, err := store.Upsert(registry.Entry{
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "new"), State: registry.StateAttached,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(),
		Transition: registry.ReattachTransition{
			Phase: registry.TransitionReserved, OldID: old.ID, OldRoot: old.Root, OldAgent: old.Agent,
			OldAdapter: old.Adapter, OldTarget: old.Target, OldOwnerSet: true,
			OldOwner: old.WakeOwner, OldBinding: old.WakeBinding,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	blocking := appExactBlockingWake(old)
	wake := &appSequenceWake{replies: []appWakeReply{{err: blocking}, {err: errors.New("new wake failed")}}}
	lifecycle := &appSequenceLifecycle{replies: []appLifecycleReply{
		{result: appRetireResult(old, "eligible", "owner_gone")},
		{result: appRetireResult(old, "retired", "retired_exact")},
	}}
	reconciler := supervisor.Reconciler{Wake: wake, Adapter: appProbeOK{}, InjectVia: "/bin/sh", WakeTimeout: time.Second}
	updated, ready, err := recoverDetachedRegistration(context.Background(), store, []registry.Entry{old}, lifecycle, reconciler, next)
	if err == nil || ready || updated.Transition.Phase != registry.TransitionOldRetired {
		t.Fatalf("updated=%#v ready=%v err=%v", updated, ready, err)
	}
	loaded, loadErr := store.Load()
	if loadErr != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != next.Target ||
		loaded.Entries[0].Transition.Phase != registry.TransitionOldRetired {
		t.Fatalf("old-retired transition=%#v err=%v", loaded.Entries, loadErr)
	}
}

func TestRecoverDetachedRegistrationPreservesPendingPhaseOnAmbiguousRetirement(t *testing.T) {
	dir := t.TempDir()
	store := registry.New(filepath.Join(dir, "registry.json"))
	old := registry.Entry{
		ID:   registry.EntryID(dir, "codex", "file", filepath.Join(dir, "old")),
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "old"), State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
	}
	next, err := store.Upsert(registry.Entry{
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "new"), State: registry.StateAttached,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(),
		Transition: registry.ReattachTransition{
			Phase: registry.TransitionReserved, OldID: old.ID, OldRoot: old.Root, OldAgent: old.Agent,
			OldAdapter: old.Adapter, OldTarget: old.Target, OldOwnerSet: true,
			OldOwner: old.WakeOwner, OldBinding: old.WakeBinding,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	blocking := appExactBlockingWake(old)
	wake := &appSequenceWake{replies: []appWakeReply{{err: blocking}}}
	lifecycle := &appSequenceLifecycle{replies: []appLifecycleReply{
		{result: appRetireResult(old, "eligible", "owner_gone")},
		{err: errors.New("retirement response lost")},
	}}
	reconciler := supervisor.Reconciler{Wake: wake, Adapter: appProbeOK{}, InjectVia: "/bin/sh", WakeTimeout: time.Second}
	updated, ready, err := recoverDetachedRegistration(context.Background(), store, []registry.Entry{old}, lifecycle, reconciler, next)
	if err == nil || ready || updated.Transition.Phase != registry.TransitionRetirePending {
		t.Fatalf("updated=%#v ready=%v err=%v", updated, ready, err)
	}
	loaded, loadErr := store.Load()
	if loadErr != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != next.Target ||
		loaded.Entries[0].Transition.Phase != registry.TransitionRetirePending {
		t.Fatalf("pending transition=%#v err=%v", loaded.Entries, loadErr)
	}
}

func TestRecoverDetachedRegistrationDoesNotTreatBackoffDeferralAsReady(t *testing.T) {
	dir := t.TempDir()
	store := registry.New(filepath.Join(dir, "registry.json"))
	old := registry.Entry{
		ID:   registry.EntryID(dir, "codex", "file", filepath.Join(dir, "old")),
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "old"), State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
	}
	next, err := store.Upsert(registry.Entry{
		Root: dir, Agent: "codex", Adapter: "file", Target: filepath.Join(dir, "new"), State: registry.StateAttached,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), BackoffUntil: time.Now().Add(time.Hour),
		Transition: registry.ReattachTransition{
			Phase: registry.TransitionOldRetired, OldID: old.ID, OldRoot: old.Root, OldAgent: old.Agent,
			OldAdapter: old.Adapter, OldTarget: old.Target, OldOwnerSet: true,
			OldOwner: old.WakeOwner, OldBinding: old.WakeBinding,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wake := &appSequenceWake{}
	lifecycle := &appSequenceLifecycle{}
	reconciler := supervisor.Reconciler{Wake: wake, Adapter: appProbeOK{}, InjectVia: "/bin/sh", WakeTimeout: time.Second}
	updated, ready, err := recoverDetachedRegistration(context.Background(), store, []registry.Entry{old}, lifecycle, reconciler, next)
	if err == nil || ready || updated.Transition.Phase != registry.TransitionOldRetired || len(wake.starts) != 0 || len(lifecycle.requests) != 0 {
		t.Fatalf("updated=%#v ready=%v err=%v starts=%#v lifecycle=%#v", updated, ready, err, wake.starts, lifecycle.requests)
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
	if _, err := store.Upsert(registry.Entry{Root: "/tmp/first", Agent: "codex", Adapter: "cmux", Target: firstTarget}); err != nil {
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
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript(`#!/bin/sh
printf 'wake\n' >> "$AMQ_KEEPALIVE_AMQ_CALLS"
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
umask 077
printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$ready"
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
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript("#!/bin/sh\necho 'existing wake target differs' >&2\nexit 7\n"), 0o700); err != nil {
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
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript(`#!/bin/sh
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
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript(`#!/bin/sh
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
umask 077
printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$ready"
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
	exited := filepath.Join(dir, "exited")
	t.Setenv("AMQ_KEEPALIVE_STARTED", started)
	t.Setenv("AMQ_KEEPALIVE_ALLOW_READY", allowReady)
	t.Setenv("AMQ_KEEPALIVE_LATE_READY", lateReady)
	t.Setenv("AMQ_KEEPALIVE_RELEASE", release)
	t.Setenv("AMQ_KEEPALIVE_EXITED", exited)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript(`#!/bin/sh
ready=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then ready="$arg"; fi
  previous="$arg"
done
[ -n "$ready" ] || exit 11
: > "$AMQ_KEEPALIVE_STARTED"
while [ ! -f "$AMQ_KEEPALIVE_ALLOW_READY" ]; do sleep 0.01; done
umask 077
printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$ready"
: > "$AMQ_KEEPALIVE_LATE_READY"
while [ ! -f "$AMQ_KEEPALIVE_RELEASE" ]; do sleep 0.01; done
: > "$AMQ_KEEPALIVE_EXITED"
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
	waitForPath(t, exited, 2*time.Second)

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

func TestReattachRefusesLegacyUnboundOldWakeAndRestoresOldRow(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "probe-room")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	newTarget := "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2"
	store := registry.New(registryPath)
	if _, err := store.Upsert(registry.Entry{
		Root: root, BaseRoot: dir, SessionName: "probe-room", Agent: "codex",
		Adapter: "cmux", Target: oldTarget, State: registry.StateDetached,
	}); err != nil {
		t.Fatalf("Upsert old entry: %v", err)
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\necho '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2\",\"tty\":\"ttys102\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	argsLog := filepath.Join(dir, "amq-args.log")
	firstStart := filepath.Join(dir, "first-start")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	t.Setenv("AMQ_KEEPALIVE_FIRST_START", firstStart)
	t.Setenv("AMQ_KEEPALIVE_TEST_ROOT", root)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript(`#!/bin/sh
printf 'CALL %s\n' "$*" >> "$AMQ_KEEPALIVE_ARGS_LOG"
if [ "$1" = "wake" ] && [ "${2:-}" = "retire" ]; then
  echo '{"status":"retired","agent":"codex","pid":4242}'
  exit 0
fi
if [ ! -f "$AMQ_KEEPALIVE_FIRST_START" ]; then
  : > "$AMQ_KEEPALIVE_FIRST_START"
  result=""
  previous=""
  for arg in "$@"; do
    if [ "$previous" = "--result-file" ]; then result="$arg"; fi
    previous="$arg"
  done
  [ -n "$result" ] || exit 12
  umask 077
  printf '{"schema":1,"status":"failed","reason_code":"existing_wake_blocking","root":"%s","agent":"codex","current_generation":"generation-1","current_target_digest":"sha256:target-1","current_wake_mode":"owner_bound"}\n' "$AMQ_KEEPALIVE_TEST_ROOT" > "$result"
  echo 'existing wake target differs' >&2
  exit 7
fi
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then umask 077; printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$arg"; fi
  previous="$arg"
done
`), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	fakeKeepalive := filepath.Join(dir, "amq-keepalive")
	if err := os.WriteFile(fakeKeepalive, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write fake keepalive: %v", err)
	}
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{"reattach",
		"--registry", registryPath,
		"--adapter", "cmux",
		"--target", newTarget,
		"--root", root,
		"--base-root", dir,
		"--session", "probe-room",
		"--me", "codex",
		"--amq", fakeAMQ,
		"--self", fakeKeepalive,
		"--retire-detached",
	})
	if code != 1 || !strings.Contains(stderr.String(), "lacks owner-bound generation metadata") {
		t.Fatalf("code=%d stderr=%s, want legacy-unbound safety gate", code, stderr.String())
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Target != oldTarget {
		t.Fatalf("entries = %#v, want old row restored", loaded.Entries)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	log := string(data)
	if strings.Count(log, "CALL wake -root") != 1 || strings.Contains(log, "wake retire") || !strings.Contains(log, newTarget) {
		t.Fatalf("retirement gate must make one non-destructive start attempt and no retire call:\n%s", log)
	}
}

func TestReattachRetireDetachedStartsWhenOldTargetMissingAndWakeLockAlreadyAbsent(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "missing-lock-room")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	newTarget := "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2"
	store := registry.New(registryPath)
	if _, err := store.Upsert(registry.Entry{
		Root: root, BaseRoot: dir, SessionName: "missing-lock-room", Agent: "codex",
		Adapter: "cmux", Target: oldTarget, State: registry.StateDetached,
	}); err != nil {
		t.Fatalf("Upsert old entry: %v", err)
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\necho '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2\",\"tty\":\"ttys102\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	argsLog := filepath.Join(dir, "amq-args.log")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript(`#!/bin/sh
printf 'CALL %s\n' "$*" >> "$AMQ_KEEPALIVE_ARGS_LOG"
if [ "$1" = "wake" ] && [ "${2:-}" = "retire" ]; then
  echo '{"status":"refused","reason":"no wake lock present; wake process absence cannot be proven"}'
  exit 1
fi
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then umask 077; printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$arg"; fi
  previous="$arg"
done
`), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	fakeKeepalive := filepath.Join(dir, "amq-keepalive")
	if err := os.WriteFile(fakeKeepalive, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write fake keepalive: %v", err)
	}
	runApp(t, "reattach",
		"--registry", registryPath,
		"--adapter", "cmux",
		"--target", newTarget,
		"--root", root,
		"--base-root", dir,
		"--session", "missing-lock-room",
		"--me", "codex",
		"--amq", fakeAMQ,
		"--self", fakeKeepalive,
		"--retire-detached",
	)
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Target != newTarget || loaded.Entries[0].State != registry.StateActive {
		t.Fatalf("entries = %#v, want one active new target", loaded.Entries)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	log := string(data)
	if strings.Contains(log, "wake retire") {
		t.Fatalf("already-absent wake should start directly without retirement:\n%s", log)
	}
	if starts := strings.Count(log, "CALL wake -root"); starts != 1 {
		t.Fatalf("wake starts = %d, want 1:\n%s", starts, log)
	}
}

func TestReattachDoesNotRetryThroughLegacyUnboundRetirement(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "retire-race-room")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	newTarget := "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2"
	store := registry.New(registryPath)
	if _, err := store.Upsert(registry.Entry{
		Root: root, BaseRoot: dir, SessionName: "retire-race-room", Agent: "codex",
		Adapter: "cmux", Target: oldTarget, State: registry.StateDetached,
	}); err != nil {
		t.Fatalf("Upsert old entry: %v", err)
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\necho '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2\",\"tty\":\"ttys102\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	argsLog := filepath.Join(dir, "amq-args.log")
	firstStart := filepath.Join(dir, "first-start")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	t.Setenv("AMQ_KEEPALIVE_FIRST_START", firstStart)
	t.Setenv("AMQ_KEEPALIVE_TEST_ROOT", root)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript(`#!/bin/sh
printf 'CALL %s\n' "$*" >> "$AMQ_KEEPALIVE_ARGS_LOG"
if [ "$1" = "wake" ] && [ "${2:-}" = "retire" ]; then
  echo '{"status":"refused","reason":"no wake lock present; wake process absence cannot be proven"}'
  exit 1
fi
if [ ! -f "$AMQ_KEEPALIVE_FIRST_START" ]; then
  : > "$AMQ_KEEPALIVE_FIRST_START"
  result=""
  previous=""
  for arg in "$@"; do
    if [ "$previous" = "--result-file" ]; then result="$arg"; fi
    previous="$arg"
  done
  [ -n "$result" ] || exit 12
  umask 077
  printf '{"schema":1,"status":"failed","reason_code":"existing_wake_blocking","root":"%s","agent":"codex","current_generation":"generation-1","current_target_digest":"sha256:target-1","current_wake_mode":"owner_bound"}\n' "$AMQ_KEEPALIVE_TEST_ROOT" > "$result"
  echo 'existing wake target differs' >&2
  exit 7
fi
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then umask 077; printf '{"schema":1,"generation":"generation-1","target_digest":"sha256:target-1"}\n' > "$arg"; fi
  previous="$arg"
done
`), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	fakeKeepalive := filepath.Join(dir, "amq-keepalive")
	if err := os.WriteFile(fakeKeepalive, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write fake keepalive: %v", err)
	}
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{"reattach",
		"--registry", registryPath,
		"--adapter", "cmux",
		"--target", newTarget,
		"--root", root,
		"--base-root", dir,
		"--session", "retire-race-room",
		"--me", "codex",
		"--amq", fakeAMQ,
		"--self", fakeKeepalive,
		"--retire-detached",
	})
	if code != 1 || !strings.Contains(stderr.String(), "lacks owner-bound generation metadata") {
		t.Fatalf("code=%d stderr=%s, want legacy-unbound safety gate", code, stderr.String())
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Target != oldTarget {
		t.Fatalf("entries = %#v, want old row restored", loaded.Entries)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	log := string(data)
	if starts := strings.Count(log, "CALL wake -root"); starts != 1 {
		t.Fatalf("wake starts = %d, want one non-destructive attempt:\n%s", starts, log)
	}
	if strings.Contains(log, "wake retire") {
		t.Fatalf("unsafe wake retire was invoked:\n%s", log)
	}
}

func TestReattachRetireDetachedNeverRetargetsLiveCmuxWake(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "active-room")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	newTarget := "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2"
	store := registry.New(registryPath)
	if _, err := store.Upsert(registry.Entry{Root: root, BaseRoot: dir, SessionName: "active-room", Agent: "codex", Adapter: "cmux", Target: oldTarget}); err != nil {
		t.Fatalf("Upsert old entry: %v", err)
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\necho '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"F901D722-6789-4BBB-9818-C4E97F20BEB3\",\"tty\":\"ttys101\"},{\"id\":\"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2\",\"tty\":\"ttys102\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	argsLog := filepath.Join(dir, "amq-args.log")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, appManagedAMQScript("#!/bin/sh\nprintf 'CALL %s\\n' \"$*\" >> \"$AMQ_KEEPALIVE_ARGS_LOG\"\necho 'existing wake target differs' >&2\nexit 7\n"), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"reattach", "--registry", registryPath, "--adapter", "cmux", "--target", newTarget,
		"--root", root, "--base-root", dir, "--session", "active-room", "--me", "codex",
		"--amq", fakeAMQ, "--self", filepath.Join(dir, "amq-keepalive"), "--retire-detached",
	})
	if code != 1 || !strings.Contains(stderr.String(), "amq wake exited before becoming ready") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != oldTarget {
		t.Fatalf("live target registry changed: entries=%#v err=%v", loaded.Entries, err)
	}
	data, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	if strings.Contains(string(data), "wake retire") {
		t.Fatalf("live old target was retired:\n%s", data)
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

func TestRetireSessionHardGatePreservesAllEntriesAndInvokesNoAMQ(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "dashboard")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	entries := []registry.Entry{
		{Root: root, BaseRoot: dir, SessionName: "dashboard", Agent: "codex", Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", State: registry.StateDetached},
		{Root: root, BaseRoot: dir, SessionName: "dashboard", Agent: "claude", Adapter: "cmux", Target: "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2", State: registry.StateDetached},
		{Root: root, BaseRoot: dir, SessionName: "dashboard", Agent: "observer", Adapter: "file", Target: filepath.Join(dir, "observer.txt"), State: registry.StateActive},
	}
	for _, entry := range entries {
		if _, err := store.Upsert(entry); err != nil {
			t.Fatalf("Upsert(%s): %v", entry.Agent, err)
		}
	}

	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\necho '{\"windows\":[]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	argsLog := filepath.Join(dir, "amq-args.log")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
printf '%s\n' "$@" >> "$AMQ_KEEPALIVE_ARGS_LOG"
agent=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-me" ]; then agent="$arg"; fi
  previous="$arg"
done
printf '{"status":"retired","agent":"%s","pid":4242}\n' "$agent"
`), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	fakeKeepalive := filepath.Join(dir, "amq-keepalive")
	if err := os.WriteFile(fakeKeepalive, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write fake keepalive: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), []string{
		"retire-session",
		"--registry", registryPath,
		"--root", root,
		"--agents", "codex,claude",
		"--adapter", "cmux",
		"--amq", fakeAMQ,
		"--self", fakeKeepalive,
	})
	if code != 1 || !strings.Contains(stderr.String(), "wake_gc_v1") {
		t.Fatalf("retire-session code=%d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Entries) != 3 {
		t.Fatalf("entries after gated retirement = %#v, want all preserved", loaded.Entries)
	}
	if data, err := os.ReadFile(argsLog); err == nil && len(data) > 0 {
		t.Fatalf("gated retire-session invoked AMQ: %s", data)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect AMQ log: %v", err)
	}
}

func TestRetireSessionRefusesWhenTargetStillExists(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "dashboard")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	for _, entry := range []registry.Entry{
		{Root: root, Agent: "codex", Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", State: registry.StateDetached},
		{Root: root, Agent: "claude", Adapter: "cmux", Target: "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2", State: registry.StateDetached},
	} {
		if _, err := store.Upsert(entry); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\necho '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"F901D722-6789-4BBB-9818-C4E97F20BEB3\",\"tty\":\"ttys101\"},{\"id\":\"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2\",\"tty\":\"ttys102\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte("#!/bin/sh\nexit 99\n"), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"retire-session", "--registry", registryPath, "--root", root, "--amq", fakeAMQ,
	})
	if code != 1 || !strings.Contains(stderr.String(), "still exists") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 2 {
		t.Fatalf("registry changed after refusal: entries=%#v err=%v", loaded.Entries, err)
	}
}

func TestRetireSessionGatePreventsPartialRetirement(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "dashboard")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("MkdirAll root: %v", err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	for _, entry := range []registry.Entry{
		{Root: root, Agent: "codex", Adapter: "cmux", Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", State: registry.StateDetached},
		{Root: root, Agent: "claude", Adapter: "cmux", Target: "cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2", State: registry.StateDetached},
	} {
		if _, err := store.Upsert(entry); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\necho '{\"windows\":[]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	argsLog := filepath.Join(dir, "amq-args.log")
	t.Setenv("AMQ_KEEPALIVE_ARGS_LOG", argsLog)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
printf '%s\n' "$@" >> "$AMQ_KEEPALIVE_ARGS_LOG"
agent=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-me" ]; then agent="$arg"; fi
  previous="$arg"
done
if [ "$agent" = "claude" ]; then
  printf '%s\n' '{"status":"refused","agent":"claude","reason":"target changed"}'
  exit 7
fi
printf '%s\n' '{"status":"retired","agent":"codex","pid":4242}'
`), 0o700); err != nil {
		t.Fatalf("write fake AMQ: %v", err)
	}
	fakeKeepalive := filepath.Join(dir, "amq-keepalive")
	if err := os.WriteFile(fakeKeepalive, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write fake keepalive: %v", err)
	}
	var stderr bytes.Buffer
	code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"retire-session", "--registry", registryPath, "--root", root,
		"--amq", fakeAMQ, "--self", fakeKeepalive,
	})
	if code != 1 || !strings.Contains(stderr.String(), "identity-safe-retire") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 2 {
		t.Fatalf("retirement gate changed registry: entries=%#v err=%v", loaded.Entries, err)
	}
	if data, err := os.ReadFile(argsLog); err == nil && len(data) > 0 {
		t.Fatalf("retirement gate invoked AMQ: %s", data)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect AMQ log: %v", err)
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

func (appFailingWake) Env(context.Context) (amq.Env, error) {
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (appFailingWake) RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	return amq.RetireWakeResult{}, errors.New("unexpected retirement")
}

func (appFailingWake) RepairWake(context.Context, string, string) (amq.WakeRepairResult, error) {
	return amq.WakeRepairResult{Status: "refused", Reason: "unverified wake lock; refusing repair"}, errors.New("exit status 1")
}

func (appFailingWake) StartWake(context.Context, amq.StartWakeRequest) (amq.WakeBinding, error) {
	return amq.WakeBinding{}, errors.New("unverified wake lock; refusing second injector")
}

func TestSuperviseWarnsOncePerPersistentFailureTransition(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	_, err := registry.New(registryPath).Upsert(registry.Entry{
		Root:             "/tmp/amq-root",
		Agent:            "codex",
		Adapter:          "file",
		Target:           filepath.Join(dir, "inbox.txt"),
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
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

func (*appCountingWake) Env(context.Context) (amq.Env, error) {
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (*appCountingWake) RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	return amq.RetireWakeResult{}, errors.New("unexpected retirement")
}

func (w *appCountingWake) StartWake(_ context.Context, req amq.StartWakeRequest) (amq.WakeBinding, error) {
	w.starts = append(w.starts, req)
	return amq.WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"}, nil
}

type appCancelingWake struct {
	cancel context.CancelFunc
	starts int
}

func (*appCancelingWake) Env(context.Context) (amq.Env, error) {
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (*appCancelingWake) RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	return amq.RetireWakeResult{}, errors.New("unexpected retirement")
}

func (w *appCancelingWake) StartWake(ctx context.Context, _ amq.StartWakeRequest) (amq.WakeBinding, error) {
	w.starts++
	w.cancel()
	return amq.WakeBinding{}, ctx.Err()
}

type appCapabilityGateWake struct {
	capability bool
	envCalls   int
	starts     int
}

func (w *appCapabilityGateWake) Env(context.Context) (amq.Env, error) {
	w.envCalls++
	var capabilities []string
	if w.capability {
		capabilities = []string{amq.CapabilityWakeGCV1}
	}
	return amq.Env{Capabilities: capabilities}, nil
}

func (w *appCapabilityGateWake) StartWake(context.Context, amq.StartWakeRequest) (amq.WakeBinding, error) {
	w.starts++
	return amq.WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"}, nil
}

func (*appCapabilityGateWake) RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	return amq.RetireWakeResult{}, errors.New("unexpected retirement")
}

func TestSupervisorAutoGCDisabledGatesStartWakeOnCapability(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := filepath.Join(dir, "inbox")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.New(registryPath).Upsert(registry.Entry{
		Root: dir, Agent: "codex", Adapter: "file", Target: target,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
	}); err != nil {
		t.Fatal(err)
	}

	blocked := &appCapabilityGateWake{}
	if _, err := (App{Stdout: io.Discard, Stderr: io.Discard}).superviseOnceWithGCState(
		context.Background(), registryPath, blocked, "/bin/amq-keepalive", time.Second, supervisor.GCPolicy{},
	); err == nil || blocked.envCalls != 1 || blocked.starts != 0 {
		t.Fatalf("blocked capability: env=%d starts=%d err=%v", blocked.envCalls, blocked.starts, err)
	}

	allowed := &appCapabilityGateWake{capability: true}
	if _, err := (App{Stdout: io.Discard, Stderr: io.Discard}).superviseOnceWithGCState(
		context.Background(), registryPath, allowed, "/bin/amq-keepalive", time.Second, supervisor.GCPolicy{},
	); err != nil || allowed.envCalls != 1 || allowed.starts != 1 {
		t.Fatalf("allowed capability: env=%d starts=%d err=%v", allowed.envCalls, allowed.starts, err)
	}
}

func TestSuperviseLoopKeepsCatchUpDelayAfterPendingBatchError(t *testing.T) {
	wantErr := errors.New("durable batch failed")
	var waits []time.Duration
	err := runSuperviseLoop(context.Background(), time.Hour, io.Discard,
		func(bool) (supervisionPass, error) {
			return supervisionPass{AttemptedRoot: "/tmp/root", PendingGCRoots: true}, wantErr
		},
		func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return context.Canceled
		},
	)
	if err != nil || len(waits) != 1 || waits[0] != supervisor.GCCatchUpInterval {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
}

func TestSuperviseLoopUsesCatchUpDelayForEarlyFailureWithoutBatchState(t *testing.T) {
	wantErr := errors.New("registry could not be loaded")
	var waits []time.Duration
	err := runSuperviseLoop(context.Background(), time.Hour, io.Discard,
		func(bool) (supervisionPass, error) { return supervisionPass{}, wantErr },
		func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return context.Canceled
		},
	)
	if err != nil || len(waits) != 1 || waits[0] != supervisor.GCCatchUpInterval {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
}

type failingDiagnosticWriter struct{ err error }

func (w failingDiagnosticWriter) Write([]byte) (int, error) { return 0, w.err }

func TestSuperviseLoopReturnsDiagnosticWriteFailure(t *testing.T) {
	passErr := errors.New("supervise failed")
	writeErr := errors.New("diagnostic sink failed")
	waited := false
	err := runSuperviseLoop(context.Background(), time.Hour, failingDiagnosticWriter{err: writeErr},
		func(bool) (supervisionPass, error) { return supervisionPass{}, passErr },
		func(context.Context, time.Duration) error { waited = true; return context.Canceled },
	)
	if !errors.Is(err, passErr) || !errors.Is(err, writeErr) || waited {
		t.Fatalf("err=%v waited=%v", err, waited)
	}
}

func TestGCTransitionDiagnosticWriteFailureIsReturnedWithoutPersistingObservation(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	entry := registry.Entry{
		ID: "entry", Root: filepath.Join(dir, "root"), Agent: "worker", Adapter: "file", Target: target, State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
	}
	if err := registry.New(registryPath).Save(registry.File{Entries: []registry.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	writeErr := errors.New("diagnostic sink failed")
	_, err := (App{Stdout: io.Discard, Stderr: failingDiagnosticWriter{err: writeErr}}).superviseOnceWithGCState(
		context.Background(), registryPath, &appGCWake{}, "/bin/sh", time.Second,
		supervisor.GCPolicy{AutoGC: true, OwnerGrace: supervisor.MinOwnerGoneGrace, RetiredRetention: supervisor.MinRetiredRetention, Timeout: time.Second},
	)
	if !errors.Is(err, writeErr) || !strings.Contains(err.Error(), "GC transition diagnostic") {
		t.Fatalf("err=%v", err)
	}
	loaded, loadErr := registry.New(registryPath).Load()
	if loadErr != nil || !loaded.Entries[0].OwnerGoneSince.IsZero() {
		t.Fatalf("diagnostic failure persisted observation: entry=%#v err=%v", loaded.Entries[0], loadErr)
	}
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
			Root: fmt.Sprintf("/tmp/cancel-%d", index), Agent: "codex", Adapter: "file", Target: target,
			WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
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
		if _, err := store.Upsert(registry.Entry{Root: filepath.Join(dir, string(rune('a'+i))), Agent: "codex", Adapter: "cmux", Target: target,
			WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding()}); err != nil {
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
		Target: "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3", State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
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
		{ID: registry.EntryID("/tmp/one", "codex", "cmux", upper), Root: "/tmp/one", Agent: "codex", Adapter: "cmux", Target: upper, State: registry.StateActive, WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding()},
		{ID: registry.EntryID("/tmp/two", "claude", "cmux", lower), Root: "/tmp/two", Agent: "claude", Adapter: "cmux", Target: lower, State: registry.StateActive, WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding()},
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

func TestGCDryRunLeavesLegacyUnboundRegistryUntouched(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	store := registry.New(registryPath)
	entry, err := store.Upsert(registry.Entry{
		Root: "/tmp/old-session", Agent: "codex", Adapter: "cmux", Target: target,
		State: registry.StateDetached, DetachedSince: time.Now().Add(-48 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	calls := filepath.Join(dir, "cmux-calls.log")
	t.Setenv("AMQ_KEEPALIVE_CMUX_CALLS", calls)
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AMQ_KEEPALIVE_CMUX_CALLS\"\nprintf '%s\\n' '{\"windows\":[]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	var dry bytes.Buffer
	code := (App{Stdout: &dry, Stderr: &bytes.Buffer{}}).Run(context.Background(), []string{
		"gc", "--registry", registryPath, "--min-detached-age", "5m",
	})
	if code != 0 || !strings.Contains(dry.String(), `"status": "skipped"`) || !strings.Contains(dry.String(), `"applied": false`) {
		t.Fatalf("dry-run code=%d output=%s", code, dry.String())
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].ID != entry.ID {
		t.Fatalf("dry-run mutated registry: entries=%#v err=%v", loaded.Entries, err)
	}

	amqCalls := filepath.Join(dir, "amq-calls.log")
	t.Setenv("AMQ_KEEPALIVE_AMQ_CALLS", amqCalls)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$AMQ_KEEPALIVE_AMQ_CALLS"
if [ "$1" = "env" ]; then
  printf '%s\n' '{"schema_version":1,"amq_version":"test","root":"/tmp/root","base_root":"/tmp","session_name":"root","in_session":true,"me":"worker","project":"test","root_source":"flag","peers":{},"capabilities":["wake_gc_v1"]}'
  exit 0
fi
printf '%s\n' '{"status":"retired","agent":"codex","pid":4242}'
`), 0o700); err != nil {
		t.Fatalf("write fake amq: %v", err)
	}
	var applied bytes.Buffer
	var applyErr bytes.Buffer
	code = (App{Stdout: &applied, Stderr: &applyErr}).Run(context.Background(), []string{
		"gc", "--registry", registryPath, "--min-detached-age", "5m", "--apply",
		"--amq", fakeAMQ, "--self", "/bin/amq-keepalive",
	})
	if code != 0 || !strings.Contains(applied.String(), `"status": "skipped"`) || applyErr.Len() != 0 {
		t.Fatalf("apply code=%d stdout=%s stderr=%s", code, applied.String(), applyErr.String())
	}
	loaded, err = store.Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].ID != entry.ID {
		t.Fatalf("apply registry entries=%#v err=%v", loaded.Entries, err)
	}
	if data, err := os.ReadFile(amqCalls); err != nil || strings.Contains(string(data), "wake retire") {
		t.Fatalf("legacy entry reached wake retirement: data=%q err=%v", data, err)
	}
	if data, err := os.ReadFile(calls); err == nil && len(data) > 0 {
		t.Fatalf("GC consulted terminal presence even though owner identity is authoritative: %s", data)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect cmux calls: %v", err)
	}
}

func TestGCPolicyCLIRejectsUnsafeOverrides(t *testing.T) {
	if err := validateGCPolicy(5*time.Minute, 24*time.Hour, 5*time.Second); err != nil {
		t.Fatalf("safe boundary rejected: %v", err)
	}
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "gc-grace", args: []string{"gc", "--owner-gone-grace", "4m59s"}, want: "at least 5m0s"},
		{name: "gc-retention", args: []string{"gc", "--retired-retention", "23h59m"}, want: "at least 24h0m0s"},
		{name: "gc-cap-removed", args: []string{"gc", "--max-retirements", "2"}, want: "flag provided but not defined"},
		{name: "gc-timeout", args: []string{"gc", "--timeout", "6s"}, want: "(0,5s]"},
		{name: "supervisor", args: []string{"supervise", "--once", "--owner-gone-grace", "1s"}, want: "at least 5m0s"},
		{name: "launchd", args: []string{"install-launchd", "--no-load", "--retired-retention", "1h"}, want: "at least 24h0m0s"},
		{name: "launchd-legacy-cap-invalid", args: []string{"install-launchd", "--no-load", "--gc-max-per-pass", "0"}, want: "must be greater than zero"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			if code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), test.args); code != 1 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("code=%d stderr=%s want=%q", code, stderr.String(), test.want)
			}
		})
	}
}

func TestSuperviseAcceptsButIgnoresLegacyLaunchdGCMaxFlag(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), []string{
		"supervise", "--once", "--registry", registryPath, "--gc-max-per-pass", "999",
	})
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("legacy plist argv code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if code := (App{Stdout: &bytes.Buffer{}, Stderr: &stderr}).Run(context.Background(), []string{
		"supervise", "--once", "--registry", registryPath, "--gc-max-per-pass", "0",
	}); code != 1 || !strings.Contains(stderr.String(), "must be greater than zero") {
		t.Fatalf("invalid legacy cap code=%d stderr=%s", code, stderr.String())
	}
}

func TestInstallLaunchdAcceptsButOmitsLegacyGCMaxFlag(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	plistPath := filepath.Join(dir, "LaunchAgents", "compat.plist")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), []string{
		"install-launchd", "--no-load", "--label", "com.example.compat", "--plist", plistPath,
		"--registry", filepath.Join(dir, "registry.json"), "--self", "/bin/sh", "--amq", "/bin/sh",
		"--gc-max-per-pass", "999",
	})
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("legacy install argv code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	plist, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(plist, []byte("--gc-max-per-pass")) {
		t.Fatalf("new plist retained deprecated cap:\n%s", plist)
	}
}

func TestGCDryRunPreviewsV1WithoutAnyFilesystemMutation(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	v1 := []byte(`{"schema_version":1,"entries":[{"id":"legacy","root":"/tmp/root","agent":"codex","adapter":"file","target":"/tmp/target","state":"active"}]}` + "\n")
	if err := os.WriteFile(registryPath, v1, 0o640); err != nil {
		t.Fatal(err)
	}
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte("#!/bin/sh\nprintf '%s\\n' '{\"schema_version\":1,\"amq_version\":\"test\",\"root\":\"/tmp/root\",\"base_root\":\"/tmp\",\"session_name\":\"root\",\"in_session\":true,\"me\":\"worker\",\"project\":\"test\",\"root_source\":\"flag\",\"peers\":{},\"capabilities\":[\"wake_gc_v1\"]}'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	beforeEntries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	beforeNames := make([]string, 0, len(beforeEntries))
	for _, entry := range beforeEntries {
		beforeNames = append(beforeNames, entry.Name())
	}
	beforeInfo, err := os.Stat(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &bytes.Buffer{}}).Run(context.Background(), []string{
		"gc", "--registry", registryPath, "--amq", fakeAMQ,
	})
	if code != 0 || !strings.Contains(stdout.String(), `"applied": false`) || !strings.Contains(stdout.String(), `"owner_unbound"`) {
		t.Fatalf("code=%d output=%s", code, stdout.String())
	}
	after, err := os.ReadFile(registryPath)
	if err != nil || !bytes.Equal(after, v1) {
		t.Fatalf("v1 registry changed: data=%q err=%v", after, err)
	}
	afterInfo, err := os.Stat(registryPath)
	if err != nil || afterInfo.Mode() != beforeInfo.Mode() {
		t.Fatalf("registry mode changed: before=%v after=%v err=%v", beforeInfo.Mode(), afterInfo.Mode(), err)
	}
	afterEntries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	afterNames := make([]string, 0, len(afterEntries))
	for _, entry := range afterEntries {
		afterNames = append(afterNames, entry.Name())
	}
	if !reflect.DeepEqual(beforeNames, afterNames) {
		t.Fatalf("dry-run created filesystem artifacts: before=%v after=%v", beforeNames, afterNames)
	}
}

func TestGCDryRunNeverUsesTerminalPresenceAsRetirementProof(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("cmux adapter requires macOS")
	}
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	for index, target := range []string{
		"cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3",
		"cmux:surface:B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2",
	} {
		if _, err := store.Upsert(registry.Entry{
			Root: fmt.Sprintf("/tmp/gc-collision-%d", index), Agent: fmt.Sprintf("agent-%d", index),
			Adapter: "cmux", Target: target, State: registry.StateDetached,
			DetachedSince: time.Now().Add(-48 * time.Hour),
		}); err != nil {
			t.Fatalf("Upsert collision row: %v", err)
		}
	}
	fakeCmux := filepath.Join(dir, "cmux")
	if err := os.WriteFile(fakeCmux, []byte("#!/bin/sh\nprintf '%s\\n' '{\"windows\":[{\"workspaces\":[{\"panes\":[{\"surfaces\":[{\"id\":\"F901D722-6789-4BBB-9818-C4E97F20BEB3\",\"tty\":\"ttys011\"},{\"id\":\"B8A8C4A7-3C88-4DAD-93BE-97E9701D07D2\",\"tty\":\"/dev/ttys011\"}]}]}]}]}'\n"), 0o700); err != nil {
		t.Fatalf("write fake cmux: %v", err)
	}
	t.Setenv("CMUX_BUNDLED_CLI_PATH", fakeCmux)
	var stdout bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &bytes.Buffer{}}).Run(context.Background(), []string{
		"gc", "--registry", registryPath, "--min-detached-age", "5m",
	})
	if code != 0 || strings.Count(stdout.String(), `"status": "skipped"`) != 2 {
		t.Fatalf("code=%d output=%s, want both legacy aliases skipped", code, stdout.String())
	}
}

func TestGCApplySurfacesCapabilityProbeFailureWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	called := filepath.Join(dir, "amq-called")
	t.Setenv("AMQ_KEEPALIVE_CALLED", called)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
: > "$AMQ_KEEPALIVE_CALLED"
exit 99
`), 0o700); err != nil {
		t.Fatalf("write fake amq: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), []string{
		"gc", "--registry", registryPath, "--min-detached-age", "5m", "--apply", "--amq", fakeAMQ,
	})
	if code != 1 || !strings.Contains(stdout.String(), `"capability_error"`) || !strings.Contains(stderr.String(), "capability check failed") {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(called); err != nil {
		t.Fatalf("capability probe did not invoke AMQ env: %v", err)
	}
}

func TestGCApplyRetiresOwnerGoneWakeEvenWhenTerminalTargetExists(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := filepath.Join(dir, "terminal-still-open")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	entry, err := registry.New(registryPath).Upsert(registry.Entry{
		Root: dir, Agent: "codex", Adapter: "file", Target: target, State: registry.StateActive,
		WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(), WakeBinding: appTestWakeBinding(),
		OwnerGoneSince: time.Now().Add(-10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	callLog := filepath.Join(dir, "amq-calls")
	t.Setenv("AMQ_KEEPALIVE_TEST_ROOT", dir)
	t.Setenv("AMQ_KEEPALIVE_AMQ_CALLS", callLog)
	fakeAMQ := filepath.Join(dir, "amq")
	if err := os.WriteFile(fakeAMQ, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$AMQ_KEEPALIVE_AMQ_CALLS"
if [ "$1" = "env" ]; then
  printf '%s\n' '{"schema_version":1,"amq_version":"test","root":"/tmp/root","base_root":"/tmp","session_name":"root","in_session":true,"me":"worker","project":"test","root_source":"flag","peers":{},"capabilities":["wake_gc_v1"]}'
  exit 0
fi
status=retired
reason=retired_exact
for arg in "$@"; do
  if [ "$arg" = "--check" ]; then status=eligible; reason=owner_gone; fi
done
printf '{"schema":1,"status":"%s","reason_code":"%s","root":"%s","agent":"codex","lock":"%s/agents/codex/.wake.lock","target":"%s","generation":"generation-1","target_digest":"sha256:target-1"}\n' "$status" "$reason" "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT" "$AMQ_KEEPALIVE_TEST_ROOT/terminal-still-open"
`), 0o700); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &bytes.Buffer{}}).Run(context.Background(), []string{
		"gc", "--registry", registryPath, "--apply", "--owner-gone-grace", "5m",
		"--amq", fakeAMQ, "--self", "/bin/sh",
	})
	if code != 0 || !strings.Contains(stdout.String(), `"status": "retired"`) {
		t.Fatalf("code=%d output=%s", code, stdout.String())
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].ID != entry.ID || loaded.Entries[0].State != registry.StateRetired {
		t.Fatalf("retired registry=%#v err=%v", loaded.Entries, err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("GC deleted terminal target: %v", err)
	}
	data, err := os.ReadFile(callLog)
	if err != nil || strings.Count(string(data), "wake retire") != 2 || !strings.Contains(string(data), "--check") {
		t.Fatalf("retire calls=%q err=%v", data, err)
	}
}

type appGCWake struct {
	starts  int
	retires []amq.RetireWakeRequest
}

type appTransitionWake struct {
	starts  int
	retires int
}

func (w *appTransitionWake) StartWake(context.Context, amq.StartWakeRequest) (amq.WakeBinding, error) {
	w.starts++
	return amq.WakeBinding{Generation: "generation-2", TargetDigest: "sha256:target-2"}, nil
}

func (*appTransitionWake) Env(context.Context) (amq.Env, error) {
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (w *appTransitionWake) RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	w.retires++
	return amq.RetireWakeResult{}, errors.New("same-target recovery must not retire")
}

func TestSupervisorSameTargetCrashRecoveryClearsTransition(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	target := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	owner := appTestWakeOwner()
	binding := appTestWakeBinding()
	entry := registry.Entry{
		ID: registry.EntryID(dir, "codex", "file", target), Root: dir, Agent: "codex", Adapter: "file", Target: target,
		State: registry.StateAttached, WakeOwnerPresent: true, WakeOwner: owner,
		Transition: registry.ReattachTransition{
			Phase: registry.TransitionReserved, Revision: 1,
			OldID: registry.EntryID(dir, "codex", "file", target), OldRoot: dir, OldAgent: "codex",
			OldAdapter: "file", OldTarget: target, OldOwnerSet: true, OldOwner: owner, OldBinding: binding,
		},
	}
	if err := registry.New(registryPath).Save(registry.File{Entries: []registry.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	wake := &appTransitionWake{}
	results, err := (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}}).superviseOnceWithGC(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, supervisor.GCPolicy{}, 1,
	)
	if err != nil || len(results) != 1 || results[0].Action != supervisor.ActionEnsured || wake.starts != 1 || wake.retires != 0 {
		t.Fatalf("results=%#v starts=%d retires=%d err=%v", results, wake.starts, wake.retires, err)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Transition.Active() || loaded.Entries[0].WakeBinding.Generation != "generation-2" {
		t.Fatalf("recovered entry=%#v err=%v", loaded.Entries, err)
	}
}

func (w *appGCWake) StartWake(context.Context, amq.StartWakeRequest) (amq.WakeBinding, error) {
	w.starts++
	return amq.WakeBinding{}, errors.New("owner-gone wake must not restart")
}

func (*appGCWake) Env(context.Context) (amq.Env, error) {
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (w *appGCWake) RetireWake(_ context.Context, request amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	w.retires = append(w.retires, request)
	status, reason := "retired", "retired_exact"
	if request.Check {
		status, reason = "eligible", "owner_gone"
	}
	return amq.RetireWakeResult{
		Status: status, ReasonCode: reason, Root: request.Root, Agent: request.Me,
		Generation: request.Generation, TargetDigest: request.TargetDigest,
	}, nil
}

func TestSupervisorBatchPassDefersOtherRootObservationToFollowUp(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	now := time.Now().UTC()
	entries := make([]registry.Entry, 0, 2)
	for index := 0; index < 2; index++ {
		target := filepath.Join(dir, fmt.Sprintf("target-%d", index))
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		root := filepath.Join(dir, fmt.Sprintf("root-%d", index))
		entry := registry.Entry{
			ID:   fmt.Sprintf("entry-%d", index),
			Root: root, Agent: fmt.Sprintf("agent-%d", index), Adapter: "file", Target: target, State: registry.StateActive,
			WakeOwnerPresent: true, WakeOwner: appTestWakeOwner(),
			WakeBinding: registry.WakeBinding{Generation: fmt.Sprintf("generation-%d", index), TargetDigest: fmt.Sprintf("sha256:target-%d", index)},
		}
		if index == 0 {
			entry.OwnerGoneSince = now.Add(-10 * time.Minute)
		}
		entries = append(entries, entry)
	}
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	wake := &appGCWake{}
	policy := supervisor.GCPolicy{AutoGC: true, OwnerGrace: 5 * time.Minute, RetiredRetention: 24 * time.Hour, Timeout: time.Second}
	pass, err := (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second,
		policy,
	)
	if err != nil || len(pass.Results) != 1 || !pass.BatchExecuted || wake.starts != 0 || len(wake.retires) != 2 {
		t.Fatalf("pass=%#v starts=%d retires=%#v err=%v", pass, wake.starts, wake.retires, err)
	}
	if _, err := (App{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, Now: func() time.Time { return now.Add(supervisor.GCCatchUpInterval) }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, policy,
	); err != nil {
		t.Fatal(err)
	}
	if wake.starts != 0 || len(wake.retires) != 3 {
		t.Fatalf("follow-up starts=%d retires=%#v", wake.starts, wake.retires)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	retired := 0
	observed := 0
	for _, entry := range loaded.Entries {
		if entry.State == registry.StateRetired {
			retired++
		}
		if entry.ID == "entry-1" && !entry.OwnerGoneSince.IsZero() {
			observed++
		}
	}
	if retired != 1 || observed != 1 {
		t.Fatalf("retired=%d observed=%d entries=%#v, want one retirement and later observation persistence", retired, observed, loaded.Entries)
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

func appTestWakeOwner() registry.WakeOwner {
	return registry.WakeOwner{PID: 42, ProcessStart: "start-1", BootID: "boot-1", SessionID: 42}
}

func appTestWakeBinding() registry.WakeBinding {
	return registry.WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"}
}

func appRetireResult(entry registry.Entry, status, reason string) amq.RetireWakeResult {
	return amq.RetireWakeResult{
		Status: status, ReasonCode: reason, Root: entry.Root, Agent: entry.Agent,
		Generation: entry.WakeBinding.Generation, TargetDigest: entry.WakeBinding.TargetDigest,
	}
}

func appExactBlockingWake(old registry.Entry) *amq.WakeStartError {
	return &amq.WakeStartError{
		Result: amq.WakeCommandResult{
			Schema: 1, Status: "failed", ReasonCode: "existing_wake_blocking",
			Root: old.Root, Agent: old.Agent, CurrentWakeMode: "owner_bound",
			CurrentGeneration: old.WakeBinding.Generation, CurrentTargetDigest: old.WakeBinding.TargetDigest,
		},
		Cause: errors.New("old wake blocks the requested target"),
	}
}

func appManagedAMQScript(body string) []byte {
	const shebang = "#!/bin/sh\n"
	const capability = `if [ "$1" = "env" ]; then
  printf '%s\n' '{"schema_version":1,"amq_version":"test","root":"/tmp/root","base_root":"/tmp","session_name":"root","in_session":true,"me":"worker","project":"test","root_source":"flag","peers":{},"capabilities":["wake_gc_v1"]}'
  exit 0
fi
`
	return []byte(strings.Replace(body, shebang, shebang+capability, 1))
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
