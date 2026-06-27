package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
)

type fakeWake struct {
	repairs     []repairResult
	repairCalls int
	starts      []amq.StartWakeRequest
	startErr    error
}

type repairResult struct {
	result amq.WakeRepairResult
	err    error
}

func (f *fakeWake) RepairWake(ctx context.Context, root, me string) (amq.WakeRepairResult, error) {
	f.repairCalls++
	if len(f.repairs) == 0 {
		return amq.WakeRepairResult{Status: "already-running"}, nil
	}
	next := f.repairs[0]
	f.repairs = f.repairs[1:]
	return next.result, next.err
}

func (f *fakeWake) StartWake(ctx context.Context, req amq.StartWakeRequest) error {
	f.starts = append(f.starts, req)
	return f.startErr
}

type probeAdapter struct {
	err error
}

func (p probeAdapter) Probe(ctx context.Context, target string) error {
	return p.err
}

func TestNoLockStartsWake(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{repairs: []repairResult{{
		result: amq.WakeRepairResult{Status: "refused", Reason: "no wake lock present"},
		err:    errors.New("exit status 1"),
	}}}

	updated, result := testReconciler(wake, probeAdapter{}, now).Reconcile(context.Background(), testEntry())

	if result.Action != ActionStarted {
		t.Fatalf("action = %q, want %q", result.Action, ActionStarted)
	}
	if !result.Started {
		t.Fatal("Started = false, want true")
	}
	if len(wake.starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(wake.starts))
	}
	if wake.starts[0].Timeout != 7*time.Second {
		t.Fatalf("start timeout = %s, want 7s", wake.starts[0].Timeout)
	}
	if updated.State != registry.StateActive {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateActive)
	}
	if updated.FailureCount != 0 || !updated.BackoffUntil.IsZero() || updated.LastError != "" {
		t.Fatalf("active entry retained failure data: %+v", updated)
	}
}

func TestAlreadyRunningDoesNotStartWake(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{repairs: []repairResult{{
		result: amq.WakeRepairResult{Status: "already-running"},
	}}}

	updated, result := testReconciler(wake, probeAdapter{}, now).Reconcile(context.Background(), testEntry())

	if result.Action != ActionAlreadyRunning {
		t.Fatalf("action = %q, want %q", result.Action, ActionAlreadyRunning)
	}
	if len(wake.starts) != 0 {
		t.Fatalf("starts = %d, want 0", len(wake.starts))
	}
	if updated.State != registry.StateActive {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateActive)
	}
}

func TestStaleRepairDoesNotStartWake(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{repairs: []repairResult{{
		result: amq.WakeRepairResult{Status: "repaired"},
	}}}

	updated, result := testReconciler(wake, probeAdapter{}, now).Reconcile(context.Background(), testEntry())

	if result.Action != ActionRepaired {
		t.Fatalf("action = %q, want %q", result.Action, ActionRepaired)
	}
	if len(wake.starts) != 0 {
		t.Fatalf("starts = %d, want 0", len(wake.starts))
	}
	if updated.State != registry.StateActive {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateActive)
	}
}

func TestStartFreshStartsWithoutRepair(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{repairs: []repairResult{{
		result: amq.WakeRepairResult{Status: "already-running"},
	}}}

	updated, result := testReconciler(wake, probeAdapter{}, now).StartFresh(context.Background(), testEntry())

	if wake.repairCalls != 0 {
		t.Fatalf("repairCalls = %d, want 0", wake.repairCalls)
	}
	if len(wake.starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(wake.starts))
	}
	if result.Action != ActionStarted {
		t.Fatalf("action = %q, want %q", result.Action, ActionStarted)
	}
	if updated.State != registry.StateActive {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateActive)
	}
}

func TestStartFreshDoesNotAcceptAlreadyRunningAsSuccess(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{startErr: amq.ErrAlreadyRunning}

	updated, result := testReconciler(wake, probeAdapter{}, now).StartFresh(context.Background(), testEntry())

	if result.Action != ActionStartFailed {
		t.Fatalf("action = %q, want %q", result.Action, ActionStartFailed)
	}
	if !errors.Is(result.Error, amq.ErrAlreadyRunning) {
		t.Fatalf("error = %v, want ErrAlreadyRunning", result.Error)
	}
	if updated.State != registry.StateAttached {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateAttached)
	}
	if updated.LastSupervisorDecision != ActionStartFailed {
		t.Fatalf("LastSupervisorDecision = %q, want %q", updated.LastSupervisorDecision, ActionStartFailed)
	}
}

func TestUnverifiedRepairBacksOffWithoutStart(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{repairs: []repairResult{{
		result: amq.WakeRepairResult{Status: "refused", Reason: "unverified wake lock; refusing repair"},
		err:    errors.New("exit status 1"),
	}}}

	reconciler := testReconciler(wake, probeAdapter{}, now)
	reconciler.Jitter = func(delay time.Duration) time.Duration { return delay + delay/10 }
	updated, result := reconciler.Reconcile(context.Background(), testEntry())

	if result.Action != ActionBackoff {
		t.Fatalf("action = %q, want %q", result.Action, ActionBackoff)
	}
	if len(wake.starts) != 0 {
		t.Fatalf("starts = %d, want 0", len(wake.starts))
	}
	if updated.State != registry.StateAttached {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateAttached)
	}
	if updated.FailureCount != 1 {
		t.Fatalf("FailureCount = %d, want 1", updated.FailureCount)
	}
	if !updated.BackoffUntil.After(now) {
		t.Fatalf("BackoffUntil = %v, want after %v", updated.BackoffUntil, now)
	}
	if got, want := updated.BackoffUntil.Sub(now), 1100*time.Millisecond; got != want {
		t.Fatalf("BackoffUntil-now = %v, want %v", got, want)
	}
}

func TestDetachedTargetDoesNotTouchAMQ(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{}

	updated, result := testReconciler(wake, probeAdapter{err: errors.New("target gone")}, now).Reconcile(context.Background(), testEntry())

	if result.Action != ActionDetached {
		t.Fatalf("action = %q, want %q", result.Action, ActionDetached)
	}
	if result.AMQTouched {
		t.Fatal("AMQTouched = true, want false")
	}
	if len(wake.starts) != 0 {
		t.Fatalf("starts = %d, want 0", len(wake.starts))
	}
	if updated.State != registry.StateDetached {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateDetached)
	}
}

func TestIdempotenceNoDuplicateStart(t *testing.T) {
	now := fixedNow()
	wake := &fakeWake{repairs: []repairResult{
		{
			result: amq.WakeRepairResult{Status: "refused", Reason: "no inject-via wake target"},
			err:    errors.New("exit status 1"),
		},
		{
			result: amq.WakeRepairResult{Status: "already-running"},
		},
	}}
	reconciler := testReconciler(wake, probeAdapter{}, now)

	updated, result := reconciler.Reconcile(context.Background(), testEntry())
	if result.Action != ActionStarted {
		t.Fatalf("first action = %q, want %q", result.Action, ActionStarted)
	}
	updated, result = reconciler.Reconcile(context.Background(), updated)

	if result.Action != ActionAlreadyRunning {
		t.Fatalf("second action = %q, want %q", result.Action, ActionAlreadyRunning)
	}
	if len(wake.starts) != 1 {
		t.Fatalf("starts = %d, want 1", len(wake.starts))
	}
	if updated.State != registry.StateActive {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateActive)
	}
}

func TestNilWakeBackoffDoesNotReportAMQTouched(t *testing.T) {
	now := fixedNow()

	updated, result := Reconciler{
		Adapter:     probeAdapter{},
		Now:         func() time.Time { return now },
		BackoffBase: time.Second,
		Jitter:      func(delay time.Duration) time.Duration { return delay },
	}.Reconcile(context.Background(), testEntry())

	if result.Action != ActionBackoff {
		t.Fatalf("action = %q, want %q", result.Action, ActionBackoff)
	}
	if result.AMQTouched {
		t.Fatal("AMQTouched = true, want false")
	}
	if updated.State != registry.StateAttached {
		t.Fatalf("state = %q, want %q", updated.State, registry.StateAttached)
	}
}

func testReconciler(wake *fakeWake, adapter probeAdapter, now time.Time) Reconciler {
	return Reconciler{
		Wake:        wake,
		Adapter:     adapter,
		Now:         func() time.Time { return now },
		BackoffBase: time.Second,
		Jitter:      func(delay time.Duration) time.Duration { return delay },
		InjectVia:   "/bin/amq-keepalive",
		WakeTimeout: 7 * time.Second,
	}
}

func testEntry() registry.Entry {
	return registry.Entry{
		ID:      "entry-1",
		Root:    "/tmp/amq-root",
		Agent:   "codex",
		Adapter: "file",
		Target:  "/tmp/inbox.txt",
		State:   registry.StateAttached,
	}
}

func fixedNow() time.Time {
	return time.Date(2026, 6, 26, 15, 0, 0, 0, time.UTC)
}
