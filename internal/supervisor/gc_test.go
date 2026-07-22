package supervisor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
)

type gcReply struct {
	result amq.RetireWakeResult
	err    error
}

type fakeLifecycle struct {
	replies  []gcReply
	requests []amq.RetireWakeRequest
}

func TestGarbageCollectorDefersPendingManualRetirementByteEquivalent(t *testing.T) {
	entry := gcTestEntry()
	entry.ManualRetirementIntent.PlanID = "pending"
	wake := &fakeLifecycle{}
	updated, result := (GarbageCollector{Wake: wake, CapabilityAvailable: true}).ProcessWithBudget(context.Background(), entry, true, true)
	if updated != entry || result.Status != GCStatusSkipped || result.ReasonCode != "manual_retirement_pending" || result.AMQTouched || len(wake.requests) != 0 {
		t.Fatalf("pending GC updated=%#v result=%#v requests=%#v", updated, result, wake.requests)
	}
}

func (f *fakeLifecycle) RetireWake(_ context.Context, request amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	f.requests = append(f.requests, request)
	if len(f.replies) == 0 {
		return amq.RetireWakeResult{}, errors.New("unexpected retire request")
	}
	reply := f.replies[0]
	f.replies = f.replies[1:]
	return reply.result, reply.err
}

func TestGarbageCollectorRequiresTwoOwnerGoneObservations(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	wake := &fakeLifecycle{replies: []gcReply{
		{result: gcResultFor("eligible", "owner_gone")},
		{result: gcResultFor("eligible", "owner_gone")},
		{result: gcResultFor("eligible", "owner_gone")},
		{result: gcResultFor("retired", "retired_exact")},
	}}
	collector := GarbageCollector{
		Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
		Policy: GCPolicy{OwnerGrace: 5 * time.Minute, RetiredRetention: 24 * time.Hour, Timeout: time.Second},
		Now:    func() time.Time { return now },
	}
	entry := gcTestEntry()
	first, firstResult := collector.Process(context.Background(), entry, true)
	if firstResult.Status != GCStatusOwnerGoneSeen || !first.OwnerGoneSince.Equal(now) || len(wake.requests) != 1 || !wake.requests[0].Check {
		t.Fatalf("first=%#v result=%#v requests=%#v", first, firstResult, wake.requests)
	}
	collector.Now = func() time.Time { return now.Add(4 * time.Minute) }
	second, secondResult := collector.Process(context.Background(), first, true)
	if secondResult.Status != GCStatusOwnerGoneSeen || second.State == registry.StateRetired || len(wake.requests) != 2 {
		t.Fatalf("second=%#v result=%#v requests=%#v", second, secondResult, wake.requests)
	}
	collector.Now = func() time.Time { return now.Add(5 * time.Minute) }
	retired, retiredResult := collector.Process(context.Background(), second, true)
	if retiredResult.Status != GCStatusRetired || retired.State != registry.StateRetired || len(wake.requests) != 4 || !wake.requests[2].Check || wake.requests[3].Check {
		t.Fatalf("retired=%#v result=%#v requests=%#v", retired, retiredResult, wake.requests)
	}
}

func TestGarbageCollectorLiveOrUnknownOwnerFailsClosedAndClearsObservation(t *testing.T) {
	entry := gcTestEntry()
	entry.OwnerGoneSince = time.Now().Add(-time.Hour)
	for _, code := range []string{"owner_live", "owner_uninspectable"} {
		wake := &fakeLifecycle{replies: []gcReply{{
			result: gcResultFor("refused", code), err: errors.New("refused"),
		}}}
		collector := GarbageCollector{Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true}
		updated, result := collector.Process(context.Background(), entry, true)
		if result.Status != GCStatusSkipped || !updated.OwnerGoneSince.IsZero() || updated.State == registry.StateRetired {
			t.Fatalf("code=%s updated=%#v result=%#v", code, updated, result)
		}
	}
}

func TestGarbageCollectorPinsOwnerLiveResetToExactEcho(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	entry := gcTestEntry()
	entry.OwnerGoneSince = now.Add(-time.Hour)
	entry.GCFailureCount = 2
	entry.GCBackoffUntil = now.Add(-time.Minute)
	collectorFor := func(wake *fakeLifecycle) GarbageCollector {
		return GarbageCollector{Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true, Now: func() time.Time { return now }}
	}
	exactWake := &fakeLifecycle{replies: []gcReply{{result: gcResultFor("refused", "owner_live"), err: errors.New("owner remains live")}}}
	exact, exactResult := collectorFor(exactWake).Process(context.Background(), entry, true)
	if !exact.OwnerGoneSince.IsZero() || exact.GCFailureCount != 0 || !exact.GCBackoffUntil.IsZero() || exactResult.ReasonCode != "owner_live" {
		t.Fatalf("exact owner_live did not clear stale GC state: updated=%#v result=%#v", exact, exactResult)
	}
	mismatch := gcResultFor("refused", "owner_live")
	mismatch.Root = "/tmp/unrelated-root"
	mismatchWake := &fakeLifecycle{replies: []gcReply{{result: mismatch, err: errors.New("unrelated owner remains live")}}}
	failed, failedResult := collectorFor(mismatchWake).Process(context.Background(), entry, true)
	if failed.GCFailureCount != 3 || failed.GCBackoffUntil.IsZero() || failedResult.Reason != "unrelated owner remains live" {
		t.Fatalf("mismatched owner_live was treated as exact: updated=%#v result=%#v", failed, failedResult)
	}
}

func TestGarbageCollectorSafeSupersededMarksOnlyOldRowRetired(t *testing.T) {
	wake := &fakeLifecycle{replies: []gcReply{{result: amq.RetireWakeResult{
		Status: "superseded", ReasonCode: "generation_superseded",
		Root: "/tmp/root", Agent: "codex", Generation: "generation-1", TargetDigest: "sha256:target-1",
		CurrentGeneration: "generation-2", CurrentTargetDigest: "sha256:target-2", CurrentWakeMode: "owner_bound",
	}}}}
	collector := GarbageCollector{Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true}
	updated, result := collector.Process(context.Background(), gcTestEntry(), true)
	if updated.State != registry.StateRetired || result.Status != GCStatusRetired || updated.WakeBinding.Generation != "generation-1" {
		t.Fatalf("updated=%#v result=%#v", updated, result)
	}
}

func TestGarbageCollectorBacksOffLifecycleFailures(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	wake := &fakeLifecycle{replies: []gcReply{
		{err: errors.New("amq lifecycle unavailable")},
		{err: errors.New("amq lifecycle unavailable")},
	}}
	collector := GarbageCollector{
		Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
		Now: func() time.Time { return now },
	}
	entry := gcTestEntry()
	entry.OwnerGoneSince = now.Add(-time.Hour)
	first, firstResult := collector.Process(context.Background(), entry, true)
	if firstResult.ReasonCode != "lifecycle_error" || first.GCFailureCount != 1 ||
		!first.GCBackoffUntil.Equal(now.Add(time.Minute)) || !first.OwnerGoneSince.IsZero() || len(wake.requests) != 1 {
		t.Fatalf("first=%#v result=%#v requests=%#v", first, firstResult, wake.requests)
	}
	collector.Now = func() time.Time { return now.Add(30 * time.Second) }
	deferred, deferredResult := collector.Process(context.Background(), first, true)
	if deferredResult.ReasonCode != "gc_backoff_active" || deferred.GCFailureCount != first.GCFailureCount ||
		deferred.GCBackoffUntil != first.GCBackoffUntil || deferred.LastGCReason != "gc_backoff_active" || len(wake.requests) != 1 {
		t.Fatalf("deferred=%#v result=%#v requests=%#v", deferred, deferredResult, wake.requests)
	}
	collector.Now = func() time.Time { return now.Add(time.Minute) }
	second, _ := collector.Process(context.Background(), first, true)
	if second.GCFailureCount != 2 || !second.GCBackoffUntil.Equal(now.Add(3*time.Minute)) || len(wake.requests) != 2 {
		t.Fatalf("second=%#v requests=%#v", second, wake.requests)
	}
}

func TestGarbageCollectorDryRunDoesNotMutateFailureState(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	wake := &fakeLifecycle{replies: []gcReply{{err: errors.New("amq lifecycle unavailable")}}}
	collector := GarbageCollector{
		Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
		Now: func() time.Time { return now },
	}
	entry := gcTestEntry()
	updated, result := collector.Process(context.Background(), entry, false)
	if updated != entry || result.ReasonCode != "lifecycle_error" {
		t.Fatalf("updated=%#v result=%#v", updated, result)
	}
}

func TestGarbageCollectorLegacyAndRetentionPolicy(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	wake := &fakeLifecycle{}
	collector := GarbageCollector{
		Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
		Policy: GCPolicy{RetiredRetention: 24 * time.Hour}, Now: func() time.Time { return now },
	}
	legacy := gcTestEntry()
	legacy.LegacyUnbound = true
	legacy.WakeOwnerPresent = false
	if _, result := collector.Process(context.Background(), legacy, true); result.Status != GCStatusSkipped || len(wake.requests) != 0 {
		t.Fatalf("legacy result=%#v requests=%#v", result, wake.requests)
	}
	retired := gcTestEntry()
	retired.State = registry.StateRetired
	retired.RetiredAt = now.Add(-24 * time.Hour)
	if _, result := collector.Process(context.Background(), retired, true); result.Status != GCStatusPurgeCandidate || !result.Purge {
		t.Fatalf("retention result=%#v", result)
	}
}

func TestGarbageCollectorEnforcesInternalSafetyFloors(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	wake := &fakeLifecycle{replies: []gcReply{{result: gcResultFor("eligible", "owner_gone")}}}
	collector := GarbageCollector{
		Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
		Policy: GCPolicy{OwnerGrace: time.Second, RetiredRetention: time.Second, Timeout: 10 * time.Second},
		Now:    func() time.Time { return now },
	}
	entry := gcTestEntry()
	entry.OwnerGoneSince = now.Add(-time.Minute)
	updated, result := collector.Process(context.Background(), entry, true)
	if result.Status != GCStatusOwnerGoneSeen || updated.State == registry.StateRetired || len(wake.requests) != 1 {
		t.Fatalf("updated=%#v result=%#v requests=%#v", updated, result, wake.requests)
	}
	if wake.requests[0].Timeout != MaxLifecycleTimeout {
		t.Fatalf("timeout=%s want=%s", wake.requests[0].Timeout, MaxLifecycleTimeout)
	}

	retired := gcTestEntry()
	retired.State = registry.StateRetired
	retired.RetiredAt = now.Add(-time.Hour)
	if _, retained := collector.Process(context.Background(), retired, true); retained.Purge || retained.Status == GCStatusPurgeCandidate {
		t.Fatalf("sub-floor retention purged row: %#v", retained)
	}
}

func TestRetirePreflightedRejectsTransitioningMemberWithoutLifecycleIO(t *testing.T) {
	wake := &fakeLifecycle{}
	collector := GarbageCollector{Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true}
	entry := gcTestEntry()
	entry.Transition = registry.ReattachTransition{Phase: registry.TransitionReserved}
	updated, result := collector.RetirePreflighted(context.Background(), entry)
	if updated != entry || result.Status != GCStatusSkipped || result.ReasonCode != "batch_member_invalid" || len(wake.requests) != 0 {
		t.Fatalf("updated=%#v result=%#v requests=%#v", updated, result, wake.requests)
	}
}

func TestGarbageCollectorPreflightSafetyMatrix(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		want   string
		mutate func(*GarbageCollector, *registry.Entry)
	}{
		{
			name: "retired retention active",
			want: "retention_active",
			mutate: func(_ *GarbageCollector, entry *registry.Entry) {
				entry.State = registry.StateRetired
				entry.RetiredAt = now.Add(-time.Hour)
			},
		},
		{
			name: "reattach transition active",
			want: "transition_active",
			mutate: func(_ *GarbageCollector, entry *registry.Entry) {
				entry.Transition = registry.ReattachTransition{Phase: registry.TransitionReserved}
			},
		},
		{
			name: "capability check failed",
			want: "capability_check_failed",
			mutate: func(collector *GarbageCollector, _ *registry.Entry) {
				collector.CapabilityError = errors.New("env unavailable")
			},
		},
		{
			name: "capability unavailable",
			want: "capability_unavailable",
			mutate: func(collector *GarbageCollector, _ *registry.Entry) {
				collector.CapabilityAvailable = false
			},
		},
		{
			name: "binding unavailable",
			want: "binding_unavailable",
			mutate: func(_ *GarbageCollector, entry *registry.Entry) {
				entry.WakeBinding = registry.WakeBinding{}
			},
		},
		{
			name: "lifecycle unavailable",
			want: "lifecycle_unavailable",
			mutate: func(collector *GarbageCollector, _ *registry.Entry) {
				collector.Wake = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wake := &fakeLifecycle{}
			collector := GarbageCollector{
				Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
				Now: func() time.Time { return now },
			}
			entry := gcTestEntry()
			test.mutate(&collector, &entry)
			_, result := collector.ProcessWithBudget(context.Background(), entry, true, true)
			if result.ReasonCode != test.want || len(wake.requests) != 0 {
				t.Fatalf("result=%#v requests=%#v", result, wake.requests)
			}
		})
	}
}

func TestGarbageCollectorBudgetAndUnprovenOwnerBranches(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	newCollector := func(wake *fakeLifecycle) GarbageCollector {
		return GarbageCollector{
			Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
			Policy: GCPolicy{OwnerGrace: MinOwnerGoneGrace},
			Now:    func() time.Time { return now },
		}
	}

	t.Run("dry run safe check cannot mutate", func(t *testing.T) {
		wake := &fakeLifecycle{replies: []gcReply{{result: gcResultFor("already_retired", "tombstone_match")}}}
		updated, result := newCollector(wake).ProcessWithBudget(context.Background(), gcTestEntry(), false, false)
		if result.Status != GCStatusEligible || updated.State != registry.StateActive || len(wake.requests) != 1 || !wake.requests[0].Check {
			t.Fatalf("updated=%#v result=%#v requests=%#v", updated, result, wake.requests)
		}
	})

	t.Run("unproven owner clears stale observation without lifecycle failure", func(t *testing.T) {
		wake := &fakeLifecycle{replies: []gcReply{{result: gcResultFor("refused", "owner_uninspectable")}}}
		entry := gcTestEntry()
		entry.OwnerGoneSince = now.Add(-time.Hour)
		entry.GCFailureCount = 2
		entry.GCBackoffUntil = now.Add(-time.Minute)
		updated, result := newCollector(wake).Process(context.Background(), entry, true)
		if result.Status != GCStatusSkipped || !updated.OwnerGoneSince.IsZero() || updated.GCFailureCount != 0 || !updated.GCBackoffUntil.IsZero() {
			t.Fatalf("updated=%#v result=%#v", updated, result)
		}
	})

	t.Run("first observation dry run remains non-mutating", func(t *testing.T) {
		wake := &fakeLifecycle{replies: []gcReply{{result: gcResultFor("eligible", "owner_gone")}}}
		entry := gcTestEntry()
		updated, result := newCollector(wake).Process(context.Background(), entry, false)
		if updated != entry || result.Status != GCStatusOwnerGoneSeen || result.Reason != "first positive owner-gone observation; dry-run did not persist it" {
			t.Fatalf("updated=%#v result=%#v", updated, result)
		}
	})

	t.Run("grace satisfied but retirement budget exhausted", func(t *testing.T) {
		wake := &fakeLifecycle{replies: []gcReply{{result: gcResultFor("eligible", "owner_gone")}}}
		entry := gcTestEntry()
		entry.OwnerGoneSince = now.Add(-MinOwnerGoneGrace)
		updated, result := newCollector(wake).ProcessWithBudget(context.Background(), entry, true, false)
		if updated.State != registry.StateActive || result.Status != GCStatusEligible || len(wake.requests) != 1 {
			t.Fatalf("updated=%#v result=%#v requests=%#v", updated, result, wake.requests)
		}
	})

	for _, test := range []struct {
		name   string
		second gcReply
		want   string
	}{
		{name: "retire lifecycle error", second: gcReply{err: errors.New("retire failed")}, want: "retire failed"},
		{name: "retire response unproven", second: gcReply{result: gcResultFor("refused", "owner_live")}, want: "retirement did not positively prove"},
	} {
		t.Run(test.name, func(t *testing.T) {
			wake := &fakeLifecycle{replies: []gcReply{{result: gcResultFor("eligible", "owner_gone")}, test.second}}
			entry := gcTestEntry()
			entry.OwnerGoneSince = now.Add(-MinOwnerGoneGrace)
			updated, result := newCollector(wake).Process(context.Background(), entry, true)
			if result.Status != GCStatusSkipped || updated.State != registry.StateActive || updated.GCFailureCount != 1 || len(wake.requests) != 2 || !strings.Contains(result.Reason, test.want) {
				t.Fatalf("updated=%#v result=%#v requests=%#v", updated, result, wake.requests)
			}
		})
	}
}

func TestRetirePreflightedOutcomeMatrix(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	superseded := gcResultFor("superseded", "generation_superseded")
	superseded.CurrentGeneration = "generation-2"
	superseded.CurrentTargetDigest = "sha256:target-2"
	superseded.CurrentWakeMode = "owner_bound"
	tests := []struct {
		name       string
		reply      gcReply
		wantState  registry.State
		wantStatus string
		wantFails  int
	}{
		{name: "superseded terminalizes old row", reply: gcReply{result: superseded}, wantState: registry.StateRetired, wantStatus: GCStatusSkipped},
		{name: "retired exact", reply: gcReply{result: gcResultFor("retired", "retired_exact")}, wantState: registry.StateRetired, wantStatus: GCStatusRetired},
		{name: "lifecycle error backs off", reply: gcReply{err: errors.New("retire failed")}, wantState: registry.StateActive, wantStatus: GCStatusSkipped, wantFails: 1},
		{name: "unproven result backs off", reply: gcReply{result: gcResultFor("eligible", "owner_gone")}, wantState: registry.StateActive, wantStatus: GCStatusSkipped, wantFails: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wake := &fakeLifecycle{replies: []gcReply{test.reply}}
			collector := GarbageCollector{
				Wake: wake, InjectVia: "/bin/sh", CapabilityAvailable: true,
				Now: func() time.Time { return now },
			}
			updated, result := collector.RetirePreflighted(context.Background(), gcTestEntry())
			if updated.State != test.wantState || result.Status != test.wantStatus || updated.GCFailureCount != test.wantFails || len(wake.requests) != 1 || wake.requests[0].Check {
				t.Fatalf("updated=%#v result=%#v requests=%#v", updated, result, wake.requests)
			}
		})
	}
}

func TestGarbageCollectorSafetyHelpers(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	entry := gcTestEntry()
	entry.GCFailureCount = 5
	updated, result := gcFailure(entry, now, true, GCResult{Reason: "boom"})
	if result.ReasonCode != "lifecycle_error" || updated.GCFailureCount != 6 || !updated.GCBackoffUntil.Equal(now.Add(15*time.Minute)) {
		t.Fatalf("updated=%#v result=%#v", updated, result)
	}
	if got := gcReasonKey(GCResult{Reason: "fallback"}); got != "fallback" {
		t.Fatalf("gcReasonKey=%q", got)
	}
	if _, err := canonicalRetirementRoot(""); err == nil {
		t.Fatal("empty retirement root accepted")
	}
	if EligibleRetirementResult(amq.RetireWakeRequest{}, amq.RetireWakeResult{Status: "eligible", ReasonCode: "owner_gone"}) {
		t.Fatal("eligible result with empty identity accepted")
	}
	collector := GarbageCollector{Policy: GCPolicy{OwnerGrace: 10 * time.Minute, RetiredRetention: 48 * time.Hour, Timeout: time.Second}}
	if collector.ownerGrace() != 10*time.Minute || collector.retention() != 48*time.Hour || collector.timeout() != time.Second {
		t.Fatalf("policy helpers ignored explicit safe values: %#v", collector.Policy)
	}
	before := time.Now().UTC()
	if got := collector.now(); got.Before(before) || got.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("default now=%s before=%s", got, before)
	}
}

func gcTestEntry() registry.Entry {
	return registry.Entry{
		ID: "entry-1", Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/target",
		State:            registry.StateActive,
		WakeOwnerPresent: true,
		WakeOwner:        registry.WakeOwner{PID: 42, ProcessStart: "start-1", BootID: "boot-1", SessionID: 42},
		WakeBinding:      registry.WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"},
	}
}

func gcResultFor(status, reason string) amq.RetireWakeResult {
	return amq.RetireWakeResult{
		Status: status, ReasonCode: reason, Root: "/tmp/root", Agent: "codex",
		Generation: "generation-1", TargetDigest: "sha256:target-1",
	}
}
