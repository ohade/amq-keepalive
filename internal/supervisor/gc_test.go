package supervisor

import (
	"context"
	"errors"
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
