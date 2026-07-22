package supervisor

import (
	"context"
	"fmt"
	"time"

	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
)

const (
	GCStatusSkipped        = "skipped"
	GCStatusOwnerGoneSeen  = "owner_gone_observed"
	GCStatusEligible       = "eligible"
	GCStatusRetired        = "retired"
	GCStatusPurgeCandidate = "purge_candidate"
)

type WakeLifecycle interface {
	RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error)
}

type GCPolicy struct {
	AutoGC           bool
	OwnerGrace       time.Duration
	RetiredRetention time.Duration
	Timeout          time.Duration
}

type GCResult struct {
	EntryID          string        `json:"entry_id"`
	Status           string        `json:"status"`
	ReasonCode       string        `json:"reason_code,omitempty"`
	Reason           string        `json:"reason,omitempty"`
	Capability       bool          `json:"capability"`
	OwnerBound       bool          `json:"owner_bound"`
	BindingComplete  bool          `json:"binding_complete"`
	OwnerGoneAge     time.Duration `json:"owner_gone_age,omitempty"`
	RetirementStatus string        `json:"retirement_status,omitempty"`
	AMQTouched       bool          `json:"amq_touched"`
	Purge            bool          `json:"purge,omitempty"`
}

type GarbageCollector struct {
	Wake                WakeLifecycle
	InjectVia           string
	CapabilityAvailable bool
	CapabilityError     error
	Policy              GCPolicy
	Now                 func() time.Time
}

func (g GarbageCollector) Process(ctx context.Context, entry registry.Entry, apply bool) (updated registry.Entry, result GCResult) {
	now := g.now()
	result = GCResult{
		EntryID: entry.ID, Status: GCStatusSkipped,
		Capability:      g.CapabilityAvailable,
		OwnerBound:      entry.WakeOwnerPresent && entry.WakeOwner.Strong() && !entry.LegacyUnbound,
		BindingComplete: entry.WakeBinding.Complete(),
	}
	updated = entry
	defer func() {
		if !apply {
			return
		}
		updated.LastGCDecision = result.Status
		updated.LastGCReason = gcReasonKey(result)
	}()
	if entry.State == registry.StateRetired {
		if !entry.RetiredAt.IsZero() && now.Sub(entry.RetiredAt) >= g.retention() {
			result.Status = GCStatusPurgeCandidate
			result.ReasonCode = "retention_elapsed"
			result.Purge = true
			result.Reason = "retired registry diagnostic retention elapsed"
			return entry, result
		}
		result.ReasonCode = "retention_active"
		result.Reason = "retired registry row is inside diagnostic retention"
		return entry, result
	}
	if entry.Transition.Active() {
		result.ReasonCode = "transition_active"
		result.Reason = "reattach transition must recover before garbage collection"
		return entry, result
	}
	if g.CapabilityError != nil {
		result.ReasonCode = "capability_check_failed"
		result.Reason = "AMQ capability check failed: " + g.CapabilityError.Error()
		return entry, result
	}
	if !g.CapabilityAvailable {
		result.ReasonCode = "capability_unavailable"
		result.Reason = "AMQ does not advertise wake_gc_v1"
		return entry, result
	}
	if !result.OwnerBound {
		result.ReasonCode = "owner_unbound"
		result.Reason = "legacy or incomplete wake owner is never auto-retired"
		return entry, result
	}
	if !result.BindingComplete {
		result.ReasonCode = "binding_unavailable"
		result.Reason = "exact wake generation/digest binding is unavailable"
		return entry, result
	}
	if g.Wake == nil {
		result.ReasonCode = "lifecycle_unavailable"
		result.Reason = "AMQ lifecycle runner is unavailable"
		return entry, result
	}
	if !entry.GCBackoffUntil.IsZero() && now.Before(entry.GCBackoffUntil) {
		result.ReasonCode = "gc_backoff_active"
		result.Reason = fmt.Sprintf("AMQ lifecycle retry is deferred until %s", entry.GCBackoffUntil.Format(time.RFC3339))
		return entry, result
	}

	checkReq := g.request(entry, true)
	checked, checkErr := g.Wake.RetireWake(ctx, checkReq)
	result.AMQTouched = true
	result.ReasonCode = checked.ReasonCode
	result.RetirementStatus = checked.Status
	if SafeRetirementResult(checkReq, checked) {
		return markRetired(entry, now, checked), retiredResult(result, checked)
	}
	if checked.Status == "refused" && checked.ReasonCode == "owner_live" {
		entry.OwnerGoneSince = time.Time{}
		entry = clearGCFailure(entry)
		result.Reason = fmt.Sprintf("owner absence was not positively established: %s", checked.ReasonCode)
		return entry, result
	}
	if checkErr != nil || checked.Status != "eligible" {
		if checkErr != nil {
			result.Reason = checkErr.Error()
			return gcFailure(entry, now, apply, result)
		} else {
			entry.OwnerGoneSince = time.Time{}
			entry = clearGCFailure(entry)
			result.Reason = fmt.Sprintf("owner absence was not positively established: %s", checked.ReasonCode)
		}
		return entry, result
	}
	entry = clearGCFailure(entry)

	if entry.OwnerGoneSince.IsZero() {
		result.Status = GCStatusOwnerGoneSeen
		if apply {
			entry.OwnerGoneSince = now
			result.Reason = "first positive owner-gone observation recorded"
		} else {
			result.Reason = "first positive owner-gone observation; dry-run did not persist it"
		}
		return entry, result
	}
	result.OwnerGoneAge = now.Sub(entry.OwnerGoneSince)
	if result.OwnerGoneAge < g.ownerGrace() {
		result.Status = GCStatusOwnerGoneSeen
		result.Reason = "second positive owner-gone observation is inside grace period"
		return entry, result
	}
	result.Status = GCStatusEligible
	result.Reason = "two positive owner-gone observations satisfy the grace period"
	if !apply {
		return entry, result
	}

	retireReq := g.request(entry, false)
	retired, retireErr := g.Wake.RetireWake(ctx, retireReq)
	result.ReasonCode = retired.ReasonCode
	result.RetirementStatus = retired.Status
	if SafeRetirementResult(retireReq, retired) {
		return markRetired(entry, now, retired), retiredResult(result, retired)
	}
	if retireErr != nil {
		result.Status = GCStatusSkipped
		result.Reason = retireErr.Error()
		return gcFailure(entry, now, apply, result)
	}
	result.Status = GCStatusSkipped
	result.Reason = "retirement did not positively prove the old generation inactive"
	return gcFailure(entry, now, apply, result)
}

func (g GarbageCollector) request(entry registry.Entry, check bool) amq.RetireWakeRequest {
	return amq.RetireWakeRequest{
		Root: entry.Root, Me: entry.Agent, InjectVia: g.InjectVia,
		Adapter: entry.Adapter, Target: entry.Target,
		Generation: entry.WakeBinding.Generation, TargetDigest: entry.WakeBinding.TargetDigest,
		RequireOwnerGone: true, Check: check, Timeout: g.timeout(),
	}
}

func SafeRetirementResult(req amq.RetireWakeRequest, result amq.RetireWakeResult) bool {
	switch result.Status {
	case "retired":
		return result.ReasonCode == "retired_exact"
	case "already_retired":
		return result.ReasonCode == "tombstone_match"
	case "superseded":
		return amq.SupersededProvesReplacement(req, result)
	default:
		return false
	}
}

func markRetired(entry registry.Entry, now time.Time, result amq.RetireWakeResult) registry.Entry {
	entry.State = registry.StateRetired
	entry.RetiredAt = now
	entry.RetirementOutcome = result.Status
	entry.RetirementReason = result.ReasonCode
	entry.OwnerGoneSince = time.Time{}
	entry.GCFailureCount = 0
	entry.GCBackoffUntil = time.Time{}
	entry.BackoffUntil = time.Time{}
	entry.NextHealthCheck = time.Time{}
	entry.LastError = ""
	entry.LastSupervisorDecision = GCStatusRetired
	return entry
}

func gcFailure(entry registry.Entry, now time.Time, apply bool, result GCResult) (registry.Entry, GCResult) {
	if result.ReasonCode == "" {
		result.ReasonCode = "lifecycle_error"
	}
	if !apply {
		return entry, result
	}
	entry.OwnerGoneSince = time.Time{}
	entry.GCFailureCount++
	delay := time.Minute
	for i := 1; i < entry.GCFailureCount && delay < 15*time.Minute; i++ {
		delay *= 2
	}
	if delay > 15*time.Minute {
		delay = 15 * time.Minute
	}
	entry.GCBackoffUntil = now.Add(delay)
	return entry, result
}

func clearGCFailure(entry registry.Entry) registry.Entry {
	entry.GCFailureCount = 0
	entry.GCBackoffUntil = time.Time{}
	return entry
}

func gcReasonKey(result GCResult) string {
	if result.ReasonCode != "" {
		return result.ReasonCode
	}
	return result.Reason
}

func retiredResult(result GCResult, retired amq.RetireWakeResult) GCResult {
	result.Status = GCStatusRetired
	result.ReasonCode = retired.ReasonCode
	result.RetirementStatus = retired.Status
	result.Reason = "old exact wake generation is positively inactive"
	return result
}

func (g GarbageCollector) now() time.Time {
	if g.Now != nil {
		return g.Now().UTC()
	}
	return time.Now().UTC()
}

func (g GarbageCollector) ownerGrace() time.Duration {
	if g.Policy.OwnerGrace < 0 {
		return 5 * time.Minute
	}
	return g.Policy.OwnerGrace
}

func (g GarbageCollector) retention() time.Duration {
	if g.Policy.RetiredRetention < 0 {
		return 24 * time.Hour
	}
	return g.Policy.RetiredRetention
}

func (g GarbageCollector) timeout() time.Duration {
	if g.Policy.Timeout > 0 && g.Policy.Timeout <= 5*time.Second {
		return g.Policy.Timeout
	}
	return 5 * time.Second
}
