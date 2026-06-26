package supervisor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
)

const (
	ActionBackoff        = "backoff"
	ActionDetached       = "detached"
	ActionAlreadyRunning = "already_running"
	ActionRepaired       = "repaired"
	ActionStarted        = "started"
	ActionStartFailed    = "start_failed"
)

type Adapter interface {
	Probe(ctx context.Context, target string) error
}

type WakeRunner interface {
	RepairWake(ctx context.Context, root, me string) (amq.WakeRepairResult, error)
	StartWake(ctx context.Context, req amq.StartWakeRequest) error
}

type Reconciler struct {
	Wake        WakeRunner
	Adapter     Adapter
	Now         func() time.Time
	BackoffBase time.Duration
	BackoffMax  time.Duration
	InjectVia   string
}

type Result struct {
	Action     string
	Started    bool
	AMQTouched bool
	Error      error
}

func (r Reconciler) Reconcile(ctx context.Context, entry registry.Entry) (registry.Entry, Result) {
	now := r.now()
	entry.LastSeenBySupervisor = now

	if !entry.BackoffUntil.IsZero() && now.Before(entry.BackoffUntil) {
		entry.LastSupervisorDecision = ActionBackoff
		return entry, Result{Action: ActionBackoff}
	}

	if r.Adapter == nil {
		return r.markBackoff(entry, now, errors.New("adapter is not configured"), ActionBackoff)
	}
	if err := r.Adapter.Probe(ctx, entry.Target); err != nil {
		entry.State = registry.StateDetached
		entry.LastError = err.Error()
		entry.LastSupervisorDecision = ActionDetached
		return entry, Result{Action: ActionDetached, Error: err}
	}

	if r.Wake == nil {
		return r.markBackoff(entry, now, errors.New("amq runner is not configured"), ActionBackoff)
	}

	repair, repairErr := r.Wake.RepairWake(ctx, entry.Root, entry.Agent)
	status := strings.ToLower(strings.TrimSpace(repair.Status))
	switch status {
	case "already-running", "running", "active":
		return markActive(entry, now, ActionAlreadyRunning), Result{Action: ActionAlreadyRunning, AMQTouched: true}
	case "repaired":
		return markActive(entry, now, ActionRepaired), Result{Action: ActionRepaired, AMQTouched: true}
	case "refused":
		if isMissingWakeTarget(repair, repairErr) {
			return r.startWake(ctx, entry, now)
		}
		return r.markBackoff(entry, now, combineRepairError(repair, repairErr), ActionBackoff)
	case "error", "":
		if isMissingWakeTarget(repair, repairErr) {
			return r.startWake(ctx, entry, now)
		}
		return r.markBackoff(entry, now, combineRepairError(repair, repairErr), ActionBackoff)
	default:
		if repairErr != nil {
			return r.markBackoff(entry, now, combineRepairError(repair, repairErr), ActionBackoff)
		}
		return r.markBackoff(entry, now, fmt.Errorf("unrecognized amq wake repair status %q", repair.Status), ActionBackoff)
	}
}

func (r Reconciler) startWake(ctx context.Context, entry registry.Entry, now time.Time) (registry.Entry, Result) {
	err := r.Wake.StartWake(ctx, amq.StartWakeRequest{
		Root:      entry.Root,
		Me:        entry.Agent,
		InjectVia: r.InjectVia,
		Adapter:   entry.Adapter,
		Target:    entry.Target,
	})
	if err == nil || errors.Is(err, amq.ErrAlreadyRunning) {
		return markActive(entry, now, ActionStarted), Result{Action: ActionStarted, Started: true, AMQTouched: true}
	}
	return r.markBackoff(entry, now, err, ActionStartFailed)
}

func (r Reconciler) markBackoff(entry registry.Entry, now time.Time, err error, action string) (registry.Entry, Result) {
	entry.State = registry.StateAttached
	entry.FailureCount++
	entry.BackoffUntil = now.Add(r.backoff(entry.FailureCount))
	entry.LastSupervisorDecision = action
	if err != nil {
		entry.LastError = err.Error()
	}
	return entry, Result{Action: action, AMQTouched: action != ActionDetached, Error: err}
}

func markActive(entry registry.Entry, now time.Time, action string) registry.Entry {
	entry.State = registry.StateActive
	entry.LastSeenBySupervisor = now
	entry.FailureCount = 0
	entry.BackoffUntil = time.Time{}
	entry.LastError = ""
	entry.LastSupervisorDecision = action
	return entry
}

func (r Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r Reconciler) backoff(failureCount int) time.Duration {
	base := r.BackoffBase
	if base <= 0 {
		base = time.Second
	}
	maxDelay := r.BackoffMax
	if maxDelay <= 0 {
		maxDelay = time.Minute
	}
	if failureCount < 1 {
		failureCount = 1
	}
	delay := base
	for i := 1; i < failureCount; i++ {
		delay *= 2
		if delay >= maxDelay {
			return maxDelay
		}
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func isMissingWakeTarget(result amq.WakeRepairResult, err error) bool {
	text := strings.ToLower(result.Text())
	if err != nil {
		text += " " + strings.ToLower(err.Error())
	}
	if strings.Contains(text, "unverified") {
		return false
	}
	return strings.Contains(text, "no wake lock") ||
		strings.Contains(text, "no saved") ||
		strings.Contains(text, "no inject-via") ||
		strings.Contains(text, "missing wake")
}

func combineRepairError(result amq.WakeRepairResult, err error) error {
	parts := []string{}
	if text := result.Text(); text != "" {
		parts = append(parts, text)
	}
	if err != nil {
		parts = append(parts, err.Error())
	}
	if len(parts) == 0 {
		return errors.New("amq wake repair failed")
	}
	return errors.New(strings.Join(parts, ": "))
}
