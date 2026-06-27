package supervisor

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
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
	Jitter      func(time.Duration) time.Duration
	InjectVia   string
	WakeTimeout time.Duration
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

	if blocked, result, ok := r.checkLocalReadiness(ctx, entry, now); ok {
		return blocked, result
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
			return r.startWake(ctx, entry, now, true)
		}
		return r.markBackoff(entry, now, combineRepairError(repair, repairErr), ActionBackoff, true)
	case "error", "":
		if isMissingWakeTarget(repair, repairErr) {
			return r.startWake(ctx, entry, now, true)
		}
		return r.markBackoff(entry, now, combineRepairError(repair, repairErr), ActionBackoff, true)
	default:
		if repairErr != nil {
			return r.markBackoff(entry, now, combineRepairError(repair, repairErr), ActionBackoff, true)
		}
		return r.markBackoff(entry, now, fmt.Errorf("unrecognized amq wake repair status %q", repair.Status), ActionBackoff, true)
	}
}

func (r Reconciler) StartFresh(ctx context.Context, entry registry.Entry) (registry.Entry, Result) {
	now := r.now()
	entry.LastSeenBySupervisor = now

	if blocked, result, ok := r.checkLocalReadiness(ctx, entry, now); ok {
		return blocked, result
	}

	return r.startWake(ctx, entry, now, false)
}

func (r Reconciler) checkLocalReadiness(ctx context.Context, entry registry.Entry, now time.Time) (registry.Entry, Result, bool) {
	if !entry.BackoffUntil.IsZero() && now.Before(entry.BackoffUntil) {
		entry.LastSupervisorDecision = ActionBackoff
		return entry, Result{Action: ActionBackoff}, true
	}

	if r.Adapter == nil {
		updated, result := r.markBackoff(entry, now, errors.New("adapter is not configured"), ActionBackoff, false)
		return updated, result, true
	}
	if err := r.Adapter.Probe(ctx, entry.Target); err != nil {
		entry.State = registry.StateDetached
		entry.LastError = err.Error()
		entry.LastSupervisorDecision = ActionDetached
		return entry, Result{Action: ActionDetached, Error: err}, true
	}

	if r.Wake == nil {
		updated, result := r.markBackoff(entry, now, errors.New("amq runner is not configured"), ActionBackoff, false)
		return updated, result, true
	}
	return entry, Result{}, false
}

func (r Reconciler) startWake(ctx context.Context, entry registry.Entry, now time.Time, allowAlreadyRunning bool) (registry.Entry, Result) {
	err := r.Wake.StartWake(ctx, amq.StartWakeRequest{
		Root:      entry.Root,
		Me:        entry.Agent,
		InjectVia: r.InjectVia,
		Adapter:   entry.Adapter,
		Target:    entry.Target,
		Timeout:   r.WakeTimeout,
	})
	if err == nil || (allowAlreadyRunning && errors.Is(err, amq.ErrAlreadyRunning)) {
		return markActive(entry, now, ActionStarted), Result{Action: ActionStarted, Started: true, AMQTouched: true}
	}
	return r.markBackoff(entry, now, err, ActionStartFailed, true)
}

func (r Reconciler) markBackoff(entry registry.Entry, now time.Time, err error, action string, amqTouched bool) (registry.Entry, Result) {
	entry.State = registry.StateAttached
	entry.FailureCount++
	entry.BackoffUntil = now.Add(r.backoff(entry.FailureCount))
	entry.LastSupervisorDecision = action
	if err != nil {
		entry.LastError = err.Error()
	}
	return entry, Result{Action: action, AMQTouched: amqTouched, Error: err}
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
	return r.jitter(delay, maxDelay)
}

func (r Reconciler) jitter(delay time.Duration, maxDelay time.Duration) time.Duration {
	if r.Jitter != nil {
		jittered := r.Jitter(delay)
		if jittered < 0 {
			return 0
		}
		if jittered > maxDelay {
			return maxDelay
		}
		return jittered
	}
	if delay <= 0 {
		return delay
	}
	window := delay / 5
	if window <= 0 {
		return delay
	}
	span := int64(window*2) + 1
	offset, err := rand.Int(rand.Reader, big.NewInt(span))
	if err != nil {
		return delay
	}
	jittered := delay - window + time.Duration(offset.Int64())
	if jittered < 0 {
		return 0
	}
	if jittered > maxDelay {
		return maxDelay
	}
	return jittered
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
