package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ohade/amq-keepalive/internal/adapter"
	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/executable"
	"github.com/ohade/amq-keepalive/internal/registry"
	"github.com/ohade/amq-keepalive/internal/supervisor"
)

const retireSessionPlanSchema = 1

type retireSessionOptions struct {
	RegistryPath string
	Root         string
	AdapterName  string
	Agents       string
	AMQPath      string
	Self         string
	Apply        bool
	ConfirmPlan  string
	Timeout      time.Duration
}

type retireSessionRetirer interface {
	RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error)
}

type retireSessionPlanMember struct {
	EntryID   string `json:"entry_id"`
	Agent     string `json:"agent"`
	Target    string `json:"target"`
	RowDigest string `json:"row_digest"`
	entry     registry.Entry
	pending   registry.ManualRetirementIntent
	receipt   registry.ManualRetirementReceipt
}

type retireSessionPlan struct {
	Schema         int                       `json:"schema"`
	PlanID         string                    `json:"plan_id"`
	RegistryPath   string                    `json:"registry_path"`
	Root           string                    `json:"root"`
	Adapter        string                    `json:"adapter"`
	Agents         []string                  `json:"agents"`
	AMQExecutable  string                    `json:"amq_executable"`
	InjectVia      string                    `json:"inject_via"`
	AMQIdentity    executable.Identity       `json:"amq_identity"`
	InjectIdentity executable.Identity       `json:"inject_via_identity"`
	TimeoutNanos   int64                     `json:"timeout_nanos"`
	Members        []retireSessionPlanMember `json:"members"`
}

type retireSessionAppliedMember struct {
	EntryID      string `json:"entry_id"`
	Agent        string `json:"agent"`
	Generation   string `json:"generation"`
	TargetDigest string `json:"target_digest"`
}

type retireSessionApplyResult struct {
	Schema  int                          `json:"schema"`
	PlanID  string                       `json:"plan_id"`
	Applied bool                         `json:"applied"`
	Partial bool                         `json:"partial,omitempty"`
	Retired []retireSessionAppliedMember `json:"retired"`
}

var persistRetireSessionEntry = func(store *registry.Store, before, after registry.Entry) (registry.UpdateResult, error) {
	return store.UpdateEntries([]registry.EntryUpdate{{Before: before, After: after}})
}

var persistRetireSessionIntents = func(store *registry.Store, root string, updates []registry.EntryUpdate) (registry.UpdateResult, error) {
	return store.EnrollManualRetirementIntents(root, updates)
}

func (a App) retireSessionWithOptions(ctx context.Context, opts retireSessionOptions, lifecycle retireSessionRetirer, selected adapter.Adapter) (any, error) {
	if strings.TrimSpace(opts.RegistryPath) == "" {
		return nil, errors.New("--registry is required")
	}
	if strings.TrimSpace(opts.Self) == "" {
		return nil, errors.New("--self is required")
	}
	if opts.Timeout <= 0 || opts.Timeout > supervisor.MaxLifecycleTimeout {
		return nil, fmt.Errorf("--timeout must be greater than zero and no longer than %s", supervisor.MaxLifecycleTimeout)
	}
	if lifecycle == nil {
		return nil, errors.New("AMQ retirement lifecycle is required")
	}
	if !opts.Apply && strings.TrimSpace(opts.ConfirmPlan) != "" {
		return nil, errors.New("--confirm-plan requires --apply")
	}

	store := registry.New(opts.RegistryPath)
	previewFile, err := store.LoadPreview()
	if err != nil {
		return nil, err
	}
	preview, err := buildRetireSessionPlan(ctx, previewFile, opts, selected)
	if err != nil {
		return nil, err
	}
	if !opts.Apply {
		return &preview, nil
	}
	confirmed := strings.TrimSpace(opts.ConfirmPlan)
	if confirmed == "" {
		return &preview, errors.New("--apply requires --confirm-plan with the exact preview plan_id")
	}
	if confirmed != preview.PlanID {
		return &preview, fmt.Errorf("--confirm-plan %q does not match current plan_id %q", confirmed, preview.PlanID)
	}
	opts.ConfirmPlan = confirmed

	result := &retireSessionApplyResult{Schema: retireSessionPlanSchema, PlanID: preview.PlanID, Retired: []retireSessionAppliedMember{}}
	err = store.WithRegistrationLockContext(ctx, func() error {
		file, err := store.Load()
		if err != nil {
			return err
		}
		current, err := buildRetireSessionPlan(ctx, file, opts, selected)
		if err != nil {
			return fmt.Errorf("retirement plan changed before apply: %w", err)
		}
		result.PlanID = current.PlanID
		if current.PlanID != confirmed {
			return fmt.Errorf("retirement plan changed before apply: confirmed %q, current %q", confirmed, current.PlanID)
		}

		preflight := make(map[string]amq.RetireWakeResult, len(current.Members))
		preflightErrors := make([]error, 0)
		for _, member := range current.Members {
			if member.receipt.Active() {
				continue
			}
			if member.pending.Active() {
				preflight[member.Agent] = retireResultFromIntent(member.pending)
				continue
			}
			request := manualRetireSessionRequest(current, member, true, amq.RetireWakeResult{})
			checked, checkErr := lifecycle.RetireWake(ctx, request)
			if checkErr == nil {
				checkErr = validateManualRetireSessionCheck(current.Root, member.Agent, checked)
			}
			if checkErr != nil {
				preflightErrors = append(preflightErrors, fmt.Errorf("agent %s: status=%s reason_code=%s: %w", member.Agent, checked.Status, checked.ReasonCode, checkErr))
				continue
			}
			preflight[member.Agent] = checked
		}
		if len(preflightErrors) != 0 {
			return fmt.Errorf("manual retirement preflight refused; no wake retirement signals were sent: %w", errors.Join(preflightErrors...))
		}

		// Enroll every unresolved member in one exact compare-and-save batch.
		// The persistence layer verifies that this is the complete unresolved
		// root membership before changing any row, so a sibling race or crash
		// cannot leave a durable prefix which freezes the rest of the root.
		enrollment := make([]registry.EntryUpdate, 0, len(current.Members))
		enrolledEntries := make(map[string]registry.Entry, len(current.Members))
		enrollmentTime := a.now()
		newIntents := 0
		for index := range current.Members {
			member := &current.Members[index]
			if member.receipt.Active() {
				continue
			}
			pendingEntry := member.entry
			if !member.pending.Active() {
				pendingEntry.ManualRetirementIntent = manualRetirementIntent(current, *member, preflight[member.Agent], enrollmentTime)
				newIntents++
			}
			enrollment = append(enrollment, registry.EntryUpdate{Before: member.entry, After: pendingEntry})
			enrolledEntries[member.EntryID] = pendingEntry
		}
		if len(enrollment) != 0 {
			cas, persistErr := persistRetireSessionIntents(store, current.Root, enrollment)
			if persistErr != nil {
				return fmt.Errorf("whole-root manual retirement enrollment was not persisted; no wake retirement signals were sent in this run: %w", persistErr)
			}
			if cas.Updated != newIntents || cas.Skipped != 0 {
				return errors.New("whole-root manual retirement enrollment changed unexpectedly; no wake retirement signals were sent in this run")
			}
			for index := range current.Members {
				member := &current.Members[index]
				if member.receipt.Active() {
					continue
				}
				member.entry = enrolledEntries[member.EntryID]
				member.pending = member.entry.ManualRetirementIntent
			}
		}

		// The adapter target is external to AMQ's metadata transaction. When
		// any work remains, narrow that unavoidable race by re-proving every
		// member of the frozen plan absent immediately before the next
		// mutation. Exact receipts suppress duplicate mutation, but they do
		// not remove their members from the whole-root safety barrier: a
		// receipted sibling may have reappeared since plan construction.
		hasUnresolved := false
		for _, member := range current.Members {
			if !member.receipt.Active() {
				hasUnresolved = true
				break
			}
		}
		if hasUnresolved {
			for _, member := range current.Members {
				probeErr := selected.Probe(ctx, member.Target)
				switch {
				case probeErr == nil:
					return fmt.Errorf("manual retirement target reappeared for agent %s after durable enrollment; no wake retirement signals were sent and the root remains pending", member.Agent)
				case !errors.Is(probeErr, adapter.ErrTargetNotFound):
					return fmt.Errorf("manual retirement target absence became ambiguous for agent %s after durable enrollment; no wake retirement signals were sent and the root remains pending: %w", member.Agent, probeErr)
				}
			}
		}

		for index := range current.Members {
			member := &current.Members[index]
			if member.receipt.Active() {
				result.Retired = append(result.Retired, retireSessionAppliedMember{
					EntryID: member.EntryID, Agent: member.Agent,
					Generation: member.receipt.Generation, TargetDigest: member.receipt.TargetDigest,
				})
				continue
			}
			checked := preflight[member.Agent]
			request := manualRetireSessionRequest(current, *member, false, checked)
			retired, retireErr := lifecycle.RetireWake(ctx, request)
			if retireErr == nil {
				retireErr = validateManualRetireSessionMutation(current.Root, member.Agent, checked, retired)
			}
			if retireErr != nil {
				result.Partial = true
				return fmt.Errorf("manual retirement stopped at agent %s after persisting %d success(es): status=%s reason_code=%s: %w", member.Agent, len(result.Retired), retired.Status, retired.ReasonCode, retireErr)
			}

			updated := retiredLegacySessionEntry(member.entry, member.RowDigest, a.now(), retired)
			cas, casErr := persistRetireSessionEntry(store, member.entry, updated)
			if casErr != nil {
				result.Partial = true
				return fmt.Errorf("wake for agent %s retired but exact registry persistence failed after %d persisted success(es): %w", member.Agent, len(result.Retired), casErr)
			}
			if cas.Updated != 1 || cas.Skipped != 0 {
				result.Partial = true
				return fmt.Errorf("wake for agent %s retired but its registry row changed before exact persistence; no newer row was overwritten", member.Agent)
			}
			result.Retired = append(result.Retired, retireSessionAppliedMember{
				EntryID: member.EntryID, Agent: member.Agent,
				Generation: retired.Generation, TargetDigest: retired.TargetDigest,
			})
		}
		result.Applied = true
		return nil
	})
	return result, err
}

func buildRetireSessionPlan(ctx context.Context, file registry.File, opts retireSessionOptions, selected adapter.Adapter) (retireSessionPlan, error) {
	registryPath, err := canonicalRetireSessionRegistry(opts.RegistryPath)
	if err != nil {
		return retireSessionPlan{}, fmt.Errorf("resolve --registry: %w", err)
	}
	root, err := canonicalRetireSessionRoot(opts.Root)
	if err != nil {
		return retireSessionPlan{}, fmt.Errorf("resolve --root: %w", err)
	}
	agents, err := parseRequiredAgents(opts.Agents)
	if err != nil {
		return retireSessionPlan{}, err
	}
	adapterName := strings.TrimSpace(opts.AdapterName)
	if adapterName == "" || selected == nil || selected.Name() != adapterName {
		return retireSessionPlan{}, fmt.Errorf("exact adapter %q is unavailable", adapterName)
	}
	amqIdentity, err := executable.Capture(opts.AMQPath)
	if err != nil {
		return retireSessionPlan{}, fmt.Errorf("resolve --amq: %w", err)
	}
	injectIdentity, err := executable.Capture(opts.Self)
	if err != nil {
		return retireSessionPlan{}, fmt.Errorf("resolve --self: %w", err)
	}
	for _, batch := range file.GCRootBatches {
		if batch.CanonicalRoot == root {
			return retireSessionPlan{}, fmt.Errorf("GC root batch %s is active for %s", batch.ID, root)
		}
	}

	members := make([]retireSessionPlanMember, 0, len(agents))
	for _, agent := range agents {
		matches := make([]registry.Entry, 0, 1)
		for _, entry := range file.Entries {
			if entry.Adapter != adapterName || entry.Agent != agent {
				continue
			}
			if entry.State == registry.StateRetired && (!opts.Apply || opts.ConfirmPlan == "" || entry.ManualRetirementReceipt.PlanID != opts.ConfirmPlan) {
				continue
			}
			entryRoot, pathErr := canonicalRetireSessionRoot(entry.Root)
			if pathErr != nil {
				return retireSessionPlan{}, fmt.Errorf("canonicalize registry root for agent %s: %w", agent, pathErr)
			}
			if entryRoot == root {
				matches = append(matches, entry)
			}
		}
		if len(matches) != 1 {
			return retireSessionPlan{}, fmt.Errorf("expected exactly one unresolved or exactly-receipted %s registry entry for agent %s at %s, found %d", adapterName, agent, root, len(matches))
		}
		entry := matches[0]
		if entry.State != registry.StateRetired && !legacyOrUnboundRetireSessionEntry(entry) {
			return retireSessionPlan{}, fmt.Errorf("registry entry %s for agent %s is owner-bound; retire-session accepts only legacy/unbound rows", entry.ID, agent)
		}
		if entry.State != registry.StateRetired && entry.Transition.Active() {
			return retireSessionPlan{}, fmt.Errorf("registry entry %s for agent %s has an active reattach transition", entry.ID, agent)
		}
		target, normalizeErr := normalizedTarget(selected, entry.Target)
		if normalizeErr != nil {
			return retireSessionPlan{}, fmt.Errorf("normalize target for agent %s: %w", agent, normalizeErr)
		}
		probeErr := selected.Probe(ctx, target)
		if probeErr == nil {
			return retireSessionPlan{}, fmt.Errorf("refusing to retire agent %s: adapter target %s still exists", agent, target)
		}
		if !errors.Is(probeErr, adapter.ErrTargetNotFound) {
			return retireSessionPlan{}, fmt.Errorf("refusing to retire agent %s because target absence is not proven: %w", agent, probeErr)
		}
		rowDigest := ""
		switch {
		case entry.ManualRetirementIntent.Active():
			rowDigest = entry.ManualRetirementIntent.RowDigest
		case entry.ManualRetirementReceipt.Active():
			rowDigest = entry.ManualRetirementReceipt.RowDigest
		default:
			fingerprintEntry := entry
			fingerprintEntry.ManualRetirementIntent = registry.ManualRetirementIntent{}
			fingerprintEntry.ManualRetirementReceipt = registry.ManualRetirementReceipt{}
			rowJSON, marshalErr := json.Marshal(fingerprintEntry)
			if marshalErr != nil {
				return retireSessionPlan{}, fmt.Errorf("fingerprint registry row for agent %s: %w", agent, marshalErr)
			}
			rowDigest = sha256Hex(rowJSON)
		}
		members = append(members, retireSessionPlanMember{
			EntryID: entry.ID, Agent: agent, Target: target, RowDigest: rowDigest, entry: entry,
			pending: entry.ManualRetirementIntent, receipt: entry.ManualRetirementReceipt,
		})
	}
	memberIDs := make(map[string]struct{}, len(members))
	for _, member := range members {
		memberIDs[member.EntryID] = struct{}{}
	}
	for _, entry := range file.Entries {
		// Retired history from an older plan is not a current listener. An exact
		// receipt from the plan being replayed remains part of that plan and must
		// still be named, just like every unresolved row.
		if entry.State == registry.StateRetired &&
			(!opts.Apply || opts.ConfirmPlan == "" || entry.ManualRetirementReceipt.PlanID != opts.ConfirmPlan) {
			continue
		}
		entryRoot, pathErr := canonicalRetireSessionRoot(entry.Root)
		if pathErr != nil {
			return retireSessionPlan{}, fmt.Errorf("canonicalize registry root for entry %s: %w", entry.ID, pathErr)
		}
		if entryRoot != root {
			continue
		}
		if _, included := memberIDs[entry.ID]; !included {
			return retireSessionPlan{}, fmt.Errorf("retire-session requires exact whole-root membership: registry entry %s for agent %s and adapter %s at %s was omitted", entry.ID, entry.Agent, entry.Adapter, root)
		}
	}
	plan := retireSessionPlan{
		Schema: retireSessionPlanSchema, RegistryPath: registryPath, Root: root, Adapter: adapterName,
		Agents: append([]string(nil), agents...), AMQExecutable: amqIdentity.Path,
		InjectVia: injectIdentity.Path, AMQIdentity: amqIdentity, InjectIdentity: injectIdentity,
		TimeoutNanos: int64(opts.Timeout), Members: members,
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return retireSessionPlan{}, err
	}
	plan.PlanID = sha256Hex(planJSON)
	for _, member := range plan.Members {
		if member.pending.Active() && !manualIntentMatchesPlan(member.pending, plan, member) {
			return retireSessionPlan{}, fmt.Errorf("pending manual retirement for agent %s does not match the exact confirmed plan and transport", member.Agent)
		}
		if member.receipt.Active() && (member.receipt.PlanID != plan.PlanID || member.receipt.RowDigest != member.RowDigest) {
			return retireSessionPlan{}, fmt.Errorf("manual retirement receipt for agent %s does not match the exact confirmed plan", member.Agent)
		}
	}
	return plan, nil
}

func manualRetireSessionRequest(plan retireSessionPlan, member retireSessionPlanMember, check bool, binding amq.RetireWakeResult) amq.RetireWakeRequest {
	return amq.RetireWakeRequest{
		Root: plan.Root, Me: member.Agent, InjectVia: plan.InjectVia, Adapter: plan.Adapter, Target: member.Target,
		Generation: binding.Generation, TargetDigest: binding.TargetDigest,
		ManualPreflightReason: binding.ReasonCode,
		Manual:                true, Check: check, Timeout: time.Duration(plan.TimeoutNanos),
		ExpectedAMQIdentity: plan.AMQIdentity, ExpectedInjectIdentity: plan.InjectIdentity,
	}
}

func manualRetirementIntent(plan retireSessionPlan, member retireSessionPlanMember, binding amq.RetireWakeResult, now time.Time) registry.ManualRetirementIntent {
	return registry.ManualRetirementIntent{
		PlanID: plan.PlanID, RowDigest: member.RowDigest, Root: plan.Root, Agent: member.Agent,
		Adapter: plan.Adapter, Target: member.Target, AMQExecutable: plan.AMQExecutable, InjectVia: plan.InjectVia,
		AMQIdentity: plan.AMQIdentity, InjectIdentity: plan.InjectIdentity,
		TimeoutNanos: plan.TimeoutNanos, Generation: binding.Generation, TargetDigest: binding.TargetDigest,
		ReasonCode: binding.ReasonCode, StartedAt: now,
	}
}

func manualIntentMatchesPlan(intent registry.ManualRetirementIntent, plan retireSessionPlan, member retireSessionPlanMember) bool {
	return intent.PlanID == plan.PlanID && intent.RowDigest == member.RowDigest && intent.Root == plan.Root &&
		intent.Agent == member.Agent && intent.Adapter == plan.Adapter && intent.Target == member.Target &&
		intent.AMQExecutable == plan.AMQExecutable && intent.InjectVia == plan.InjectVia &&
		intent.AMQIdentity == plan.AMQIdentity && intent.InjectIdentity == plan.InjectIdentity && intent.TimeoutNanos == plan.TimeoutNanos &&
		intent.Generation != "" && intent.TargetDigest != "" && validManualPreflightReason(intent.ReasonCode)
}

func retireResultFromIntent(intent registry.ManualRetirementIntent) amq.RetireWakeResult {
	return amq.RetireWakeResult{
		Schema: 1, Status: "eligible", ReasonCode: intent.ReasonCode, Root: intent.Root, Agent: intent.Agent,
		Generation: intent.Generation, TargetDigest: intent.TargetDigest,
	}
}

func validateManualRetireSessionCheck(root, agent string, result amq.RetireWakeResult) error {
	if result.Schema != 1 || result.Status != "eligible" || !validManualPreflightReason(result.ReasonCode) {
		return fmt.Errorf("unexpected manual preflight contract: schema=%d status=%q reason_code=%q", result.Schema, result.Status, result.ReasonCode)
	}
	if result.Generation == "" || result.TargetDigest == "" {
		return errors.New("manual preflight lacks exact generation/digest binding")
	}
	return validateRetireSessionIdentity(root, agent, result)
}

func validateManualRetireSessionMutation(root, agent string, checked, retired amq.RetireWakeResult) error {
	validCompletion := retired.Status == "retired" && validManualCompletion(checked.ReasonCode, retired.ReasonCode) ||
		checked.ReasonCode == amq.ManualEligibleReason && retired.Status == "already_retired" && retired.ReasonCode == "tombstone_match"
	if retired.Schema != 1 || !validCompletion {
		return fmt.Errorf("unexpected manual retirement contract: schema=%d status=%q reason_code=%q", retired.Schema, retired.Status, retired.ReasonCode)
	}
	if retired.Generation != checked.Generation || retired.TargetDigest != checked.TargetDigest {
		return errors.New("manual retirement result does not match its exact preflight generation/digest")
	}
	return validateRetireSessionIdentity(root, agent, retired)
}

func validManualPreflightReason(reason string) bool {
	return reason == amq.ManualEligibleReason || reason == amq.ManualAbsentEligibleReason
}

func validManualCompletion(preflight, completion string) bool {
	return preflight == amq.ManualEligibleReason && completion == amq.ManualRetiredReason ||
		preflight == amq.ManualAbsentEligibleReason && completion == amq.ManualAbsentRetiredReason
}

func validateRetireSessionIdentity(root, agent string, result amq.RetireWakeResult) error {
	wantRoot, err := canonicalRetireSessionRoot(root)
	if err != nil {
		return err
	}
	gotRoot, err := canonicalRetireSessionRoot(result.Root)
	if err != nil || gotRoot != wantRoot || result.Agent != agent {
		return errors.New("manual retirement response root/agent identity mismatch")
	}
	return nil
}

func retiredLegacySessionEntry(entry registry.Entry, rowDigest string, now time.Time, result amq.RetireWakeResult) registry.Entry {
	entry.State = registry.StateRetired
	entry.RetiredAt = now
	entry.RetirementOutcome = result.Status
	entry.RetirementReason = result.ReasonCode
	entry.OwnerGoneSince = time.Time{}
	entry.GCFailureCount = 0
	entry.GCBackoffUntil = time.Time{}
	entry.GCQuarantinedAt = time.Time{}
	entry.GCQuarantineReason = ""
	entry.LastGCDecision = "retired"
	entry.LastGCReason = result.ReasonCode
	entry.LastError = ""
	entry.ManualRetirementReceipt = registry.ManualRetirementReceipt{
		PlanID: entry.ManualRetirementIntent.PlanID, RowDigest: rowDigest,
		Generation: result.Generation, TargetDigest: result.TargetDigest,
		CompletedAt: now, PreflightReasonCode: entry.ManualRetirementIntent.ReasonCode, ReasonCode: result.ReasonCode,
	}
	entry.ManualRetirementIntent = registry.ManualRetirementIntent{}
	return entry
}

func legacyOrUnboundRetireSessionEntry(entry registry.Entry) bool {
	return entry.LegacyUnbound || !entry.WakeOwnerPresent || !entry.WakeOwner.Strong() || !entry.WakeBinding.Complete()
}

func canonicalRetireSessionRoot(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("--root is required")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(real), nil
	}
	return abs, nil
}

func canonicalRetireSessionRegistry(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("registry path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	real = filepath.Clean(real)
	info, err := os.Stat(real)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%q is not a regular registry file", real)
	}
	return real, nil
}

func parseRequiredAgents(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("--agents is required and must explicitly name every agent")
	}
	parts := strings.Split(raw, ",")
	agents := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		agent := strings.TrimSpace(part)
		if agent == "" {
			return nil, errors.New("--agents must contain non-empty handles")
		}
		if seen[agent] {
			return nil, fmt.Errorf("--agents contains duplicate handle %q", agent)
		}
		seen[agent] = true
		agents = append(agents, agent)
	}
	sort.Strings(agents)
	return agents, nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
