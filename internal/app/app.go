package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ohade/amq-keepalive/internal/adapter"
	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/executable"
	"github.com/ohade/amq-keepalive/internal/hookinstall"
	"github.com/ohade/amq-keepalive/internal/launchd"
	"github.com/ohade/amq-keepalive/internal/registry"
	"github.com/ohade/amq-keepalive/internal/supervisor"
)

type App struct {
	Stdout io.Writer
	Stderr io.Writer
	Now    func() time.Time
}

type wakeLifecycle interface {
	supervisor.WakeRunner
	Env(context.Context) (amq.Env, error)
	RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error)
}

var errIdentitySafeWakeRetireUnavailable = errors.New("AMQ does not advertise the wake_gc_v1 identity-safe-retire capability")

func (a App) Run(ctx context.Context, args []string) int {
	if a.Stdout == nil {
		a.Stdout = os.Stdout
	}
	if a.Stderr == nil {
		a.Stderr = os.Stderr
	}
	if len(args) == 0 {
		a.usage(a.Stderr)
		return 2
	}

	var err error
	switch args[0] {
	case "-h", "--help", "help":
		a.usage(a.Stdout)
		return 0
	case "attach":
		err = a.attach(ctx, args[1:])
	case "reattach":
		err = a.reattach(ctx, args[1:])
	case "supervise":
		err = a.supervise(ctx, args[1:])
	case "inject":
		err = a.inject(ctx, args[1:])
	case "doctor":
		err = a.doctor(ctx, args[1:])
	case "gc":
		err = a.gc(ctx, args[1:])
	case "retire-session":
		err = a.retireSession(ctx, args[1:])
	case "forget":
		err = a.forget(ctx, args[1:])
	case "install-launchd":
		err = a.installLaunchd(ctx, args[1:])
	case "install-hook":
		err = a.installHook(args[1:])
	case "uninstall":
		err = a.uninstallLaunchd(ctx, args[1:])
	default:
		fmt.Fprintf(a.Stderr, "unknown command %q\n", args[0])
		a.usage(a.Stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintln(a.Stderr, err)
		return 1
	}
	return 0
}

type registerOptions struct {
	RegistryPath   string
	AdapterName    string
	Target         string
	BaselineFile   string
	BaselineDigest string
	Root           string
	BaseRoot       string
	SessionName    string
	Me             string
	AMQPath        string
	Self           string
	WakeTimeout    time.Duration
	NoStart        bool
	Replace        bool
	RetireDetached bool
}

type registerResult struct {
	Entry          registry.Entry   `json:"entry"`
	RemovedEntries []registry.Entry `json:"removed_entries,omitempty"`
}

func (a App) attach(ctx context.Context, args []string) error {
	return a.register(ctx, args, false)
}

func (a App) reattach(ctx context.Context, args []string) error {
	return a.register(ctx, args, true)
}

func (a App) register(ctx context.Context, args []string, replace bool) error {
	commandName := "attach"
	if replace {
		commandName = "reattach"
	}
	fs := flag.NewFlagSet(commandName, flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	adapterName := fs.String("adapter", "file", "adapter name")
	target := fs.String("target", "", "adapter target")
	baselineFile := fs.String("baseline-file", "", "pre-launch unread-floor manifest from amq coop exec --defer-wake")
	root := fs.String("root", "", "AMQ root")
	baseRoot := fs.String("base-root", "", "AMQ base root")
	sessionName := fs.String("session", "", "AMQ session name")
	me := fs.String("me", "", "AMQ agent handle")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path for --inject-via")
	wakeTimeout := fs.Duration("wake-ready-timeout", 10*time.Second, "maximum time to wait for amq wake readiness")
	noStart := fs.Bool("no-start", false, "register without starting/reconciling wake")
	retireDetached := fs.Bool("retire-detached", true, "recover a blocked reattach through owner-bound exact wake retirement")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return a.registerWithOptions(ctx, registerOptions{
		RegistryPath:   *registryPath,
		AdapterName:    *adapterName,
		Target:         *target,
		BaselineFile:   *baselineFile,
		Root:           *root,
		BaseRoot:       *baseRoot,
		SessionName:    *sessionName,
		Me:             *me,
		AMQPath:        *amqPath,
		Self:           *self,
		WakeTimeout:    *wakeTimeout,
		NoStart:        *noStart,
		Replace:        replace,
		RetireDetached: *retireDetached,
	})
}

func (a App) registerWithOptions(ctx context.Context, opts registerOptions) error {
	envCLI := amq.NewCLI(opts.AMQPath)
	wakeOwner, wakeOwnerErr := amq.WakeOwnerFromEnvironment()
	var amqEnvironment amq.Env
	var amqEnvironmentErr error
	if !opts.NoStart || opts.Root == "" || opts.Me == "" || opts.BaseRoot == "" || opts.SessionName == "" {
		amqEnvironment, amqEnvironmentErr = envCLI.Env(ctx)
		if amqEnvironmentErr != nil && (opts.Root == "" || opts.Me == "") {
			return amqEnvironmentErr
		}
		if opts.Root == "" {
			opts.Root = amqEnvironment.Root
		}
		if opts.BaseRoot == "" {
			opts.BaseRoot = amqEnvironment.BaseRoot
		}
		if opts.SessionName == "" {
			opts.SessionName = amqEnvironment.SessionName
		}
		if opts.Me == "" {
			opts.Me = amqEnvironment.Me
		}
	}
	opts.Root, opts.BaseRoot = normalizeAMQPaths(opts.Root, opts.BaseRoot, opts.SessionName)
	if opts.BaselineFile != "" {
		opts.BaselineFile = filepath.Clean(strings.TrimSpace(opts.BaselineFile))
		if !filepath.IsAbs(opts.BaselineFile) {
			return errors.New("--baseline-file must be absolute")
		}
		digest, err := amq.BaselineDigest(opts.BaselineFile)
		if err != nil {
			return fmt.Errorf("--baseline-file: %w", err)
		}
		opts.BaselineDigest = digest
	}

	adapters := adapter.DefaultRegistry()
	selected, err := adapters.Get(opts.AdapterName)
	if err != nil {
		return err
	}
	if opts.Target == "" {
		discoverer, ok := selected.(adapter.Discoverer)
		if !ok {
			return errors.New("--target is required")
		}
		discovered, err := discoverer.Discover(ctx)
		if err != nil {
			return err
		}
		opts.Target = discovered
	}
	if normalizer, ok := selected.(adapter.TargetNormalizer); ok {
		normalized, err := normalizer.NormalizeTarget(opts.Target)
		if err != nil {
			return err
		}
		opts.Target = normalized
	}
	if !opts.NoStart {
		if capabilityErr := managedWakeCapabilityError(amqEnvironmentErr, amqEnvironment); capabilityErr != nil {
			return capabilityErr
		}
	}
	store := registry.New(opts.RegistryPath)
	next := registry.Entry{
		ID:               registry.EntryID(opts.Root, opts.Me, opts.AdapterName, opts.Target),
		Root:             opts.Root,
		BaseRoot:         opts.BaseRoot,
		SessionName:      opts.SessionName,
		Agent:            opts.Me,
		Adapter:          opts.AdapterName,
		Target:           opts.Target,
		BaselineFile:     opts.BaselineFile,
		BaselineDigest:   opts.BaselineDigest,
		State:            registry.StateAttached,
		WakeOwnerPresent: wakeOwnerErr == nil,
		WakeOwner: registry.WakeOwner{
			PID: wakeOwner.PID, ProcessStart: wakeOwner.ProcessStart,
			BootID: wakeOwner.BootID, SessionID: wakeOwner.SessionID,
		},
	}
	reconciler := supervisor.Reconciler{
		Wake:        envCLI,
		Adapter:     selected,
		InjectVia:   opts.Self,
		WakeTimeout: opts.WakeTimeout,
	}

	var entry registry.Entry
	var removed []registry.Entry
	err = store.WithRegistrationLockContext(ctx, func() error {
		// Refresh existence and physical ownership only after acquiring the
		// cross-process registration lease. The lease remains held through wake
		// readiness and registry commit, so a racing claimant observes this
		// transaction's committed owner before it can touch AMQ.
		inventory, err := registrationTargetInventory(ctx, selected, next.Target)
		if err != nil {
			return err
		}
		file, err := store.Load()
		if err != nil {
			return err
		}
		if err := registrationGCBatchError(file, next.Root); err != nil {
			return err
		}
		if inventory != nil {
			if err := checkPhysicalTargetAvailable(file, selected, inventory, next, opts.Replace); err != nil {
				return err
			}
			reconciler.Adapter = targetInventoryProbe{inventory: inventory}
		}

		if opts.Replace {
			if err := store.CheckTargetAvailable(next, true); err != nil {
				return err
			}
			// Persist an inactive reservation before touching AMQ. If this process
			// crashes after readiness, the supervisor can recover the registered
			// candidate; there is no post-readiness commit window which could leave
			// a live wake unregistered.
			entry, removed, err = store.ReplaceSessionAdapter(next)
			if err != nil || opts.NoStart {
				return err
			}
			next = entry
			if readinessErr := managedWakeReadinessError(wakeOwnerErr, amqEnvironmentErr, amqEnvironment); readinessErr != nil {
				next.LastError = readinessErr.Error()
				next.LastSupervisorDecision = supervisor.ActionBackoff
				if updateErr := store.UpdateEntry(next); updateErr != nil {
					return errors.Join(readinessErr, updateErr)
				}
				return readinessErr
			}
			wakeReady := false
			if !opts.NoStart {
				if opts.RetireDetached {
					var recoverErr error
					previousForRecovery := removed
					if next.Transition.Active() {
						previousForRecovery = []registry.Entry{transitionPreviousEntry(next.Transition)}
					}
					next, wakeReady, recoverErr = recoverDetachedRegistration(ctx, store, previousForRecovery, envCLI, reconciler, next)
					if recoverErr != nil {
						return resolveRegistrationReadinessFailure(store, entry, next, removed, recoverErr)
					}
				}
				if !wakeReady {
					updated, result := reconciler.StartFresh(ctx, next)
					if result.Error != nil {
						return resolveRegistrationReadinessFailure(store, entry, updated, removed, result.Error)
					}
					next = updated
				}
			}
			next.Transition = registry.ReattachTransition{}
			if err := store.UpdateEntry(next); err != nil {
				return fmt.Errorf("wake is ready and its attached registry reservation remains recoverable, but marking it active failed: %w", err)
			}
			entry = next
			return nil
		}

		entry, err = store.Upsert(next)
		if err != nil || opts.NoStart {
			return err
		}
		if readinessErr := managedWakeReadinessError(wakeOwnerErr, amqEnvironmentErr, amqEnvironment); readinessErr != nil {
			entry.LastError = readinessErr.Error()
			entry.LastSupervisorDecision = supervisor.ActionBackoff
			if updateErr := store.UpdateEntry(entry); updateErr != nil {
				return errors.Join(readinessErr, updateErr)
			}
			return readinessErr
		}
		updated, result := reconciler.Reconcile(ctx, entry)
		if updateErr := store.UpdateEntry(updated); updateErr != nil {
			return updateErr
		}
		entry = updated
		if result.Error != nil && result.Action != supervisor.ActionDetached {
			return result.Error
		}
		return nil
	})
	if err != nil {
		return err
	}
	if opts.Replace {
		return printJSON(a.Stdout, registerResult{Entry: entry, RemovedEntries: removed})
	}
	return printJSON(a.Stdout, entry)
}

func managedWakeCapabilityError(environmentErr error, environment amq.Env) error {
	if environmentErr != nil {
		return fmt.Errorf("AMQ wake unavailable; messages remain queued: capability check failed: %w", environmentErr)
	}
	if !environment.HasCapability(amq.CapabilityWakeGCV1) {
		return fmt.Errorf("AMQ wake unavailable; messages remain queued: %w", errIdentitySafeWakeRetireUnavailable)
	}
	return nil
}

func managedWakeReadinessError(ownerErr, environmentErr error, environment amq.Env) error {
	if ownerErr != nil {
		return fmt.Errorf("AMQ wake unavailable; messages remain queued: %w", ownerErr)
	}
	return managedWakeCapabilityError(environmentErr, environment)
}

func resolveRegistrationReadinessFailure(
	store *registry.Store,
	reservation registry.Entry,
	candidate registry.Entry,
	removed []registry.Entry,
	readinessErr error,
) error {
	if errors.Is(readinessErr, amq.ErrWakeReadinessUncertain) ||
		candidate.Transition.Phase == registry.TransitionRetirePending ||
		candidate.Transition.Phase == registry.TransitionOldRetired {
		if candidate != reservation {
			if updateErr := store.UpdateEntry(candidate); updateErr != nil {
				return errors.Join(
					readinessErr,
					fmt.Errorf("wake readiness is uncertain and the attached reservation remains, but recording its retry state failed: %w", updateErr),
				)
			}
		}
		if candidate.Transition.Phase == registry.TransitionRetirePending {
			return fmt.Errorf("old wake retirement has an ambiguous outcome; the new inactive registry transition was preserved for exact recovery: %w", readinessErr)
		}
		if candidate.Transition.Phase == registry.TransitionOldRetired {
			return fmt.Errorf("old wake is retired; the new inactive registry transition was preserved for supervisor convergence: %w", readinessErr)
		}
		return fmt.Errorf("wake readiness is uncertain; the attached registry reservation was preserved for supervisor convergence: %w", readinessErr)
	}
	restored, restoreErr := store.RestoreSessionAdapterIfUnchanged(reservation, removed)
	if restoreErr != nil {
		return errors.Join(readinessErr, fmt.Errorf("restore previous registry entries after wake readiness failure: %w", restoreErr))
	}
	if !restored {
		return errors.Join(readinessErr, errors.New("registry reservation changed before previous entries could be restored; the attached reservation was preserved for supervisor recovery"))
	}
	return readinessErr
}

type targetInventoryProbe struct {
	inventory adapter.TargetInventory
}

func (p targetInventoryProbe) Probe(_ context.Context, target string) error {
	return p.inventory.Probe(target)
}

func registrationTargetInventory(ctx context.Context, selected adapter.Adapter, target string) (adapter.TargetInventory, error) {
	provider, ok := selected.(adapter.InventoryProvider)
	if !ok {
		return nil, selected.Probe(ctx, target)
	}
	inventory, err := provider.Inventory(ctx)
	if err != nil {
		return nil, err
	}
	if inventory == nil {
		return nil, errors.New("adapter returned a nil target inventory")
	}
	if err := inventory.Probe(target); err != nil {
		return nil, err
	}
	return inventory, nil
}

func checkPhysicalTargetAvailable(
	file registry.File,
	selected adapter.Adapter,
	inventory adapter.TargetInventory,
	candidate registry.Entry,
	ignoreSameRootAgent bool,
) error {
	candidateKey, err := inventory.OwnershipKey(candidate.Target)
	if err != nil {
		return fmt.Errorf("resolve requested physical target ownership: %w", err)
	}
	for _, existing := range file.Entries {
		if existing.State == registry.StateRetired || existing.Adapter != candidate.Adapter || existing.ID == candidate.ID {
			continue
		}
		if ignoreSameRootAgent && existing.Root == candidate.Root && existing.Agent == candidate.Agent {
			continue
		}
		target, err := normalizedTarget(selected, existing.Target)
		if err != nil {
			return fmt.Errorf("resolve registered target ownership for %s@%s: %w", existing.Agent, existing.Root, err)
		}
		existingKey, err := inventory.OwnershipKey(target)
		if errors.Is(err, adapter.ErrTargetNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("resolve registered physical target ownership for %s@%s: %w", existing.Agent, existing.Root, err)
		}
		if existingKey == candidateKey {
			return fmt.Errorf(
				"%w: adapter=%q physical_identity=%q requested_target=%q requested_by=%s@%s existing_target=%q existing_owner=%s@%s existing_id=%s",
				registry.ErrTargetOwned, candidate.Adapter, candidateKey, candidate.Target, candidate.Agent, candidate.Root,
				existing.Target, existing.Agent, existing.Root, existing.ID,
			)
		}
	}
	return nil
}

// recoverDetachedRegistration converges a durable old-to-new reservation. It
// always starts the new wake first. Only an exact, owner-gone, generation-bound
// old wake may be retired; after that positive transition the old registry row
// is never restored, even if the retry fails.
func recoverDetachedRegistration(
	ctx context.Context,
	store *registry.Store,
	previousEntries []registry.Entry,
	lifecycle interface {
		Env(context.Context) (amq.Env, error)
		RetireWake(context.Context, amq.RetireWakeRequest) (amq.RetireWakeResult, error)
	},
	reconciler supervisor.Reconciler,
	next registry.Entry,
) (registry.Entry, bool, error) {
	matches := make([]registry.Entry, 0, 1)
	for _, entry := range previousEntries {
		if entry.Root == next.Root && entry.Agent == next.Agent && entry.State != registry.StateRetired {
			matches = append(matches, entry)
		}
	}
	if len(matches) == 0 {
		return next, false, nil
	}
	if len(matches) != 1 {
		return next, false, fmt.Errorf("refusing detached wake recovery for %s at %s: expected one registry entry, found %d", next.Agent, next.Root, len(matches))
	}
	previous := matches[0]
	updated, initialStart := reconciler.StartFresh(ctx, next)
	if initialStart.Error == nil && initialStart.Action == supervisor.ActionEnsured {
		return updated, true, nil
	}
	if initialStart.Error == nil {
		return updated, false, fmt.Errorf("recover detached %s wake start was deferred with action %q", previous.Agent, initialStart.Action)
	}
	if errors.Is(initialStart.Error, amq.ErrWakeReadinessUncertain) {
		return updated, false, fmt.Errorf("recover detached %s wake has uncertain readiness: %w", previous.Agent, initialStart.Error)
	}
	if next.Transition.Phase == registry.TransitionOldRetired {
		return updated, false, fmt.Errorf("old wake is already retired; new inactive reservation retained for supervisor retry: %w", initialStart.Error)
	}
	var structuredStart *amq.WakeStartError
	if !errors.As(initialStart.Error, &structuredStart) {
		return updated, false, fmt.Errorf("recover detached %s wake: exact-target start did not prove an old-wake conflict; refusing retirement: %w", previous.Agent, initialStart.Error)
	}
	if !previous.WakeOwnerPresent || !previous.WakeOwner.Strong() || !previous.WakeBinding.Complete() {
		return updated, false, fmt.Errorf("recover detached %s wake: exact-target start failed: %v; old wake lacks owner-bound generation metadata", previous.Agent, initialStart.Error)
	}
	if blockerErr := amq.ValidateExistingWakeBlocker(previous.Root, previous.Agent, amq.WakeBinding{
		Generation: previous.WakeBinding.Generation, TargetDigest: previous.WakeBinding.TargetDigest,
	}, structuredStart.Result); blockerErr != nil {
		return updated, false, fmt.Errorf("recover detached %s wake: start failure did not prove the persisted old wake is the blocker: %w", previous.Agent, blockerErr)
	}
	environment, environmentErr := lifecycle.Env(ctx)
	if environmentErr != nil {
		return updated, false, fmt.Errorf("recover detached %s wake: wake_gc_v1 capability check failed: %w", previous.Agent, environmentErr)
	}
	if !environment.HasCapability(amq.CapabilityWakeGCV1) {
		return updated, false, fmt.Errorf("recover detached %s wake: AMQ does not advertise wake_gc_v1; refusing retirement", previous.Agent)
	}
	request := amq.RetireWakeRequest{
		Root: previous.Root, Me: previous.Agent, InjectVia: reconciler.InjectVia,
		Adapter: previous.Adapter, Target: previous.Target,
		Generation: previous.WakeBinding.Generation, TargetDigest: previous.WakeBinding.TargetDigest,
		RequireOwnerGone: true, Timeout: 5 * time.Second,
	}
	checkRequest := request
	checkRequest.Check = true
	checked, checkErr := lifecycle.RetireWake(ctx, checkRequest)
	if !supervisor.SafeRetirementResult(checkRequest, checked) && (checkErr != nil || checked.Status != "eligible") {
		if checkErr == nil {
			checkErr = fmt.Errorf("status=%q reason_code=%q", checked.Status, checked.ReasonCode)
		}
		return updated, false, fmt.Errorf("recover detached %s wake: owner-gone check refused: %w", previous.Agent, checkErr)
	}
	retired := checked
	retireErr := checkErr
	if checked.Status == "eligible" {
		next.Transition.Phase = registry.TransitionRetirePending
		if err := store.UpdateEntry(next); err != nil {
			return next, false, fmt.Errorf("refusing old wake retirement because the durable pending transition could not be recorded: %w", err)
		}
		retired, retireErr = lifecycle.RetireWake(ctx, request)
	}
	if !supervisor.SafeRetirementResult(request, retired) {
		if retireErr == nil {
			retireErr = errors.New("AMQ did not positively prove the old generation inactive")
		}
		pending := next
		pending.FailureCount = updated.FailureCount
		pending.BackoffUntil = updated.BackoffUntil
		pending.LastError = retireErr.Error()
		pending.LastSupervisorDecision = supervisor.ActionStartFailed
		return pending, false, fmt.Errorf("recover detached %s wake retirement refused: %w", previous.Agent, retireErr)
	}
	next.Transition.Phase = registry.TransitionOldRetired
	next.RetirementOutcome = retired.Status
	next.RetirementReason = retired.ReasonCode
	if err := store.UpdateEntry(next); err != nil {
		return next, false, fmt.Errorf("old wake is retired but preserving the new transition failed: %w", err)
	}
	updated, retried := reconciler.StartFresh(ctx, next)
	if retried.Error != nil || retried.Action != supervisor.ActionEnsured {
		if retried.Error == nil {
			retried.Error = fmt.Errorf("wake start was deferred with action %q", retried.Action)
		}
		return updated, false, fmt.Errorf("old wake is retired; new inactive reservation retained for supervisor retry: %w", retried.Error)
	}
	return updated, true, nil
}

type gcRootBatchPlan struct {
	Canonical string
	Members   []registry.Entry
	Oversized bool
}

type supervisionPass struct {
	Results        []supervisor.Result
	AttemptedRoot  string
	PendingGCRoots bool
	BatchExecuted  bool
}

type supervisePassFunc func(bool) (supervisionPass, error)
type superviseWaitFunc func(context.Context, time.Duration) error

func (a App) now() time.Time {
	if a.Now != nil {
		return a.Now().UTC()
	}
	return time.Now().UTC()
}

func canonicalGCRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", errors.New("GC root is empty")
	}
	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", err
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return real, nil
	}
	return abs, nil
}

func gcEntryLocallyEligible(entry registry.Entry, now time.Time, policy supervisor.GCPolicy) bool {
	if entry.State == registry.StateRetired || entry.ManualRetirementIntent.Active() || !entry.GCQuarantinedAt.IsZero() || entry.Transition.Active() || entry.LegacyUnbound ||
		!entry.WakeOwnerPresent || !entry.WakeOwner.Strong() || !entry.WakeBinding.Complete() {
		return false
	}
	if !entry.GCBackoffUntil.IsZero() && now.Before(entry.GCBackoffUntil) {
		return false
	}
	if entry.LastGCDecision == supervisor.GCStatusEligible {
		return true
	}
	grace := policy.OwnerGrace
	if grace < supervisor.MinOwnerGoneGrace {
		grace = supervisor.MinOwnerGoneGrace
	}
	return !entry.OwnerGoneSince.IsZero() && now.Sub(entry.OwnerGoneSince) >= grace
}

// entryMayStartWake is deliberately conservative. A positive result does not
// mean StartWake will run (the adapter probe can still defer it), only that the
// local state does not prove that this pass cannot reach StartWake.
func entryMayStartWake(entry registry.Entry, now time.Time) bool {
	if entry.State == registry.StateRetired || entry.ManualRetirementIntent.Active() {
		return false
	}
	if !entry.NextHealthCheck.IsZero() && now.Before(entry.NextHealthCheck) {
		return false
	}
	if !entry.BackoffUntil.IsZero() && now.Before(entry.BackoffUntil) {
		return false
	}
	return !entry.LegacyUnbound && entry.WakeOwnerPresent && entry.WakeOwner.Strong()
}

func planGCRootBatch(entries []registry.Entry, now time.Time, policy supervisor.GCPolicy) (gcRootBatchPlan, error) {
	return planGCRootBatchWithAttempts(entries, nil, now, policy)
}

func planGCRootBatchForFile(file registry.File, now time.Time, policy supervisor.GCPolicy) (gcRootBatchPlan, error) {
	return planGCRootBatchWithAttempts(file.Entries, file.GCRootAttempts, now, policy)
}

func planGCRootBatchWithAttempts(entries []registry.Entry, attempts []registry.GCRootAttempt, now time.Time, policy supervisor.GCPolicy) (gcRootBatchPlan, error) {
	pendingRoots := make(map[string]struct{})
	for _, entry := range entries {
		if !entry.ManualRetirementIntent.Active() || entry.State == registry.StateRetired {
			continue
		}
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			return gcRootBatchPlan{}, err
		}
		pendingRoots[root] = struct{}{}
	}
	recentRoots := make(map[string]struct{})
	windowStart := now.Add(-supervisor.GCRootWindow)
	for _, attempt := range attempts {
		if attempt.StartedAt.IsZero() || attempt.StartedAt.Before(windowStart) {
			continue
		}
		root, err := canonicalGCRoot(attempt.CanonicalRoot)
		if err != nil {
			return gcRootBatchPlan{}, err
		}
		recentRoots[root] = struct{}{}
	}
	for _, entry := range entries {
		if entry.LastGCRootBatchAt.IsZero() || entry.LastGCRootBatchAt.Before(windowStart) {
			continue
		}
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			return gcRootBatchPlan{}, err
		}
		if _, pending := pendingRoots[root]; pending {
			continue
		}
		recentRoots[root] = struct{}{}
	}
	candidateSince := make(map[string]time.Time)
	for _, entry := range entries {
		if !gcEntryLocallyEligible(entry, now, policy) {
			continue
		}
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			return gcRootBatchPlan{}, err
		}
		if _, pending := pendingRoots[root]; pending {
			continue
		}
		if _, recent := recentRoots[root]; recent {
			continue
		}
		since := entry.OwnerGoneSince
		previous, exists := candidateSince[root]
		if !exists || since.Before(previous) {
			candidateSince[root] = since
		}
	}
	selected := ""
	var selectedSince time.Time
	for root, since := range candidateSince {
		if selected == "" || since.Before(selectedSince) || (since.Equal(selectedSince) && root < selected) {
			selected = root
			selectedSince = since
		}
	}
	if selected == "" {
		return gcRootBatchPlan{}, nil
	}
	if len(recentRoots) >= supervisor.MaxRootsPerGCWindow {
		return gcRootBatchPlan{}, nil
	}
	members := make([]registry.Entry, 0)
	for _, entry := range entries {
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			return gcRootBatchPlan{}, err
		}
		if root == selected && entry.State != registry.StateRetired {
			members = append(members, entry)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	return gcRootBatchPlan{Canonical: selected, Members: members, Oversized: len(members) > supervisor.MaxAgentsPerRootBatch}, nil
}

func persistGCRootBatchMarker(store *registry.Store, file *registry.File, plan gcRootBatchPlan, now time.Time) error {
	if plan.Canonical == "" {
		return nil
	}
	memberIDs := make(map[string]struct{}, len(plan.Members))
	for _, member := range plan.Members {
		memberIDs[member.ID] = struct{}{}
	}
	updates := make([]registry.EntryUpdate, 0)
	for index, entry := range file.Entries {
		if _, selected := memberIDs[entry.ID]; !selected || entry.State == registry.StateRetired {
			continue
		}
		updated := entry
		updated.LastGCRootBatchAt = now
		if plan.Oversized {
			updated.GCBackoffUntil = now.Add(supervisor.GCRootTerminalBackoff)
			updated.LastGCDecision = supervisor.GCStatusSkipped
			updated.LastGCReason = "root_batch_oversized"
		}
		updates = append(updates, registry.EntryUpdate{Before: entry, After: updated})
		file.Entries[index] = updated
	}
	updatedFile, err := store.RecordGCRootAttempt(plan.Canonical, now, updates)
	if err != nil {
		return err
	}
	*file = updatedFile
	return nil
}

func gcRootMemberLocalBlock(entry registry.Entry) string {
	switch {
	case entry.ManualRetirementIntent.Active():
		return "manual_retirement_pending"
	case !entry.GCQuarantinedAt.IsZero():
		return "gc_quarantined"
	case entry.Transition.Active():
		return "transition_active"
	case entry.LegacyUnbound || !entry.WakeOwnerPresent || !entry.WakeOwner.Strong():
		return "owner_unbound"
	case !entry.WakeBinding.Complete():
		return "binding_unavailable"
	case entry.ID == "" || entry.Root == "" || entry.Agent == "" || entry.Adapter == "" || entry.Target == "":
		return "identity_incomplete"
	default:
		return ""
	}
}

func locallyBlockedGCRootResults(plan gcRootBatchPlan) map[string]supervisor.Result {
	results := make(map[string]supervisor.Result, len(plan.Members))
	for _, entry := range plan.Members {
		reason := gcRootMemberLocalBlock(entry)
		if reason == "" {
			reason = "root_batch_blocked_by_sibling"
		}
		item := supervisor.GCResult{
			EntryID: entry.ID, Status: supervisor.GCStatusSkipped, ReasonCode: reason,
			Reason:          "a non-retired listener in the canonical root lacks safe frozen-batch metadata",
			OwnerBound:      entry.WakeOwnerPresent && entry.WakeOwner.Strong() && !entry.LegacyUnbound,
			BindingComplete: entry.WakeBinding.Complete(),
		}
		results[entry.ID] = gcSupervisorResult(item)
	}
	return results
}

func persistLocallyBlockedGCRoot(store *registry.Store, file *registry.File, plan gcRootBatchPlan, now time.Time) error {
	updates := make([]registry.EntryUpdate, 0, len(plan.Members))
	for _, entry := range plan.Members {
		updated := entry
		updated.LastGCRootBatchAt = now
		updated.GCBackoffUntil = now.Add(supervisor.GCRootTerminalBackoff)
		updated.LastGCDecision = supervisor.GCStatusSkipped
		updated.LastGCReason = gcRootMemberLocalBlock(entry)
		if updated.LastGCReason == "" {
			updated.LastGCReason = "root_batch_blocked_by_sibling"
		}
		updates = append(updates, registry.EntryUpdate{Before: entry, After: updated})
	}
	updatedFile, err := store.RecordGCRootAttempt(plan.Canonical, now, updates)
	if err != nil {
		return err
	}
	*file = updatedFile
	return nil
}

func gcRootPlanHasLocalBlock(plan gcRootBatchPlan) bool {
	for _, entry := range plan.Members {
		if gcRootMemberLocalBlock(entry) != "" {
			return true
		}
	}
	return false
}

func frozenGCRootBatch(plan gcRootBatchPlan, now time.Time) registry.GCRootBatch {
	members := make([]registry.GCRootBatchMember, 0, len(plan.Members))
	ids := make([]string, 0, len(plan.Members))
	for _, entry := range plan.Members {
		members = append(members, registry.GCRootBatchMember{
			EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
			WakeOwnerPresent: entry.WakeOwnerPresent, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
		})
		ids = append(ids, entry.ID)
	}
	return registry.GCRootBatch{
		ID:            registry.EntryID(plan.Canonical, "gc-root-batch", now.Format(time.RFC3339Nano), strings.Join(ids, "\x00")),
		CanonicalRoot: plan.Canonical, StartedAt: now, Phase: registry.GCRootBatchPreflight, Members: members,
	}
}

func activeGCRootBatch(file registry.File) (registry.GCRootBatch, bool, error) {
	if len(file.GCRootBatches) > 1 {
		return registry.GCRootBatch{}, false, fmt.Errorf("registry contains %d active GC root batches", len(file.GCRootBatches))
	}
	if len(file.GCRootBatches) == 0 {
		return registry.GCRootBatch{}, false, nil
	}
	return file.GCRootBatches[0], true, nil

}

func registrationGCBatchError(file registry.File, root string) error {
	canonical, err := canonicalGCRoot(root)
	if err != nil {
		return err
	}
	for _, batch := range file.GCRootBatches {
		if batch.CanonicalRoot == canonical {
			return fmt.Errorf("GC root batch %s is active for %s; registration is deferred until its frozen membership converges", batch.ID, canonical)
		}
	}
	return nil
}

func validateNoAmbiguousLiveSessions(entries []registry.Entry) error {
	counts := make(map[string]int)
	for _, entry := range entries {
		if entry.State == registry.StateRetired {
			continue
		}
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			return err
		}
		key := root + "\x00" + entry.Agent
		counts[key]++
		if counts[key] > 1 {
			return fmt.Errorf("%w: root=%q agent=%q count=%d", registry.ErrAmbiguousSession, root, entry.Agent, counts[key])
		}
	}
	return nil
}

func (a App) supervise(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("supervise", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path for --inject-via")
	once := fs.Bool("once", false, "run one supervisor pass")
	interval := fs.Duration("interval", time.Minute, "supervisor interval")
	wakeTimeout := fs.Duration("wake-ready-timeout", 10*time.Second, "maximum time to wait for amq wake readiness")
	autoGC := fs.Bool("auto-gc", false, "retire owner-bound wakes after two positive owner-gone observations")
	ownerGrace := fs.Duration("owner-gone-grace", 5*time.Minute, "minimum interval between positive owner-gone observations")
	retiredRetention := fs.Duration("retired-retention", 24*time.Hour, "diagnostic retention for retired registry rows")
	gcTimeout := fs.Duration("gc-timeout", 5*time.Second, "deadline for each AMQ lifecycle command")
	legacyGCMax := fs.Int("gc-max-per-pass", 1, "deprecated compatibility option; ignored in favor of hard root-batch limits")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval <= 0 {
		return errors.New("--interval must be greater than zero")
	}
	if *legacyGCMax <= 0 {
		return errors.New("deprecated --gc-max-per-pass must be greater than zero")
	}
	if err := validateGCPolicy(*ownerGrace, *retiredRetention, *gcTimeout); err != nil {
		return err
	}
	gcPolicy := supervisor.GCPolicy{AutoGC: *autoGC, OwnerGrace: *ownerGrace, RetiredRetention: *retiredRetention, Timeout: *gcTimeout}

	runOnce := func(emitJSON bool) (supervisionPass, error) {
		pass, err := a.superviseOnceWithGCState(ctx, *registryPath, amq.NewCLI(*amqPath), *self, *wakeTimeout, gcPolicy)
		if err != nil {
			return pass, err
		}
		if emitJSON {
			return pass, printJSON(a.Stdout, pass.Results)
		}
		return pass, nil
	}
	if *once {
		_, err := runOnce(true)
		return err
	}
	return runSuperviseLoop(ctx, *interval, a.Stderr, runOnce, waitSuperviseDelay)
}

func waitSuperviseDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runSuperviseLoop(ctx context.Context, interval time.Duration, stderr io.Writer, runOnce supervisePassFunc, wait superviseWaitFunc) error {
	for {
		pass, err := runOnce(false)
		if err != nil {
			if _, writeErr := fmt.Fprintln(stderr, err); writeErr != nil {
				return errors.Join(err, fmt.Errorf("write supervisor failure diagnostic: %w", writeErr))
			}
		}
		// A durable batch must keep its five-second recovery cadence even when
		// the pass failed. The returned pass is authoritative about pending
		// coordinator state; the error is diagnostic, not scheduling policy.
		delay := nextSuperviseDelay(interval, pass)
		if err != nil {
			// A failed pass may have stopped before it could discover or return a
			// durable coordinator. Retrying at the bounded catch-up cadence is the
			// only safe way to avoid silently stretching recovery to the ordinary
			// supervisor interval.
			delay = supervisor.GCCatchUpInterval
		}
		if waitErr := wait(ctx, delay); waitErr != nil {
			if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
				return nil
			}
			return waitErr
		}
	}
}

func nextSuperviseDelay(interval time.Duration, pass supervisionPass) time.Duration {
	if pass.BatchExecuted || (pass.AttemptedRoot != "" && pass.PendingGCRoots) {
		return supervisor.GCCatchUpInterval
	}
	return interval
}

func (a App) superviseOnce(ctx context.Context, registryPath string, wake supervisor.WakeRunner, self string, wakeTimeout time.Duration) ([]supervisor.Result, error) {
	return a.superviseOnceWithGC(ctx, registryPath, wake, self, wakeTimeout, supervisor.GCPolicy{}, 0)
}

// The final integer is retained for source compatibility with the pre-batch
// internal helper. It is deliberately ignored: GC throughput has only the
// hard root-batch limits and cannot be weakened by callers.
func (a App) superviseOnceWithGC(ctx context.Context, registryPath string, wake supervisor.WakeRunner, self string, wakeTimeout time.Duration, gcPolicy supervisor.GCPolicy, _ int) ([]supervisor.Result, error) {
	pass, err := a.superviseOnceWithGCState(ctx, registryPath, wake, self, wakeTimeout, gcPolicy)
	return pass.Results, err
}

func (a App) superviseOnceWithGCState(ctx context.Context, registryPath string, wake supervisor.WakeRunner, self string, wakeTimeout time.Duration, gcPolicy supervisor.GCPolicy) (supervisionPass, error) {
	store := registry.New(registryPath)
	var pass supervisionPass
	var capability bool
	var capabilityErr error
	lifecycle, lifecycleOK := wake.(wakeLifecycle)
	capabilityChecked := false
	if gcPolicy.AutoGC {
		capability, capabilityErr = probeWakeGCCapability(ctx, lifecycle, lifecycleOK)
		capabilityChecked = true
	}
	err := store.WithRegistrationLockContext(ctx, func() error {
		file, err := store.Load()
		if err != nil {
			return err
		}
		if err := validateNoAmbiguousLiveSessions(file.Entries); err != nil {
			return err
		}
		now := a.now()
		manualPendingEntries, err := manualRetirementPendingRootEntries(file.Entries)
		if err != nil {
			return err
		}
		batch, hasBatch, err := activeGCRootBatch(file)
		if err != nil {
			return err
		}
		var batchResults map[string]supervisor.Result
		if hasBatch {
			pass.AttemptedRoot = batch.CanonicalRoot
			pass.PendingGCRoots = true
			if !gcPolicy.AutoGC {
				if batch.Phase == registry.GCRootBatchRetiring {
					capability, capabilityErr = probeWakeGCCapability(ctx, lifecycle, lifecycleOK)
					capabilityChecked = true
					if capabilityErr != nil || !capability {
						if capabilityErr == nil {
							capabilityErr = errIdentitySafeWakeRetireUnavailable
						}
						return fmt.Errorf("persisted retiring GC root batch %s is retained because kill-switch check-only recovery is unavailable: %w", batch.ID, capabilityErr)
					}
				}
				batchResults, err = a.abortGCRootBatchForDisabledPolicy(ctx, store, &file, batch, lifecycle, capability, capabilityErr, self, gcPolicy)
				if err != nil {
					return err
				}
				pass.PendingGCRoots = false
			} else {
				if capabilityErr != nil {
					return fmt.Errorf("persisted GC root batch %s cannot resume capability check: %w", batch.ID, capabilityErr)
				}
				if !capability {
					return fmt.Errorf("persisted GC root batch %s cannot resume: %w", batch.ID, errIdentitySafeWakeRetireUnavailable)
				}
				batchResults, pass.PendingGCRoots, err = a.executeGCRootBatch(ctx, store, &file, batch, lifecycle, capability, capabilityErr, self, gcPolicy)
				if err != nil {
					return err
				}
			}
		} else if gcPolicy.AutoGC && lifecycleOK && capability && capabilityErr == nil {
			plan, planErr := planGCRootBatchForFile(file, now, gcPolicy)
			if planErr != nil {
				return planErr
			}
			if plan.Canonical != "" {
				pass.AttemptedRoot = plan.Canonical
				if plan.Oversized {
					if err := persistGCRootBatchMarker(store, &file, plan, now); err != nil {
						return err
					}
					batchResults = oversizedGCRootBatchResults(plan)
				} else if gcRootPlanHasLocalBlock(plan) {
					if err := persistLocallyBlockedGCRoot(store, &file, plan, now); err != nil {
						return err
					}
					batchResults = locallyBlockedGCRootResults(plan)
				} else {
					batch = frozenGCRootBatch(plan, now)
					file, err = store.StartGCRootBatch(batch, plan.Members)
					if err != nil {
						return err
					}
					batchResults, pass.PendingGCRoots, err = a.executeGCRootBatch(ctx, store, &file, batch, lifecycle, capability, capabilityErr, self, gcPolicy)
					if err != nil {
						return err
					}
				}
			}
		}
		if batchResults != nil {
			// A root batch can consume two lifecycle calls per member, each with
			// its own deadline. End this registration-lock lease immediately after
			// that bounded unit of work. A five-second follow-up pass reconciles
			// unrelated rows without allowing slow GC I/O to extend this lease.
			pass.BatchExecuted = true
			pass.Results = orderedBatchResults(file.Entries, batchResults)
			return nil
		}
		adapters := adapter.DefaultRegistry()
		probes := passProbes(file.Entries, adapters)
		conflicts := targetOwnershipConflicts(file, adapters)
		if anyEntryDue(file.Entries, now) {
			mergeOwnershipConflicts(conflicts, physicalOwnershipConflicts(ctx, file, probes))
		}
		pass.Results = make([]supervisor.Result, 0, len(file.Entries))
		updates := make([]registry.EntryUpdate, 0, len(file.Entries))
		purges := make([]registry.Entry, 0)
		for _, entry := range file.Entries {
			if result, selected := batchResults[entry.ID]; selected {
				pass.Results = append(pass.Results, result)
				continue
			}
			if manualPendingEntries[entry.ID] {
				item := manualRetirementRootBlockedGCResult(entry)
				pass.Results = append(pass.Results, supervisor.Result{Action: supervisor.GCStatusSkipped, GC: &item})
				continue
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				pass.Results = append(pass.Results, supervisor.Result{Action: supervisor.ActionDeferred, Error: ctxErr})
				continue
			}
			previous := entry
			if entryMayStartWake(entry, now) {
				if !capabilityChecked {
					capability, capabilityErr = probeWakeGCCapability(ctx, lifecycle, lifecycleOK)
					capabilityChecked = true
				}
				if capabilityErr != nil || !capability {
					if capabilityErr == nil {
						capabilityErr = errIdentitySafeWakeRetireUnavailable
					}
					return fmt.Errorf("supervisor wake start for registry entry %s is blocked by the wake_gc_v1 capability check: %w", entry.ID, capabilityErr)
				}
			}
			probe := probes[entry.Adapter]
			if conflictErr, ok := conflicts[entry.ID]; ok {
				probe = fixedProbeError{err: conflictErr}
			}
			reconciler := supervisor.Reconciler{
				Wake:        wake,
				Adapter:     probe,
				InjectVia:   self,
				WakeTimeout: wakeTimeout,
			}
			if entry.Transition.Active() {
				if !lifecycleOK {
					updated, startResult := reconciler.StartFresh(ctx, entry)
					if startResult.Error == nil && startResult.Action == supervisor.ActionEnsured {
						updated.Transition = registry.ReattachTransition{}
						if err := store.UpdateEntry(updated); err != nil {
							return err
						}
						if err := a.logReconcileTransition(entry, updated, startResult); err != nil {
							return err
						}
						pass.Results = append(pass.Results, startResult)
						continue
					}
					startErr := startResult.Error
					if startErr == nil {
						startErr = fmt.Errorf("wake start was deferred with action %q", startResult.Action)
					}
					updated.LastError = "reattach start remains blocked and wake_gc_v1 lifecycle operations are unavailable: " + startErr.Error()
					updated.LastSupervisorDecision = supervisor.ActionBackoff
					if err := store.UpdateEntry(updated); err != nil {
						return err
					}
					blockedResult := supervisor.Result{Action: supervisor.ActionBackoff, AMQTouched: startResult.AMQTouched, Error: errors.New(updated.LastError)}
					if err := a.logReconcileTransition(entry, updated, blockedResult); err != nil {
						return err
					}
					pass.Results = append(pass.Results, blockedResult)
					continue
				}
				old := transitionPreviousEntry(entry.Transition)
				updated, ready, recoverErr := recoverDetachedRegistration(ctx, store, []registry.Entry{old}, lifecycle, reconciler, entry)
				if ready {
					updated.Transition = registry.ReattachTransition{}
					if err := store.UpdateEntry(updated); err != nil {
						return err
					}
					recoveredResult := supervisor.Result{Action: supervisor.ActionEnsured, AMQTouched: true}
					if err := a.logReconcileTransition(entry, updated, recoveredResult); err != nil {
						return err
					}
					pass.Results = append(pass.Results, recoveredResult)
					continue
				}
				if recoverErr != nil {
					updated.LastError = recoverErr.Error()
					updated.LastSupervisorDecision = supervisor.ActionStartFailed
					if err := store.UpdateEntry(updated); err != nil {
						return err
					}
					failedResult := supervisor.Result{Action: supervisor.ActionStartFailed, AMQTouched: true, Error: recoverErr}
					if err := a.logReconcileTransition(entry, updated, failedResult); err != nil {
						return err
					}
					pass.Results = append(pass.Results, failedResult)
					continue
				}
			}
			if gcPolicy.AutoGC && !manualPendingEntries[entry.ID] {
				collector := supervisor.GarbageCollector{
					Wake: lifecycle, InjectVia: self, CapabilityAvailable: capability,
					CapabilityError: capabilityErr, Policy: gcPolicy,
				}
				collector.Now = a.Now
				gcUpdated, gcResult := collector.ProcessWithBudget(ctx, entry, true, false)
				if gcResult.Purge {
					purges = append(purges, previous)
					pass.Results = append(pass.Results, supervisor.Result{Action: supervisor.GCStatusPurgeCandidate, GC: &gcResult})
					continue
				}
				entry = gcUpdated
				if err := a.logGCTransition(previous, entry, gcResult); err != nil {
					return err
				}
				if gcResult.Status == supervisor.GCStatusRetired ||
					gcResult.Status == supervisor.GCStatusOwnerGoneSeen ||
					gcResult.Status == supervisor.GCStatusEligible ||
					entry.State == registry.StateRetired {
					if previous != entry {
						updates = append(updates, registry.EntryUpdate{Before: previous, After: entry})
					}
					pass.Results = append(pass.Results, supervisor.Result{Action: gcResult.Status, AMQTouched: gcResult.AMQTouched, GC: &gcResult})
					continue
				}
			}
			updated, result := reconciler.Reconcile(ctx, entry)
			if previous != updated {
				updates = append(updates, registry.EntryUpdate{Before: previous, After: updated})
			}
			if err := a.logReconcileTransition(previous, updated, result); err != nil {
				return err
			}
			pass.Results = append(pass.Results, result)
		}
		if _, err = store.UpdateEntries(updates); err != nil {
			return err
		}
		for _, entry := range purges {
			if _, err := store.ForgetIfUnchanged(entry); err != nil {
				return err
			}
		}
		if !pass.PendingGCRoots && gcPolicy.AutoGC && lifecycleOK && capability && capabilityErr == nil {
			latest, loadErr := store.Load()
			if loadErr != nil {
				return loadErr
			}
			if _, active, activeErr := activeGCRootBatch(latest); activeErr != nil {
				return activeErr
			} else if active {
				pass.PendingGCRoots = true
			} else {
				next, planErr := planGCRootBatchForFile(latest, now, gcPolicy)
				if planErr != nil {
					return planErr
				}
				pass.PendingGCRoots = next.Canonical != ""
			}
		}
		return nil
	})
	return pass, err
}

func manualRetirementPendingRootEntries(entries []registry.Entry) (map[string]bool, error) {
	pendingRoots := make(map[string]struct{})
	entryRoots := make(map[string]string, len(entries))
	for _, entry := range entries {
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			return nil, err
		}
		entryRoots[entry.ID] = root
		if entry.State != registry.StateRetired && entry.ManualRetirementIntent.Active() {
			pendingRoots[root] = struct{}{}
		}
	}
	blocked := make(map[string]bool)
	for id, root := range entryRoots {
		_, blocked[id] = pendingRoots[root]
	}
	return blocked, nil
}

func manualRetirementRootBlockedGCResult(entry registry.Entry) supervisor.GCResult {
	return supervisor.GCResult{
		EntryID: entry.ID, Status: supervisor.GCStatusSkipped,
		ReasonCode:      "manual_retirement_root_pending",
		Reason:          "canonical root is frozen until every exact manual retirement receipt is reconciled",
		OwnerBound:      entry.WakeOwnerPresent && entry.WakeOwner.Strong() && !entry.LegacyUnbound,
		BindingComplete: entry.WakeBinding.Complete(),
	}
}

func orderedBatchResults(entries []registry.Entry, selected map[string]supervisor.Result) []supervisor.Result {
	results := make([]supervisor.Result, 0, len(selected))
	for _, entry := range entries {
		if result, ok := selected[entry.ID]; ok {
			results = append(results, result)
		}
	}
	return results
}

func oversizedGCRootBatchResults(plan gcRootBatchPlan) map[string]supervisor.Result {
	results := make(map[string]supervisor.Result, len(plan.Members))
	for _, entry := range plan.Members {
		item := supervisor.GCResult{
			EntryID: entry.ID, Status: supervisor.GCStatusSkipped, ReasonCode: "root_batch_oversized",
			Reason:          fmt.Sprintf("canonical root has %d listener rows; hard maximum is %d", len(plan.Members), supervisor.MaxAgentsPerRootBatch),
			OwnerBound:      entry.WakeOwnerPresent && entry.WakeOwner.Strong() && !entry.LegacyUnbound,
			BindingComplete: entry.WakeBinding.Complete(), AMQTouched: false,
		}
		results[entry.ID] = supervisor.Result{Action: item.Status, GC: &item}
	}
	return results
}

func gcSupervisorResult(item supervisor.GCResult) supervisor.Result {
	return supervisor.Result{Action: item.Status, AMQTouched: item.AMQTouched, GC: &item}
}

func frozenBatchEntries(file registry.File, batch registry.GCRootBatch) ([]registry.Entry, error) {
	byID := make(map[string]registry.Entry, len(file.Entries))
	for _, entry := range file.Entries {
		byID[entry.ID] = entry
	}
	entries := make([]registry.Entry, 0, len(batch.Members))
	for _, member := range batch.Members {
		entry, ok := byID[member.EntryID]
		if !ok {
			return nil, fmt.Errorf("GC root batch %s frozen member %s is missing", batch.ID, member.EntryID)
		}
		if !member.Matches(entry) {
			return nil, fmt.Errorf("GC root batch %s frozen member %s changed identity or binding", batch.ID, member.EntryID)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func replaceFileEntry(file *registry.File, updated registry.Entry) error {
	for index := range file.Entries {
		if file.Entries[index].ID == updated.ID {
			file.Entries[index] = updated
			return nil
		}
	}
	return fmt.Errorf("registry entry %q disappeared from supervisor snapshot", updated.ID)
}

func preflightRequiresRetry(item supervisor.GCResult) bool {
	switch item.ReasonCode {
	case "capability_check_failed", "capability_unavailable", "lifecycle_unavailable", "lifecycle_error", "gc_backoff_active", "internal_error":
		return true
	default:
		return item.RetirementStatus == "error"
	}
}

func batchHaltedResult(entry registry.Entry, reason string) supervisor.Result {
	item := supervisor.GCResult{
		EntryID: entry.ID, Status: supervisor.GCStatusSkipped, ReasonCode: "root_batch_halted",
		Reason: reason, OwnerBound: entry.WakeOwnerPresent && entry.WakeOwner.Strong() && !entry.LegacyUnbound,
		BindingComplete: entry.WakeBinding.Complete(),
	}
	return gcSupervisorResult(item)
}

func probeWakeGCCapability(ctx context.Context, lifecycle wakeLifecycle, ok bool) (bool, error) {
	if !ok {
		return false, errors.New("wake runner does not implement owner-bound lifecycle operations")
	}
	environment, err := lifecycle.Env(ctx)
	if err != nil {
		return false, err
	}
	if !environment.HasCapability(amq.CapabilityWakeGCV1) {
		return false, errIdentitySafeWakeRetireUnavailable
	}
	return true, nil
}

// abortGCRootBatchForDisabledPolicy implements --auto-gc=false as a hard kill
// switch. A preflight batch is canceled without lifecycle I/O. A retiring batch
// performs check-only reconciliation so a crash-persisted tombstone or exact
// superseding generation is reflected in the registry, but it issues no retire
// mutation. The batch is then finished so registration cannot remain wedged.
func (a App) abortGCRootBatchForDisabledPolicy(
	ctx context.Context,
	store *registry.Store,
	file *registry.File,
	batch registry.GCRootBatch,
	lifecycle wakeLifecycle,
	capability bool,
	capabilityErr error,
	self string,
	policy supervisor.GCPolicy,
) (map[string]supervisor.Result, error) {
	entries, err := frozenBatchEntries(*file, batch)
	if err != nil {
		return nil, err
	}
	results := make(map[string]supervisor.Result, len(entries))
	updates := make([]registry.EntryUpdate, 0, len(entries))
	collector := supervisor.GarbageCollector{
		Wake: lifecycle, InjectVia: self, CapabilityAvailable: capability,
		CapabilityError: capabilityErr, Policy: policy, Now: a.Now,
	}
	checkOnly := batch.Phase == registry.GCRootBatchRetiring && capability && capabilityErr == nil
	unresolved := false
	for _, entry := range entries {
		updated := entry
		var item supervisor.GCResult
		if entry.State == registry.StateRetired {
			item = supervisor.GCResult{EntryID: entry.ID, Status: supervisor.GCStatusRetired, ReasonCode: "registry_already_retired", Reason: "frozen member retirement was already persisted"}
		} else if checkOnly {
			probeEntry := entry
			probeEntry.GCBackoffUntil = time.Time{}
			updated, item = collector.ProcessWithBudget(ctx, probeEntry, true, false)
			resolved := item.Status == supervisor.GCStatusRetired ||
				(item.RetirementStatus == "eligible" && item.ReasonCode == "owner_gone") ||
				(item.RetirementStatus == "refused" && item.ReasonCode == "owner_live")
			if !resolved {
				unresolved = true
			}
		} else {
			reasonCode := "auto_gc_disabled"
			reason := "automatic GC is disabled; frozen batch was canceled without retirement"
			if batch.Phase == registry.GCRootBatchRetiring && capabilityErr != nil {
				reasonCode = "capability_check_failed"
				reason = "automatic GC is disabled and check-only batch recovery capability failed: " + capabilityErr.Error()
			}
			item = supervisor.GCResult{
				EntryID: entry.ID, Status: supervisor.GCStatusSkipped, ReasonCode: reasonCode, Reason: reason,
				Capability: capability, OwnerBound: entry.WakeOwnerPresent && entry.WakeOwner.Strong() && !entry.LegacyUnbound,
				BindingComplete: entry.WakeBinding.Complete(),
			}
			updated.LastGCDecision = item.Status
			updated.LastGCReason = item.ReasonCode
		}
		if updated.State != registry.StateRetired {
			updated.GCBackoffUntil = a.now().Add(supervisor.GCRootTerminalBackoff)
		}
		results[entry.ID] = gcSupervisorResult(item)
		if updated != entry {
			updates = append(updates, registry.EntryUpdate{Before: entry, After: updated})
		}
	}
	updateResult, err := store.UpdateEntries(updates)
	if err != nil {
		return results, err
	}
	if updateResult.Skipped != 0 {
		return results, fmt.Errorf("GC root batch %s kill-switch recovery lost %d optimistic updates", batch.ID, updateResult.Skipped)
	}
	for _, update := range updates {
		if err := replaceFileEntry(file, update.After); err != nil {
			return results, err
		}
	}
	if unresolved {
		return results, fmt.Errorf("GC root batch %s is retained because kill-switch check-only recovery had an unresolved lifecycle result", batch.ID)
	}
	if _, err := store.FinishGCRootBatch(batch.ID); err != nil {
		return results, err
	}
	file.GCRootBatches = nil
	return results, nil
}

// executeGCRootBatch is deliberately two-phase. The preflight phase observes
// every frozen member and persists those non-destructive observations before
// any retirement. Only an all-eligible root advances durably to the retiring
// phase. Retirement results are then CAS-persisted one at a time, making a
// crash after AMQ's tombstone but before the registry save replay-safe.
func (a App) executeGCRootBatch(
	ctx context.Context,
	store *registry.Store,
	file *registry.File,
	batch registry.GCRootBatch,
	lifecycle wakeLifecycle,
	capability bool,
	capabilityErr error,
	self string,
	policy supervisor.GCPolicy,
) (map[string]supervisor.Result, bool, error) {
	results := make(map[string]supervisor.Result, len(batch.Members))
	collector := supervisor.GarbageCollector{
		Wake: lifecycle, InjectVia: self, CapabilityAvailable: capability,
		CapabilityError: capabilityErr, Policy: policy, Now: a.Now,
	}
	entries, err := frozenBatchEntries(*file, batch)
	if err != nil {
		return results, true, err
	}

	if batch.Phase == registry.GCRootBatchPreflight {
		updates := make([]registry.EntryUpdate, 0, len(entries))
		terminalBlock := false
		retryBlock := false
		for _, entry := range entries {
			if entry.State == registry.StateRetired {
				continue
			}
			updated, item := collector.ProcessWithBudget(ctx, entry, true, false)
			acceptable := (item.Status == supervisor.GCStatusEligible && item.RetirementStatus == "eligible" && item.ReasonCode == "owner_gone") ||
				(item.Status == supervisor.GCStatusRetired && (item.RetirementStatus == "already_retired" || item.RetirementStatus == "retired"))
			if !acceptable {
				if preflightRequiresRetry(item) {
					retryBlock = true
					updated.OwnerGoneSince = entry.OwnerGoneSince
					updated.GCBackoffUntil = a.now().Add(supervisor.GCCatchUpInterval)
				} else {
					terminalBlock = true
				}
			}
			results[entry.ID] = gcSupervisorResult(item)
			if updated != entry {
				updates = append(updates, registry.EntryUpdate{Before: entry, After: updated})
			}
		}
		if terminalBlock {
			quarantineUntil := a.now().Add(supervisor.GCRootTerminalBackoff)
			updateIndex := make(map[string]int, len(updates))
			for index := range updates {
				updateIndex[updates[index].Before.ID] = index
			}
			for _, entry := range entries {
				if entry.State == registry.StateRetired {
					continue
				}
				if index, exists := updateIndex[entry.ID]; exists {
					if updates[index].After.State != registry.StateRetired {
						updates[index].After.GCBackoffUntil = quarantineUntil
					}
					continue
				}
				updated := entry
				updated.GCBackoffUntil = quarantineUntil
				updates = append(updates, registry.EntryUpdate{Before: entry, After: updated})
			}
		}
		updateResult, updateErr := store.UpdateEntries(updates)
		if updateErr != nil {
			return results, true, updateErr
		}
		if updateResult.Skipped != 0 {
			return results, true, fmt.Errorf("GC root batch %s preflight lost %d optimistic updates", batch.ID, updateResult.Skipped)
		}
		for _, update := range updates {
			if err := replaceFileEntry(file, update.After); err != nil {
				return results, true, err
			}
		}
		if terminalBlock {
			if _, err := store.FinishGCRootBatch(batch.ID); err != nil {
				return results, true, err
			}
			file.GCRootBatches = nil
			return results, false, nil
		}
		if retryBlock {
			return results, true, nil
		}
		if _, err := store.AdvanceGCRootBatch(batch.ID, registry.GCRootBatchPreflight, registry.GCRootBatchRetiring); err != nil {
			return results, true, err
		}
		batch.Phase = registry.GCRootBatchRetiring
		file.GCRootBatches[0].Phase = registry.GCRootBatchRetiring
	}

	entries, err = frozenBatchEntries(*file, batch)
	if err != nil {
		return results, true, err
	}
	for index, entry := range entries {
		if entry.State == registry.StateRetired {
			if _, exists := results[entry.ID]; !exists {
				item := supervisor.GCResult{EntryID: entry.ID, Status: supervisor.GCStatusRetired, ReasonCode: "registry_already_retired", Reason: "frozen member retirement was already persisted"}
				results[entry.ID] = gcSupervisorResult(item)
			}
			continue
		}
		updated, item := collector.RetirePreflighted(ctx, entry)
		updated.LastGCDecision = item.Status
		updated.LastGCReason = item.ReasonCode
		if item.Status != supervisor.GCStatusRetired && updated.State != registry.StateRetired {
			// Frozen batches own their retry cadence. Preserve the already-proven
			// owner-gone observation and make the first retry eligible at the hard
			// five-second catch-up boundary instead of the ordinary one-minute GC
			// observation backoff.
			updated.OwnerGoneSince = entry.OwnerGoneSince
			updated.GCBackoffUntil = a.now().Add(supervisor.GCCatchUpInterval)
		}
		results[entry.ID] = gcSupervisorResult(item)
		if updated != entry {
			updateResult, updateErr := store.UpdateEntries([]registry.EntryUpdate{{Before: entry, After: updated}})
			if updateErr != nil {
				return results, true, updateErr
			}
			if updateResult.Skipped != 0 {
				return results, true, fmt.Errorf("GC root batch %s member %s changed before retirement result persistence", batch.ID, entry.ID)
			}
			if err := replaceFileEntry(file, updated); err != nil {
				return results, true, err
			}
		}
		if item.Status != supervisor.GCStatusRetired {
			if err := a.logGCTransition(entry, updated, item); err != nil {
				return results, true, err
			}
			for _, remaining := range entries[index+1:] {
				if _, exists := results[remaining.ID]; !exists {
					results[remaining.ID] = batchHaltedResult(remaining, "an earlier frozen member retirement failed; later members were not mutated")
				}
			}
			if item.RetirementStatus == "superseded" && updated.State == registry.StateRetired {
				if _, err := store.FinishGCRootBatch(batch.ID); err != nil {
					return results, true, err
				}
				file.GCRootBatches = nil
				return results, false, nil
			}
			return results, true, nil
		}
	}
	if _, err := store.FinishGCRootBatch(batch.ID); err != nil {
		return results, true, err
	}
	file.GCRootBatches = nil
	return results, false, nil
}

func transitionPreviousEntry(transition registry.ReattachTransition) registry.Entry {
	return registry.Entry{
		ID: transition.OldID, Root: transition.OldRoot, Agent: transition.OldAgent,
		Adapter: transition.OldAdapter, Target: transition.OldTarget,
		BaselineFile: transition.OldBaseline, BaselineDigest: transition.OldBaselineSum,
		WakeOwnerPresent: transition.OldOwnerSet, WakeOwner: transition.OldOwner,
		WakeBinding: transition.OldBinding, State: registry.StateAttached,
	}
}

func (a App) logReconcileTransition(previous, updated registry.Entry, result supervisor.Result) error {
	if previous.State == updated.State &&
		previous.LastError == updated.LastError &&
		previous.LastSupervisorDecision == updated.LastSupervisorDecision {
		return nil
	}
	w := a.Stderr
	if w == nil {
		w = os.Stderr
	}
	if result.Error != nil {
		if _, err := fmt.Fprintf(w,
			"amq-keepalive reconcile warning: action=%s root=%q agent=%q adapter=%q target=%q failure_count=%d error=%q\n",
			result.Action,
			updated.Root,
			updated.Agent,
			updated.Adapter,
			updated.Target,
			updated.FailureCount,
			result.Error.Error(),
		); err != nil {
			return fmt.Errorf("write reconcile transition diagnostic: %w", err)
		}
		return nil
	}
	if updated.State == registry.StateActive {
		if _, err := fmt.Fprintf(w,
			"amq-keepalive reconcile recovered: action=%s root=%q agent=%q adapter=%q target=%q\n",
			result.Action,
			updated.Root,
			updated.Agent,
			updated.Adapter,
			updated.Target,
		); err != nil {
			return fmt.Errorf("write reconcile transition diagnostic: %w", err)
		}
	}
	return nil
}

func (a App) logGCTransition(previous, updated registry.Entry, result supervisor.GCResult) error {
	if previous.LastGCDecision == updated.LastGCDecision && previous.LastGCReason == updated.LastGCReason {
		return nil
	}
	w := a.Stderr
	if w == nil {
		w = os.Stderr
	}
	detail := result.Reason
	if result.Status == supervisor.GCStatusSkipped && updated.GCFailureCount > 0 && updated.LastError != "" {
		detail = updated.LastError
	}
	if _, err := fmt.Fprintf(w,
		"amq-keepalive gc: status=%s reason_code=%q root=%q agent=%q generation=%q reason=%q\n",
		result.Status,
		result.ReasonCode,
		updated.Root,
		updated.Agent,
		updated.WakeBinding.Generation,
		detail,
	); err != nil {
		return fmt.Errorf("write GC transition diagnostic: %w", err)
	}
	return nil
}

type fixedProbeError struct {
	err error
}

func (p fixedProbeError) Probe(context.Context, string) error {
	return p.err
}

type passProbe struct {
	selected  adapter.Adapter
	once      sync.Once
	inventory adapter.TargetInventory
	err       error
}

type ownershipProbe interface {
	supervisor.Adapter
	OwnershipKey(ctx context.Context, target string) (string, error)
}

func newPassProbe(selected adapter.Adapter) supervisor.Adapter {
	if _, ok := selected.(adapter.InventoryProvider); !ok {
		return selected
	}
	return &passProbe{selected: selected}
}

func passProbes(entries []registry.Entry, adapters adapter.Registry) map[string]supervisor.Adapter {
	probes := make(map[string]supervisor.Adapter)
	for _, entry := range entries {
		if _, ok := probes[entry.Adapter]; ok {
			continue
		}
		selected, err := adapters.Get(entry.Adapter)
		if err != nil {
			probes[entry.Adapter] = fixedProbeError{err: err}
			continue
		}
		probes[entry.Adapter] = newPassProbe(selected)
	}
	return probes
}

func (p *passProbe) Probe(ctx context.Context, target string) error {
	inventory, err := p.loadInventory(ctx)
	if err != nil {
		return err
	}
	return inventory.Probe(target)
}

func (p *passProbe) OwnershipKey(ctx context.Context, target string) (string, error) {
	inventory, err := p.loadInventory(ctx)
	if err != nil {
		return "", err
	}
	return inventory.OwnershipKey(target)
}

func (p *passProbe) loadInventory(ctx context.Context) (adapter.TargetInventory, error) {
	provider := p.selected.(adapter.InventoryProvider)
	p.once.Do(func() {
		p.inventory, p.err = provider.Inventory(ctx)
		if p.err == nil && p.inventory == nil {
			p.err = errors.New("adapter returned a nil target inventory")
		}
	})
	if p.err != nil {
		return nil, p.err
	}
	return p.inventory, nil
}

type targetOwnerKey struct {
	adapter string
	target  string
}

func targetOwnershipConflicts(file registry.File, adapters adapter.Registry) map[string]error {
	groups := make(map[targetOwnerKey][]registry.Entry)
	for _, entry := range file.Entries {
		if entry.State == registry.StateRetired {
			continue
		}
		selected, err := adapters.Get(entry.Adapter)
		if err != nil {
			continue
		}
		target := strings.TrimSpace(entry.Target)
		if normalizer, ok := selected.(adapter.TargetNormalizer); ok {
			target, err = normalizer.NormalizeTarget(target)
			if err != nil {
				continue
			}
		}
		key := targetOwnerKey{adapter: selected.Name(), target: target}
		groups[key] = append(groups[key], entry)
	}

	conflicts := make(map[string]error)
	for key, owners := range groups {
		if len(owners) < 2 {
			continue
		}
		labels := make([]string, 0, len(owners))
		for _, owner := range owners {
			labels = append(labels, fmt.Sprintf("%s@%s (%s)", owner.Agent, owner.Root, owner.ID))
		}
		err := fmt.Errorf("adapter target ownership collision: adapter=%q target=%q owners=%s", key.adapter, key.target, strings.Join(labels, ", "))
		for _, owner := range owners {
			conflicts[owner.ID] = err
		}
	}
	return conflicts
}

type physicalTargetOwnerKey struct {
	adapter string
	key     string
}

func physicalOwnershipConflicts(ctx context.Context, file registry.File, probes map[string]supervisor.Adapter) map[string]error {
	groups := make(map[physicalTargetOwnerKey][]registry.Entry)
	conflicts := make(map[string]error)
	for _, entry := range file.Entries {
		if entry.State == registry.StateRetired {
			continue
		}
		probe, ok := probes[entry.Adapter].(ownershipProbe)
		if !ok {
			continue
		}
		key, err := probe.OwnershipKey(ctx, entry.Target)
		if errors.Is(err, adapter.ErrTargetNotFound) {
			continue
		}
		if err != nil {
			conflicts[entry.ID] = fmt.Errorf("resolve physical target ownership for adapter=%q target=%q: %w", entry.Adapter, entry.Target, err)
			continue
		}
		groups[physicalTargetOwnerKey{adapter: entry.Adapter, key: key}] = append(
			groups[physicalTargetOwnerKey{adapter: entry.Adapter, key: key}], entry,
		)
	}
	for key, owners := range groups {
		if len(owners) < 2 {
			continue
		}
		labels := make([]string, 0, len(owners))
		for _, owner := range owners {
			labels = append(labels, fmt.Sprintf("%s@%s (%s, target=%s)", owner.Agent, owner.Root, owner.ID, owner.Target))
		}
		err := fmt.Errorf(
			"physical target ownership collision: adapter=%q identity=%q owners=%s; existing wakes are not retired automatically",
			key.adapter, key.key, strings.Join(labels, ", "),
		)
		for _, owner := range owners {
			conflicts[owner.ID] = err
		}
	}
	return conflicts
}

func mergeOwnershipConflicts(destination, source map[string]error) {
	for id, err := range source {
		if _, exists := destination[id]; !exists {
			destination[id] = err
		}
	}
}

func anyEntryDue(entries []registry.Entry, now time.Time) bool {
	for _, entry := range entries {
		if entry.State == registry.StateRetired {
			continue
		}
		if !entry.NextHealthCheck.IsZero() && now.Before(entry.NextHealthCheck) {
			continue
		}
		if !entry.BackoffUntil.IsZero() && now.Before(entry.BackoffUntil) {
			continue
		}
		return true
	}
	return false
}

func (a App) inject(ctx context.Context, args []string) error {
	if len(args) != 3 {
		return errors.New("usage: amq-keepalive inject <adapter> <target> <payload>")
	}
	adapters := adapter.DefaultRegistry()
	selected, err := adapters.Get(args[0])
	if err != nil {
		return err
	}
	return selected.Inject(ctx, args[1], args[2])
}

type doctorEntry struct {
	Entry           registry.Entry `json:"entry"`
	OwnerStatus     string         `json:"owner_status"`
	BindingStatus   string         `json:"binding_status"`
	OwnerGoneAge    string         `json:"owner_gone_age,omitempty"`
	TransitionPhase string         `json:"transition_phase,omitempty"`
}

type doctorResult struct {
	SchemaVersion      int           `json:"schema_version"`
	WakeGCCapability   bool          `json:"wake_gc_capability"`
	CapabilityError    string        `json:"capability_error,omitempty"`
	ActiveGCBatchID    string        `json:"active_gc_batch_id,omitempty"`
	ActiveGCBatchPhase string        `json:"active_gc_batch_phase,omitempty"`
	Entries            []doctorEntry `json:"entries"`
}

func (a App) doctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	amqPath := fs.String("amq", "amq", "amq executable path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store := registry.New(*registryPath)
	file, err := store.LoadPreview()
	if err != nil {
		return err
	}
	result := doctorResult{SchemaVersion: file.SchemaVersion}
	if batch, active, batchErr := activeGCRootBatch(file); batchErr != nil {
		return batchErr
	} else if active {
		result.ActiveGCBatchID = batch.ID
		result.ActiveGCBatchPhase = string(batch.Phase)
	}
	if environment, envErr := amq.NewCLI(*amqPath).Env(ctx); envErr != nil {
		result.CapabilityError = envErr.Error()
	} else {
		result.WakeGCCapability = environment.HasCapability(amq.CapabilityWakeGCV1)
	}
	now := time.Now().UTC()
	for _, entry := range file.Entries {
		item := doctorEntry{Entry: entry, TransitionPhase: string(entry.Transition.Phase)}
		switch {
		case entry.LegacyUnbound:
			item.OwnerStatus = "legacy_unbound"
		case entry.WakeOwnerPresent && entry.WakeOwner.Strong():
			item.OwnerStatus = "strong"
		default:
			item.OwnerStatus = "missing_or_incomplete"
		}
		if entry.WakeBinding.Complete() {
			item.BindingStatus = "exact"
		} else {
			item.BindingStatus = "missing_or_incomplete"
		}
		if !entry.OwnerGoneSince.IsZero() {
			item.OwnerGoneAge = now.Sub(entry.OwnerGoneSince).Round(time.Second).String()
		}
		result.Entries = append(result.Entries, item)
	}
	return printJSON(a.Stdout, result)
}

type gcResult struct {
	Applied          bool                  `json:"applied"`
	AttemptedRoot    string                `json:"attempted_root,omitempty"`
	WakeGCCapability bool                  `json:"wake_gc_capability"`
	CapabilityError  string                `json:"capability_error,omitempty"`
	OwnerGoneGrace   string                `json:"owner_gone_grace"`
	RetiredRetention string                `json:"retired_retention"`
	Entries          []supervisor.GCResult `json:"entries"`
}

type abandonGCRootBatchResult struct {
	Abandoned          bool     `json:"abandoned"`
	BatchID            string   `json:"batch_id"`
	Phase              string   `json:"phase"`
	QuarantinedEntries []string `json:"quarantined_entries,omitempty"`
	RetiredEntries     []string `json:"reconciled_retired_entries,omitempty"`
	UnresolvedAMQState bool     `json:"unresolved_amq_state"`
	Warning            string   `json:"warning,omitempty"`
}

func validateGCPolicy(ownerGrace, retiredRetention, timeout time.Duration) error {
	if ownerGrace < supervisor.MinOwnerGoneGrace {
		return fmt.Errorf("GC owner-gone grace must be at least %s", supervisor.MinOwnerGoneGrace)
	}
	if retiredRetention < supervisor.MinRetiredRetention {
		return fmt.Errorf("GC retired retention must be at least %s", supervisor.MinRetiredRetention)
	}
	if timeout <= 0 || timeout > supervisor.MaxLifecycleTimeout {
		return fmt.Errorf("GC timeout must be in (0,%s]", supervisor.MaxLifecycleTimeout)
	}
	return nil
}

func (a App) gc(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gc", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path for exact injector identity")
	ownerGrace := fs.Duration("owner-gone-grace", 5*time.Minute, "minimum interval between positive owner-gone observations")
	legacyAge := fs.Duration("min-detached-age", -1, "deprecated alias for --owner-gone-grace")
	retiredRetention := fs.Duration("retired-retention", 24*time.Hour, "diagnostic retention for retired registry rows")
	timeout := fs.Duration("timeout", 5*time.Second, "deadline for each AMQ lifecycle command")
	apply := fs.Bool("apply", false, "persist observations and retire eligible exact wakes")
	abandonBatch := fs.String("abandon-batch", "", "explicitly abandon this exact stuck GC batch id")
	confirmAbandonBatch := fs.String("confirm-abandon-batch", "", "repeat the exact stuck GC batch id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *legacyAge >= 0 {
		*ownerGrace = *legacyAge
	}
	if err := validateGCPolicy(*ownerGrace, *retiredRetention, *timeout); err != nil {
		return err
	}
	if *abandonBatch != "" || *confirmAbandonBatch != "" {
		if *apply {
			return errors.New("--abandon-batch cannot be combined with --apply")
		}
		if *abandonBatch == "" || *confirmAbandonBatch == "" || *abandonBatch != *confirmAbandonBatch {
			return errors.New("--abandon-batch and --confirm-abandon-batch must repeat the same exact non-empty batch id")
		}
		return a.abandonGCRootBatch(ctx, *registryPath, amq.NewCLI(*amqPath), *self, *timeout, *abandonBatch)
	}

	store := registry.New(*registryPath)
	cli := amq.NewCLI(*amqPath)
	capability := false
	var capabilityErr error
	if environment, err := cli.Env(ctx); err != nil {
		capabilityErr = err
	} else {
		capability = environment.HasCapability(amq.CapabilityWakeGCV1)
	}
	result := gcResult{
		Applied: *apply, WakeGCCapability: capability,
		OwnerGoneGrace: ownerGrace.String(), RetiredRetention: retiredRetention.String(),
	}
	if capabilityErr != nil {
		result.CapabilityError = capabilityErr.Error()
	}
	policy := supervisor.GCPolicy{AutoGC: *apply, OwnerGrace: *ownerGrace, RetiredRetention: *retiredRetention, Timeout: *timeout}
	processFile := func(file registry.File) error {
		if *apply {
			if err := validateNoAmbiguousLiveSessions(file.Entries); err != nil {
				return err
			}
		}
		updates := make([]registry.EntryUpdate, 0)
		purges := make([]registry.Entry, 0)
		selected := make(map[string]supervisor.Result)
		manualPendingEntries, pendingErr := manualRetirementPendingRootEntries(file.Entries)
		if pendingErr != nil {
			return pendingErr
		}
		if *apply {
			batch, active, batchErr := activeGCRootBatch(file)
			if batchErr != nil {
				return batchErr
			}
			if active {
				result.AttemptedRoot = batch.CanonicalRoot
				var executeErr error
				selected, _, executeErr = a.executeGCRootBatch(ctx, store, &file, batch, cli, capability, capabilityErr, *self, policy)
				if executeErr != nil {
					return executeErr
				}
			} else if capability && capabilityErr == nil {
				plan, planErr := planGCRootBatchForFile(file, a.now(), policy)
				if planErr != nil {
					return planErr
				}
				if plan.Canonical != "" {
					result.AttemptedRoot = plan.Canonical
					if plan.Oversized {
						if err := persistGCRootBatchMarker(store, &file, plan, a.now()); err != nil {
							return err
						}
						selected = oversizedGCRootBatchResults(plan)
					} else if gcRootPlanHasLocalBlock(plan) {
						if err := persistLocallyBlockedGCRoot(store, &file, plan, a.now()); err != nil {
							return err
						}
						selected = locallyBlockedGCRootResults(plan)
					} else {
						batch = frozenGCRootBatch(plan, a.now())
						file, batchErr = store.StartGCRootBatch(batch, plan.Members)
						if batchErr != nil {
							return batchErr
						}
						selected, _, batchErr = a.executeGCRootBatch(ctx, store, &file, batch, cli, capability, capabilityErr, *self, policy)
						if batchErr != nil {
							return batchErr
						}
					}
				}
			}
		}
		if *apply && result.AttemptedRoot != "" {
			for _, entry := range file.Entries {
				if selectedResult, ok := selected[entry.ID]; ok && selectedResult.GC != nil {
					result.Entries = append(result.Entries, *selectedResult.GC)
				}
			}
			return nil
		}
		for _, entry := range file.Entries {
			if selectedResult, ok := selected[entry.ID]; ok {
				if selectedResult.GC != nil {
					result.Entries = append(result.Entries, *selectedResult.GC)
				}
				continue
			}
			if manualPendingEntries[entry.ID] {
				result.Entries = append(result.Entries, manualRetirementRootBlockedGCResult(entry))
				continue
			}
			collector := supervisor.GarbageCollector{
				Wake: cli, InjectVia: *self, CapabilityAvailable: capability,
				CapabilityError: capabilityErr, Policy: policy, Now: a.Now,
			}
			updated, item := collector.ProcessWithBudget(ctx, entry, *apply, false)
			if item.Purge && *apply {
				purges = append(purges, entry)
			} else if updated != entry && *apply {
				updates = append(updates, registry.EntryUpdate{Before: entry, After: updated})
			}
			result.Entries = append(result.Entries, item)
		}
		if *apply {
			if _, err := store.UpdateEntries(updates); err != nil {
				return err
			}
			for _, entry := range purges {
				if _, err := store.ForgetIfUnchanged(entry); err != nil {
					return err
				}
			}
		}
		return nil
	}
	var err error
	if *apply {
		err = store.WithRegistrationLockContext(ctx, func() error {
			file, loadErr := store.Load()
			if loadErr != nil {
				return loadErr
			}
			return processFile(file)
		})
	} else {
		var file registry.File
		file, err = store.LoadPreview()
		if err == nil {
			err = processFile(file)
		}
	}
	if err != nil {
		return err
	}
	if err := printJSON(a.Stdout, result); err != nil {
		return err
	}
	if *apply {
		if capabilityErr != nil {
			return fmt.Errorf("gc --apply capability check failed: %w", capabilityErr)
		}
		if !capability {
			return fmt.Errorf("gc --apply: %w", errIdentitySafeWakeRetireUnavailable)
		}
	}
	return nil
}

func (a App) abandonGCRootBatch(ctx context.Context, registryPath string, lifecycle wakeLifecycle, self string, timeout time.Duration, batchID string) error {
	store := registry.New(registryPath)
	var output abandonGCRootBatchResult
	err := store.WithRegistrationLockContext(ctx, func() error {
		file, err := store.Load()
		if err != nil {
			return err
		}
		batch, active, err := activeGCRootBatch(file)
		if err != nil {
			return err
		}
		if !active || batch.ID != batchID {
			return fmt.Errorf("exact active GC root batch %q was not found", batchID)
		}
		entries, err := frozenBatchEntries(file, batch)
		if err != nil {
			return err
		}

		capability := false
		var capabilityErr error
		if batch.Phase == registry.GCRootBatchRetiring {
			capability, capabilityErr = probeWakeGCCapability(ctx, lifecycle, lifecycle != nil)
		}
		collector := supervisor.GarbageCollector{
			Wake: lifecycle, InjectVia: self, CapabilityAvailable: capability, CapabilityError: capabilityErr,
			Policy: supervisor.GCPolicy{OwnerGrace: supervisor.MinOwnerGoneGrace, RetiredRetention: supervisor.MinRetiredRetention, Timeout: timeout},
			Now:    a.Now,
		}
		outcomes := make([]registry.GCRootBatchAbandonOutcome, 0, len(entries))
		unresolved := false
		for _, entry := range entries {
			updated := entry
			quarantine := entry.State != registry.StateRetired
			if batch.Phase == registry.GCRootBatchRetiring && capability && capabilityErr == nil && entry.State != registry.StateRetired {
				probeEntry := entry
				probeEntry.GCBackoffUntil = time.Time{}
				var item supervisor.GCResult
				updated, item = collector.ProcessWithBudget(ctx, probeEntry, true, false)
				knownUnresolved := item.RetirementStatus == "eligible" && item.ReasonCode == "owner_gone" ||
					item.RetirementStatus == "refused" && item.ReasonCode == "owner_live"
				if updated.State == registry.StateRetired {
					quarantine = false
				} else if knownUnresolved {
					quarantine = true
					unresolved = true
				} else {
					return fmt.Errorf("GC root batch %s is retained because check-only reconciliation for member %s was not conclusive: status=%s reason_code=%s", batch.ID, entry.ID, item.RetirementStatus, item.ReasonCode)
				}
			} else if batch.Phase == registry.GCRootBatchRetiring && entry.State != registry.StateRetired {
				// The explicit double confirmation authorizes registry-only escape
				// when AMQ capability discovery is unavailable. The row is
				// quarantined and never mislabeled as retired.
				unresolved = true
			}
			outcomes = append(outcomes, registry.GCRootBatchAbandonOutcome{Before: entry, After: updated, Quarantine: quarantine})
		}
		result, err := store.AbandonGCRootBatch(batch, outcomes, a.now(), "operator double-confirmed stuck GC batch abandonment")
		if err != nil {
			return err
		}
		output = abandonGCRootBatchResult{
			Abandoned: true, BatchID: batch.ID, Phase: string(batch.Phase),
			QuarantinedEntries: result.Quarantined, RetiredEntries: result.Retired,
			UnresolvedAMQState: unresolved,
		}
		if unresolved {
			output.Warning = "AMQ wake state remains unresolved; quarantined rows will not be auto-retired"
		}
		return nil
	})
	if err != nil {
		return err
	}
	return printJSON(a.Stdout, output)
}

func normalizedTarget(selected adapter.Adapter, target string) (string, error) {
	target = strings.TrimSpace(target)
	if normalizer, ok := selected.(adapter.TargetNormalizer); ok {
		return normalizer.NormalizeTarget(target)
	}
	if target == "" {
		return "", errors.New("adapter target is empty")
	}
	return target, nil
}

func (a App) forget(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("forget", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	id := fs.String("id", "", "registry entry id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	store := registry.New(*registryPath)
	removed := false
	err := store.WithRegistrationLockContext(ctx, func() error {
		var err error
		removed, err = store.Forget(*id)
		return err
	})
	if err != nil {
		return err
	}
	return printJSON(a.Stdout, map[string]any{"removed": removed})
}

func (a App) retireSession(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("retire-session", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	rootFlag := fs.String("root", "", "exact AMQ session root")
	adapterName := fs.String("adapter", "cmux", "adapter name")
	agentsFlag := fs.String("agents", "", "explicit comma-separated agent handles")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path for --inject-via")
	apply := fs.Bool("apply", false, "apply the exact previewed retirement plan")
	confirmPlan := fs.String("confirm-plan", "", "exact plan_id emitted by the read-only preview")
	timeout := fs.Duration("timeout", 5*time.Second, "deadline for each AMQ retirement command")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("retire-session accepts no positional arguments: %q", fs.Args())
	}
	selected, err := adapter.DefaultRegistry().Get(*adapterName)
	if err != nil {
		return err
	}
	amqIdentity, err := executable.Capture(*amqPath)
	if err != nil {
		return fmt.Errorf("resolve --amq: %w", err)
	}
	selfIdentity, err := executable.Capture(*self)
	if err != nil {
		return fmt.Errorf("resolve --self: %w", err)
	}
	resolvedAMQ := amqIdentity.Path
	resolvedSelf := selfIdentity.Path
	result, runErr := a.retireSessionWithOptions(ctx, retireSessionOptions{
		RegistryPath: *registryPath, Root: *rootFlag, AdapterName: *adapterName, Agents: *agentsFlag,
		AMQPath: resolvedAMQ, Self: resolvedSelf, Apply: *apply, ConfirmPlan: *confirmPlan, Timeout: *timeout,
	}, amq.NewCLI(resolvedAMQ), selected)
	if result != nil {
		if err := printJSON(a.Stdout, result); err != nil {
			return errors.Join(runErr, err)
		}
	}
	return runErr
}

func (a App) installLaunchd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("install-launchd", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	label := fs.String("label", launchd.DefaultLabel, "launchd label")
	plistPath := fs.String("plist", "", "plist path")
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path")
	interval := fs.Duration("interval", time.Minute, "supervisor interval")
	autoGC := fs.Bool("auto-gc", false, "enable owner-bound automatic wake retirement")
	ownerGrace := fs.Duration("owner-gone-grace", 5*time.Minute, "minimum interval between positive owner-gone observations")
	retention := fs.Duration("retired-retention", 24*time.Hour, "diagnostic retention for retired registry rows")
	gcTimeout := fs.Duration("gc-timeout", 5*time.Second, "deadline for each AMQ lifecycle command")
	legacyGCMax := fs.Int("gc-max-per-pass", 1, "deprecated compatibility option; ignored in favor of hard root-batch limits")
	noLoad := fs.Bool("no-load", false, "write plist without loading it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval <= 0 {
		return errors.New("launchd policy requires a positive interval")
	}
	if *legacyGCMax <= 0 {
		return errors.New("deprecated --gc-max-per-pass must be greater than zero")
	}
	if err := validateGCPolicy(*ownerGrace, *retention, *gcTimeout); err != nil {
		return err
	}
	opts := launchd.Options{
		Label:        *label,
		PlistPath:    *plistPath,
		BinaryPath:   *self,
		RegistryPath: *registryPath,
		AMQPath:      *amqPath,
		Interval:     *interval,
		AutoGC:       *autoGC,
		OwnerGrace:   *ownerGrace,
		Retention:    *retention,
		GCTimeout:    *gcTimeout,
		Load:         !*noLoad,
	}
	normalized, err := launchd.NormalizeOptions(opts)
	if err != nil {
		return err
	}
	if err := launchd.Install(ctx, normalized); err != nil {
		return err
	}
	return printJSON(a.Stdout, map[string]any{
		"label":      normalized.Label,
		"plist":      normalized.PlistPath,
		"loaded":     normalized.Load,
		"supervisor": normalized.BinaryPath,
	})
}

func (a App) installHook(args []string) error {
	fs := flag.NewFlagSet("install-hook", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	agent := fs.String("agent", hookinstall.AgentBoth, "agent config to update: claude, codex, or both")
	scriptPath := fs.String("script", "", "installed hook script path")
	binaryPath := fs.String("bin", executablePath(), "amq-keepalive binary path")
	claudeConfig := fs.String("claude-config", "", "Claude settings.json path")
	codexConfig := fs.String("codex-config", "", "Codex hooks.json path")
	timeout := fs.Duration("timeout", hookinstall.DefaultTimeout, "self-timeout for reattach work inside the hook")
	dryRun := fs.Bool("dry-run", false, "print install plan without writing files")
	if err := fs.Parse(args); err != nil {
		return err
	}
	result, err := hookinstall.Install(hookinstall.Options{
		Agent:        *agent,
		ScriptPath:   *scriptPath,
		BinaryPath:   *binaryPath,
		ClaudeConfig: *claudeConfig,
		CodexConfig:  *codexConfig,
		Timeout:      *timeout,
		DryRun:       *dryRun,
	})
	if err != nil {
		return err
	}
	return printJSON(a.Stdout, result)
}

func (a App) uninstallLaunchd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	label := fs.String("label", launchd.DefaultLabel, "launchd label")
	plistPath := fs.String("plist", "", "plist path")
	noUnload := fs.Bool("no-unload", false, "remove plist without bootout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := launchd.Uninstall(ctx, *label, *plistPath, !*noUnload); err != nil {
		return err
	}
	return printJSON(a.Stdout, map[string]any{"label": *label, "removed": true, "unloaded": !*noUnload})
}

func (a App) usage(writer io.Writer) {
	fmt.Fprintln(writer, "usage: amq-keepalive <attach|reattach|supervise|inject|doctor|gc|retire-session|forget|install-launchd|install-hook|uninstall> [options]")
}

func mustDefaultRegistryPath() string {
	path, err := registry.DefaultPath()
	if err != nil {
		return ""
	}
	return path
}

func executablePath() string {
	path, err := os.Executable()
	if err != nil {
		return os.Args[0]
	}
	return path
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func normalizeAMQPaths(root, baseRoot, sessionName string) (string, string) {
	root = strings.TrimSpace(root)
	baseRoot = strings.TrimSpace(baseRoot)
	sessionName = strings.TrimSpace(sessionName)

	if baseRoot != "" && !filepath.IsAbs(baseRoot) {
		if abs, err := filepath.Abs(baseRoot); err == nil {
			baseRoot = abs
		}
	}
	if root == "" || filepath.IsAbs(root) {
		return root, baseRoot
	}
	if baseRoot != "" && filepath.IsAbs(baseRoot) {
		if sessionName != "" && filepath.Base(root) == sessionName {
			return filepath.Join(baseRoot, sessionName), baseRoot
		}
		if filepath.Base(root) == filepath.Base(baseRoot) {
			return baseRoot, baseRoot
		}
	}
	if abs, err := filepath.Abs(root); err == nil {
		return abs, baseRoot
	}
	return root, baseRoot
}
