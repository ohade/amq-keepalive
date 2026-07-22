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
	"strings"
	"sync"
	"time"

	"github.com/ohade/amq-keepalive/internal/adapter"
	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/hookinstall"
	"github.com/ohade/amq-keepalive/internal/launchd"
	"github.com/ohade/amq-keepalive/internal/registry"
	"github.com/ohade/amq-keepalive/internal/supervisor"
)

type App struct {
	Stdout io.Writer
	Stderr io.Writer
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
	if opts.Root == "" || opts.Me == "" || opts.BaseRoot == "" || opts.SessionName == "" {
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

func managedWakeReadinessError(ownerErr, environmentErr error, _ amq.Env) error {
	if ownerErr != nil {
		return fmt.Errorf("AMQ wake unavailable; messages remain queued: %w", ownerErr)
	}
	if environmentErr != nil {
		return fmt.Errorf("AMQ wake unavailable; messages remain queued: capability check failed: %w", environmentErr)
	}
	return nil
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
		if existing.Adapter != candidate.Adapter || existing.ID == candidate.ID {
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
	if previous.Adapter == next.Adapter && previous.Target == next.Target {
		return next, false, nil
	}

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
	if !errors.As(initialStart.Error, &structuredStart) || structuredStart.Result.ReasonCode != "existing_wake_blocking" {
		return updated, false, fmt.Errorf("recover detached %s wake: exact-target start did not prove an old-wake conflict; refusing retirement: %w", previous.Agent, initialStart.Error)
	}
	if !previous.WakeOwnerPresent || !previous.WakeOwner.Strong() || !previous.WakeBinding.Complete() {
		return updated, false, fmt.Errorf("recover detached %s wake: exact-target start failed: %v; old wake lacks owner-bound generation metadata", previous.Agent, initialStart.Error)
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
	gcMax := fs.Int("gc-max-per-pass", 1, "maximum wake retirements per supervisor pass")
	gcTimeout := fs.Duration("gc-timeout", 5*time.Second, "deadline for each AMQ lifecycle command")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval <= 0 {
		return errors.New("--interval must be greater than zero")
	}
	if *ownerGrace < 0 || *retiredRetention < 0 || *gcMax < 1 || *gcTimeout <= 0 || *gcTimeout > 5*time.Second {
		return errors.New("GC policy requires non-negative grace/retention, max >= 1, and timeout in (0,5s]")
	}
	gcPolicy := supervisor.GCPolicy{AutoGC: *autoGC, OwnerGrace: *ownerGrace, RetiredRetention: *retiredRetention, Timeout: *gcTimeout}

	runOnce := func(emitJSON bool) error {
		results, err := a.superviseOnceWithGC(ctx, *registryPath, amq.NewCLI(*amqPath), *self, *wakeTimeout, gcPolicy, *gcMax)
		if err != nil {
			return err
		}
		if emitJSON {
			return printJSON(a.Stdout, results)
		}
		return nil
	}
	if *once {
		return runOnce(true)
	}
	for {
		if err := runOnce(false); err != nil {
			fmt.Fprintln(a.Stderr, err)
		}
		timer := time.NewTimer(*interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (a App) superviseOnce(ctx context.Context, registryPath string, wake supervisor.WakeRunner, self string, wakeTimeout time.Duration) ([]supervisor.Result, error) {
	return a.superviseOnceWithGC(ctx, registryPath, wake, self, wakeTimeout, supervisor.GCPolicy{}, 1)
}

func (a App) superviseOnceWithGC(ctx context.Context, registryPath string, wake supervisor.WakeRunner, self string, wakeTimeout time.Duration, gcPolicy supervisor.GCPolicy, gcMax int) ([]supervisor.Result, error) {
	store := registry.New(registryPath)
	var results []supervisor.Result
	var capability bool
	var capabilityErr error
	lifecycle, lifecycleOK := wake.(wakeLifecycle)
	if gcPolicy.AutoGC {
		if !lifecycleOK {
			capabilityErr = errors.New("wake runner does not implement owner-bound lifecycle operations")
		} else if environment, err := lifecycle.Env(ctx); err != nil {
			capabilityErr = err
		} else {
			capability = environment.HasCapability(amq.CapabilityWakeGCV1)
		}
	}
	err := store.WithRegistrationLockContext(ctx, func() error {
		file, err := store.Load()
		if err != nil {
			return err
		}
		adapters := adapter.DefaultRegistry()
		probes := passProbes(file.Entries, adapters)
		conflicts := targetOwnershipConflicts(file, adapters)
		if anyEntryDue(file.Entries, time.Now().UTC()) {
			mergeOwnershipConflicts(conflicts, physicalOwnershipConflicts(ctx, file, probes))
		}
		results = make([]supervisor.Result, 0, len(file.Entries))
		updates := make([]registry.EntryUpdate, 0, len(file.Entries))
		purges := make([]registry.Entry, 0)
		retiredThisPass := 0
		for _, entry := range file.Entries {
			if ctxErr := ctx.Err(); ctxErr != nil {
				results = append(results, supervisor.Result{Action: supervisor.ActionDeferred, Error: ctxErr})
				continue
			}
			previous := entry
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
						a.logReconcileTransition(entry, updated, startResult)
						results = append(results, startResult)
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
					a.logReconcileTransition(entry, updated, blockedResult)
					results = append(results, blockedResult)
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
					a.logReconcileTransition(entry, updated, recoveredResult)
					results = append(results, recoveredResult)
					continue
				}
				if recoverErr != nil {
					updated.LastError = recoverErr.Error()
					updated.LastSupervisorDecision = supervisor.ActionStartFailed
					if err := store.UpdateEntry(updated); err != nil {
						return err
					}
					failedResult := supervisor.Result{Action: supervisor.ActionStartFailed, AMQTouched: true, Error: recoverErr}
					a.logReconcileTransition(entry, updated, failedResult)
					results = append(results, failedResult)
					continue
				}
			}
			if gcPolicy.AutoGC {
				collector := supervisor.GarbageCollector{
					Wake: lifecycle, InjectVia: self, CapabilityAvailable: capability,
					CapabilityError: capabilityErr, Policy: gcPolicy,
				}
				apply := retiredThisPass < gcMax
				gcUpdated, gcResult := collector.Process(ctx, entry, apply)
				if gcResult.Purge {
					purges = append(purges, previous)
					results = append(results, supervisor.Result{Action: supervisor.GCStatusPurgeCandidate, GC: &gcResult})
					continue
				}
				entry = gcUpdated
				a.logGCTransition(previous, entry, gcResult)
				if previous.State != registry.StateRetired && entry.State == registry.StateRetired {
					retiredThisPass++
				}
				if gcResult.Status == supervisor.GCStatusRetired ||
					gcResult.Status == supervisor.GCStatusOwnerGoneSeen ||
					gcResult.Status == supervisor.GCStatusEligible ||
					entry.State == registry.StateRetired {
					if previous != entry {
						updates = append(updates, registry.EntryUpdate{Before: previous, After: entry})
					}
					results = append(results, supervisor.Result{Action: gcResult.Status, AMQTouched: gcResult.AMQTouched, GC: &gcResult})
					continue
				}
			}
			updated, result := reconciler.Reconcile(ctx, entry)
			if previous != updated {
				updates = append(updates, registry.EntryUpdate{Before: previous, After: updated})
			}
			a.logReconcileTransition(previous, updated, result)
			results = append(results, result)
		}
		if _, err = store.UpdateEntries(updates); err != nil {
			return err
		}
		for _, entry := range purges {
			if _, err := store.ForgetIfUnchanged(entry); err != nil {
				return err
			}
		}
		return nil
	})
	return results, err
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

func (a App) logReconcileTransition(previous, updated registry.Entry, result supervisor.Result) {
	if previous.State == updated.State &&
		previous.LastError == updated.LastError &&
		previous.LastSupervisorDecision == updated.LastSupervisorDecision {
		return
	}
	w := a.Stderr
	if w == nil {
		w = os.Stderr
	}
	if result.Error != nil {
		fmt.Fprintf(w,
			"amq-keepalive reconcile warning: action=%s root=%q agent=%q adapter=%q target=%q failure_count=%d error=%q\n",
			result.Action,
			updated.Root,
			updated.Agent,
			updated.Adapter,
			updated.Target,
			updated.FailureCount,
			result.Error.Error(),
		)
		return
	}
	if updated.State == registry.StateActive {
		fmt.Fprintf(w,
			"amq-keepalive reconcile recovered: action=%s root=%q agent=%q adapter=%q target=%q\n",
			result.Action,
			updated.Root,
			updated.Agent,
			updated.Adapter,
			updated.Target,
		)
	}
}

func (a App) logGCTransition(previous, updated registry.Entry, result supervisor.GCResult) {
	if previous.LastGCDecision == updated.LastGCDecision && previous.LastGCReason == updated.LastGCReason {
		return
	}
	w := a.Stderr
	if w == nil {
		w = os.Stderr
	}
	fmt.Fprintf(w,
		"amq-keepalive gc: status=%s reason_code=%q root=%q agent=%q generation=%q reason=%q\n",
		result.Status,
		result.ReasonCode,
		updated.Root,
		updated.Agent,
		updated.WakeBinding.Generation,
		result.Reason,
	)
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
	SchemaVersion    int           `json:"schema_version"`
	WakeGCCapability bool          `json:"wake_gc_capability"`
	CapabilityError  string        `json:"capability_error,omitempty"`
	Entries          []doctorEntry `json:"entries"`
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
	file, err := store.Load()
	if err != nil {
		return err
	}
	result := doctorResult{SchemaVersion: file.SchemaVersion}
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
	WakeGCCapability bool                  `json:"wake_gc_capability"`
	CapabilityError  string                `json:"capability_error,omitempty"`
	OwnerGoneGrace   string                `json:"owner_gone_grace"`
	RetiredRetention string                `json:"retired_retention"`
	Entries          []supervisor.GCResult `json:"entries"`
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
	maxRetirements := fs.Int("max-retirements", 1, "maximum wake retirements in this invocation")
	timeout := fs.Duration("timeout", 5*time.Second, "deadline for each AMQ lifecycle command")
	apply := fs.Bool("apply", false, "persist observations and retire eligible exact wakes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *legacyAge >= 0 {
		*ownerGrace = *legacyAge
	}
	if *ownerGrace < 0 || *retiredRetention < 0 || *maxRetirements < 1 || *timeout <= 0 || *timeout > 5*time.Second {
		return errors.New("GC policy requires non-negative grace/retention, max >= 1, and timeout in (0,5s]")
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
	err := store.WithRegistrationLockContext(ctx, func() error {
		file, err := store.Load()
		if err != nil {
			return err
		}
		updates := make([]registry.EntryUpdate, 0)
		purges := make([]registry.Entry, 0)
		retired := 0
		for _, entry := range file.Entries {
			collector := supervisor.GarbageCollector{
				Wake: cli, InjectVia: *self, CapabilityAvailable: capability,
				CapabilityError: capabilityErr, Policy: policy,
			}
			updated, item := collector.Process(ctx, entry, *apply && retired < *maxRetirements)
			if entry.State != registry.StateRetired && updated.State == registry.StateRetired {
				retired++
			}
			if item.Purge && *apply {
				purges = append(purges, entry)
			} else if updated != entry && *apply {
				updates = append(updates, registry.EntryUpdate{Before: entry, After: updated})
			}
			result.Entries = append(result.Entries, item)
		}
		if _, err := store.UpdateEntries(updates); err != nil {
			return err
		}
		for _, entry := range purges {
			if _, err := store.ForgetIfUnchanged(entry); err != nil {
				return err
			}
		}
		return nil
	})
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
	agentsFlag := fs.String("agents", "codex,claude", "comma-separated required agent handles")
	fs.String("amq", "amq", "reserved until AMQ identity-safe retirement #235")
	fs.String("self", executablePath(), "reserved until AMQ identity-safe retirement #235")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*rootFlag) == "" {
		return errors.New("--root is required")
	}
	root, err := canonicalExistingPath(*rootFlag)
	if err != nil {
		return fmt.Errorf("resolve --root: %w", err)
	}
	agents, err := parseRequiredAgents(*agentsFlag)
	if err != nil {
		return err
	}

	store := registry.New(*registryPath)
	return store.WithRegistrationLockContext(ctx, func() error {
		file, err := store.Load()
		if err != nil {
			return err
		}
		entries := make([]registry.Entry, 0, len(agents))
		for _, agent := range agents {
			matches := make([]registry.Entry, 0, 1)
			for _, entry := range file.Entries {
				entryRoot, pathErr := canonicalExistingPath(entry.Root)
				if pathErr != nil {
					continue
				}
				if entryRoot == root && entry.Adapter == *adapterName && entry.Agent == agent {
					matches = append(matches, entry)
				}
			}
			if len(matches) != 1 {
				return fmt.Errorf("expected exactly one %s registry entry for agent %s at %s, found %d", *adapterName, agent, root, len(matches))
			}
			entries = append(entries, matches[0])
		}

		adapters := adapter.DefaultRegistry()
		selected, err := adapters.Get(*adapterName)
		if err != nil {
			return err
		}
		for i := range entries {
			entry := &entries[i]
			if normalizer, ok := selected.(adapter.TargetNormalizer); ok {
				normalized, normalizeErr := normalizer.NormalizeTarget(entry.Target)
				if normalizeErr != nil {
					return fmt.Errorf("normalize target for %s: %w", entry.Agent, normalizeErr)
				}
				entry.Target = normalized
			}
			probeErr := selected.Probe(ctx, entry.Target)
			if probeErr == nil {
				return fmt.Errorf("refusing to retire %s wake: adapter target %s still exists", entry.Agent, entry.Target)
			}
			if !errors.Is(probeErr, adapter.ErrTargetNotFound) {
				return fmt.Errorf("refusing to retire %s wake because target absence is not proven: %w", entry.Agent, probeErr)
			}
		}
		return errIdentitySafeWakeRetireUnavailable
	})
}

func canonicalExistingPath(path string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return real, nil
	}
	return abs, nil
}

func parseRequiredAgents(raw string) ([]string, error) {
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
	if len(agents) == 0 {
		return nil, errors.New("--agents is required")
	}
	return agents, nil
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
	gcMax := fs.Int("gc-max-per-pass", 1, "maximum wake retirements per supervisor pass")
	gcTimeout := fs.Duration("gc-timeout", 5*time.Second, "deadline for each AMQ lifecycle command")
	noLoad := fs.Bool("no-load", false, "write plist without loading it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval <= 0 || *ownerGrace <= 0 || *retention <= 0 || *gcMax < 1 || *gcTimeout <= 0 || *gcTimeout > 5*time.Second {
		return errors.New("launchd policy requires positive interval/grace/retention, max >= 1, and timeout in (0,5s]")
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
		GCMaxPerPass: *gcMax,
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
