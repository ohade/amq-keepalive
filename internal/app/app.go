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
		err = a.doctor(args[1:])
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
	WakeOwner      string
	WakeTimeout    time.Duration
	NoStart        bool
	Replace        bool
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	wakeOwner, err := amq.WakeOwnerFromEnvironment()
	if err != nil {
		return fmt.Errorf("owner-bound attachment requires amq coop exec --defer-wake: %w", err)
	}
	return a.registerWithOptions(ctx, registerOptions{
		RegistryPath: *registryPath,
		AdapterName:  *adapterName,
		Target:       *target,
		BaselineFile: *baselineFile,
		Root:         *root,
		BaseRoot:     *baseRoot,
		SessionName:  *sessionName,
		Me:           *me,
		AMQPath:      *amqPath,
		Self:         *self,
		WakeOwner:    wakeOwner,
		WakeTimeout:  *wakeTimeout,
		NoStart:      *noStart,
		Replace:      replace,
	})
}

func (a App) registerWithOptions(ctx context.Context, opts registerOptions) error {
	envCLI := amq.NewCLI(opts.AMQPath)
	if opts.Root == "" || opts.Me == "" || opts.BaseRoot == "" || opts.SessionName == "" {
		env, err := envCLI.Env(ctx)
		if err != nil && (opts.Root == "" || opts.Me == "") {
			return err
		}
		if opts.Root == "" {
			opts.Root = env.Root
		}
		if opts.BaseRoot == "" {
			opts.BaseRoot = env.BaseRoot
		}
		if opts.SessionName == "" {
			opts.SessionName = env.SessionName
		}
		if opts.Me == "" {
			opts.Me = env.Me
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
		ID:             registry.EntryID(opts.Root, opts.Me, opts.AdapterName, opts.Target),
		Root:           opts.Root,
		BaseRoot:       opts.BaseRoot,
		SessionName:    opts.SessionName,
		Agent:          opts.Me,
		Adapter:        opts.AdapterName,
		Target:         opts.Target,
		WakeOwner:      opts.WakeOwner,
		BaselineFile:   opts.BaselineFile,
		BaselineDigest: opts.BaselineDigest,
		State:          registry.StateAttached,
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
			updated, result := reconciler.StartFresh(ctx, next)
			if result.Error != nil {
				return resolveRegistrationReadinessFailure(store, entry, updated, removed, result.Error)
			}
			next = updated
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

func resolveRegistrationReadinessFailure(
	store *registry.Store,
	reservation registry.Entry,
	candidate registry.Entry,
	removed []registry.Entry,
	readinessErr error,
) error {
	if errors.Is(readinessErr, amq.ErrWakeReadinessUncertain) {
		if candidate != reservation {
			if updateErr := store.UpdateEntry(candidate); updateErr != nil {
				return errors.Join(
					readinessErr,
					fmt.Errorf("wake readiness is uncertain and the attached reservation remains, but recording its retry state failed: %w", updateErr),
				)
			}
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

func (a App) supervise(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("supervise", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path for --inject-via")
	once := fs.Bool("once", false, "run one supervisor pass")
	interval := fs.Duration("interval", time.Minute, "supervisor interval")
	wakeTimeout := fs.Duration("wake-ready-timeout", 10*time.Second, "maximum time to wait for amq wake readiness")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval <= 0 {
		return errors.New("--interval must be greater than zero")
	}

	runOnce := func(emitJSON bool) error {
		results, err := a.superviseOnce(ctx, *registryPath, amq.NewCLI(*amqPath), *self, *wakeTimeout)
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
	store := registry.New(registryPath)
	var results []supervisor.Result
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
			updated, result := reconciler.Reconcile(ctx, entry)
			if previous != updated {
				updates = append(updates, registry.EntryUpdate{Before: previous, After: updated})
			}
			a.logReconcileTransition(previous, updated, result)
			results = append(results, result)
		}
		_, err = store.UpdateEntries(updates)
		return err
	})
	return results, err
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

func (a App) doctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store := registry.New(*registryPath)
	file, err := store.Load()
	if err != nil {
		return err
	}
	return printJSON(a.Stdout, file)
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

func (a App) installLaunchd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("install-launchd", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	label := fs.String("label", launchd.DefaultLabel, "launchd label")
	plistPath := fs.String("plist", "", "plist path")
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path")
	interval := fs.Duration("interval", time.Minute, "supervisor interval")
	noLoad := fs.Bool("no-load", false, "write plist without loading it")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts := launchd.Options{
		Label:        *label,
		PlistPath:    *plistPath,
		BinaryPath:   *self,
		RegistryPath: *registryPath,
		AMQPath:      *amqPath,
		Interval:     *interval,
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
	fmt.Fprintln(writer, "usage: amq-keepalive <attach|reattach|supervise|inject|doctor|forget|install-launchd|install-hook|uninstall> [options]")
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
