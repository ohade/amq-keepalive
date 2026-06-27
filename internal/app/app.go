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
		a.usage()
		return 2
	}

	var err error
	switch args[0] {
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
		err = a.forget(args[1:])
	case "install-launchd":
		err = a.installLaunchd(ctx, args[1:])
	case "install-hook":
		err = a.installHook(args[1:])
	case "uninstall":
		err = a.uninstallLaunchd(ctx, args[1:])
	default:
		fmt.Fprintf(a.Stderr, "unknown command %q\n", args[0])
		a.usage()
		return 2
	}
	if err != nil {
		fmt.Fprintln(a.Stderr, err)
		return 1
	}
	return 0
}

type registerOptions struct {
	RegistryPath string
	AdapterName  string
	Target       string
	Root         string
	BaseRoot     string
	SessionName  string
	Me           string
	AMQPath      string
	Self         string
	NoStart      bool
	Replace      bool
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
	root := fs.String("root", "", "AMQ root")
	baseRoot := fs.String("base-root", "", "AMQ base root")
	sessionName := fs.String("session", "", "AMQ session name")
	me := fs.String("me", "", "AMQ agent handle")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path for --inject-via")
	noStart := fs.Bool("no-start", false, "register without starting/reconciling wake")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return a.registerWithOptions(ctx, registerOptions{
		RegistryPath: *registryPath,
		AdapterName:  *adapterName,
		Target:       *target,
		Root:         *root,
		BaseRoot:     *baseRoot,
		SessionName:  *sessionName,
		Me:           *me,
		AMQPath:      *amqPath,
		Self:         *self,
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
	if err := selected.Probe(ctx, opts.Target); err != nil {
		return err
	}

	store := registry.New(opts.RegistryPath)
	next := registry.Entry{
		Root:        opts.Root,
		BaseRoot:    opts.BaseRoot,
		SessionName: opts.SessionName,
		Agent:       opts.Me,
		Adapter:     opts.AdapterName,
		Target:      opts.Target,
		State:       registry.StateAttached,
	}
	var entry registry.Entry
	var removed []registry.Entry
	if opts.Replace {
		entry, removed, err = store.ReplaceSessionAdapter(next)
	} else {
		entry, err = store.Upsert(next)
	}
	if err != nil {
		return err
	}
	if !opts.NoStart {
		reconciler := supervisor.Reconciler{
			Wake:      envCLI,
			Adapter:   selected,
			InjectVia: opts.Self,
		}
		var updated registry.Entry
		var result supervisor.Result
		if opts.Replace {
			updated, result = reconciler.StartFresh(ctx, entry)
		} else {
			updated, result = reconciler.Reconcile(ctx, entry)
		}
		if err := store.UpdateEntry(updated); err != nil {
			return err
		}
		entry = updated
		if result.Error != nil && result.Action != supervisor.ActionDetached {
			return result.Error
		}
	}
	if opts.Replace {
		return printJSON(a.Stdout, registerResult{Entry: entry, RemovedEntries: removed})
	}
	return printJSON(a.Stdout, entry)
}

func (a App) supervise(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("supervise", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	registryPath := fs.String("registry", mustDefaultRegistryPath(), "registry file path")
	amqPath := fs.String("amq", "amq", "amq executable path")
	self := fs.String("self", executablePath(), "amq-keepalive executable path for --inject-via")
	once := fs.Bool("once", false, "run one supervisor pass")
	interval := fs.Duration("interval", 10*time.Second, "supervisor interval")
	if err := fs.Parse(args); err != nil {
		return err
	}

	runOnce := func() error {
		return a.superviseOnce(ctx, *registryPath, amq.NewCLI(*amqPath), *self)
	}
	if *once {
		return runOnce()
	}
	for {
		if err := runOnce(); err != nil {
			fmt.Fprintln(a.Stderr, err)
		}
		timer := time.NewTimer(*interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (a App) superviseOnce(ctx context.Context, registryPath string, wake supervisor.WakeRunner, self string) error {
	store := registry.New(registryPath)
	file, err := store.Load()
	if err != nil {
		return err
	}
	adapters := adapter.DefaultRegistry()
	results := make([]supervisor.Result, 0, len(file.Entries))
	for _, entry := range file.Entries {
		selected, err := adapters.Get(entry.Adapter)
		if err != nil {
			entry.LastError = err.Error()
			entry.LastSupervisorDecision = supervisor.ActionBackoff
			entry.State = registry.StateAttached
			if updateErr := store.UpdateEntry(entry); updateErr != nil {
				return updateErr
			}
			results = append(results, supervisor.Result{Action: supervisor.ActionBackoff, Error: err})
			continue
		}
		reconciler := supervisor.Reconciler{
			Wake:      wake,
			Adapter:   selected,
			InjectVia: self,
		}
		updated, result := reconciler.Reconcile(ctx, entry)
		if err := store.UpdateEntry(updated); err != nil {
			return err
		}
		results = append(results, result)
	}
	return printJSON(a.Stdout, results)
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

func (a App) forget(args []string) error {
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
	removed, err := store.Forget(*id)
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
	interval := fs.Duration("interval", 10*time.Second, "supervisor interval")
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

func (a App) usage() {
	fmt.Fprintln(a.Stderr, "usage: amq-keepalive <attach|reattach|supervise|inject|doctor|forget|install-launchd|install-hook|uninstall> [options]")
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
