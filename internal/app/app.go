package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ohade/amq-keepalive/internal/adapter"
	"github.com/ohade/amq-keepalive/internal/amq"
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
	case "supervise":
		err = a.supervise(ctx, args[1:])
	case "inject":
		err = a.inject(ctx, args[1:])
	case "doctor":
		err = a.doctor(args[1:])
	case "forget":
		err = a.forget(args[1:])
	case "install-launchd", "uninstall":
		fmt.Fprintf(a.Stderr, "%s is planned for M1; M0 does not install machine services\n", args[0])
		return 3
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

func (a App) attach(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("attach", flag.ContinueOnError)
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
	if *target == "" {
		return errors.New("--target is required")
	}

	envCLI := amq.NewCLI(*amqPath)
	if *root == "" || *me == "" || *baseRoot == "" || *sessionName == "" {
		env, err := envCLI.Env(ctx)
		if err != nil && (*root == "" || *me == "") {
			return err
		}
		if *root == "" {
			*root = env.Root
		}
		if *baseRoot == "" {
			*baseRoot = env.BaseRoot
		}
		if *sessionName == "" {
			*sessionName = env.SessionName
		}
		if *me == "" {
			*me = env.Me
		}
	}

	adapters := adapter.DefaultRegistry()
	selected, err := adapters.Get(*adapterName)
	if err != nil {
		return err
	}
	if err := selected.Probe(ctx, *target); err != nil {
		return err
	}

	store := registry.New(*registryPath)
	entry, err := store.Upsert(registry.Entry{
		Root:        *root,
		BaseRoot:    *baseRoot,
		SessionName: *sessionName,
		Agent:       *me,
		Adapter:     *adapterName,
		Target:      *target,
		State:       registry.StateAttached,
	})
	if err != nil {
		return err
	}
	if !*noStart {
		reconciler := supervisor.Reconciler{
			Wake:      envCLI,
			Adapter:   selected,
			InjectVia: *self,
		}
		updated, result := reconciler.Reconcile(ctx, entry)
		if err := store.UpdateEntry(updated); err != nil {
			return err
		}
		entry = updated
		if result.Error != nil && result.Action != supervisor.ActionDetached {
			return result.Error
		}
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

func (a App) usage() {
	fmt.Fprintln(a.Stderr, "usage: amq-keepalive <attach|supervise|inject|doctor|forget|install-launchd|uninstall> [options]")
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
