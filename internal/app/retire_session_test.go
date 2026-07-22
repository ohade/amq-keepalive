package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ohade/amq-keepalive/internal/adapter"
	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
)

type retireSessionTestAdapter struct {
	probe func(context.Context, string) error
}

func (retireSessionTestAdapter) Name() string { return "synthetic" }

func (a retireSessionTestAdapter) Probe(ctx context.Context, target string) error {
	if a.probe != nil {
		return a.probe(ctx, target)
	}
	return adapter.ErrTargetNotFound
}

func (retireSessionTestAdapter) Inject(context.Context, string, string) error { return nil }

func (retireSessionTestAdapter) NormalizeTarget(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", errors.New("empty synthetic target")
	}
	return target, nil
}

type retireSessionTestLifecycle struct {
	requests       []amq.RetireWakeRequest
	checkResults   map[string]amq.RetireWakeResult
	checkErrors    map[string]error
	retireResults  map[string]amq.RetireWakeResult
	retireErrors   map[string]error
	onRetire       func(amq.RetireWakeRequest)
	retirementCall int
	signalCount    int
	tombstones     map[string]amq.RetireWakeResult
}

func (f *retireSessionTestLifecycle) RetireWake(_ context.Context, request amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	f.requests = append(f.requests, request)
	if request.Check {
		result, ok := f.checkResults[request.Me]
		if !ok {
			result = retireSessionTestResult(request, "eligible", "manual_eligible", "generation-"+request.Me, "sha256:digest-"+request.Me)
		}
		return result, f.checkErrors[request.Me]
	}
	f.retirementCall++
	if f.onRetire != nil {
		f.onRetire(request)
	}
	result, ok := f.retireResults[request.Me]
	if !ok {
		if retired, exists := f.tombstones[request.Me]; exists && retired.Generation == request.Generation && retired.TargetDigest == request.TargetDigest {
			return retireSessionTestResult(request, "already_retired", "tombstone_match", request.Generation, request.TargetDigest), nil
		}
		result = retireSessionTestResult(request, "retired", "manual_retired", request.Generation, request.TargetDigest)
	}
	retireErr := f.retireErrors[request.Me]
	if retireErr == nil && result.Status == "retired" && result.ReasonCode == "manual_retired" {
		f.signalCount++
		if f.tombstones == nil {
			f.tombstones = make(map[string]amq.RetireWakeResult)
		}
		f.tombstones[request.Me] = result
	}
	return result, retireErr
}

func retireSessionTestResult(request amq.RetireWakeRequest, status, reason, generation, digest string) amq.RetireWakeResult {
	return amq.RetireWakeResult{
		Schema: 1, Status: status, ReasonCode: reason, Root: request.Root, Agent: request.Me,
		Lock:       filepath.Join(request.Root, "agents", request.Me, ".wake.lock"),
		Target:     filepath.Join(request.Root, "agents", request.Me, ".wake.target"),
		Generation: generation, TargetDigest: digest,
	}
}

func TestRetireSessionPreviewIsDeterministicAndBytePureForSchemaV1(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "amq-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	raw := fmt.Sprintf(`{"schema_version":1,"entries":[{"id":"alpha-row","root":%q,"agent":"alpha","adapter":"synthetic","target":" surface-alpha ","state":"detached"},{"id":"beta-row","root":%q,"agent":"beta","adapter":"synthetic","target":"surface-beta","state":"active"}]}`+"\n", root, root)
	if err := os.WriteFile(registryPath, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	amqPath := writeRetireExecutable(t, dir, "amq")
	selfPath := writeRetireExecutable(t, dir, "amq-keepalive")
	before := retireSessionTreeSnapshot(t, dir)
	lifecycle := &retireSessionTestLifecycle{}
	opts := retireSessionOptions{
		RegistryPath: registryPath, Root: root, AdapterName: "synthetic", Agents: "beta,alpha",
		AMQPath: amqPath, Self: selfPath, Timeout: time.Second,
	}
	value, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	first := value.(*retireSessionPlan)
	if first.PlanID == "" || !reflect.DeepEqual(first.Agents, []string{"alpha", "beta"}) || first.Members[0].Target != "surface-alpha" {
		t.Fatalf("preview=%#v", first)
	}
	opts.Agents = "alpha,beta"
	value, err = (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
	if err != nil || value.(*retireSessionPlan).PlanID != first.PlanID {
		t.Fatalf("second preview=%#v err=%v", value, err)
	}
	after := retireSessionTreeSnapshot(t, dir)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only preview changed filesystem:\nbefore=%#v\nafter=%#v", before, after)
	}
	if len(lifecycle.requests) != 0 {
		t.Fatalf("preview invoked AMQ: %#v", lifecycle.requests)
	}
}

func TestRetireSessionTokenMismatchAndLiveTargetSignalNothing(t *testing.T) {
	dir := t.TempDir()
	root, store, opts := setupLegacyRetireSession(t, dir, "alpha")
	lifecycle := &retireSessionTestLifecycle{}
	missing := retireSessionTestAdapter{}
	value, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, missing)
	if err != nil {
		t.Fatal(err)
	}
	plan := value.(*retireSessionPlan)
	canonicalRegistry, err := canonicalRetireSessionRegistry(store.Path)
	if err != nil || plan.RegistryPath != canonicalRegistry {
		t.Fatalf("plan registry=%q canonical=%q err=%v", plan.RegistryPath, canonicalRegistry, err)
	}
	nextAMQ := writeRetireExecutable(t, dir, "amq-next")
	nextSelf := writeRetireExecutable(t, dir, "amq-keepalive-next")
	opts.Apply = true
	opts.ConfirmPlan = strings.Repeat("0", len(plan.PlanID))
	if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, missing); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("token mismatch error=%v", err)
	}
	if len(lifecycle.requests) != 0 {
		t.Fatalf("token mismatch invoked AMQ: %#v", lifecycle.requests)
	}
	missingExecutable := opts
	missingExecutable.Apply = false
	missingExecutable.ConfirmPlan = ""
	missingExecutable.AMQPath = filepath.Join(dir, "missing-amq")
	if _, err := (App{}).retireSessionWithOptions(context.Background(), missingExecutable, lifecycle, missing); err == nil {
		t.Fatal("missing AMQ executable was authorized in a preview token")
	}
	nonExecutable := filepath.Join(dir, "non-executable-amq")
	if err := os.WriteFile(nonExecutable, []byte("not executable\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missingExecutable.AMQPath = nonExecutable
	if _, err := (App{}).retireSessionWithOptions(context.Background(), missingExecutable, lifecycle, missing); err == nil || !strings.Contains(err.Error(), "not a regular executable") {
		t.Fatalf("non-executable AMQ preview error=%v", err)
	}
	if _, err := os.Stat(store.Path + ".registration.lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("token mismatch created registration lock: %v", err)
	}
	for name, mutate := range map[string]func(*retireSessionOptions){
		"amq executable": func(candidate *retireSessionOptions) { candidate.AMQPath = nextAMQ },
		"injector":       func(candidate *retireSessionOptions) { candidate.Self = nextSelf },
		"timeout":        func(candidate *retireSessionOptions) { candidate.Timeout = 2 * time.Second },
	} {
		t.Run(name+" is token bound", func(t *testing.T) {
			candidate := opts
			candidate.Apply = false
			candidate.ConfirmPlan = ""
			mutate(&candidate)
			changedValue, changedErr := (App{}).retireSessionWithOptions(context.Background(), candidate, lifecycle, missing)
			if changedErr != nil {
				t.Fatal(changedErr)
			}
			changed := changedValue.(*retireSessionPlan)
			if changed.PlanID == plan.PlanID {
				t.Fatalf("plan_id did not bind %s", name)
			}
			candidate.Apply = true
			candidate.ConfirmPlan = plan.PlanID
			if _, applyErr := (App{}).retireSessionWithOptions(context.Background(), candidate, lifecycle, missing); applyErr == nil || !strings.Contains(applyErr.Error(), "does not match") {
				t.Fatalf("stale confirmation for %s error=%v", name, applyErr)
			}
		})
	}

	live := retireSessionTestAdapter{probe: func(context.Context, string) error { return nil }}
	opts.Apply = false
	opts.ConfirmPlan = ""
	if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, live); err == nil || !strings.Contains(err.Error(), "still exists") {
		t.Fatalf("live target error=%v", err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].State == registry.StateRetired || loaded.Entries[0].Root != root {
		t.Fatalf("registry changed after local refusal: entries=%#v err=%v", loaded.Entries, err)
	}
}

func TestRetireSessionPlanBindsSamePathExecutableContent(t *testing.T) {
	dir := t.TempDir()
	_, _, opts := setupLegacyRetireSession(t, dir, "alpha")
	lifecycle := &retireSessionTestLifecycle{}
	value, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	before := value.(*retireSessionPlan)
	if err := os.WriteFile(opts.AMQPath, []byte("#!/bin/sh\nprintf changed\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	value, err = (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	after := value.(*retireSessionPlan)
	if before.PlanID == after.PlanID || before.AMQIdentity.SHA256 == after.AMQIdentity.SHA256 {
		t.Fatalf("same-path executable replacement did not change plan identity: before=%#v after=%#v", before.AMQIdentity, after.AMQIdentity)
	}
	opts.Apply, opts.ConfirmPlan = true, before.PlanID
	if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("stale same-path confirmation error=%v", err)
	}
	if len(lifecycle.requests) != 0 {
		t.Fatalf("same-path replacement invoked lifecycle: %#v", lifecycle.requests)
	}
}

func TestRetireSessionPlanBindsSamePathInjectViaContent(t *testing.T) {
	dir := t.TempDir()
	_, _, opts := setupLegacyRetireSession(t, dir, "alpha")
	value, err := (App{}).retireSessionWithOptions(context.Background(), opts, &retireSessionTestLifecycle{}, retireSessionTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	before := value.(*retireSessionPlan)
	if err := os.WriteFile(opts.Self, []byte("#!/bin/sh\nprintf changed\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	value, err = (App{}).retireSessionWithOptions(context.Background(), opts, &retireSessionTestLifecycle{}, retireSessionTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	after := value.(*retireSessionPlan)
	if before.PlanID == after.PlanID || before.InjectIdentity.SHA256 == after.InjectIdentity.SHA256 {
		t.Fatalf("same-path inject-via replacement did not change plan identity: before=%#v after=%#v", before.InjectIdentity, after.InjectIdentity)
	}
}

func TestRetireSessionRevalidatesExecutableAfterCurrentPlanCheck(t *testing.T) {
	dir := t.TempDir()
	_, store, opts := setupLegacyRetireSession(t, dir, "alpha")
	value, err := (App{}).retireSessionWithOptions(context.Background(), opts, &retireSessionTestLifecycle{}, retireSessionTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	plan := value.(*retireSessionPlan)
	probeCalls := 0
	adapterWithSwap := retireSessionTestAdapter{probe: func(context.Context, string) error {
		probeCalls++
		if probeCalls == 2 {
			replacement := filepath.Join(dir, "replacement-amq")
			if err := os.WriteFile(replacement, []byte("#!/bin/sh\nexit 99\n"), 0o700); err != nil {
				return err
			}
			if err := os.Rename(replacement, opts.AMQPath); err != nil {
				return err
			}
		}
		return adapter.ErrTargetNotFound
	}}
	opts.Apply, opts.ConfirmPlan = true, plan.PlanID
	if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, amq.NewCLI(opts.AMQPath), adapterWithSwap); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("post-plan executable replacement error=%v", err)
	}
	loaded, err := store.Load()
	if err != nil || loaded.Entries[0].ManualRetirementIntent.Active() || loaded.Entries[0].State == registry.StateRetired {
		t.Fatalf("post-plan replacement changed registry: file=%#v err=%v", loaded, err)
	}
}

func TestRetireSessionRevalidatesExecutableBetweenMembers(t *testing.T) {
	dir := t.TempDir()
	root, store, opts := setupLegacyRetireSession(t, dir, "alpha", "beta")
	marker := filepath.Join(dir, "first-preflight")
	replacement := filepath.Join(dir, "replacement-amq")
	if err := os.WriteFile(replacement, []byte("#!/bin/sh\nexit 98\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
set -eu
root=""; agent=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --root) root="$2"; shift 2 ;;
    --me) agent="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ ! -e "$RETIRE_SWAP_MARKER" ]; then
  mv "$RETIRE_SWAP_REPLACEMENT" "$RETIRE_SWAP_ORIGINAL"
  : > "$RETIRE_SWAP_MARKER"
fi
printf '{"schema":1,"status":"eligible","reason_code":"manual_eligible","root":"%s","agent":"%s","lock":"%s/agents/%s/.wake.lock","target":"%s/agents/%s/.wake.target","generation":"generation-%s","target_digest":"sha256:digest-%s"}\n' "$root" "$agent" "$root" "$agent" "$root" "$agent" "$agent" "$agent"
`
	if err := os.WriteFile(opts.AMQPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RETIRE_SWAP_MARKER", marker)
	t.Setenv("RETIRE_SWAP_REPLACEMENT", replacement)
	t.Setenv("RETIRE_SWAP_ORIGINAL", opts.AMQPath)
	value, err := (App{}).retireSessionWithOptions(context.Background(), opts, amq.NewCLI(opts.AMQPath), retireSessionTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	plan := value.(*retireSessionPlan)
	opts.Apply, opts.ConfirmPlan = true, plan.PlanID
	if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, amq.NewCLI(opts.AMQPath), retireSessionTestAdapter{}); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("between-member executable replacement error=%v root=%s", err, root)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range loaded.Entries {
		if entry.ManualRetirementIntent.Active() || entry.State == registry.StateRetired {
			t.Fatalf("between-member replacement persisted or retired row: %#v", entry)
		}
	}
}

func TestRetireSessionApplyUsesExactManualTransportAndPreservesMailboxTree(t *testing.T) {
	dir := t.TempDir()
	root, store, opts := setupLegacyRetireSession(t, dir, "alpha", "beta")
	for path, body := range map[string]string{
		filepath.Join(root, "agents", "alpha", "inbox", "message.json"): "synthetic-message\n",
		filepath.Join(root, "agents", "alpha", ".wake.target"):          "synthetic-target\n",
		filepath.Join(root, "baseline.json"):                            "synthetic-baseline\n",
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	mailboxBefore := retireSessionTreeSnapshot(t, root)
	argsLog := filepath.Join(dir, "args.log")
	fakeAMQ := filepath.Join(dir, "amq")
	script := `#!/bin/sh
set -eu
root=""
agent=""
generation=""
digest=""
check=false
manual=false
printf 'CALL' >> "$RETIRE_ARGS_LOG"
for arg in "$@"; do printf '|%s' "$arg" >> "$RETIRE_ARGS_LOG"; done
printf '\n' >> "$RETIRE_ARGS_LOG"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --root) root="$2"; shift 2 ;;
    --me) agent="$2"; shift 2 ;;
    --if-generation) generation="$2"; shift 2 ;;
    --if-target-digest) digest="$2"; shift 2 ;;
    --check) check=true; shift ;;
    --manual) manual=true; shift ;;
    *) shift ;;
  esac
done
[ "$manual" = true ] || exit 90
expected_generation="generation-$agent"
expected_digest="sha256:digest-$agent"
if [ "$check" = true ]; then
  printf '{"schema":1,"status":"eligible","reason_code":"manual_eligible","root":"%s","agent":"%s","lock":"%s/agents/%s/.wake.lock","target":"%s/agents/%s/.wake.target","generation":"%s","target_digest":"%s","future_field":true}\n' "$root" "$agent" "$root" "$agent" "$root" "$agent" "$expected_generation" "$expected_digest"
  exit 0
fi
[ "$generation" = "$expected_generation" ] || exit 91
[ "$digest" = "$expected_digest" ] || exit 92
printf '{"schema":1,"status":"retired","reason_code":"manual_retired","root":"%s","agent":"%s","lock":"%s/agents/%s/.wake.lock","target":"%s/agents/%s/.wake.target","generation":"%s","target_digest":"%s"}\n' "$root" "$agent" "$root" "$agent" "$root" "$agent" "$generation" "$digest"
`
	if err := os.WriteFile(fakeAMQ, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RETIRE_ARGS_LOG", argsLog)
	opts.AMQPath = fakeAMQ
	selected := retireSessionTestAdapter{}
	value, err := (App{}).retireSessionWithOptions(context.Background(), opts, amq.NewCLI(fakeAMQ), selected)
	if err != nil {
		t.Fatal(err)
	}
	plan := value.(*retireSessionPlan)
	opts.Apply = true
	opts.ConfirmPlan = plan.PlanID
	value, err = (App{Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }}).retireSessionWithOptions(context.Background(), opts, amq.NewCLI(fakeAMQ), selected)
	if err != nil {
		t.Fatal(err)
	}
	applied := value.(*retireSessionApplyResult)
	if !applied.Applied || applied.Partial || len(applied.Retired) != 2 {
		t.Fatalf("apply result=%#v", applied)
	}
	mailboxAfter := retireSessionTreeSnapshot(t, root)
	if !reflect.DeepEqual(mailboxBefore, mailboxAfter) {
		t.Fatalf("mailbox tree changed:\nbefore=%#v\nafter=%#v", mailboxBefore, mailboxAfter)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 2 {
		t.Fatalf("Load entries=%#v err=%v", loaded.Entries, err)
	}
	for _, entry := range loaded.Entries {
		if entry.State != registry.StateRetired || entry.RetiredAt.IsZero() || entry.RetirementReason != "manual_retired" {
			t.Fatalf("entry not durably retained as retired: %#v", entry)
		}
	}
	logData, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(logData)), "\n")
	if len(lines) != 4 {
		t.Fatalf("AMQ calls=%d\n%s", len(lines), logData)
	}
	for index, line := range lines {
		if !strings.Contains(line, "|--manual") || !strings.Contains(line, "|--inject-via|"+plan.InjectVia+"|--inject-arg|inject|--inject-arg|synthetic|--inject-arg|surface-") || strings.Contains(line, "--require-owner-gone") {
			t.Fatalf("call %d transport=%s", index, line)
		}
		if index < 2 && (!strings.Contains(line, "|--check") || strings.Contains(line, "--if-generation")) {
			t.Fatalf("preflight call %d=%s", index, line)
		}
		if index >= 2 && (strings.Contains(line, "|--check") || !strings.Contains(line, "|--if-generation|generation-") || !strings.Contains(line, "|--if-target-digest|sha256:digest-")) {
			t.Fatalf("mutation call %d=%s", index, line)
		}
	}
}

func TestRetireSessionStaticPreflightRefusalsNeverSignal(t *testing.T) {
	t.Run("later member refusal blocks the entire mutation phase", func(t *testing.T) {
		dir := t.TempDir()
		_, store, opts := setupLegacyRetireSession(t, dir, "alpha", "beta")
		lifecycle := &retireSessionTestLifecycle{
			checkResults: map[string]amq.RetireWakeResult{
				"beta": {Schema: 1, Status: "refused", ReasonCode: "manual_target_mismatch", Root: opts.Root, Agent: "beta"},
			},
			checkErrors: map[string]error{"beta": errors.New("synthetic later refusal")},
		}
		previewValue, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil {
			t.Fatal(err)
		}
		opts.Apply = true
		opts.ConfirmPlan = previewValue.(*retireSessionPlan).PlanID
		if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{}); err == nil || !strings.Contains(err.Error(), "no wake retirement signals") {
			t.Fatalf("preflight error=%v", err)
		}
		if lifecycle.retirementCall != 0 || len(lifecycle.requests) != 2 || !lifecycle.requests[0].Check || !lifecycle.requests[1].Check {
			t.Fatalf("all-member preflight calls=%#v retirement=%d", lifecycle.requests, lifecycle.retirementCall)
		}
		loaded, err := store.Load()
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range loaded.Entries {
			if entry.State == registry.StateRetired {
				t.Fatalf("static all-member refusal retired %#v", entry)
			}
		}
	})

	for _, reason := range []string{"manual_lock_missing", "manual_raw_wake", "manual_wake_unverified", "manual_target_mismatch"} {
		t.Run(reason, func(t *testing.T) {
			dir := t.TempDir()
			_, store, opts := setupLegacyRetireSession(t, dir, "alpha")
			lifecycle := &retireSessionTestLifecycle{
				checkResults: map[string]amq.RetireWakeResult{"alpha": {Schema: 1, Status: "refused", ReasonCode: reason, Root: opts.Root, Agent: "alpha"}},
				checkErrors:  map[string]error{"alpha": errors.New("synthetic static refusal")},
			}
			previewValue, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
			if err != nil {
				t.Fatal(err)
			}
			opts.Apply = true
			opts.ConfirmPlan = previewValue.(*retireSessionPlan).PlanID
			if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{}); err == nil || !strings.Contains(err.Error(), "no wake retirement signals") {
				t.Fatalf("preflight error=%v", err)
			}
			if lifecycle.retirementCall != 0 || len(lifecycle.requests) != 1 || !lifecycle.requests[0].Check {
				t.Fatalf("static refusal calls=%#v retirement=%d", lifecycle.requests, lifecycle.retirementCall)
			}
			loaded, err := store.Load()
			if err != nil || loaded.Entries[0].State == registry.StateRetired {
				t.Fatalf("static refusal changed row=%#v err=%v", loaded.Entries, err)
			}
		})
	}
}

func TestRetireSessionPartialPersistenceReplayAndCASRace(t *testing.T) {
	t.Run("partial persists first success", func(t *testing.T) {
		dir := t.TempDir()
		_, store, opts := setupLegacyRetireSession(t, dir, "alpha", "beta")
		lifecycle := &retireSessionTestLifecycle{
			retireResults: map[string]amq.RetireWakeResult{"beta": {Schema: 1, Status: "refused", ReasonCode: "manual_wake_changed", Root: opts.Root, Agent: "beta"}},
			retireErrors:  map[string]error{"beta": errors.New("synthetic post-preflight race")},
		}
		value, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil {
			t.Fatal(err)
		}
		opts.Apply = true
		opts.ConfirmPlan = value.(*retireSessionPlan).PlanID
		value, err = (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err == nil || !strings.Contains(err.Error(), "after persisting 1 success") {
			t.Fatalf("partial error=%v result=%#v", err, value)
		}
		partial := value.(*retireSessionApplyResult)
		if !partial.Partial || partial.Applied || len(partial.Retired) != 1 || partial.Retired[0].Agent != "alpha" {
			t.Fatalf("partial result=%#v", partial)
		}
		loaded, err := store.Load()
		if err != nil {
			t.Fatal(err)
		}
		states := map[string]registry.State{}
		for _, entry := range loaded.Entries {
			states[entry.Agent] = entry.State
		}
		if states["alpha"] != registry.StateRetired || states["beta"] == registry.StateRetired {
			t.Fatalf("partial states=%#v", states)
		}
	})

	t.Run("successful replay does not signal twice", func(t *testing.T) {
		dir := t.TempDir()
		_, _, opts := setupLegacyRetireSession(t, dir, "alpha")
		lifecycle := &retireSessionTestLifecycle{}
		value, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil {
			t.Fatal(err)
		}
		opts.Apply = true
		opts.ConfirmPlan = value.(*retireSessionPlan).PlanID
		if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{}); err != nil {
			t.Fatal(err)
		}
		calls := lifecycle.retirementCall
		replayed, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil || !replayed.(*retireSessionApplyResult).Applied || len(replayed.(*retireSessionApplyResult).Retired) != 1 {
			t.Fatalf("receipt replay result=%#v err=%v", replayed, err)
		}
		if lifecycle.retirementCall != calls {
			t.Fatalf("replay signaled again: before=%d after=%d", calls, lifecycle.retirementCall)
		}
	})

	t.Run("reattach race is not overwritten", func(t *testing.T) {
		dir := t.TempDir()
		root, store, opts := setupLegacyRetireSession(t, dir, "alpha")
		lifecycle := &retireSessionTestLifecycle{}
		value, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil {
			t.Fatal(err)
		}
		var reattachErr error
		lifecycle.onRetire = func(amq.RetireWakeRequest) {
			_, _, raceErr := store.ReplaceSessionAdapter(registry.Entry{
				Root: root, Agent: "alpha", Adapter: "synthetic", Target: "surface-raced",
				State: registry.StateAttached, LegacyUnbound: true,
			})
			reattachErr = raceErr
		}
		opts.Apply = true
		opts.ConfirmPlan = value.(*retireSessionPlan).PlanID
		value, err = (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil {
			t.Fatalf("manual retirement after blocked reattach result=%#v err=%v", value, err)
		}
		if reattachErr == nil || !strings.Contains(reattachErr.Error(), "pending exact manual retirement") {
			t.Fatalf("reattach was not blocked: %v", reattachErr)
		}
		loaded, err := store.Load()
		if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Target != "surface-alpha" || loaded.Entries[0].State != registry.StateRetired {
			t.Fatalf("blocked reattach changed exact row: entries=%#v err=%v", loaded.Entries, err)
		}
		if lifecycle.retirementCall != 1 {
			t.Fatalf("race retirement calls=%d", lifecycle.retirementCall)
		}
	})
}

func TestRetireSessionCrashReplayFailpoints(t *testing.T) {
	originalPersist := persistRetireSessionEntry
	t.Cleanup(func() { persistRetireSessionEntry = originalPersist })

	t.Run("all bindings must persist before first signal", func(t *testing.T) {
		persistRetireSessionEntry = originalPersist
		dir := t.TempDir()
		_, store, opts := setupLegacyRetireSession(t, dir, "alpha", "beta")
		lifecycle := &retireSessionTestLifecycle{}
		preview, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil {
			t.Fatal(err)
		}
		opts.Apply = true
		opts.ConfirmPlan = preview.(*retireSessionPlan).PlanID
		calls := 0
		persistRetireSessionEntry = func(store *registry.Store, before, after registry.Entry) (registry.UpdateResult, error) {
			calls++
			if calls == 2 {
				return registry.UpdateResult{}, errors.New("injected second-intent persistence failure")
			}
			return originalPersist(store, before, after)
		}
		if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{}); err == nil || !strings.Contains(err.Error(), "no wake retirement signals") {
			t.Fatalf("intent failure error=%v", err)
		}
		if lifecycle.signalCount != 0 || lifecycle.retirementCall != 0 {
			t.Fatalf("intent failure signals=%d mutations=%d", lifecycle.signalCount, lifecycle.retirementCall)
		}
		loaded, err := store.Load()
		if err != nil {
			t.Fatal(err)
		}
		pending := 0
		for _, entry := range loaded.Entries {
			if entry.ManualRetirementIntent.Active() {
				pending++
			}
		}
		if pending != 1 {
			t.Fatalf("persisted intents=%d entries=%#v, want one durable prefix and zero signals", pending, loaded.Entries)
		}
	})

	t.Run("crash after intent before AMQ resumes exact binding", func(t *testing.T) {
		persistRetireSessionEntry = originalPersist
		dir := t.TempDir()
		_, store, opts := setupLegacyRetireSession(t, dir, "alpha")
		lifecycle := &retireSessionTestLifecycle{retireErrors: map[string]error{"alpha": errors.New("injected pre-signal crash")}}
		preview, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil {
			t.Fatal(err)
		}
		opts.Apply = true
		opts.ConfirmPlan = preview.(*retireSessionPlan).PlanID
		if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{}); err == nil {
			t.Fatal("pre-signal crash unexpectedly succeeded")
		}
		loaded, err := store.Load()
		if err != nil || !loaded.Entries[0].ManualRetirementIntent.Active() || lifecycle.signalCount != 0 {
			t.Fatalf("pre-signal crash row=%#v signals=%d err=%v", loaded.Entries, lifecycle.signalCount, err)
		}
		delete(lifecycle.retireErrors, "alpha")
		requestCount := len(lifecycle.requests)
		replayed, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
		if err != nil || !replayed.(*retireSessionApplyResult).Applied || lifecycle.signalCount != 1 {
			t.Fatalf("pre-signal replay result=%#v signals=%d err=%v", replayed, lifecycle.signalCount, err)
		}
		if len(lifecycle.requests) != requestCount+1 || lifecycle.requests[len(lifecycle.requests)-1].Check {
			t.Fatalf("pending replay performed an unbound check: %#v", lifecycle.requests[requestCount:])
		}
	})

	for _, failure := range []struct {
		name string
		fail func() (registry.UpdateResult, error)
	}{
		{name: "AMQ success before registry save", fail: func() (registry.UpdateResult, error) {
			return registry.UpdateResult{}, errors.New("injected completion save failure")
		}},
		{name: "completion CAS skip", fail: func() (registry.UpdateResult, error) {
			return registry.UpdateResult{Skipped: 1}, nil
		}},
	} {
		t.Run(failure.name, func(t *testing.T) {
			persistRetireSessionEntry = originalPersist
			dir := t.TempDir()
			_, store, opts := setupLegacyRetireSession(t, dir, "alpha")
			lifecycle := &retireSessionTestLifecycle{}
			preview, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
			if err != nil {
				t.Fatal(err)
			}
			opts.Apply = true
			opts.ConfirmPlan = preview.(*retireSessionPlan).PlanID
			calls := 0
			persistRetireSessionEntry = func(store *registry.Store, before, after registry.Entry) (registry.UpdateResult, error) {
				calls++
				if calls == 2 {
					return failure.fail()
				}
				return originalPersist(store, before, after)
			}
			if _, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{}); err == nil {
				t.Fatal("post-signal persistence failure unexpectedly succeeded")
			}
			loaded, err := store.Load()
			if err != nil || !loaded.Entries[0].ManualRetirementIntent.Active() || loaded.Entries[0].State == registry.StateRetired || lifecycle.signalCount != 1 {
				t.Fatalf("post-signal failure row=%#v signals=%d err=%v", loaded.Entries, lifecycle.signalCount, err)
			}
			persistRetireSessionEntry = originalPersist
			requestCount := len(lifecycle.requests)
			replayed, err := (App{}).retireSessionWithOptions(context.Background(), opts, lifecycle, retireSessionTestAdapter{})
			if err != nil || !replayed.(*retireSessionApplyResult).Applied || lifecycle.signalCount != 1 {
				t.Fatalf("receipt replay result=%#v signals=%d err=%v", replayed, lifecycle.signalCount, err)
			}
			if len(lifecycle.requests) != requestCount+1 || lifecycle.requests[len(lifecycle.requests)-1].Check {
				t.Fatalf("receipt replay performed an unbound check: %#v", lifecycle.requests[requestCount:])
			}
			loaded, err = store.Load()
			if err != nil || loaded.Entries[0].State != registry.StateRetired || loaded.Entries[0].ManualRetirementIntent.Active() ||
				loaded.Entries[0].ManualRetirementReceipt.ReasonCode != "tombstone_match" {
				t.Fatalf("receipt replay did not converge: entries=%#v err=%v", loaded.Entries, err)
			}
		})
	}
}

func setupLegacyRetireSession(t *testing.T, dir string, agents ...string) (string, *registry.Store, retireSessionOptions) {
	t.Helper()
	root := filepath.Join(dir, "amq-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	store := registry.New(filepath.Join(dir, "registry.json"))
	for _, agent := range agents {
		if _, err := store.Upsert(registry.Entry{
			Root: root, Agent: agent, Adapter: "synthetic", Target: "surface-" + agent,
			State: registry.StateDetached, LegacyUnbound: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return root, store, retireSessionOptions{
		RegistryPath: store.Path, Root: root, AdapterName: "synthetic", Agents: strings.Join(agents, ","),
		AMQPath: writeRetireExecutable(t, dir, "amq"), Self: writeRetireExecutable(t, dir, "amq-keepalive"), Timeout: time.Second,
	}
}

func writeRetireExecutable(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func retireSessionTreeSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	snapshot := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := info.Mode().String()
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			value += "\x00" + string(data)
		}
		snapshot[relative] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
