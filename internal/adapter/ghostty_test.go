package adapter

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
)

type fakeCommandRunner struct {
	output []byte
	err    error
	calls  []commandCall
}

type commandCall struct {
	name string
	args []string
}

func (f *fakeCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, commandCall{name: name, args: append([]string{}, args...)})
	return f.output, f.err
}

func TestGhosttyDiscoverReturnsTrimmedWindowTitle(t *testing.T) {
	skipNonDarwin(t)
	runner := &fakeCommandRunner{output: []byte("team-upgrader_v3\n")}
	target, err := (Ghostty{Runner: runner}).Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if target != "team-upgrader_v3" {
		t.Fatalf("target = %q, want team-upgrader_v3", target)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "osascript" {
		t.Fatalf("calls = %#v, want one osascript call", runner.calls)
	}
}

func TestGhosttyProbePassesTargetAsArgument(t *testing.T) {
	skipNonDarwin(t)
	runner := &fakeCommandRunner{output: []byte("ok\n")}
	err := (Ghostty{Runner: runner}).Probe(context.Background(), "Team Alpha")
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	call := runner.calls[0]
	if got := call.args[len(call.args)-1]; got != "Team Alpha" {
		t.Fatalf("last osascript arg = %q, want target", got)
	}
}

func TestGhosttyInjectPassesTargetAndPayloadAsArguments(t *testing.T) {
	skipNonDarwin(t)
	runner := &fakeCommandRunner{}
	payload := "AMQ [team-upgrader_v3]: message from claude"
	err := (Ghostty{Runner: runner}).Inject(context.Background(), "Team Alpha", payload)
	if err != nil {
		t.Fatalf("Inject() error = %v", err)
	}
	call := runner.calls[0]
	if got := call.args[len(call.args)-2]; got != "Team Alpha" {
		t.Fatalf("target arg = %q, want Team Alpha", got)
	}
	if got := call.args[len(call.args)-1]; got != payload {
		t.Fatalf("payload arg = %q, want payload", got)
	}
	if !strings.Contains(call.args[1], "the clipboard") {
		t.Fatalf("script does not appear to use clipboard paste: %q", call.args[1])
	}
}

func TestGhosttyErrorsIncludeCommandOutput(t *testing.T) {
	skipNonDarwin(t)
	runner := &fakeCommandRunner{output: []byte("accessibility denied"), err: errors.New("exit status 1")}
	err := (Ghostty{Runner: runner}).Probe(context.Background(), "Team Alpha")
	if err == nil {
		t.Fatal("Probe() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "accessibility denied") {
		t.Fatalf("error = %v, want command output", err)
	}
}

func skipNonDarwin(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("Ghostty adapter uses macOS Accessibility")
	}
}
