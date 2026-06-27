package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ohade/amq-keepalive/internal/registry"
)

func TestReattachReplacesCurrentSessionAdapterEntry(t *testing.T) {
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	oldTarget := filepath.Join(dir, "old-inbox.txt")
	newTarget := filepath.Join(dir, "new-inbox.txt")
	otherTarget := filepath.Join(dir, "other-inbox.txt")

	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", oldTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--no-start",
	)
	runApp(t, "attach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", otherTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "claude",
		"--no-start",
	)

	runApp(t, "reattach",
		"--registry", registryPath,
		"--adapter", "file",
		"--target", newTarget,
		"--root", "/tmp/amq-root",
		"--base-root", "/tmp",
		"--session", "amq-root",
		"--me", "codex",
		"--no-start",
	)

	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Entries) != 2 {
		t.Fatalf("entries = %d, want 2: %#v", len(loaded.Entries), loaded.Entries)
	}
	targets := map[string]string{}
	for _, entry := range loaded.Entries {
		targets[entry.Agent] = entry.Target
	}
	if targets["codex"] != newTarget {
		t.Fatalf("codex target = %q, want %q", targets["codex"], newTarget)
	}
	if targets["claude"] != otherTarget {
		t.Fatalf("claude target = %q, want %q", targets["claude"], otherTarget)
	}
}

func TestNormalizeAMQPathsUsesAbsoluteBaseSessionRoot(t *testing.T) {
	root, base := normalizeAMQPaths(".agent-mail/team-upgrader_v3", "/Users/test/git/.agent-mail", "team-upgrader_v3")
	if root != "/Users/test/git/.agent-mail/team-upgrader_v3" {
		t.Fatalf("root = %q, want absolute session root", root)
	}
	if base != "/Users/test/git/.agent-mail" {
		t.Fatalf("base = %q, want absolute base root", base)
	}
}

func TestInstallHookCommandWritesRequestedConfig(t *testing.T) {
	dir := t.TempDir()
	binaryPath := filepath.Join(dir, "amq-keepalive")
	if err := os.WriteFile(binaryPath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	runApp(t, "install-hook",
		"--agent", "codex",
		"--script", filepath.Join(dir, "hook.sh"),
		"--bin", binaryPath,
		"--codex-config", filepath.Join(dir, "hooks.json"),
		"--timeout", "1s",
	)
	if _, err := os.Stat(filepath.Join(dir, "hook.sh")); err != nil {
		t.Fatalf("hook not installed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "hooks.json"))
	if err != nil {
		t.Fatalf("read hooks config: %v", err)
	}
	if !bytes.Contains(data, []byte("AMQ_KEEPALIVE_TIMEOUT_SECONDS='1'")) {
		t.Fatalf("hooks config missing timeout command:\n%s", data)
	}
}

func runApp(t *testing.T, args ...string) {
	t.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := (App{Stdout: &stdout, Stderr: &stderr}).Run(context.Background(), args)
	if code != 0 {
		t.Fatalf("Run(%v) = %d\nstdout:\n%s\nstderr:\n%s", args, code, stdout.String(), stderr.String())
	}
}
