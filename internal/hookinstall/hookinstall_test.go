package hookinstall

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInstallBothWritesScriptAndMergesConfigs(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "hooks", "amq-keepalive-session-start.sh")
	binaryPath := writeExecutable(t, filepath.Join(dir, "amq-keepalive"))
	claudeConfig := filepath.Join(dir, "claude", "settings.json")
	codexConfig := filepath.Join(dir, "codex", "hooks.json")
	mustWrite(t, claudeConfig, []byte(`{"hooks":{"SessionStart":[{"matcher":"resume","hooks":[{"type":"command","command":"existing"}]}]}}`))
	mustWrite(t, codexConfig, []byte(`{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"existing","timeout":5000}]}]}}`))

	result, err := Install(Options{
		Agent:        AgentBoth,
		ScriptPath:   scriptPath,
		BinaryPath:   binaryPath,
		ClaudeConfig: claudeConfig,
		CodexConfig:  codexConfig,
		Timeout:      time.Second,
	})
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Script.Changed {
		t.Fatal("script changed = false, want true")
	}
	if result.Configs[AgentClaude].Backup == "" || result.Configs[AgentCodex].Backup == "" {
		t.Fatalf("expected config backups, got %#v", result.Configs)
	}
	info, err := os.Stat(scriptPath)
	if err != nil {
		t.Fatalf("stat script: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("script mode = %o, want 755", info.Mode().Perm())
	}
	scriptData, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read script: %v", err)
	}
	if string(scriptData) != SessionStartScript {
		t.Fatal("installed script does not match embedded script")
	}

	claude := readJSON(t, claudeConfig)
	if countCommand(claude, result.Commands[AgentClaude]) != 1 {
		t.Fatalf("Claude command not installed exactly once:\n%s", mustMarshal(t, claude))
	}
	if !strings.Contains(result.Commands[AgentClaude], "AMQ_KEEPALIVE_TIMEOUT_SECONDS='1'") {
		t.Fatalf("Claude command missing timeout env: %q", result.Commands[AgentClaude])
	}

	codex := readJSON(t, codexConfig)
	if countCommand(codex, result.Commands[AgentCodex]) != 1 {
		t.Fatalf("Codex command not installed exactly once:\n%s", mustMarshal(t, codex))
	}
	if !strings.Contains(mustMarshal(t, codex), `"timeout": 6000`) {
		t.Fatalf("Codex hook timeout missing or wrong:\n%s", mustMarshal(t, codex))
	}
}

func TestInstallIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	binaryPath := writeExecutable(t, filepath.Join(dir, "amq-keepalive"))
	claudeConfig := filepath.Join(dir, "settings.json")

	opts := Options{
		Agent:        AgentClaude,
		ScriptPath:   filepath.Join(dir, "hook.sh"),
		BinaryPath:   binaryPath,
		ClaudeConfig: claudeConfig,
		Timeout:      time.Second,
	}
	first, err := Install(opts)
	if err != nil {
		t.Fatalf("first Install() error = %v", err)
	}
	second, err := Install(opts)
	if err != nil {
		t.Fatalf("second Install() error = %v", err)
	}
	if second.Configs[AgentClaude].Changed {
		t.Fatal("second config install changed file, want idempotent no-op")
	}
	doc := readJSON(t, claudeConfig)
	if countCommand(doc, first.Commands[AgentClaude]) != 1 {
		t.Fatalf("command count != 1 after repeat install:\n%s", mustMarshal(t, doc))
	}
}

func TestDryRunDoesNotWriteFiles(t *testing.T) {
	dir := t.TempDir()
	binaryPath := writeExecutable(t, filepath.Join(dir, "amq-keepalive"))
	scriptPath := filepath.Join(dir, "hook.sh")
	configPath := filepath.Join(dir, "settings.json")

	result, err := Install(Options{
		Agent:        AgentClaude,
		ScriptPath:   scriptPath,
		BinaryPath:   binaryPath,
		ClaudeConfig: configPath,
		Timeout:      time.Second,
		DryRun:       true,
	})
	if err != nil {
		t.Fatalf("Install(dry-run) error = %v", err)
	}
	if !result.DryRun || result.Commands[AgentClaude] == "" {
		t.Fatalf("bad dry-run result: %#v", result)
	}
	if _, err := os.Stat(scriptPath); !os.IsNotExist(err) {
		t.Fatalf("script stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("config stat err = %v, want not exist", err)
	}
}

func TestEmbeddedScriptMatchesRepositoryHook(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "hooks", "amq-keepalive-session-start.sh"))
	if err != nil {
		t.Fatalf("read repository hook: %v", err)
	}
	if string(data) != SessionStartScript {
		t.Fatal("embedded SessionStart script drifted from hooks/amq-keepalive-session-start.sh")
	}
}

func writeExecutable(t *testing.T, path string) string {
	t.Helper()
	mustWrite(t, path, []byte("#!/bin/sh\nexit 0\n"))
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("chmod executable: %v", err)
	}
	return path
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readJSON(t *testing.T, path string) map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var doc map[string]interface{}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal json: %v\n%s", err, data)
	}
	return doc
}

func countCommand(doc map[string]interface{}, command string) int {
	count := 0
	hooks, _ := doc["hooks"].(map[string]interface{})
	for _, entry := range interfaceArray(hooks["SessionStart"]) {
		entryObj, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		for _, hook := range interfaceArray(entryObj["hooks"]) {
			hookObj, ok := hook.(map[string]interface{})
			if ok && hookObj["command"] == command {
				count++
			}
		}
	}
	return count
}

func mustMarshal(t *testing.T, v interface{}) string {
	t.Helper()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}
