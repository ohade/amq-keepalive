package executable

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCaptureAndOpenVerifiedRejectSamePathReplacement(t *testing.T) {
	path := writeTestExecutable(t, "#!/bin/sh\nprintf first\\n\n")
	identity, err := Capture(path)
	if err != nil {
		t.Fatal(err)
	}
	if !identity.Complete() || filepath.Base(identity.Path) != filepath.Base(path) {
		t.Fatalf("identity=%#v", identity)
	}
	if file, err := OpenVerified(identity); err != nil {
		t.Fatal(err)
	} else {
		_ = file.Close()
	}
	replacement := filepath.Join(filepath.Dir(path), "replacement")
	if err := os.WriteFile(replacement, []byte("#!/bin/sh\nprintf second\\n\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenVerified(identity); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("same-path replacement error=%v", err)
	}
}

func TestCaptureRejectsWritableExecutable(t *testing.T) {
	path := writeTestExecutable(t, "#!/bin/sh\nexit 0\n")
	if err := os.Chmod(path, 0o722); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(path); err == nil || !strings.Contains(err.Error(), "writable") {
		t.Fatalf("writable executable error=%v", err)
	}
}

func TestIdentityValidationFailureMatrix(t *testing.T) {
	if _, err := Capture(""); err == nil {
		t.Fatal("empty command accepted")
	}
	if _, err := Capture(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing command accepted")
	}
	nonExecutable := filepath.Join(t.TempDir(), "non-executable")
	if err := os.WriteFile(nonExecutable, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(nonExecutable); err == nil || !strings.Contains(err.Error(), "regular executable") {
		t.Fatalf("non-executable error=%v", err)
	}
	if _, err := Capture(t.TempDir()); err == nil || !strings.Contains(err.Error(), "regular executable") {
		t.Fatalf("directory executable error=%v", err)
	}
	unsafeDir := t.TempDir()
	if err := os.Chmod(unsafeDir, 0o777); err != nil {
		t.Fatal(err)
	}
	unsafeExecutable := filepath.Join(unsafeDir, "tool")
	if err := os.WriteFile(unsafeExecutable, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(unsafeExecutable); err == nil || !strings.Contains(err.Error(), "unsafely") {
		t.Fatalf("unsafe parent error=%v", err)
	}
	if _, err := OpenVerified(Identity{}); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("incomplete identity error=%v", err)
	}
}

func TestIdentityLookPathVerifyAndNoFollowBranches(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lookup-tool")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	identity, err := Capture("lookup-tool")
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(identity); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := Verify(identity); err == nil {
		t.Fatal("removed executable retained its identity")
	}
	target := writeTestExecutable(t, "#!/bin/sh\nexit 0\n")
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if file, err := openNoFollow(link); err == nil {
		_ = file.Close()
		t.Fatal("no-follow open accepted a symlink")
	}
}

func TestCommandContextRejectsChangedIdentityBeforeSnapshot(t *testing.T) {
	path := writeTestExecutable(t, "#!/bin/sh\nexit 0\n")
	identity, err := Capture(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 7\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CommandContext(context.Background(), identity); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("changed command identity error=%v", err)
	}
}

func TestCommandContextExecutesPinnedDescriptor(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("descriptor execution is supported on Darwin and Linux")
	}
	path := writeTestExecutable(t, "#!/bin/sh\nprintf '%s' \"$1\"\n")
	identity, err := Capture(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd, cleanup, err := CommandContext(context.Background(), identity, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.Output()
	cleanup()
	if err != nil || strings.TrimSpace(string(output)) != "pinned" {
		t.Fatalf("descriptor command output=%q err=%v", output, err)
	}
}

func TestCommandContextRunsVerifiedBytesAfterSourceReplacement(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("pinned execution is supported on Darwin and Linux")
	}
	path := writeTestExecutable(t, "#!/bin/sh\nprintf first\n")
	identity, err := Capture(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd, cleanup, err := CommandContext(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(filepath.Dir(path), "replacement-after-open")
	if err := os.WriteFile(replacement, []byte("#!/bin/sh\nprintf second\n"), 0o700); err != nil {
		cleanup()
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		cleanup()
		t.Fatal(err)
	}
	output, err := cmd.Output()
	cleanup()
	if err != nil || strings.TrimSpace(string(output)) != "first" {
		t.Fatalf("pinned replacement output=%q err=%v", output, err)
	}
}

func TestCommandContextExecutesGoBinarySnapshot(t *testing.T) {
	if os.Getenv("AMQ_KEEPALIVE_EXEC_HELPER") == "1" {
		fmt.Print("go-binary-ok")
		return
	}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("pinned execution is supported on Darwin and Linux")
	}
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := Capture(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd, cleanup, err := CommandContext(context.Background(), identity, "-test.run=TestCommandContextExecutesGoBinarySnapshot")
	if err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "AMQ_KEEPALIVE_EXEC_HELPER=1")
	output, err := cmd.Output()
	cleanup()
	if err != nil || !strings.Contains(string(output), "go-binary-ok") {
		t.Fatalf("Go binary snapshot output=%q err=%v", output, err)
	}
}

func writeTestExecutable(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tool")
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
