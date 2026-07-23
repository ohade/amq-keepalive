package amq

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWakeReadyCleanupBoundsDirectoryWork(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	fs := &fakeWakeReadyCleanupFS{}
	for i := 0; i < maxWakeReadyCleanupEntries+5; i++ {
		fs.entries = append(fs.entries, &fakeWakeReadyDirEntry{
			name: "wake-stale-" + time.Unix(int64(i), 0).Format("150405.000000000"),
			info: fakeWakeReadyFileInfo{modTime: now.Add(-staleWakeReadyMarkerAge - time.Hour)},
		})
	}

	err := scavengeStaleWakeReadyMarkersWithFS(fs, "/readiness", now)
	if err == nil || !strings.Contains(err.Error(), "128-entry work limit") {
		t.Fatalf("cleanup warning = %v, want bounded-work diagnostic", err)
	}
	if fs.readN != maxWakeReadyCleanupEntries+1 {
		t.Fatalf("ReadDir(%d), want %d", fs.readN, maxWakeReadyCleanupEntries+1)
	}
	if len(fs.removed) != maxWakeReadyCleanupEntries {
		t.Fatalf("removed %d markers, want bounded %d", len(fs.removed), maxWakeReadyCleanupEntries)
	}
	infoCalls := 0
	for _, entry := range fs.entries {
		infoCalls += entry.(*fakeWakeReadyDirEntry).infoCalls
	}
	if infoCalls != maxWakeReadyCleanupEntries {
		t.Fatalf("Info called %d times, want bounded %d", infoCalls, maxWakeReadyCleanupEntries)
	}
}

func TestWakeReadyCleanupAggregatesFilesystemFailures(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name string
		fs   *fakeWakeReadyCleanupFS
		want string
	}{
		{
			name: "open",
			fs:   &fakeWakeReadyCleanupFS{openErr: errors.New("open denied")},
			want: "open denied",
		},
		{
			name: "read",
			fs:   &fakeWakeReadyCleanupFS{readErr: errors.New("read interrupted")},
			want: "read interrupted",
		},
		{
			name: "info",
			fs: &fakeWakeReadyCleanupFS{entries: []os.DirEntry{
				&fakeWakeReadyDirEntry{name: "wake-info", infoErr: errors.New("stat denied")},
			}},
			want: "stat denied",
		},
		{
			name: "remove",
			fs: &fakeWakeReadyCleanupFS{
				entries: []os.DirEntry{&fakeWakeReadyDirEntry{
					name: "wake-remove", info: fakeWakeReadyFileInfo{modTime: now.Add(-staleWakeReadyMarkerAge - time.Hour)},
				}},
				removeErr: map[string]error{"wake-remove": errors.New("remove denied")},
			},
			want: "remove denied",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := scavengeStaleWakeReadyMarkersWithFS(test.fs, "/readiness", now)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("cleanup warning = %v, want %q", err, test.want)
			}
		})
	}

	fs := &fakeWakeReadyCleanupFS{readErr: errors.New("read failed"), closeErr: errors.New("close failed")}
	for i := 0; i < maxWakeReadyCleanupErrors+3; i++ {
		fs.entries = append(fs.entries, &fakeWakeReadyDirEntry{
			name:    "wake-info-" + string(rune('a'+i)),
			infoErr: errors.New("info failed " + string(rune('a'+i))),
		})
	}
	err := scavengeStaleWakeReadyMarkersWithFS(fs, "/readiness", now)
	if err == nil || !strings.Contains(err.Error(), "additional cleanup warning(s) omitted") ||
		!strings.Contains(err.Error(), "read failed") || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("aggregate cleanup warning = %v", err)
	}
}

func TestStartWakeSurfacesCleanupWarningWithoutFailingReadyWake(t *testing.T) {
	cache := t.TempDir()
	t.Setenv("AMQ_KEEPALIVE_CACHE_DIR", cache)
	dir := t.TempDir()
	fakeAMQ := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
previous=""
for arg in "$@"; do
  if [ "$previous" = "-ready-file" ]; then
    printf '{"schema":1,"generation":"generation-warning","target_digest":"sha256:warning"}\n' > "$arg"
  fi
  previous="$arg"
done
`)

	var warnings []error
	cleanupCalls := 0
	cli := NewCLI(fakeAMQ).WithWarningSink(func(err error) { warnings = append(warnings, err) })
	cli.wakeReadyCleanup = func(string, time.Time) error {
		cleanupCalls++
		return errors.New("read readiness directory: injected cleanup failure")
	}
	cli.wakeReadyCleanupNow = func() time.Time { return time.Unix(1_700_000_000, 0) }

	binding, err := cli.StartWake(context.Background(), StartWakeRequest{
		Root: "/tmp/amq-root", Me: "codex", InjectVia: "/tmp/amq-keepalive",
		Adapter: "cmux", Target: "cmux:surface:warning", Timeout: 5 * time.Second,
		Owner: testWakeOwner(),
	})
	if err != nil {
		t.Fatalf("StartWake returned cleanup warning as failure: %v", err)
	}
	if !binding.Complete() {
		t.Fatalf("binding = %#v, want complete ready wake", binding)
	}
	if cleanupCalls != 1 {
		t.Fatalf("cleanup called %d times, want once per StartWake attempt", cleanupCalls)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].Error(), "injected cleanup failure") {
		t.Fatalf("warning sink = %v, want visible cleanup diagnostic", warnings)
	}
}

func TestWithWarningSinkNilRestoresVisibleDefault(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stderr
	os.Stderr = writer
	t.Cleanup(func() {
		os.Stderr = previous
		_ = writer.Close()
		_ = reader.Close()
	})

	cli := NewCLI("amq").WithWarningSink(nil)
	if cli.wakeReadyWarningSink == nil {
		t.Fatal("nil warning sink suppressed cleanup diagnostics")
	}
	cli.wakeReadyWarningSink(errors.New("visible cleanup warning"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "amq-keepalive warning: visible cleanup warning") {
		t.Fatalf("stderr=%q, want visible warning", data)
	}
}

type fakeWakeReadyCleanupFS struct {
	entries   []os.DirEntry
	openErr   error
	readErr   error
	closeErr  error
	removeErr map[string]error
	readN     int
	removed   []string
}

func (f *fakeWakeReadyCleanupFS) Open(string) (wakeReadyCleanupDir, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	return &fakeWakeReadyCleanupDir{fs: f}, nil
}

func (f *fakeWakeReadyCleanupFS) Remove(path string) error {
	name := filepath.Base(path)
	if err := f.removeErr[name]; err != nil {
		return err
	}
	f.removed = append(f.removed, name)
	return nil
}

type fakeWakeReadyCleanupDir struct {
	fs *fakeWakeReadyCleanupFS
}

func (d *fakeWakeReadyCleanupDir) ReadDir(n int) ([]os.DirEntry, error) {
	d.fs.readN = n
	entries := d.fs.entries
	if len(entries) > n {
		entries = entries[:n]
	}
	if d.fs.readErr != nil {
		return entries, d.fs.readErr
	}
	if len(d.fs.entries) < n {
		return entries, io.EOF
	}
	return entries, nil
}

func (d *fakeWakeReadyCleanupDir) Close() error { return d.fs.closeErr }

type fakeWakeReadyDirEntry struct {
	name      string
	dir       bool
	info      os.FileInfo
	infoErr   error
	infoCalls int
}

func (e *fakeWakeReadyDirEntry) Name() string      { return e.name }
func (e *fakeWakeReadyDirEntry) IsDir() bool       { return e.dir }
func (e *fakeWakeReadyDirEntry) Type() os.FileMode { return 0 }
func (e *fakeWakeReadyDirEntry) Info() (os.FileInfo, error) {
	e.infoCalls++
	if e.infoErr != nil {
		return nil, e.infoErr
	}
	return e.info, nil
}

type fakeWakeReadyFileInfo struct {
	modTime time.Time
}

func (fakeWakeReadyFileInfo) Name() string         { return "wake-marker" }
func (fakeWakeReadyFileInfo) Size() int64          { return 0 }
func (fakeWakeReadyFileInfo) Mode() os.FileMode    { return 0o600 }
func (f fakeWakeReadyFileInfo) ModTime() time.Time { return f.modTime }
func (fakeWakeReadyFileInfo) IsDir() bool          { return false }
func (fakeWakeReadyFileInfo) Sys() any             { return nil }
