package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var _ = func(first, second Entry) bool { return first == second }

func TestStoreUpsertRoundTripAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".amq-keepalive", "registry.json")
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	store := New(path)
	store.Now = func() time.Time { return now }

	entry, err := store.Upsert(Entry{
		Root:           "/tmp/amq-root",
		Agent:          "codex",
		Adapter:        "file",
		Target:         "/tmp/inbox.txt",
		BaselineFile:   "/tmp/wake-baseline.json",
		BaselineDigest: "sha256:abc",
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if entry.ID == "" {
		t.Fatal("entry ID is empty")
	}
	if entry.State != StateAttached {
		t.Fatalf("state = %q, want %q", entry.State, StateAttached)
	}
	if !entry.LastAttach.Equal(now) {
		t.Fatalf("LastAttach = %v, want %v", entry.LastAttach, now)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.SchemaVersion != SchemaVersion {
		t.Fatalf("schema = %d, want %d", loaded.SchemaVersion, SchemaVersion)
	}
	if len(loaded.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(loaded.Entries))
	}
	if loaded.Entries[0].ID != entry.ID {
		t.Fatalf("loaded ID = %q, want %q", loaded.Entries[0].ID, entry.ID)
	}
	if loaded.Entries[0].BaselineFile != entry.BaselineFile || loaded.Entries[0].BaselineDigest != entry.BaselineDigest {
		t.Fatalf("baseline binding did not round trip: %+v", loaded.Entries[0])
	}

	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("dir mode = %v, want 0700", got)
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat registry: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("file mode = %v, want 0600", got)
	}
}

func TestStoreForget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	store := New(path)
	entry, err := store.Upsert(Entry{
		Root:    "/tmp/amq-root",
		Agent:   "codex",
		Adapter: "file",
		Target:  "/tmp/inbox.txt",
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	removed, err := store.Forget(entry.ID)
	if err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if !removed {
		t.Fatal("Forget() removed = false, want true")
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(loaded.Entries))
	}
}

func TestStoreForgetManyRemovesRequestedEntriesInOneSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	store := New(path)
	var ids []string
	for _, agent := range []string{"codex", "claude", "observer"} {
		entry, err := store.Upsert(Entry{Root: "/tmp/amq-root", Agent: agent, Adapter: "file", Target: "/tmp/" + agent})
		if err != nil {
			t.Fatalf("Upsert(%s): %v", agent, err)
		}
		ids = append(ids, entry.ID)
	}
	removed, err := store.ForgetMany(ids[:2])
	if err != nil || removed != 2 {
		t.Fatalf("ForgetMany removed=%d err=%v", removed, err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Agent != "observer" {
		t.Fatalf("entries=%#v, want observer only", loaded.Entries)
	}
}

func TestStoreForgetManyRefusesPartialMatchWithoutRemovingAnything(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	store := New(path)
	entry, err := store.Upsert(Entry{Root: "/tmp/amq-root", Agent: "codex", Adapter: "file", Target: "/tmp/codex"})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	removed, err := store.ForgetMany([]string{entry.ID, "missing-id"})
	if err == nil || removed != 0 {
		t.Fatalf("ForgetMany removed=%d err=%v, want refusal", removed, err)
	}
	loaded, loadErr := store.Load()
	if loadErr != nil || len(loaded.Entries) != 1 || loaded.Entries[0].ID != entry.ID {
		t.Fatalf("registry changed after partial-match refusal: entries=%#v err=%v", loaded.Entries, loadErr)
	}
}

func TestStoreRejectsSecondOwnerForSameAdapterTarget(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	target := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	if _, err := store.Upsert(Entry{Root: "/tmp/first", Agent: "codex", Adapter: "cmux", Target: target}); err != nil {
		t.Fatalf("Upsert(first) error = %v", err)
	}
	_, err := store.Upsert(Entry{Root: "/tmp/second", Agent: "claude", Adapter: "cmux", Target: target})
	if !errors.Is(err, ErrTargetOwned) {
		t.Fatalf("Upsert(second) error = %v, want ErrTargetOwned", err)
	}
	loaded, loadErr := store.Load()
	if loadErr != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Root != "/tmp/first" {
		t.Fatalf("registry changed after collision: entries=%#v err=%v", loaded.Entries, loadErr)
	}
}

func TestStoreRejectsCanonicalCmuxTargetOwnedByLegacyLowercaseRow(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	lower := "cmux:surface:f901d722-6789-4bbb-9818-c4e97f20beb3"
	legacy := Entry{
		ID: EntryID("/tmp/first", "codex", "cmux", lower), Root: "/tmp/first", Agent: "codex",
		Adapter: "cmux", Target: lower, State: StateActive,
	}
	if err := store.Save(File{SchemaVersion: SchemaVersion, Entries: []Entry{legacy}}); err != nil {
		t.Fatalf("Save legacy row: %v", err)
	}
	upper := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	_, err := store.Upsert(Entry{Root: "/tmp/second", Agent: "claude", Adapter: "cmux", Target: upper})
	if !errors.Is(err, ErrTargetOwned) {
		t.Fatalf("Upsert(canonical) error = %v, want ErrTargetOwned", err)
	}
	loaded, loadErr := store.Load()
	if loadErr != nil || len(loaded.Entries) != 1 || loaded.Entries[0] != legacy {
		t.Fatalf("legacy registry changed: entries=%#v err=%v", loaded.Entries, loadErr)
	}
}

func TestRegistrationLockWaitHonorsContextCancellation(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- store.WithRegistrationLock(func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	called := false
	err := store.WithRegistrationLockContext(ctx, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatalf("wait error=%v called=%v, want canceled acquisition", err, called)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("holder error: %v", err)
	}
}

func TestStoreReplacePreflightRejectsTargetOwnedByDifferentSession(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	target := "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	if _, err := store.Upsert(Entry{Root: "/tmp/first", Agent: "codex", Adapter: "cmux", Target: target}); err != nil {
		t.Fatalf("Upsert(first) error = %v", err)
	}
	err := store.CheckTargetAvailable(Entry{Root: "/tmp/second", Agent: "codex", Adapter: "cmux", Target: target}, true)
	if !errors.Is(err, ErrTargetOwned) {
		t.Fatalf("CheckTargetAvailable() error = %v, want ErrTargetOwned", err)
	}
}

func TestStoreBatchUpdateCASPreservesConcurrentChangesAndNewEntries(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	first, err := store.Upsert(Entry{Root: "/tmp/first", Agent: "codex", Adapter: "file", Target: "/tmp/first.txt"})
	if err != nil {
		t.Fatalf("Upsert(first): %v", err)
	}
	second, err := store.Upsert(Entry{Root: "/tmp/second", Agent: "codex", Adapter: "file", Target: "/tmp/second.txt"})
	if err != nil {
		t.Fatalf("Upsert(second): %v", err)
	}
	snapshot, err := store.Load()
	if err != nil {
		t.Fatalf("Load snapshot: %v", err)
	}
	firstBefore, _ := findTestEntry(snapshot.Entries, first.ID)
	secondBefore, _ := findTestEntry(snapshot.Entries, second.ID)

	firstConcurrent := firstBefore
	firstConcurrent.LastError = "concurrent reattach won"
	if err := store.UpdateEntry(firstConcurrent); err != nil {
		t.Fatalf("UpdateEntry(concurrent): %v", err)
	}
	third, err := store.Upsert(Entry{Root: "/tmp/third", Agent: "codex", Adapter: "file", Target: "/tmp/third.txt"})
	if err != nil {
		t.Fatalf("Upsert(third): %v", err)
	}
	firstAfter := firstBefore
	firstAfter.State = StateActive
	secondAfter := secondBefore
	secondAfter.State = StateActive
	result, err := store.UpdateEntries([]EntryUpdate{
		{Before: firstBefore, After: firstAfter},
		{Before: secondBefore, After: secondAfter},
	})
	if err != nil {
		t.Fatalf("UpdateEntries: %v", err)
	}
	if result.Updated != 1 || result.Skipped != 1 {
		t.Fatalf("result = %+v, want one update and one stale skip", result)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load final: %v", err)
	}
	gotFirst, ok := findTestEntry(loaded.Entries, first.ID)
	if !ok || gotFirst.LastError != firstConcurrent.LastError || gotFirst.State == StateActive {
		t.Fatalf("concurrent first entry was clobbered: %#v", gotFirst)
	}
	gotSecond, ok := findTestEntry(loaded.Entries, second.ID)
	if !ok || gotSecond.State != StateActive {
		t.Fatalf("second entry was not updated: %#v", gotSecond)
	}
	if _, ok := findTestEntry(loaded.Entries, third.ID); !ok {
		t.Fatalf("concurrently added third entry was lost: %#v", loaded.Entries)
	}
}

func TestStoreForgetIfUnchangedSkipsNewerState(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	entry, err := store.Upsert(Entry{Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/inbox.txt"})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	newer := entry
	newer.LastError = "newer state"
	if err := store.UpdateEntry(newer); err != nil {
		t.Fatalf("UpdateEntry: %v", err)
	}
	removed, err := store.ForgetIfUnchanged(entry)
	if err != nil || removed {
		t.Fatalf("ForgetIfUnchanged removed=%v err=%v, want safe skip", removed, err)
	}
}

func TestStoreDoesNotChmodExistingCustomRegistryDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "custom")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}
	store := New(filepath.Join(dir, "registry.json"))
	_, err := store.Upsert(Entry{
		Root:    "/tmp/amq-root",
		Agent:   "codex",
		Adapter: "file",
		Target:  "/tmp/inbox.txt",
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("dir mode = %v, want existing 0755 preserved", got)
	}
}

func TestStoreReplaceSessionAdapterFailsClosedBeforeMutatingMultipleLiveRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	store := New(path)
	first := Entry{
		ID:      EntryID("/tmp/amq-root", "codex", "file", "/tmp/old-inbox.txt"),
		Root:    "/tmp/amq-root",
		Agent:   "codex",
		Adapter: "file",
		Target:  "/tmp/old-inbox.txt",
		State:   StateActive,
	}
	second := Entry{
		ID:      EntryID("/tmp/amq-root", "codex", "ghostty", "ghostty:terminal:old"),
		Root:    "/tmp/amq-root",
		Agent:   "codex",
		Adapter: "ghostty",
		Target:  "ghostty:terminal:old",
		State:   StateActive,
	}
	keep := Entry{
		ID:      EntryID("/tmp/amq-root", "claude", "file", "/tmp/claude-inbox.txt"),
		Root:    "/tmp/amq-root",
		Agent:   "claude",
		Adapter: "file",
		Target:  "/tmp/claude-inbox.txt",
		State:   StateActive,
	}
	if err := store.Save(File{SchemaVersion: SchemaVersion, Entries: []Entry{first, second, keep}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	_, removed, err := store.ReplaceSessionAdapter(Entry{
		Root:    "/tmp/amq-root",
		Agent:   "codex",
		Adapter: "cmux",
		Target:  "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3",
	})
	if !errors.Is(err, ErrAmbiguousSession) || len(removed) != 0 {
		t.Fatalf("ReplaceSessionAdapter() removed=%#v error=%v, want fail-closed ambiguity", removed, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("registry changed on ambiguous replacement:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestStoreRestoresPreviousRowsOnlyWhileReservationIsUnchanged(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	retired, err := store.Upsert(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/history", State: StateRetired,
		RetiredAt: time.Date(2026, 6, 26, 11, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Upsert retired: %v", err)
	}
	previous, err := store.Upsert(Entry{Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/old"})
	if err != nil {
		t.Fatalf("Upsert previous: %v", err)
	}
	reservation, removed, err := store.ReplaceSessionAdapter(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/new", State: StateAttached,
	})
	if err != nil || len(removed) != 1 || removed[0] != previous {
		t.Fatalf("Replace reservation=%#v removed=%#v err=%v", reservation, removed, err)
	}
	restored, err := store.RestoreSessionAdapterIfUnchanged(reservation, removed)
	if err != nil || !restored {
		t.Fatalf("RestoreSessionAdapterIfUnchanged restored=%v err=%v", restored, err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 2 {
		t.Fatalf("restored entries=%#v err=%v", loaded.Entries, err)
	}
	if got, ok := findTestEntry(loaded.Entries, retired.ID); !ok || got != retired {
		t.Fatalf("retired history changed during restore: got=%#v ok=%v", got, ok)
	}
	if got, ok := findTestEntry(loaded.Entries, previous.ID); !ok || got != previous {
		t.Fatalf("live row was not restored: got=%#v ok=%v", got, ok)
	}

	reservation, removed, err = store.ReplaceSessionAdapter(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/new", State: StateAttached,
	})
	if err != nil {
		t.Fatalf("Replace second reservation: %v", err)
	}
	changed := reservation
	changed.LastError = "supervisor observed reservation"
	if err := store.UpdateEntry(changed); err != nil {
		t.Fatalf("Update reservation: %v", err)
	}
	restored, err = store.RestoreSessionAdapterIfUnchanged(reservation, removed)
	if err != nil || restored {
		t.Fatalf("changed reservation restored=%v err=%v, want safe skip", restored, err)
	}
}

func TestStoreConcurrentUpsertsDoNotLoseEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	store := New(path)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Upsert(Entry{
				Root:    "/tmp/amq-root",
				Agent:   "codex",
				Adapter: "file",
				Target:  filepath.Join("/tmp", "inbox", string(rune('a'+i))),
			})
			if err != nil {
				t.Errorf("Upsert(%d) error = %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Entries) != 20 {
		t.Fatalf("entries = %d, want 20", len(loaded.Entries))
	}
}

func TestStoreConcurrentSameTargetReplacementsConverge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	store := New(path)
	if _, err := store.Upsert(Entry{
		Root:    "/tmp/amq-root",
		Agent:   "codex",
		Adapter: "ghostty",
		Target:  "ghostty:terminal:old",
	}); err != nil {
		t.Fatalf("Upsert(old) error = %v", err)
	}

	const target = "cmux:surface:F901D722-6789-4BBB-9818-C4E97F20BEB3"
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := store.ReplaceSessionAdapter(Entry{
				Root:    "/tmp/amq-root",
				Agent:   "codex",
				Adapter: "cmux",
				Target:  target,
			}); err != nil {
				t.Errorf("ReplaceSessionAdapter() error = %v", err)
			}
		}()
	}
	wg.Wait()

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].Adapter != "cmux" || loaded.Entries[0].Target != target {
		t.Fatalf("entries = %#v, want one converged cmux registration", loaded.Entries)
	}
}

func TestStoreCorruptRegistryReturnsTypedError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store := New(path)

	_, err := store.Load()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load() error = %v, want ErrCorrupt", err)
	}
}

func TestStoreMigratesV1WithSecureBackupAndLegacyFailClosedState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	v1 := []byte(`{"schema_version":1,"entries":[{"id":"entry-1","root":"/tmp/root","agent":"codex","adapter":"file","target":"/tmp/target","state":"active"}]}` + "\n")
	if err := os.WriteFile(path, v1, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := New(path).Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SchemaVersion != 2 || len(loaded.Entries) != 1 || !loaded.Entries[0].LegacyUnbound || loaded.Entries[0].WakeOwnerPresent {
		t.Fatalf("migrated registry=%#v", loaded)
	}
	backup := path + ".v1.bak"
	backupData, err := os.ReadFile(backup)
	if err != nil || !bytes.Equal(backupData, v1) {
		t.Fatalf("backup=%q err=%v", backupData, err)
	}
	if info, err := os.Stat(backup); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("backup mode=%v err=%v", info.Mode().Perm(), err)
	}
	var disk File
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &disk) != nil || disk.SchemaVersion != 2 {
		t.Fatalf("migrated disk=%q err=%v", data, err)
	}
}

func TestStoreLoadPreviewMigratesV1OnlyInMemoryWithoutFilesystemWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	v1 := []byte(`{"schema_version":1,"entries":[{"id":"entry-1","root":"/tmp/root","agent":"codex","adapter":"file","target":"/tmp/target","state":"active","wake_owner_present":true,"wake_binding":{"generation":"old","target_digest":"old"}}]}` + "\n")
	if err := os.WriteFile(path, v1, 0o600); err != nil {
		t.Fatal(err)
	}

	loaded, err := New(path).LoadPreview()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SchemaVersion != SchemaVersion || len(loaded.Entries) != 1 || !loaded.Entries[0].LegacyUnbound || loaded.Entries[0].WakeOwnerPresent || loaded.Entries[0].WakeBinding.Complete() {
		t.Fatalf("preview migration=%#v", loaded)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(onDisk, v1) {
		t.Fatalf("preview mutated registry: data=%q err=%v", onDisk, err)
	}
	for _, forbidden := range []string{path + ".lock", path + ".v1.bak"} {
		if _, err := os.Lstat(forbidden); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("preview created %q: err=%v", forbidden, err)
		}
	}
}

func TestStoreLoadPreviewOfMissingRegistryDoesNotCreateParent(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "missing", "nested")
	loaded, err := New(filepath.Join(parent, "registry.json")).LoadPreview()
	if err != nil || loaded.SchemaVersion != SchemaVersion || len(loaded.Entries) != 0 {
		t.Fatalf("LoadPreview()=%#v err=%v", loaded, err)
	}
	if _, err := os.Lstat(parent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadPreview created parent %q: err=%v", parent, err)
	}
}

func TestStoreAmbiguousV1ReplacementDoesNotMigrateOrCreateBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	v1 := []byte(`{"schema_version":1,"entries":[{"id":"one","root":"/tmp/root","agent":"codex","adapter":"file","target":"/tmp/one","state":"active"},{"id":"two","root":"/tmp/root","agent":"codex","adapter":"file","target":"/tmp/two","state":"active"}]}` + "\n")
	if err := os.WriteFile(path, v1, 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := New(path).ReplaceSessionAdapter(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/new",
	})
	if !errors.Is(err, ErrAmbiguousSession) {
		t.Fatalf("ReplaceSessionAdapter() error=%v, want ErrAmbiguousSession", err)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(onDisk, v1) {
		t.Fatalf("ambiguous replacement migrated v1: data=%q err=%v", onDisk, err)
	}
	if _, err := os.Lstat(path + ".v1.bak"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ambiguous replacement created backup: err=%v", err)
	}
}

func TestStoreV1BackupConcurrentPublicationIsCompleteAndNoOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	store := New(path)
	v1 := []byte(`{"schema_version":1,"entries":[{"id":"entry-1"}]}` + "\n")

	const writers = 16
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.backupV1Registry(v1)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("backupV1Registry() error=%v", err)
		}
	}
	backup, err := os.ReadFile(path + ".v1.bak")
	if err != nil || !bytes.Equal(backup, v1) {
		t.Fatalf("published backup=%q err=%v", backup, err)
	}
	info, err := os.Stat(path + ".v1.bak")
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("backup info=%v err=%v", info, err)
	}
	leftovers, err := filepath.Glob(filepath.Join(dir, ".registry-v1-backup-*.tmp"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("backup temp leftovers=%v err=%v", leftovers, err)
	}
}

func TestStoreV1BackupPublicationFailureLeavesRegistryAndDestinationUntouched(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	v1 := []byte(`{"schema_version":1,"entries":[]}` + "\n")
	if err := os.WriteFile(path, v1, 0o600); err != nil {
		t.Fatal(err)
	}
	backupPath := path + ".v1.bak"
	if err := os.Mkdir(backupPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := New(path).Load(); err == nil || !strings.Contains(err.Error(), "not a secure 0600 regular file") {
		t.Fatalf("Load() error=%v, want secure destination refusal", err)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(onDisk, v1) {
		t.Fatalf("failed backup publication changed registry: data=%q err=%v", onDisk, err)
	}
	info, err := os.Stat(backupPath)
	if err != nil || !info.IsDir() {
		t.Fatalf("backup destination changed: info=%v err=%v", info, err)
	}
	leftovers, err := filepath.Glob(filepath.Join(dir, ".registry-v1-backup-*.tmp"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("backup temp leftovers=%v err=%v", leftovers, err)
	}
}

func TestStoreRefusesV1MigrationWhenExistingBackupDoesNotMatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	v1 := []byte(`{"schema_version":1,"entries":[]}` + "\n")
	if err := os.WriteFile(path, v1, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".v1.bak", []byte("different\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(path).Load(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("Load() error=%v, want mismatched backup refusal", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, v1) {
		t.Fatalf("v1 registry changed after refusal: data=%q err=%v", data, err)
	}
}

func TestReplaceSessionAdapterPersistsComparableReattachTransition(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	old, err := store.Upsert(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/old",
		WakeOwnerPresent: true,
		WakeOwner:        WakeOwner{PID: 42, ProcessStart: "start-1", BootID: "boot-1", SessionID: 42},
		WakeBinding:      WakeBinding{Generation: "generation-1", TargetDigest: "sha256:target-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	next, removed, err := store.ReplaceSessionAdapter(Entry{Root: old.Root, Agent: old.Agent, Adapter: "file", Target: "/tmp/new"})
	if err != nil || len(removed) != 1 {
		t.Fatalf("next=%#v removed=%#v err=%v", next, removed, err)
	}
	transition := next.Transition
	if transition.Phase != TransitionReserved || transition.Revision != 1 || transition.OldID != old.ID ||
		transition.OldOwner != old.WakeOwner || transition.OldBinding != old.WakeBinding {
		t.Fatalf("transition=%#v old=%#v", transition, old)
	}
	replacement, removedAgain, err := store.ReplaceSessionAdapter(Entry{
		Root: old.Root, Agent: old.Agent, Adapter: "file", Target: "/tmp/newer",
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 84, ProcessStart: "start-2", BootID: "boot-1"},
	})
	if err != nil || len(removedAgain) != 1 {
		t.Fatalf("replacement=%#v removed=%#v err=%v", replacement, removedAgain, err)
	}
	if replacement.Transition.Revision != 2 || replacement.Transition.OldID != old.ID ||
		replacement.Transition.OldBinding != old.WakeBinding {
		t.Fatalf("nested transition lost original exact wake: %#v", replacement.Transition)
	}
}

func TestRetiredEntryDoesNotBlockImmediateTargetReplacement(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	if _, err := store.Upsert(Entry{Root: "/tmp/old", Agent: "codex", Adapter: "file", Target: "/tmp/shared", State: StateRetired}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upsert(Entry{Root: "/tmp/new", Agent: "claude", Adapter: "file", Target: "/tmp/shared"}); err != nil {
		t.Fatalf("retired row blocked replacement: %v", err)
	}
}

func TestReplaceSessionAdapterIgnoresRetiredHistoryWhenSelectingLiveTransition(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	retired, err := store.Upsert(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/retired", State: StateRetired,
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 10, ProcessStart: "old", BootID: "boot"},
		WakeBinding: WakeBinding{Generation: "retired-generation", TargetDigest: "retired-digest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	live, err := store.Upsert(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/live", State: StateActive,
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 20, ProcessStart: "live", BootID: "boot"},
		WakeBinding: WakeBinding{Generation: "live-generation", TargetDigest: "live-digest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	next, removed, err := store.ReplaceSessionAdapter(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/next",
	})
	if err != nil || len(removed) != 1 || removed[0] != live {
		t.Fatalf("next=%#v removed=%#v err=%v", next, removed, err)
	}
	if next.Transition.OldID != live.ID || next.Transition.OldID == retired.ID || next.Transition.OldBinding != live.WakeBinding {
		t.Fatalf("transition=%#v retired=%#v live=%#v", next.Transition, retired, live)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := findTestEntry(loaded.Entries, retired.ID); !ok || got != retired {
		t.Fatalf("retired history was not preserved: got=%#v ok=%v entries=%#v", got, ok, loaded.Entries)
	}
}

func TestReplaceSessionAdapterArchivesSameTupleRetiredHistoryWithStableIdentity(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	retired, err := store.Upsert(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/same", State: StateRetired,
		RetiredAt:   time.Date(2026, 6, 26, 11, 0, 0, 0, time.UTC),
		WakeBinding: WakeBinding{Generation: "retired-generation", TargetDigest: "retired-digest"},
	})
	if err != nil {
		t.Fatal(err)
	}
	next, removed, err := store.ReplaceSessionAdapter(Entry{
		Root: retired.Root, Agent: retired.Agent, Adapter: retired.Adapter, Target: retired.Target,
	})
	if err != nil || len(removed) != 0 {
		t.Fatalf("ReplaceSessionAdapter() next=%#v removed=%#v err=%v", next, removed, err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 2 {
		t.Fatalf("Load() entries=%#v err=%v", loaded.Entries, err)
	}
	var history Entry
	for _, entry := range loaded.Entries {
		if entry.State == StateRetired {
			history = entry
		}
	}
	if history.ID == "" || history.ID == retired.ID || !strings.HasPrefix(history.ID, "retired-") {
		t.Fatalf("retired archival identity=%q original=%q", history.ID, retired.ID)
	}
	wantHistory := retired
	wantHistory.ID = history.ID
	if history != wantHistory {
		t.Fatalf("archived history changed beyond identity:\ngot=%#v\nwant=%#v", history, wantHistory)
	}

	archiveID := history.ID
	_, removed, err = store.ReplaceSessionAdapter(Entry{
		Root: retired.Root, Agent: retired.Agent, Adapter: retired.Adapter, Target: retired.Target,
	})
	if err != nil || len(removed) != 1 || removed[0].ID != next.ID {
		t.Fatalf("second replacement removed=%#v err=%v", removed, err)
	}
	loaded, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := findTestEntry(loaded.Entries, archiveID); !ok || got != history {
		t.Fatalf("archival identity was not immutable: got=%#v ok=%v", got, ok)
	}

	newerHistory := history
	newerHistory.LastGCDecision = "retained"
	if err := store.UpdateEntry(newerHistory); err != nil {
		t.Fatal(err)
	}
	if removed, err := store.ForgetIfUnchanged(history); err != nil || removed {
		t.Fatalf("stale archival CAS removed=%v err=%v", removed, err)
	}
	if removed, err := store.ForgetIfUnchanged(newerHistory); err != nil || !removed {
		t.Fatalf("exact archival CAS removed=%v err=%v", removed, err)
	}
}

func TestStoreUpsertPreservesRetiredHistoryOnSameTupleCollision(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	retired, err := store.Upsert(Entry{
		Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/same", State: StateRetired,
		RetiredAt: time.Date(2026, 6, 26, 11, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	live, err := store.Upsert(Entry{
		Root: retired.Root, Agent: retired.Agent, Adapter: retired.Adapter, Target: retired.Target, State: StateAttached,
	})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.Entries) != 2 {
		t.Fatalf("entries=%#v err=%v", loaded.Entries, err)
	}
	if got, ok := findTestEntry(loaded.Entries, live.ID); !ok || got.State == StateRetired {
		t.Fatalf("live row missing after collision: got=%#v ok=%v", got, ok)
	}
	retiredCount := 0
	for _, entry := range loaded.Entries {
		if entry.State == StateRetired {
			retiredCount++
			if entry.ID == retired.ID {
				t.Fatalf("retired row kept colliding logical ID: %#v", entry)
			}
		}
	}
	if retiredCount != 1 {
		t.Fatalf("retired history count=%d entries=%#v", retiredCount, loaded.Entries)
	}
}

func TestStoreGCRootBatchFreezesMembershipAndPreservesWindowMarkers(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	entries := []Entry{
		{ID: "first", Root: "/tmp/root", Agent: "first", Adapter: "file", Target: "/tmp/first", State: StateActive, WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 1, ProcessStart: "one", BootID: "boot"}, WakeBinding: WakeBinding{Generation: "g1", TargetDigest: "d1"}},
		{ID: "second", Root: "/tmp/root", Agent: "second", Adapter: "file", Target: "/tmp/second", State: StateActive, WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 2, ProcessStart: "two", BootID: "boot"}, WakeBinding: WakeBinding{Generation: "g2", TargetDigest: "d2"}},
	}
	if err := store.Save(File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	batch := GCRootBatch{ID: "batch-1", CanonicalRoot: "/tmp/root", StartedAt: now, Phase: GCRootBatchPreflight}
	for _, entry := range entries {
		batch.Members = append(batch.Members, GCRootBatchMember{
			EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
			WakeOwnerPresent: entry.WakeOwnerPresent, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
		})
	}
	started, err := store.StartGCRootBatch(batch, entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(started.GCRootBatches) != 1 {
		t.Fatalf("started batches=%#v", started.GCRootBatches)
	}
	for _, entry := range started.Entries {
		if !entry.LastGCRootBatchAt.Equal(now) {
			t.Fatalf("entry %q marker=%s", entry.ID, entry.LastGCRootBatchAt)
		}
	}
	if advanced, err := store.AdvanceGCRootBatch(batch.ID, GCRootBatchPreflight, GCRootBatchRetiring); err != nil || !advanced {
		t.Fatalf("advanced=%v err=%v", advanced, err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded.GCRootBatches) != 1 || loaded.GCRootBatches[0].Phase != GCRootBatchRetiring {
		t.Fatalf("advanced file=%#v err=%v", loaded, err)
	}
	if finished, err := store.FinishGCRootBatch(batch.ID); err != nil || !finished {
		t.Fatalf("finished=%v err=%v", finished, err)
	}
	loaded, err = store.Load()
	if err != nil || len(loaded.GCRootBatches) != 0 {
		t.Fatalf("finished file=%#v err=%v", loaded, err)
	}
	for _, entry := range loaded.Entries {
		if !entry.LastGCRootBatchAt.Equal(now) {
			t.Fatalf("finish erased rolling marker for %q", entry.ID)
		}
	}
}

func TestAbandonGCRootBatchIsAtomicAndQuarantinesUnresolvedMembers(t *testing.T) {
	now := time.Date(2026, 7, 22, 14, 0, 0, 0, time.UTC)
	root, err := canonicalRegistryRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	entries := []Entry{
		{ID: "retired", Root: root, Agent: "codex", Adapter: "file", Target: filepath.Join(root, "codex"), State: StateActive, WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 1, ProcessStart: "one", BootID: "boot"}, WakeBinding: WakeBinding{Generation: "g1", TargetDigest: "d1"}},
		{ID: "unresolved", Root: root, Agent: "claude", Adapter: "file", Target: filepath.Join(root, "claude"), State: StateActive, WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 2, ProcessStart: "two", BootID: "boot"}, WakeBinding: WakeBinding{Generation: "g2", TargetDigest: "d2"}},
	}
	if err := store.Save(File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	batch := GCRootBatch{ID: "batch-abandon", CanonicalRoot: root, StartedAt: now, Phase: GCRootBatchPreflight}
	for _, entry := range entries {
		batch.Members = append(batch.Members, GCRootBatchMember{
			EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
			WakeOwnerPresent: true, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
		})
	}
	if _, err := store.StartGCRootBatch(batch, entries); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceGCRootBatch(batch.ID, GCRootBatchPreflight, GCRootBatchRetiring); err != nil {
		t.Fatal(err)
	}
	batch.Phase = GCRootBatchRetiring
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	retired := loaded.Entries[0]
	unresolved := loaded.Entries[1]
	if retired.ID != "retired" {
		retired, unresolved = unresolved, retired
	}
	retiredAfter := retired
	retiredAfter.State = StateRetired
	retiredAfter.RetiredAt = now
	retiredAfter.RetirementOutcome = "already_retired"
	retiredAfter.RetirementReason = "tombstone_match"
	outcomes := []GCRootBatchAbandonOutcome{
		{Before: retired, After: retiredAfter},
		{Before: unresolved, After: unresolved, Quarantine: true},
	}

	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	originalSave := saveAbandonedGCRootBatch
	saveAbandonedGCRootBatch = func(*Store, File) error { return errors.New("injected save failure") }
	_, abandonErr := store.AbandonGCRootBatch(batch, outcomes, now, "operator confirmed stuck batch")
	saveAbandonedGCRootBatch = originalSave
	if abandonErr == nil || !strings.Contains(abandonErr.Error(), "injected save failure") {
		t.Fatalf("abandon error=%v", abandonErr)
	}
	afterFailure, err := os.ReadFile(store.Path)
	if err != nil || !bytes.Equal(afterFailure, before) {
		t.Fatalf("registry changed after failed atomic save: err=%v", err)
	}

	result, err := store.AbandonGCRootBatch(batch, outcomes, now, "operator confirmed stuck batch")
	if err != nil || len(result.Retired) != 1 || len(result.Quarantined) != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	final, err := store.Load()
	if err != nil || len(final.GCRootBatches) != 0 {
		t.Fatalf("final batches=%#v err=%v", final.GCRootBatches, err)
	}
	for _, entry := range final.Entries {
		switch entry.ID {
		case "retired":
			if entry.State != StateRetired || !entry.GCQuarantinedAt.IsZero() {
				t.Fatalf("retired outcome=%#v", entry)
			}
		case "unresolved":
			if entry.State == StateRetired || !entry.GCQuarantinedAt.Equal(now) || entry.GCQuarantineReason == "" {
				t.Fatalf("quarantined outcome=%#v", entry)
			}
		}
	}
}

func TestStoreGCRootBatchRejectsStaleOrOversizedMembership(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	entry := Entry{
		ID: "entry", Root: "/tmp/root", Agent: "codex", Adapter: "file", Target: "/tmp/target", State: StateActive,
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 1, ProcessStart: "start", BootID: "boot"},
		WakeBinding: WakeBinding{Generation: "generation", TargetDigest: "digest"},
	}
	if err := store.Save(File{Entries: []Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	member := GCRootBatchMember{
		EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
		WakeOwnerPresent: entry.WakeOwnerPresent, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
	}
	batch := GCRootBatch{ID: "batch", CanonicalRoot: entry.Root, StartedAt: time.Now().UTC(), Phase: GCRootBatchPreflight, Members: []GCRootBatchMember{member}}
	stale := entry
	stale.Target = "/tmp/changed"
	if _, err := store.StartGCRootBatch(batch, []Entry{stale}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("stale membership error=%v", err)
	}
	oversized := batch
	oversized.ID = "oversized"
	oversized.Members = make([]GCRootBatchMember, MaxGCRootBatchMembers+1)
	for index := range oversized.Members {
		oversized.Members[index] = member
		oversized.Members[index].EntryID = fmt.Sprintf("entry-%d", index)
	}
	if _, err := store.StartGCRootBatch(oversized, nil); err == nil || !strings.Contains(err.Error(), "hard maximum") {
		t.Fatalf("oversized membership error=%v", err)
	}
}

func TestStoreLoadRejectsMalformedPersistedGCRootBatches(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalRegistryRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{
		ID: "entry", Root: root, Agent: "codex", Adapter: "file", Target: filepath.Join(root, "target"), State: StateActive,
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 1, ProcessStart: "start", BootID: "boot"},
		WakeBinding: WakeBinding{Generation: "generation", TargetDigest: "digest"}, LastGCRootBatchAt: now,
	}
	member := GCRootBatchMember{
		EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
		WakeOwnerPresent: true, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
	}
	valid := File{
		SchemaVersion: SchemaVersion, Entries: []Entry{entry},
		GCRootAttempts: []GCRootAttempt{{CanonicalRoot: canonical, StartedAt: now}},
		GCRootBatches:  []GCRootBatch{{ID: "batch", CanonicalRoot: canonical, StartedAt: now, Phase: GCRootBatchRetiring, Members: []GCRootBatchMember{member}}},
	}
	cases := []struct {
		name   string
		mutate func(*File)
		want   string
	}{
		{name: "unknown phase", mutate: func(file *File) { file.GCRootBatches[0].Phase = "unknown" }, want: "invalid phase"},
		{name: "oversized", mutate: func(file *File) {
			for index := 1; index <= MaxGCRootBatchMembers; index++ {
				copy := member
				copy.EntryID = fmt.Sprintf("entry-%d", index)
				copy.Agent = fmt.Sprintf("agent-%d", index)
				file.GCRootBatches[0].Members = append(file.GCRootBatches[0].Members, copy)
			}
		}, want: "hard maximum"},
		{name: "wrong canonical root", mutate: func(file *File) { file.GCRootBatches[0].Members[0].Root = filepath.Join(dir, "other") }, want: "outside canonical root"},
		{name: "weak binding", mutate: func(file *File) { file.GCRootBatches[0].Members[0].WakeBinding = WakeBinding{} }, want: "strong owner-bound"},
		{name: "missing attempt", mutate: func(file *File) { file.GCRootAttempts = nil }, want: "lacks its durable"},
		{name: "omitted listener", mutate: func(file *File) {
			other := entry
			other.ID, other.Agent, other.Target = "other", "claude", filepath.Join(root, "other")
			file.Entries = append(file.Entries, other)
		}, want: "omits current listener"},
		{name: "transition activated after freeze", mutate: func(file *File) {
			file.Entries[0].Transition = ReattachTransition{Phase: TransitionReserved}
		}, want: "not transition-free"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			file := valid
			file.Entries = append([]Entry(nil), valid.Entries...)
			file.GCRootAttempts = append([]GCRootAttempt(nil), valid.GCRootAttempts...)
			file.GCRootBatches = append([]GCRootBatch(nil), valid.GCRootBatches...)
			file.GCRootBatches[0].Members = append([]GCRootBatchMember(nil), valid.GCRootBatches[0].Members...)
			test.mutate(&file)
			path := filepath.Join(t.TempDir(), "registry.json")
			data, err := json.Marshal(file)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := New(path).Load(); err == nil || !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load error=%v, want corrupt containing %q", err, test.want)
			}
		})
	}
}

func TestValidateGCRootBatchRejectsSafetyInvariantMatrix(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	root, err := canonicalRegistryRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	member := GCRootBatchMember{
		EntryID: "entry", Root: root, Agent: "codex", Adapter: "file", Target: filepath.Join(root, "target"),
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 1, ProcessStart: "start", BootID: "boot"},
		WakeBinding: WakeBinding{Generation: "generation", TargetDigest: "digest"},
	}
	valid := GCRootBatch{
		ID: "batch", CanonicalRoot: root, StartedAt: now, Phase: GCRootBatchPreflight,
		Members: []GCRootBatchMember{member},
	}
	tests := []struct {
		name   string
		mutate func(*GCRootBatch)
		want   string
	}{
		{name: "missing identity", mutate: func(batch *GCRootBatch) { batch.ID = "" }, want: "required"},
		{name: "invalid phase", mutate: func(batch *GCRootBatch) { batch.Phase = "unknown" }, want: "invalid phase"},
		{name: "empty membership", mutate: func(batch *GCRootBatch) { batch.Members = nil }, want: "no members"},
		{name: "noncanonical root", mutate: func(batch *GCRootBatch) { batch.CanonicalRoot += "/." }, want: "canonical root"},
		{name: "incomplete member", mutate: func(batch *GCRootBatch) { batch.Members[0].Target = "" }, want: "incomplete member"},
		{name: "duplicate member", mutate: func(batch *GCRootBatch) { batch.Members = append(batch.Members, batch.Members[0]) }, want: "duplicate member"},
		{name: "member outside root", mutate: func(batch *GCRootBatch) { batch.Members[0].Root = t.TempDir() }, want: "outside canonical root"},
		{name: "weak owner", mutate: func(batch *GCRootBatch) { batch.Members[0].WakeOwnerPresent = false }, want: "strong owner-bound"},
		{name: "duplicate agent", mutate: func(batch *GCRootBatch) {
			other := batch.Members[0]
			other.EntryID = "other"
			other.Target = filepath.Join(root, "other")
			batch.Members = append(batch.Members, other)
		}, want: "multiple live members"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := valid
			batch.Members = append([]GCRootBatchMember(nil), valid.Members...)
			test.mutate(&batch)
			if err := validateGCRootBatch(batch); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateGCRootBatch error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateRegistryFileRejectsCoordinatorCorruptionMatrix(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	root, err := canonicalRegistryRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{
		ID: "entry", Root: root, Agent: "codex", Adapter: "file", Target: filepath.Join(root, "target"), State: StateActive,
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 1, ProcessStart: "start", BootID: "boot"},
		WakeBinding: WakeBinding{Generation: "generation", TargetDigest: "digest"}, LastGCRootBatchAt: now,
	}
	member := GCRootBatchMember{
		EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
		WakeOwnerPresent: entry.WakeOwnerPresent, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
	}
	valid := File{
		Entries:        []Entry{entry},
		GCRootAttempts: []GCRootAttempt{{CanonicalRoot: root, StartedAt: now}},
		GCRootBatches: []GCRootBatch{{
			ID: "batch", CanonicalRoot: root, StartedAt: now, Phase: GCRootBatchRetiring,
			Members: []GCRootBatchMember{member},
		}},
	}
	tests := []struct {
		name   string
		mutate func(*File)
		want   string
	}{
		{name: "multiple active batches", mutate: func(file *File) { file.GCRootBatches = append(file.GCRootBatches, file.GCRootBatches[0]) }, want: "active GC root batches"},
		{name: "too many attempts", mutate: func(file *File) {
			file.GCRootBatches = nil
			file.GCRootAttempts = make([]GCRootAttempt, MaxGCRootAttempts+1)
		}, want: "hard maximum"},
		{name: "invalid attempt", mutate: func(file *File) { file.GCRootAttempts[0].StartedAt = time.Time{} }, want: "invalid GC root attempt"},
		{name: "duplicate attempt", mutate: func(file *File) { file.GCRootAttempts = append(file.GCRootAttempts, file.GCRootAttempts[0]) }, want: "duplicate GC root attempt"},
		{name: "duplicate entry", mutate: func(file *File) {
			file.GCRootBatches = nil
			file.GCRootAttempts = nil
			file.Entries = append(file.Entries, file.Entries[0])
		}, want: "duplicate entry id"},
		{name: "member row mismatch", mutate: func(file *File) { file.Entries[0].Target = filepath.Join(root, "changed") }, want: "does not match"},
		{name: "active row has invalid root", mutate: func(file *File) {
			other := entry
			other.ID, other.Agent, other.Target, other.Root = "other", "claude", filepath.Join(root, "other"), ""
			file.Entries = append(file.Entries, other)
		}, want: "root is empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := valid
			file.Entries = append([]Entry(nil), valid.Entries...)
			file.GCRootAttempts = append([]GCRootAttempt(nil), valid.GCRootAttempts...)
			file.GCRootBatches = append([]GCRootBatch(nil), valid.GCRootBatches...)
			file.GCRootBatches[0].Members = append([]GCRootBatchMember(nil), valid.GCRootBatches[0].Members...)
			test.mutate(&file)
			if err := validateRegistryFile(file); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateRegistryFile error=%v, want %q", err, test.want)
			}
		})
	}

	retired := entry
	retired.ID, retired.Agent, retired.Target, retired.State = "retired", "claude", filepath.Join(root, "retired"), StateRetired
	withRetiredHistory := valid
	withRetiredHistory.Entries = append([]Entry{entry}, retired)
	if err := validateRegistryFile(withRetiredHistory); err != nil {
		t.Fatalf("retired history outside frozen membership rejected: %v", err)
	}
}

func TestAppendGCRootAttemptEnforcesRollingDistinctRootLimit(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	existing := []GCRootAttempt{
		{CanonicalRoot: "/tmp/expired", StartedAt: now.Add(-GCRootAttemptWindow - time.Second)},
		{CanonicalRoot: "/tmp/z", StartedAt: now},
	}
	next, err := appendGCRootAttempt(existing, "/tmp/a", now)
	if err != nil || len(next) != 2 || next[0].CanonicalRoot != "/tmp/a" || next[1].CanonicalRoot != "/tmp/z" {
		t.Fatalf("next=%#v err=%v", next, err)
	}
	if _, err := appendGCRootAttempt(next, "/tmp/a", now.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("duplicate attempt error=%v", err)
	}
	full := make([]GCRootAttempt, MaxGCRootAttempts)
	for index := range full {
		full[index] = GCRootAttempt{CanonicalRoot: fmt.Sprintf("/tmp/root-%d", index), StartedAt: now}
	}
	if _, err := appendGCRootAttempt(full, "/tmp/overflow", now); err == nil || !strings.Contains(err.Error(), "hard maximum") {
		t.Fatalf("overflow attempt error=%v", err)
	}
}

func TestStoreGCRootCoordinatorInputCASAndReplayGuards(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	root, err := canonicalRegistryRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{
		ID: "entry", Root: root, Agent: "codex", Adapter: "file", Target: filepath.Join(root, "target"), State: StateActive,
		WakeOwnerPresent: true, WakeOwner: WakeOwner{PID: 1, ProcessStart: "start", BootID: "boot"},
		WakeBinding: WakeBinding{Generation: "generation", TargetDigest: "digest"},
	}
	member := GCRootBatchMember{
		EntryID: entry.ID, Root: entry.Root, Agent: entry.Agent, Adapter: entry.Adapter, Target: entry.Target,
		WakeOwnerPresent: entry.WakeOwnerPresent, WakeOwner: entry.WakeOwner, WakeBinding: entry.WakeBinding,
	}
	batch := GCRootBatch{
		ID: "batch", CanonicalRoot: root, StartedAt: now, Phase: GCRootBatchPreflight,
		Members: []GCRootBatchMember{member},
	}
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	if err := store.Save(File{Entries: []Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartGCRootBatch(batch, nil); err == nil || !strings.Contains(err.Error(), "expected 0 rows") {
		t.Fatalf("expected-count error=%v", err)
	}
	emptyID := entry
	emptyID.ID = ""
	if _, err := store.StartGCRootBatch(batch, []Entry{emptyID}); err == nil || !strings.Contains(err.Error(), "entry id is required") {
		t.Fatalf("empty expected id error=%v", err)
	}
	secondMember := member
	secondMember.EntryID, secondMember.Agent, secondMember.Target = "second", "claude", filepath.Join(root, "second")
	twoMemberBatch := batch
	twoMemberBatch.Members = []GCRootBatchMember{member, secondMember}
	if _, err := store.StartGCRootBatch(twoMemberBatch, []Entry{entry, entry}); err == nil || !strings.Contains(err.Error(), "duplicate expected") {
		t.Fatalf("duplicate expected error=%v", err)
	}
	mismatch := batch
	mismatch.Members = append([]GCRootBatchMember(nil), batch.Members...)
	mismatch.Members[0].Target = filepath.Join(root, "other")
	if _, err := store.StartGCRootBatch(mismatch, []Entry{entry}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("member mismatch error=%v", err)
	}
	stale := entry
	stale.Target = filepath.Join(root, "stale")
	staleBatch := batch
	staleBatch.Members = append([]GCRootBatchMember(nil), batch.Members...)
	staleBatch.Members[0].Target = stale.Target
	if _, err := store.StartGCRootBatch(staleBatch, []Entry{stale}); err == nil || !strings.Contains(err.Error(), "changed before membership") {
		t.Fatalf("stale membership CAS error=%v", err)
	}
	if _, err := store.StartGCRootBatch(batch, []Entry{entry}); err != nil {
		t.Fatalf("start valid batch: %v", err)
	}
	if _, err := store.StartGCRootBatch(batch, []Entry{entry}); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("second active batch error=%v", err)
	}
	if advanced, err := store.AdvanceGCRootBatch(batch.ID, GCRootBatchPreflight, GCRootBatchRetiring); err != nil || !advanced {
		t.Fatalf("advance=%v err=%v", advanced, err)
	}
	if advanced, err := store.AdvanceGCRootBatch(batch.ID, GCRootBatchPreflight, GCRootBatchRetiring); err != nil || advanced {
		t.Fatalf("idempotent advance=%v err=%v", advanced, err)
	}
	if _, err := store.AdvanceGCRootBatch(batch.ID, GCRootBatchPreflight, "done"); err == nil || !strings.Contains(err.Error(), "phase is") {
		t.Fatalf("wrong-phase advance error=%v", err)
	}
	if _, err := store.AdvanceGCRootBatch("missing", GCRootBatchPreflight, GCRootBatchRetiring); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing advance error=%v", err)
	}
	if finished, err := store.FinishGCRootBatch(batch.ID); err != nil || !finished {
		t.Fatalf("finish=%v err=%v", finished, err)
	}
	if finished, err := store.FinishGCRootBatch("missing"); err != nil || finished {
		t.Fatalf("missing finish=%v err=%v", finished, err)
	}
}

func TestStoreRecordGCRootAttemptValidatesUpdatesAndWindow(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	root, err := canonicalRegistryRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entry := Entry{ID: "entry", Root: root, Agent: "codex", Adapter: "file", Target: filepath.Join(root, "target"), State: StateActive}
	store := New(filepath.Join(t.TempDir(), "registry.json"))
	if err := store.Save(File{Entries: []Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordGCRootAttempt("", now, nil); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("empty root error=%v", err)
	}
	wrongID := EntryUpdate{Before: entry, After: entry}
	wrongID.After.ID = "other"
	if _, err := store.RecordGCRootAttempt(root, now, []EntryUpdate{wrongID}); err == nil || !strings.Contains(err.Error(), "unchanged entry id") {
		t.Fatalf("changed id error=%v", err)
	}
	validUpdate := EntryUpdate{Before: entry, After: entry}
	validUpdate.After.LastGCRootBatchAt = now
	if _, err := store.RecordGCRootAttempt(root, now, []EntryUpdate{validUpdate, validUpdate}); err == nil || !strings.Contains(err.Error(), "duplicate update") {
		t.Fatalf("duplicate update error=%v", err)
	}
	stale := validUpdate
	stale.Before.Target = filepath.Join(root, "stale")
	stale.After.Target = stale.Before.Target
	if _, err := store.RecordGCRootAttempt(root, now, []EntryUpdate{stale}); err == nil || !strings.Contains(err.Error(), "changed before") {
		t.Fatalf("stale update error=%v", err)
	}
	updated, err := store.RecordGCRootAttempt(root, now, []EntryUpdate{validUpdate})
	if err != nil || len(updated.GCRootAttempts) != 1 || !updated.Entries[0].LastGCRootBatchAt.Equal(now) {
		t.Fatalf("updated=%#v err=%v", updated, err)
	}
	if _, err := store.RecordGCRootAttempt(root, now.Add(time.Second), nil); err == nil || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("duplicate rolling root error=%v", err)
	}
}

func findTestEntry(entries []Entry, id string) (Entry, bool) {
	for _, entry := range entries {
		if entry.ID == id {
			return entry, true
		}
	}
	return Entry{}, false
}
