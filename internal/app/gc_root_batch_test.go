package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ohade/amq-keepalive/internal/amq"
	"github.com/ohade/amq-keepalive/internal/registry"
	"github.com/ohade/amq-keepalive/internal/supervisor"
)

func TestPlanGCRootBatchUsesOldestOwnerGoneThenCanonicalRoot(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	base := t.TempDir()
	policy := supervisor.GCPolicy{OwnerGrace: supervisor.MinOwnerGoneGrace}

	t.Run("oldest owner absence wins before root name", func(t *testing.T) {
		olderRoot := filepath.Join(base, "z-older")
		newerRoot := filepath.Join(base, "a-newer")
		plan, err := planGCRootBatch([]registry.Entry{
			gcRootBatchEntry("newer", newerRoot, now.Add(-10*time.Minute)),
			gcRootBatchEntry("older", olderRoot, now.Add(-20*time.Minute)),
		}, now, policy)
		if err != nil {
			t.Fatal(err)
		}
		want, err := canonicalGCRoot(olderRoot)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Canonical != want {
			t.Fatalf("plan=%#v, want oldest root %q", plan, want)
		}
	})

	t.Run("canonical root breaks equal-time tie", func(t *testing.T) {
		alphaRoot := filepath.Join(base, "a-tied")
		zuluRoot := filepath.Join(base, "z-tied")
		since := now.Add(-20 * time.Minute)
		plan, err := planGCRootBatch([]registry.Entry{
			gcRootBatchEntry("zulu", zuluRoot, since),
			gcRootBatchEntry("alpha", alphaRoot, since),
		}, now, policy)
		if err != nil {
			t.Fatal(err)
		}
		want, err := canonicalGCRoot(alphaRoot)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Canonical != want {
			t.Fatalf("plan=%#v, want canonical tie-break root %q", plan, want)
		}
	})
}

func TestPlanGCRootBatchRefusesMoreThanEightListeners(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	root := filepath.Join(t.TempDir(), "shared-root")
	policy := supervisor.GCPolicy{OwnerGrace: supervisor.MinOwnerGoneGrace}

	for _, test := range []struct {
		name      string
		listeners int
		oversized bool
	}{
		{name: "eight allowed", listeners: supervisor.MaxAgentsPerRootBatch, oversized: false},
		{name: "nine refused", listeners: supervisor.MaxAgentsPerRootBatch + 1, oversized: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			entries := make([]registry.Entry, 0, test.listeners)
			for index := 0; index < test.listeners; index++ {
				entries = append(entries, gcRootBatchEntry(
					fmt.Sprintf("listener-%d", index), root, now.Add(-10*time.Minute),
				))
			}
			plan, err := planGCRootBatch(entries, now, policy)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Canonical == "" || plan.Oversized != test.oversized {
				t.Fatalf("plan=%#v, want oversized=%t", plan, test.oversized)
			}
			if !test.oversized {
				return
			}

			registryPath := filepath.Join(t.TempDir(), "registry.json")
			store := registry.New(registryPath)
			if err := store.Save(registry.File{Entries: entries}); err != nil {
				t.Fatal(err)
			}
			file, err := store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if err := persistGCRootBatchMarker(store, &file, plan, now); err != nil {
				t.Fatal(err)
			}
			persisted, err := store.Load()
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range persisted.Entries {
				if entry.LastGCDecision != supervisor.GCStatusSkipped || entry.LastGCReason != "root_batch_oversized" {
					t.Fatalf("oversized entry %q decision=(%q,%q), want durable refusal", entry.ID, entry.LastGCDecision, entry.LastGCReason)
				}
				if !entry.LastGCRootBatchAt.Equal(now) || !entry.GCBackoffUntil.Equal(now.Add(15*time.Minute)) {
					t.Fatalf("oversized entry %q marker=%s backoff=%s", entry.ID, entry.LastGCRootBatchAt, entry.GCBackoffUntil)
				}
			}
		})
	}

	t.Run("all non-retired listener rows count toward the hard limit", func(t *testing.T) {
		entries := make([]registry.Entry, 0, supervisor.MaxAgentsPerRootBatch+1)
		for index := 0; index < supervisor.MaxAgentsPerRootBatch+1; index++ {
			entry := gcRootBatchEntry(fmt.Sprintf("mixed-listener-%d", index), root, now.Add(-10*time.Minute))
			if index > 0 {
				entry.LegacyUnbound = true
			}
			entries = append(entries, entry)
		}
		plan, err := planGCRootBatch(entries, now, policy)
		if err != nil {
			t.Fatal(err)
		}
		if !plan.Oversized {
			t.Fatalf("plan=%#v for %d total listener rows, want oversized refusal", plan, len(entries))
		}
	})
}

func TestRootBatchWindowSurvivesAppAndStoreRestart(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	policy := supervisor.GCPolicy{OwnerGrace: supervisor.MinOwnerGoneGrace}
	entries := make([]registry.Entry, 0, supervisor.MaxRootsPerGCWindow+1)
	for index := 0; index < supervisor.MaxRootsPerGCWindow+1; index++ {
		entries = append(entries, gcRootBatchEntry(
			fmt.Sprintf("entry-%d", index),
			filepath.Join(dir, fmt.Sprintf("root-%d", index)),
			now.Add(-time.Duration(20-index)*time.Minute),
		))
	}

	store := registry.New(registryPath)
	if err := store.Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	file, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < supervisor.MaxRootsPerGCWindow; pass++ {
		plan, err := planGCRootBatch(file.Entries, now, policy)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Canonical == "" {
			t.Fatalf("pass %d did not select a root", pass)
		}
		if err := persistGCRootBatchMarker(store, &file, plan, now); err != nil {
			t.Fatal(err)
		}
		retireMarkedRootForRootBatchTest(t, store, &file, plan.Canonical, now)
	}

	// A new App and Store simulate the supervisor restarting inside the rolling
	// minute. The durable per-entry markers must still exhaust the five-root cap.
	restarted := App{Now: func() time.Time { return now }}
	restartedStore := registry.New(registryPath)
	reloaded, err := restartedStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planGCRootBatch(reloaded.Entries, restarted.now(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Canonical != "" {
		t.Fatalf("restart selected sixth root inside window: %#v", plan)
	}

	afterWindow := now.Add(supervisor.GCRootWindow + time.Nanosecond)
	plan, err = planGCRootBatch(reloaded.Entries, afterWindow, policy)
	if err != nil {
		t.Fatal(err)
	}
	want, err := canonicalGCRoot(filepath.Join(dir, fmt.Sprintf("root-%d", supervisor.MaxRootsPerGCWindow)))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Canonical != want {
		t.Fatalf("after window plan=%#v, want remaining root %q", plan, want)
	}
}

func TestPersistGCRootBatchMarkerIsDurableBeforeLifecycleAttempt(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	selectedRoot := filepath.Join(dir, "selected")
	otherRoot := filepath.Join(dir, "other")
	file := registry.File{Entries: []registry.Entry{
		gcRootBatchEntry("selected-dead", selectedRoot, now.Add(-20*time.Minute)),
		gcRootBatchEntry("selected-sibling", selectedRoot, now.Add(-10*time.Minute)),
		gcRootBatchEntry("other", otherRoot, now.Add(-5*time.Minute)),
	}}
	store := registry.New(registryPath)
	if err := store.Save(file); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planGCRootBatch(loaded.Entries, now, supervisor.GCPolicy{OwnerGrace: supervisor.MinOwnerGoneGrace})
	if err != nil {
		t.Fatal(err)
	}
	if err := persistGCRootBatchMarker(store, &loaded, plan, now); err != nil {
		t.Fatal(err)
	}

	// This reload represents the first lifecycle callback observing the
	// registry. The marker must already cover every non-retired row in the
	// selected collaboration root, and no row in another root.
	onDisk, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range onDisk.Entries {
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			t.Fatal(err)
		}
		if root == plan.Canonical {
			if !entry.LastGCRootBatchAt.Equal(now) {
				t.Fatalf("selected entry %q marker=%s, want %s", entry.ID, entry.LastGCRootBatchAt, now)
			}
		} else if !entry.LastGCRootBatchAt.IsZero() {
			t.Fatalf("unselected entry %q unexpectedly marked at %s", entry.ID, entry.LastGCRootBatchAt)
		}
	}
}

func TestSuperviseRootBatchLocalMetadataBlockerRecordsAttemptWithoutLifecycle(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "shared-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	entries := []registry.Entry{
		gcRootBatchEntry("00-dead", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("01-live", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("02-legacy", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("03-incomplete", root, now.Add(-20*time.Minute)),
	}
	entries[2].LegacyUnbound = true
	entries[3].WakeBinding = registry.WakeBinding{}
	for _, entry := range entries {
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}

	wake := &gcRootBatchWitnessWake{registryPath: registryPath}
	pass, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second,
		supervisor.GCPolicy{
			AutoGC: true, OwnerGrace: supervisor.MinOwnerGoneGrace,
			RetiredRetention: supervisor.MinRetiredRetention, Timeout: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalGCRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if pass.AttemptedRoot != canonical {
		t.Fatalf("attempted root=%q, want %q", pass.AttemptedRoot, canonical)
	}
	if wake.captured || wake.starts != 0 || len(wake.checks) != 0 || wake.mutations != 0 {
		t.Fatalf("local blocker reached lifecycle: captured=%v starts=%d checks=%d mutations=%d", wake.captured, wake.starts, len(wake.checks), wake.mutations)
	}

	onDisk, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(onDisk.GCRootBatches) != 0 || len(onDisk.GCRootAttempts) != 1 {
		t.Fatalf("local blocker durable state batches=%#v attempts=%#v", onDisk.GCRootBatches, onDisk.GCRootAttempts)
	}
	for _, entry := range onDisk.Entries {
		if entry.State == registry.StateRetired {
			t.Fatalf("mixed-root blocker retired entry %q", entry.ID)
		}
		if !entry.LastGCRootBatchAt.Equal(now) {
			t.Fatalf("mixed-root blocker did not mark entry %q", entry.ID)
		}
	}
}

func TestSuperviseRootBatchPersistsFrozenPreflightAndLiveBlockerPreventsAllMutations(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "shared-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	entries := []registry.Entry{
		gcRootBatchEntry("00-dead", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("01-live", root, now.Add(-20*time.Minute)),
	}
	for _, entry := range entries {
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	wake := &gcRootBatchWitnessWake{registryPath: registryPath}
	_, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if wake.snapshotErr != nil || !wake.captured || len(wake.snapshot.GCRootBatches) != 1 {
		t.Fatalf("first check snapshot=%#v err=%v", wake.snapshot.GCRootBatches, wake.snapshotErr)
	}
	batch := wake.snapshot.GCRootBatches[0]
	if batch.Phase != registry.GCRootBatchPreflight || len(batch.Members) != len(entries) || len(wake.checks) != len(entries) || wake.mutations != 0 {
		t.Fatalf("batch=%#v checks=%d mutations=%d", batch, len(wake.checks), wake.mutations)
	}
	onDisk, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(onDisk.GCRootBatches) != 0 {
		t.Fatalf("live-blocked batch remained active: %#v", onDisk.GCRootBatches)
	}
	for _, entry := range onDisk.Entries {
		if entry.State == registry.StateRetired {
			t.Fatalf("live blocker allowed sibling retirement: %#v", entry)
		}
	}
}

func TestSuperviseRootBatchPersistsRetiringPhaseBeforeFirstMutation(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "shared-root")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	entries := []registry.Entry{
		gcRootBatchEntry("00-dead", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("02-dead", root, now.Add(-20*time.Minute)),
	}
	for _, entry := range entries {
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}

	wake := &gcRootBatchWitnessWake{registryPath: registryPath}
	_, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second,
		supervisor.GCPolicy{
			AutoGC: true, OwnerGrace: supervisor.MinOwnerGoneGrace,
			RetiredRetention: supervisor.MinRetiredRetention, Timeout: time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if wake.snapshotErr != nil || len(wake.snapshot.GCRootBatches) != 1 || wake.snapshot.GCRootBatches[0].Phase != registry.GCRootBatchPreflight {
		t.Fatalf("first check snapshot=%#v err=%v, want durable preflight", wake.snapshot.GCRootBatches, wake.snapshotErr)
	}
	if wake.mutationSnapshotErr != nil || len(wake.mutationSnapshot.GCRootBatches) != 1 ||
		wake.mutationSnapshot.GCRootBatches[0].Phase != registry.GCRootBatchRetiring {
		t.Fatalf("first mutation snapshot=%#v err=%v, want durable retiring phase", wake.mutationSnapshot.GCRootBatches, wake.mutationSnapshotErr)
	}
	if len(wake.checks) != len(entries) || wake.mutations != len(entries) {
		t.Fatalf("checks=%d mutations=%d, want %d each", len(wake.checks), wake.mutations, len(entries))
	}
	onDisk, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(onDisk.GCRootBatches) != 0 {
		t.Fatalf("completed batch remained active: %#v", onDisk.GCRootBatches)
	}
	for _, entry := range onDisk.Entries {
		if entry.State != registry.StateRetired {
			t.Fatalf("entry %q state=%q, want retired", entry.ID, entry.State)
		}
	}
}

func TestCanonicalRootAliasesShareOneBatchAndWindowSlot(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	realRoot := filepath.Join(dir, "real-root")
	aliasRoot := filepath.Join(dir, "alias-root")
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realRoot, aliasRoot); err != nil {
		t.Fatal(err)
	}
	entries := []registry.Entry{
		gcRootBatchEntry("real", realRoot, now.Add(-20*time.Minute)),
		gcRootBatchEntry("alias", aliasRoot, now.Add(-20*time.Minute)),
	}
	plan, err := planGCRootBatch(entries, now, supervisor.GCPolicy{OwnerGrace: supervisor.MinOwnerGoneGrace})
	if err != nil {
		t.Fatal(err)
	}
	wantRoot, err := canonicalGCRoot(realRoot)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Canonical != wantRoot || len(plan.Members) != 2 {
		t.Fatalf("alias plan=%#v, want one canonical root %q with two members", plan, wantRoot)
	}
	if err := registrationGCBatchError(registry.File{GCRootBatches: []registry.GCRootBatch{{
		ID: "frozen", CanonicalRoot: wantRoot,
	}}}, aliasRoot); err == nil {
		t.Fatal("symlink-alias registration was allowed to change frozen batch membership")
	}
	for index := range entries {
		entries[index].LastGCRootBatchAt = now
		entries[index].State = registry.StateRetired
	}
	for index := 0; index < supervisor.MaxRootsPerGCWindow-1; index++ {
		entry := gcRootBatchEntry(fmt.Sprintf("other-%d", index), filepath.Join(dir, fmt.Sprintf("other-root-%d", index)), now.Add(-10*time.Minute))
		entry.LastGCRootBatchAt = now
		entry.State = registry.StateRetired
		entries = append(entries, entry)
	}
	remaining := gcRootBatchEntry("remaining", filepath.Join(dir, "remaining-root"), now.Add(-10*time.Minute))
	entries = append(entries, remaining)
	blocked, err := planGCRootBatch(entries, now, supervisor.GCPolicy{OwnerGrace: supervisor.MinOwnerGoneGrace})
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Canonical != "" {
		t.Fatalf("alias roots consumed more than one window identity: %#v", blocked)
	}
}

func TestRootBatchCrashAfterTombstoneReplaysOnRestart(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := gcRootBatchEntry("dead", root, now.Add(-20*time.Minute))
	if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(dir, "registry.json")
	if err := registry.New(registryPath).Save(registry.File{Entries: []registry.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	tombstones := map[string]bool{}
	crashing := &gcBatchScriptWake{tombstones: tombstones, panicAfterTombstone: true}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
			context.Background(), registryPath, crashing, "/bin/sh", time.Second, gcBatchPolicy(),
		)
	}()
	if recovered == nil || len(crashing.mutations) != 1 {
		t.Fatalf("recovered=%v mutations=%#v, want crash after first AMQ tombstone", recovered, crashing.mutations)
	}
	afterCrash, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(afterCrash.GCRootBatches) != 1 || afterCrash.GCRootBatches[0].Phase != registry.GCRootBatchRetiring || afterCrash.Entries[0].State == registry.StateRetired {
		t.Fatalf("after crash registry=%#v", afterCrash)
	}

	replay := &gcBatchScriptWake{tombstones: tombstones}
	pass, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now.Add(supervisor.GCCatchUpInterval) }}).superviseOnceWithGCState(
		context.Background(), registryPath, replay, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.checks) != 0 || len(replay.mutations) != 1 || pass.PendingGCRoots {
		t.Fatalf("replay checks=%#v mutations=%#v pass=%#v", replay.checks, replay.mutations, pass)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State != registry.StateRetired || loaded.Entries[0].RetirementOutcome != "already_retired" {
		t.Fatalf("replayed registry=%#v", loaded)
	}
}

func TestRootBatchStopsOnFirstMutationErrorAndResumesAtFiveSeconds(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entries := []registry.Entry{
		gcRootBatchEntry("00-first", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("01-second", root, now.Add(-20*time.Minute)),
	}
	for _, entry := range entries {
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	registryPath := filepath.Join(dir, "registry.json")
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	first := &gcBatchScriptWake{tombstones: map[string]bool{}, failFirstMutation: true}
	pass, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, first, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.checks) != 2 || len(first.mutations) != 1 || !pass.PendingGCRoots || nextSuperviseDelay(time.Minute, pass) != supervisor.GCCatchUpInterval {
		t.Fatalf("checks=%d mutations=%d pass=%#v", len(first.checks), len(first.mutations), pass)
	}
	failed, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(failed.GCRootBatches) != 1 || failed.GCRootBatches[0].Phase != registry.GCRootBatchRetiring ||
		!failed.Entries[0].GCBackoffUntil.Equal(now.Add(supervisor.GCCatchUpInterval)) {
		t.Fatalf("failed batch=%#v", failed)
	}

	second := &gcBatchScriptWake{tombstones: first.tombstones}
	pass, err = (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now.Add(supervisor.GCCatchUpInterval) }}).superviseOnceWithGCState(
		context.Background(), registryPath, second, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.checks) != 0 || len(second.mutations) != 2 || pass.PendingGCRoots {
		t.Fatalf("resume checks=%d mutations=%d pass=%#v", len(second.checks), len(second.mutations), pass)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State != registry.StateRetired || loaded.Entries[1].State != registry.StateRetired {
		t.Fatalf("resumed registry=%#v", loaded)
	}
}

func TestRootBatchHardFiveRootsWindowAndCatchUpAcrossRestart(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	entries := make([]registry.Entry, 0, supervisor.MaxRootsPerGCWindow+1)
	for index := 0; index < supervisor.MaxRootsPerGCWindow+1; index++ {
		root := filepath.Join(dir, fmt.Sprintf("root-%d", index))
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		entry := gcRootBatchEntry(fmt.Sprintf("entry-%d", index), root, now.Add(-time.Duration(30-index)*time.Minute))
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	registryPath := filepath.Join(dir, "registry.json")
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}}
	for passIndex := 0; passIndex < supervisor.MaxRootsPerGCWindow; passIndex++ {
		passNow := now.Add(time.Duration(passIndex) * supervisor.GCCatchUpInterval)
		before := len(wake.mutations)
		pass, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return passNow }}).superviseOnceWithGCState(
			context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(wake.mutations) != before+1 || pass.AttemptedRoot == "" {
			t.Fatalf("pass %d mutations=%d attempted=%q", passIndex, len(wake.mutations)-before, pass.AttemptedRoot)
		}
		if passIndex < supervisor.MaxRootsPerGCWindow-1 && (!pass.PendingGCRoots || nextSuperviseDelay(time.Minute, pass) != supervisor.GCCatchUpInterval) {
			t.Fatalf("pass %d did not request five-second catch-up: %#v", passIndex, pass)
		}
		if passIndex == supervisor.MaxRootsPerGCWindow-1 && pass.PendingGCRoots {
			t.Fatalf("fifth root bypassed hard rolling-window stop: %#v", pass)
		}
	}
	restartNow := now.Add(time.Duration(supervisor.MaxRootsPerGCWindow) * supervisor.GCCatchUpInterval)
	before := len(wake.mutations)
	blocked, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return restartNow }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(wake.mutations) != before || blocked.AttemptedRoot != "" {
		t.Fatalf("restart escaped durable window: mutations=%d pass=%#v", len(wake.mutations)-before, blocked)
	}

	afterWindow := now.Add(supervisor.GCRootWindow + time.Second)
	resumed, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return afterWindow }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(wake.mutations) != before+1 || resumed.AttemptedRoot == "" {
		t.Fatalf("expired window did not admit sixth root: mutations=%d pass=%#v", len(wake.mutations)-before, resumed)
	}
}

func TestSuperviseOversizedRootTouchesNoLifecycle(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entries := make([]registry.Entry, 0, supervisor.MaxAgentsPerRootBatch+1)
	for index := 0; index < supervisor.MaxAgentsPerRootBatch+1; index++ {
		entry := gcRootBatchEntry(fmt.Sprintf("entry-%02d", index), root, now.Add(-20*time.Minute))
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	registryPath := filepath.Join(dir, "registry.json")
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}}
	pass, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if pass.AttemptedRoot == "" || len(wake.checks) != 0 || len(wake.mutations) != 0 {
		t.Fatalf("oversized pass=%#v checks=%d mutations=%d", pass, len(wake.checks), len(wake.mutations))
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GCRootBatches) != 0 || len(loaded.GCRootAttempts) != 1 {
		t.Fatalf("oversized root created an active batch: %#v", loaded.GCRootBatches)
	}
	for _, entry := range loaded.Entries {
		if entry.LastGCReason != "root_batch_oversized" || !entry.GCBackoffUntil.Equal(now.Add(15*time.Minute)) {
			t.Fatalf("oversized entry=%#v", entry)
		}
	}
}

func TestRootBatchPreflightConvergesExactTombstoneBeforeRetiringSibling(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entries := []registry.Entry{
		gcRootBatchEntry("00-tombstoned", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("01-dead", root, now.Add(-20*time.Minute)),
	}
	for _, entry := range entries {
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	registryPath := filepath.Join(dir, "registry.json")
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	wake := &gcBatchScriptWake{tombstones: map[string]bool{gcBatchEntryKey(entries[0]): true}}
	_, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(wake.checks) != 2 || len(wake.mutations) != 1 || wake.mutations[0].Me != entries[1].Agent {
		t.Fatalf("checks=%#v mutations=%#v", wake.checks, wake.mutations)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State != registry.StateRetired || loaded.Entries[1].State != registry.StateRetired || loaded.Entries[0].RetirementOutcome != "already_retired" {
		t.Fatalf("tombstone convergence=%#v", loaded)
	}
}

func TestRootBatchRetiringSupersededTerminalizesWithoutMutatingLaterMembers(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entries := []registry.Entry{
		gcRootBatchEntry("00-superseded", root, now.Add(-20*time.Minute)),
		gcRootBatchEntry("01-later", root, now.Add(-20*time.Minute)),
	}
	for _, entry := range entries {
		if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	registryPath := filepath.Join(dir, "registry.json")
	seedGCRootBatch(t, registryPath, entries, now, registry.GCRootBatchRetiring)
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}, supersedeFirst: true}
	pass, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now.Add(supervisor.GCCatchUpInterval) }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(wake.checks) != 0 || len(wake.mutations) != 1 || pass.PendingGCRoots {
		t.Fatalf("superseded checks=%d mutations=%d pass=%#v", len(wake.checks), len(wake.mutations), pass)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State != registry.StateRetired || loaded.Entries[0].RetirementOutcome != "superseded" || loaded.Entries[1].State == registry.StateRetired {
		t.Fatalf("superseded terminal state=%#v", loaded)
	}
	if err := registrationGCBatchError(loaded, root); err != nil {
		t.Fatalf("superseded batch kept registration blocked: %v", err)
	}
}

func TestTerminallyBlockedOldRootsCannotStarveLaterCleanRoot(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	entries := make([]registry.Entry, 0, supervisor.MaxRootsPerGCWindow*2+1)
	liveAgents := make(map[string]bool)
	for rootIndex := 0; rootIndex < supervisor.MaxRootsPerGCWindow; rootIndex++ {
		root := filepath.Join(dir, fmt.Sprintf("blocked-%d", rootIndex))
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		dead := gcRootBatchEntry(fmt.Sprintf("dead-%d", rootIndex), root, now.Add(-time.Duration(40-rootIndex)*time.Minute))
		live := gcRootBatchEntry(fmt.Sprintf("live-%d", rootIndex), root, dead.OwnerGoneSince)
		liveAgents[live.Agent] = true
		for _, entry := range []registry.Entry{dead, live} {
			if err := os.WriteFile(entry.Target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			entries = append(entries, entry)
		}
	}
	cleanRoot := filepath.Join(dir, "clean")
	if err := os.Mkdir(cleanRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	clean := gcRootBatchEntry("clean", cleanRoot, now.Add(-10*time.Minute))
	if err := os.WriteFile(clean.Target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, clean)
	registryPath := filepath.Join(dir, "registry.json")
	if err := registry.New(registryPath).Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}, liveAgents: liveAgents}
	for passIndex := 0; passIndex < supervisor.MaxRootsPerGCWindow; passIndex++ {
		passNow := now.Add(time.Duration(passIndex) * supervisor.GCCatchUpInterval)
		if _, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return passNow }}).superviseOnceWithGCState(
			context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
		); err != nil {
			t.Fatal(err)
		}
	}
	if len(wake.mutations) != 0 {
		t.Fatalf("blocked roots issued mutations: %#v", wake.mutations)
	}
	afterWindow := now.Add(supervisor.GCRootWindow + time.Second)
	if _, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return afterWindow }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, gcBatchPolicy(),
	); err != nil {
		t.Fatal(err)
	}
	if len(wake.mutations) != 1 || wake.mutations[0].Root != cleanRoot {
		t.Fatalf("clean root remained starved; mutations=%#v", wake.mutations)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range loaded.Entries {
		if entry.Root != cleanRoot && entry.State != registry.StateRetired && !entry.GCBackoffUntil.After(afterWindow) {
			t.Fatalf("blocked entry lacked durable quarantine: %#v", entry)
		}
	}
}

func TestAutoGCDisabledCancelsPreflightWithoutLifecycleIO(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := gcRootBatchEntry("entry", root, now.Add(-20*time.Minute))
	registryPath := filepath.Join(dir, "registry.json")
	seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchPreflight)
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}}
	pass, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, supervisor.GCPolicy{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if wake.envCalls != 0 || len(wake.checks) != 0 || len(wake.mutations) != 0 || pass.PendingGCRoots {
		t.Fatalf("disabled preflight env=%d checks=%d mutations=%d pass=%#v", wake.envCalls, len(wake.checks), len(wake.mutations), pass)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State == registry.StateRetired {
		t.Fatalf("disabled preflight file=%#v err=%v", loaded, err)
	}
}

func TestAutoGCDisabledRetiringUsesCheckOnlyAndConvergesTombstone(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := gcRootBatchEntry("entry", root, now.Add(-20*time.Minute))
	registryPath := filepath.Join(dir, "registry.json")
	seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchRetiring)
	wake := &gcBatchScriptWake{tombstones: map[string]bool{gcBatchEntryKey(entry): true}}
	_, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, supervisor.GCPolicy{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if wake.envCalls != 1 || len(wake.checks) != 1 || len(wake.mutations) != 0 {
		t.Fatalf("disabled retiring env=%d checks=%d mutations=%d", wake.envCalls, len(wake.checks), len(wake.mutations))
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State != registry.StateRetired {
		t.Fatalf("disabled retiring file=%#v err=%v", loaded, err)
	}
}

func TestAutoGCDisabledRetiringRetainsBatchWhenCapabilityUnavailable(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := gcRootBatchEntry("entry", root, now.Add(-20*time.Minute))
	registryPath := filepath.Join(dir, "registry.json")
	seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchRetiring)
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}, envErr: errors.New("capability unavailable")}
	_, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, supervisor.GCPolicy{},
	)
	if err == nil || !strings.Contains(err.Error(), "batch") || len(wake.checks) != 0 || len(wake.mutations) != 0 {
		t.Fatalf("capability failure err=%v checks=%d mutations=%d", err, len(wake.checks), len(wake.mutations))
	}
	loaded, loadErr := registry.New(registryPath).Load()
	if loadErr != nil || len(loaded.GCRootBatches) != 1 || loaded.Entries[0].State == registry.StateRetired {
		t.Fatalf("capability failure lost batch=%#v err=%v", loaded, loadErr)
	}
}

func TestAutoGCDisabledRetiringRetainsBatchOnUnprovenRefusal(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := gcRootBatchEntry("entry", root, now.Add(-20*time.Minute))
	registryPath := filepath.Join(dir, "registry.json")
	seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchRetiring)
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}, refusedReason: "owner_uninspectable"}
	_, err := (App{Stdout: io.Discard, Stderr: io.Discard, Now: func() time.Time { return now }}).superviseOnceWithGCState(
		context.Background(), registryPath, wake, "/bin/sh", time.Second, supervisor.GCPolicy{},
	)
	if err == nil || !strings.Contains(err.Error(), "retained") || len(wake.checks) != 1 || len(wake.mutations) != 0 {
		t.Fatalf("unproven refusal err=%v checks=%d mutations=%d", err, len(wake.checks), len(wake.mutations))
	}
	loaded, loadErr := registry.New(registryPath).Load()
	if loadErr != nil || len(loaded.GCRootBatches) != 1 || loaded.Entries[0].State == registry.StateRetired {
		t.Fatalf("unproven refusal lost batch=%#v err=%v", loaded, loadErr)
	}
}

func TestExplicitAbandonPreflightQuarantinesWithoutLifecycleIO(t *testing.T) {
	now := time.Date(2026, 7, 22, 17, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	entry := gcRootBatchEntry("codex", dir, now.Add(-10*time.Minute))
	batch := seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchPreflight)
	wake := &gcBatchScriptWake{tombstones: map[string]bool{}}
	var stdout bytes.Buffer
	err := (App{Stdout: &stdout, Stderr: io.Discard, Now: func() time.Time { return now }}).abandonGCRootBatch(
		context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second, batch.ID,
	)
	if err != nil || wake.envCalls != 0 || len(wake.checks) != 0 || len(wake.mutations) != 0 {
		t.Fatalf("env=%d checks=%d mutations=%d err=%v", wake.envCalls, len(wake.checks), len(wake.mutations), err)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.GCRootBatches) != 0 || len(loaded.Entries) != 1 || loaded.Entries[0].GCQuarantinedAt.IsZero() {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	if !strings.Contains(stdout.String(), `"unresolved_amq_state": false`) {
		t.Fatalf("output=%s", stdout.String())
	}
}

func TestExplicitAbandonRetiringWithoutCapabilityIsLoudRegistryOnlyEscape(t *testing.T) {
	now := time.Date(2026, 7, 22, 17, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	entry := gcRootBatchEntry("codex", dir, now.Add(-10*time.Minute))
	batch := seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchRetiring)
	wake := &gcBatchScriptWake{envErr: errors.New("capability unavailable"), tombstones: map[string]bool{}}
	var stdout bytes.Buffer
	err := (App{Stdout: &stdout, Stderr: io.Discard, Now: func() time.Time { return now }}).abandonGCRootBatch(
		context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second, batch.ID,
	)
	if err != nil || wake.envCalls != 1 || len(wake.checks) != 0 || len(wake.mutations) != 0 {
		t.Fatalf("env=%d checks=%d mutations=%d err=%v", wake.envCalls, len(wake.checks), len(wake.mutations), err)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State == registry.StateRetired || loaded.Entries[0].GCQuarantinedAt.IsZero() {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	if !strings.Contains(stdout.String(), `"unresolved_amq_state": true`) || !strings.Contains(stdout.String(), "remains unresolved") {
		t.Fatalf("output=%s", stdout.String())
	}
}

func TestExplicitAbandonRetiringReconcilesCheckOnlyTombstone(t *testing.T) {
	now := time.Date(2026, 7, 22, 17, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	registryPath := filepath.Join(dir, "registry.json")
	entry := gcRootBatchEntry("codex", dir, now.Add(-10*time.Minute))
	batch := seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchRetiring)
	key := gcBatchEntryKey(entry)
	wake := &gcBatchScriptWake{tombstones: map[string]bool{key: true}}
	var stdout bytes.Buffer
	err := (App{Stdout: &stdout, Stderr: io.Discard, Now: func() time.Time { return now }}).abandonGCRootBatch(
		context.Background(), registryPath, wake, "/bin/amq-keepalive", time.Second, batch.ID,
	)
	if err != nil || len(wake.checks) != 1 || len(wake.mutations) != 0 {
		t.Fatalf("checks=%d mutations=%d err=%v", len(wake.checks), len(wake.mutations), err)
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.GCRootBatches) != 0 || loaded.Entries[0].State != registry.StateRetired || !loaded.Entries[0].GCQuarantinedAt.IsZero() {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
}

func TestGCRootAttemptLedgerSurvivesReattachAndStillBlocksSixthRoot(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	attempts := make([]registry.GCRootAttempt, 0, supervisor.MaxRootsPerGCWindow)
	var carrier registry.Entry
	for index := 0; index < supervisor.MaxRootsPerGCWindow; index++ {
		root := filepath.Join(dir, fmt.Sprintf("attempted-%d", index))
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		canonical, err := canonicalGCRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		attempts = append(attempts, registry.GCRootAttempt{CanonicalRoot: canonical, StartedAt: now})
		if index == 0 {
			carrier = gcRootBatchEntry("carrier", root, now.Add(-20*time.Minute))
			carrier.LastGCRootBatchAt = now
		}
	}
	sixthRoot := filepath.Join(dir, "sixth")
	if err := os.Mkdir(sixthRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	sixth := gcRootBatchEntry("sixth", sixthRoot, now.Add(-20*time.Minute))
	registryPath := filepath.Join(dir, "registry.json")
	store := registry.New(registryPath)
	if err := store.Save(registry.File{Entries: []registry.Entry{carrier, sixth}, GCRootAttempts: attempts}); err != nil {
		t.Fatal(err)
	}
	newTarget := filepath.Join(carrier.Root, "replacement.target")
	_, removed, err := store.ReplaceSessionAdapter(registry.Entry{
		Root: carrier.Root, Agent: carrier.Agent, Adapter: carrier.Adapter, Target: newTarget, State: registry.StateAttached,
		WakeOwnerPresent: true, WakeOwner: carrier.WakeOwner,
	})
	if err != nil || len(removed) != 1 {
		t.Fatalf("reattach removed=%#v err=%v", removed, err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GCRootAttempts) != supervisor.MaxRootsPerGCWindow {
		t.Fatalf("reattach erased attempt ledger: %#v", loaded.GCRootAttempts)
	}
	for _, entry := range loaded.Entries {
		if entry.Agent == carrier.Agent && !entry.LastGCRootBatchAt.IsZero() {
			t.Fatalf("test no longer proves top-level ledger independence: replacement retained row marker %#v", entry)
		}
	}
	plan, err := planGCRootBatchForFile(loaded, now.Add(30*time.Second), gcBatchPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Canonical != "" {
		t.Fatalf("sixth root escaped rolling cap after reattach: %#v", plan)
	}
}

func TestForgetCannotDeleteFrozenBatchMember(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	entry := gcRootBatchEntry("entry", root, now.Add(-20*time.Minute))
	registryPath := filepath.Join(dir, "registry.json")
	seedGCRootBatch(t, registryPath, []registry.Entry{entry}, now, registry.GCRootBatchPreflight)
	var stderr bytes.Buffer
	code := (App{Stdout: io.Discard, Stderr: &stderr}).Run(context.Background(), []string{
		"forget", "--registry", registryPath, "--id", entry.ID,
	})
	if code != 1 || !strings.Contains(stderr.String(), "GC root batch") {
		t.Fatalf("forget code=%d stderr=%s", code, stderr.String())
	}
	loaded, err := registry.New(registryPath).Load()
	if err != nil || len(loaded.GCRootBatches) != 1 || len(loaded.Entries) != 1 || loaded.Entries[0].ID != entry.ID {
		t.Fatalf("forget changed frozen registry=%#v err=%v", loaded, err)
	}
}

func gcBatchEntryKey(entry registry.Entry) string {
	return entry.Root + "\x00" + entry.Agent + "\x00" + entry.WakeBinding.Generation + "\x00" + entry.WakeBinding.TargetDigest
}

func seedGCRootBatch(t *testing.T, registryPath string, entries []registry.Entry, now time.Time, phase registry.GCRootBatchPhase) registry.GCRootBatch {
	t.Helper()
	store := registry.New(registryPath)
	if err := store.Save(registry.File{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalGCRoot(entries[0].Root)
	if err != nil {
		t.Fatal(err)
	}
	plan := gcRootBatchPlan{Canonical: canonical, Members: append([]registry.Entry(nil), entries...)}
	sort.Slice(plan.Members, func(i, j int) bool { return plan.Members[i].ID < plan.Members[j].ID })
	batch := frozenGCRootBatch(plan, now)
	if _, err := store.StartGCRootBatch(batch, plan.Members); err != nil {
		t.Fatal(err)
	}
	if phase == registry.GCRootBatchRetiring {
		if _, err := store.AdvanceGCRootBatch(batch.ID, registry.GCRootBatchPreflight, registry.GCRootBatchRetiring); err != nil {
			t.Fatal(err)
		}
		batch.Phase = registry.GCRootBatchRetiring
	}
	return batch
}

type gcBatchScriptWake struct {
	tombstones          map[string]bool
	checks              []amq.RetireWakeRequest
	mutations           []amq.RetireWakeRequest
	envCalls            int
	envErr              error
	checkErr            error
	refusedReason       string
	liveAgents          map[string]bool
	panicAfterTombstone bool
	failFirstMutation   bool
	supersedeFirst      bool
}

func (*gcBatchScriptWake) StartWake(context.Context, amq.StartWakeRequest) (amq.WakeBinding, error) {
	return amq.WakeBinding{}, errors.New("root batch member must not restart")
}

func (w *gcBatchScriptWake) Env(context.Context) (amq.Env, error) {
	w.envCalls++
	if w.envErr != nil {
		return amq.Env{}, w.envErr
	}
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (w *gcBatchScriptWake) RetireWake(_ context.Context, request amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	result := amq.RetireWakeResult{
		Root: request.Root, Agent: request.Me, Generation: request.Generation, TargetDigest: request.TargetDigest,
	}
	key := request.Root + "\x00" + request.Me + "\x00" + request.Generation + "\x00" + request.TargetDigest
	if request.Check {
		w.checks = append(w.checks, request)
		if w.checkErr != nil {
			return amq.RetireWakeResult{}, w.checkErr
		}
		if w.liveAgents[request.Me] {
			result.Status, result.ReasonCode = "refused", "owner_live"
			return result, errors.New("owner remains live")
		}
		if w.refusedReason != "" {
			result.Status, result.ReasonCode = "refused", w.refusedReason
			return result, errors.New("retirement proof was not established")
		}
		if w.tombstones[key] {
			result.Status, result.ReasonCode = "already_retired", "tombstone_match"
			return result, nil
		}
		result.Status, result.ReasonCode = "eligible", "owner_gone"
		return result, nil
	}
	w.mutations = append(w.mutations, request)
	if w.supersedeFirst {
		w.supersedeFirst = false
		result.Status, result.ReasonCode = "superseded", "generation_superseded"
		result.CurrentWakeMode = "owner_bound"
		result.CurrentGeneration = request.Generation + "-replacement"
		result.CurrentTargetDigest = request.TargetDigest + "-replacement"
		return result, nil
	}
	if w.failFirstMutation {
		w.failFirstMutation = false
		result.Status, result.ReasonCode = "error", "internal_error"
		return result, errors.New("injected lifecycle mutation failure")
	}
	if w.tombstones[key] {
		result.Status, result.ReasonCode = "already_retired", "tombstone_match"
		return result, nil
	}
	w.tombstones[key] = true
	if w.panicAfterTombstone {
		w.panicAfterTombstone = false
		panic("injected crash after durable AMQ tombstone")
	}
	result.Status, result.ReasonCode = "retired", "retired_exact"
	return result, nil
}

func gcBatchPolicy() supervisor.GCPolicy {
	return supervisor.GCPolicy{
		AutoGC: true, OwnerGrace: supervisor.MinOwnerGoneGrace,
		RetiredRetention: supervisor.MinRetiredRetention, Timeout: time.Second,
	}
}

type gcRootBatchWitnessWake struct {
	registryPath        string
	starts              int
	mutations           int
	checks              []amq.RetireWakeRequest
	captured            bool
	snapshot            registry.File
	snapshotErr         error
	mutationCaptured    bool
	mutationSnapshot    registry.File
	mutationSnapshotErr error
}

func (w *gcRootBatchWitnessWake) StartWake(context.Context, amq.StartWakeRequest) (amq.WakeBinding, error) {
	w.starts++
	return amq.WakeBinding{}, fmt.Errorf("root batch member must not restart")
}

func (*gcRootBatchWitnessWake) Env(context.Context) (amq.Env, error) {
	return amq.Env{Capabilities: []string{amq.CapabilityWakeGCV1}}, nil
}

func (w *gcRootBatchWitnessWake) RetireWake(_ context.Context, request amq.RetireWakeRequest) (amq.RetireWakeResult, error) {
	if !w.captured {
		w.captured = true
		w.snapshot, w.snapshotErr = registry.New(w.registryPath).Load()
	}
	if !request.Check && !w.mutationCaptured {
		w.mutationCaptured = true
		w.mutationSnapshot, w.mutationSnapshotErr = registry.New(w.registryPath).Load()
	}
	result := amq.RetireWakeResult{
		Root: request.Root, Agent: request.Me,
		Generation: request.Generation, TargetDigest: request.TargetDigest,
	}
	if request.Check {
		w.checks = append(w.checks, request)
		if request.Me == "01-live" {
			result.Status = "refused"
			result.ReasonCode = "owner_live"
			return result, nil
		}
		result.Status = "eligible"
		result.ReasonCode = "owner_gone"
		return result, nil
	}
	w.mutations++
	result.Status = "retired"
	result.ReasonCode = "retired_exact"
	return result, nil
}

func gcRootBatchEntry(id, root string, ownerGoneSince time.Time) registry.Entry {
	return registry.Entry{
		ID: id, Root: root, Agent: id, Adapter: "file", Target: filepath.Join(root, id+".target"),
		State: registry.StateActive, WakeOwnerPresent: true,
		WakeOwner:      registry.WakeOwner{PID: 42, ProcessStart: "start-1", BootID: "boot-1", SessionID: 42},
		WakeBinding:    registry.WakeBinding{Generation: "generation-" + id, TargetDigest: "sha256:" + id},
		OwnerGoneSince: ownerGoneSince,
	}
}

func retireMarkedRootForRootBatchTest(t *testing.T, store *registry.Store, file *registry.File, canonicalRoot string, now time.Time) {
	t.Helper()
	for index, entry := range file.Entries {
		root, err := canonicalGCRoot(entry.Root)
		if err != nil {
			t.Fatal(err)
		}
		if root != canonicalRoot || entry.State == registry.StateRetired {
			continue
		}
		entry.State = registry.StateRetired
		entry.RetiredAt = now
		if err := store.UpdateEntry(entry); err != nil {
			t.Fatal(err)
		}
		file.Entries[index] = entry
	}
}
