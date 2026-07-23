//go:build darwin

package executable

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type cleanupErrorCloser struct {
	err error
}

func (c cleanupErrorCloser) Close() error {
	return c.err
}

func TestCleanupDarwinSnapshotAggregatesAndIgnoresAbsentPaths(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "executable")
	if err := os.WriteFile(snapshotPath, []byte("snapshot"), 0o500); err != nil {
		t.Fatal(err)
	}
	snapshotErr := errors.New("injected snapshot removal failure")
	directoryErr := errors.New("injected directory removal failure")
	closeErr := errors.New("injected source close failure")
	err := cleanupDarwinSnapshotWith(snapshotPath, dir, cleanupErrorCloser{err: closeErr}, func(path string) error {
		switch path {
		case snapshotPath:
			return snapshotErr
		case dir:
			return directoryErr
		default:
			return nil
		}
	})
	if !errors.Is(err, snapshotErr) || !errors.Is(err, directoryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("cleanup error=%v, want all removal and close failures", err)
	}

	if err := cleanupDarwinSnapshot(snapshotPath, dir, io.NopCloser(strings.NewReader(""))); err != nil {
		t.Fatalf("real cleanup error=%v", err)
	}
	if err := cleanupDarwinSnapshot(snapshotPath, dir, io.NopCloser(strings.NewReader(""))); err != nil {
		t.Fatalf("idempotent absent-path cleanup error=%v", err)
	}
}
