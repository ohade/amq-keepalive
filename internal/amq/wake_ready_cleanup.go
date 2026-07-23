package amq

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const staleWakeReadyMarkerAge = 24 * time.Hour
const maxWakeReadyCleanupEntries = 128
const maxWakeReadyCleanupErrors = 8

type wakeReadyCleanupDir interface {
	ReadDir(int) ([]os.DirEntry, error)
	Close() error
}

type wakeReadyCleanupFS interface {
	Open(string) (wakeReadyCleanupDir, error)
	Remove(string) error
}

type osWakeReadyCleanupFS struct{}

func (osWakeReadyCleanupFS) Open(path string) (wakeReadyCleanupDir, error) {
	return os.Open(path)
}

func (osWakeReadyCleanupFS) Remove(path string) error { return os.Remove(path) }

// newWakeReadyPaths prepares both per-start paths after one bounded cleanup.
// Cleanup diagnostics are non-fatal; reserving either destination remains a
// hard error because AMQ cannot safely publish readiness without both paths.
func (c CLI) newWakeReadyPaths() (string, string, error) {
	dir, err := wakeReadyDirectory()
	if err != nil {
		return "", "", err
	}
	cleanup := c.wakeReadyCleanup
	if cleanup == nil {
		cleanup = scavengeStaleWakeReadyMarkers
	}
	now := time.Now()
	if c.wakeReadyCleanupNow != nil {
		now = c.wakeReadyCleanupNow()
	}
	if warning := cleanup(dir, now); warning != nil && c.wakeReadyWarningSink != nil {
		c.wakeReadyWarningSink(warning)
	}
	ready, err := reserveWakeReadyPath(dir)
	if err != nil {
		return "", "", err
	}
	result, err := reserveWakeReadyPath(dir)
	if err != nil {
		return "", "", err
	}
	return ready, result, nil
}

// newWakeReadyPath is retained for package callers that need one destination.
// StartWake uses newWakeReadyPaths so cleanup runs only once per attempt.
func newWakeReadyPath() (string, string, error) {
	c := NewCLI("")
	dir, err := wakeReadyDirectory()
	if err != nil {
		return "", "", err
	}
	if warning := scavengeStaleWakeReadyMarkers(dir, time.Now()); warning != nil && c.wakeReadyWarningSink != nil {
		c.wakeReadyWarningSink(warning)
	}
	path, err := reserveWakeReadyPath(dir)
	return dir, path, err
}

func wakeReadyDirectory() (string, error) {
	cacheDir := strings.TrimSpace(os.Getenv("AMQ_KEEPALIVE_CACHE_DIR"))
	if cacheDir == "" {
		var err error
		cacheDir, err = os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("resolve user cache directory for wake readiness: %w", err)
		}
	}
	dir := filepath.Join(cacheDir, "amq-keepalive", "readiness")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create wake readiness directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", fmt.Errorf("inspect wake readiness directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("wake readiness path %q must be a real directory", dir)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", fmt.Errorf("secure wake readiness directory: %w", err)
	}
	return dir, nil
}

func reserveWakeReadyPath(dir string) (string, error) {
	placeholder, err := os.CreateTemp(dir, "wake-*")
	if err != nil {
		return "", fmt.Errorf("reserve wake readiness path: %w", err)
	}
	path := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close wake readiness placeholder: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("prepare wake readiness destination: %w", err)
	}
	return path, nil
}

func scavengeStaleWakeReadyMarkers(dir string, now time.Time) error {
	return scavengeStaleWakeReadyMarkersWithFS(osWakeReadyCleanupFS{}, dir, now)
}

// scavengeStaleWakeReadyMarkersWithFS performs one bounded directory read and
// at most maxWakeReadyCleanupEntries Info/remove attempts. Reading one extra
// entry makes truncation visible without allowing work to grow with the dir.
func scavengeStaleWakeReadyMarkersWithFS(fs wakeReadyCleanupFS, dir string, now time.Time) error {
	warnings := wakeReadyCleanupWarnings{}
	directory, err := fs.Open(dir)
	if err != nil {
		warnings.add(fmt.Errorf("open readiness directory: %w", err))
		return warnings.err()
	}
	entries, readErr := directory.ReadDir(maxWakeReadyCleanupEntries + 1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		warnings.add(fmt.Errorf("read readiness directory: %w", readErr))
	}
	if closeErr := directory.Close(); closeErr != nil {
		warnings.add(fmt.Errorf("close readiness directory: %w", closeErr))
	}
	if len(entries) > maxWakeReadyCleanupEntries {
		entries = entries[:maxWakeReadyCleanupEntries]
		warnings.add(fmt.Errorf("cleanup reached the %d-entry work limit", maxWakeReadyCleanupEntries))
	} else if len(entries) == maxWakeReadyCleanupEntries && readErr == nil {
		warnings.add(fmt.Errorf("cleanup may have reached the %d-entry work limit", maxWakeReadyCleanupEntries))
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "wake-") {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			warnings.add(fmt.Errorf("inspect readiness marker %q: %w", entry.Name(), infoErr))
			continue
		}
		if now.Sub(info.ModTime()) < staleWakeReadyMarkerAge {
			continue
		}
		if removeErr := fs.Remove(filepath.Join(dir, entry.Name())); removeErr != nil {
			warnings.add(fmt.Errorf("remove stale readiness marker %q: %w", entry.Name(), removeErr))
		}
	}
	return warnings.err()
}

type wakeReadyCleanupWarnings struct {
	total  int
	values []error
}

func (w *wakeReadyCleanupWarnings) add(err error) {
	if err == nil {
		return
	}
	w.total++
	if len(w.values) < maxWakeReadyCleanupErrors {
		w.values = append(w.values, err)
	}
}

func (w wakeReadyCleanupWarnings) err() error {
	if w.total == 0 {
		return nil
	}
	values := append([]error(nil), w.values...)
	if omitted := w.total - len(w.values); omitted > 0 {
		values = append(values, fmt.Errorf("%d additional cleanup warning(s) omitted", omitted))
	}
	return fmt.Errorf("wake readiness cleanup: %w", errors.Join(values...))
}
