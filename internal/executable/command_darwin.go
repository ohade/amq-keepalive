//go:build darwin

package executable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

func commandContext(ctx context.Context, expected Identity, args ...string) (*exec.Cmd, func() error, error) {
	file, err := OpenVerified(expected)
	if err != nil {
		return nil, nil, err
	}
	// Darwin rejects execve of /dev/fd/N with EACCES and Go exposes no fexecve.
	// Copy from the already-verified descriptor into a private, randomly named
	// executable snapshot. The ambient source path is never reopened, so a
	// replacement after OpenVerified cannot change the bytes execve receives.
	dir, err := os.MkdirTemp("", "amq-keepalive-exec-*")
	if err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	snapshotPath := filepath.Join(dir, "executable")
	cleanup := func() error {
		return cleanupDarwinSnapshot(snapshotPath, dir, file)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	snapshot, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, errors.Join(err, snapshot.Close(), cleanup())
	}
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(snapshot, hash), file); err != nil {
		return nil, nil, errors.Join(err, snapshot.Close(), cleanup())
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != expected.SHA256 {
		return nil, nil, errors.Join(
			fmt.Errorf("verified executable changed while snapshotting %q", expected.Path),
			snapshot.Close(),
			cleanup(),
		)
	}
	if err := snapshot.Sync(); err != nil {
		return nil, nil, errors.Join(err, snapshot.Close(), cleanup())
	}
	if err := snapshot.Close(); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	if err := os.Chmod(snapshotPath, 0o500); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	snapshotIdentity, err := Capture(snapshotPath)
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("verify executable snapshot: %w", err), cleanup())
	}
	if snapshotIdentity.SHA256 != expected.SHA256 || snapshotIdentity.Size != expected.Size {
		return nil, nil, errors.Join(errors.New("verify executable snapshot: identity mismatch"), cleanup())
	}
	cmd := exec.CommandContext(ctx, snapshotPath, args...)
	cmd.Args[0] = expected.Path
	return cmd, cleanup, nil
}

func cleanupDarwinSnapshot(snapshotPath, dir string, source io.Closer) error {
	return cleanupDarwinSnapshotWith(snapshotPath, dir, source, os.Remove)
}

func cleanupDarwinSnapshotWith(snapshotPath, dir string, source io.Closer, remove func(string) error) error {
	var cleanupErrors []error
	if err := remove(snapshotPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("remove verified executable snapshot: %w", err))
	}
	if err := remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("remove verified executable snapshot directory: %w", err))
	}
	if err := source.Close(); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("close verified executable source: %w", err))
	}
	return errors.Join(cleanupErrors...)
}
