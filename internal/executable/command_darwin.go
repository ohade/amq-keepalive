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

func commandContext(ctx context.Context, expected Identity, args ...string) (*exec.Cmd, func(), error) {
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
		_ = file.Close()
		return nil, nil, err
	}
	cleanup := func() {
		_ = os.Remove(filepath.Join(dir, "executable"))
		_ = os.Remove(dir)
		_ = file.Close()
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		cleanup()
		return nil, nil, err
	}
	snapshotPath := filepath.Join(dir, "executable")
	snapshot, err := os.OpenFile(snapshotPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = snapshot.Close()
		cleanup()
		return nil, nil, err
	}
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(snapshot, hash), file); err != nil {
		_ = snapshot.Close()
		cleanup()
		return nil, nil, err
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != expected.SHA256 {
		_ = snapshot.Close()
		cleanup()
		return nil, nil, fmt.Errorf("verified executable changed while snapshotting %q", expected.Path)
	}
	if err := snapshot.Sync(); err != nil {
		_ = snapshot.Close()
		cleanup()
		return nil, nil, err
	}
	if err := snapshot.Close(); err != nil {
		cleanup()
		return nil, nil, err
	}
	if err := os.Chmod(snapshotPath, 0o500); err != nil {
		cleanup()
		return nil, nil, err
	}
	snapshotIdentity, err := Capture(snapshotPath)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("verify executable snapshot: %w", err)
	}
	if snapshotIdentity.SHA256 != expected.SHA256 || snapshotIdentity.Size != expected.Size {
		cleanup()
		return nil, nil, errors.New("verify executable snapshot: identity mismatch")
	}
	cmd := exec.CommandContext(ctx, snapshotPath, args...)
	cmd.Args[0] = expected.Path
	return cmd, cleanup, nil
}
