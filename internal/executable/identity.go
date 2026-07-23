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
	"strings"
)

// Identity is a comparable, durable description of one executable. Path is
// fully resolved; the stat tuple and content digest bind the file behind that
// path rather than trusting the path string at apply time.
type Identity struct {
	Path         string `json:"path"`
	SHA256       string `json:"sha256"`
	Device       uint64 `json:"device"`
	Inode        uint64 `json:"inode"`
	Size         int64  `json:"size"`
	Mode         uint32 `json:"mode"`
	UID          uint32 `json:"uid"`
	GID          uint32 `json:"gid"`
	ModTimeNanos int64  `json:"mod_time_nanos"`
}

func (i Identity) Complete() bool {
	if !filepath.IsAbs(i.Path) || len(i.SHA256) != sha256.Size*2 || i.Inode == 0 || i.Size < 0 || i.Mode == 0 {
		return false
	}
	_, err := hex.DecodeString(i.SHA256)
	return err == nil
}

func Capture(command string) (Identity, error) {
	resolved, err := resolve(command)
	if err != nil {
		return Identity{}, err
	}
	if err := validateParents(resolved); err != nil {
		return Identity{}, err
	}
	file, err := openNoFollow(resolved)
	if err != nil {
		return Identity{}, err
	}
	defer file.Close()
	return identityFromOpenFile(resolved, file)
}

// OpenVerified pins the exact file descriptor after revalidating the path,
// metadata, ownership, permissions, and content digest. Callers that execute
// the returned descriptor avoid the validate-then-exec path replacement race.
func OpenVerified(expected Identity) (*os.File, error) {
	if !expected.Complete() {
		return nil, errors.New("expected executable identity is incomplete")
	}
	if err := validateParents(expected.Path); err != nil {
		return nil, err
	}
	file, err := openNoFollow(expected.Path)
	if err != nil {
		return nil, err
	}
	actual, err := identityFromOpenFile(expected.Path, file)
	if err != nil {
		file.Close()
		return nil, err
	}
	if actual != expected {
		file.Close()
		return nil, fmt.Errorf("executable identity mismatch for %q", expected.Path)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func Verify(expected Identity) error {
	file, err := OpenVerified(expected)
	if err != nil {
		return err
	}
	return file.Close()
}

// CommandContext executes the verified open descriptor where the platform
// supports it. The returned cleanup must be called after Start or Run, and its
// error must be surfaced so verified snapshots or descriptors cannot leak
// silently.
func CommandContext(ctx context.Context, expected Identity, args ...string) (*exec.Cmd, func() error, error) {
	return commandContext(ctx, expected, args...)
}

func resolve(command string) (string, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return "", errors.New("command path is required")
	}
	if !strings.ContainsRune(command, filepath.Separator) {
		resolved, err := exec.LookPath(command)
		if err != nil {
			return "", err
		}
		command = resolved
	}
	abs, err := filepath.Abs(filepath.Clean(command))
	if err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return filepath.Clean(real), nil
}

func identityFromOpenFile(path string, file *os.File) (Identity, error) {
	info, err := file.Stat()
	if err != nil {
		return Identity{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return Identity{}, fmt.Errorf("%q is not a regular executable file", path)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return Identity{}, fmt.Errorf("%q is group- or world-writable", path)
	}
	device, inode, uid, gid, err := statIdentity(info)
	if err != nil {
		return Identity{}, err
	}
	euid := uint32(os.Geteuid())
	if uid != 0 && uid != euid {
		return Identity{}, fmt.Errorf("%q is owned by untrusted uid %d", path, uid)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Identity{}, err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return Identity{}, err
	}
	return Identity{
		Path: path, SHA256: hex.EncodeToString(hash.Sum(nil)), Device: device, Inode: inode,
		Size: info.Size(), Mode: uint32(info.Mode()), UID: uid, GID: gid, ModTimeNanos: info.ModTime().UnixNano(),
	}, nil
}

func validateParents(path string) error {
	euid := uint32(os.Geteuid())
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		info, err := os.Lstat(dir)
		if err != nil {
			return fmt.Errorf("inspect executable parent %q: %w", dir, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("executable parent %q is not a real directory", dir)
		}
		_, _, uid, _, err := statIdentity(info)
		if err != nil {
			return err
		}
		if uid != 0 && uid != euid {
			return fmt.Errorf("executable parent %q is owned by untrusted uid %d", dir, uid)
		}
		if info.Mode().Perm()&0o022 != 0 && !(uid == 0 && info.Mode()&os.ModeSticky != 0) {
			return fmt.Errorf("executable parent %q is unsafely group- or world-writable", dir)
		}
		if dir == filepath.Dir(dir) {
			break
		}
	}
	return nil
}
