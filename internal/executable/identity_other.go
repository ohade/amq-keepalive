//go:build !darwin && !linux

package executable

import (
	"context"
	"errors"
	"os"
	"os/exec"
)

func openNoFollow(path string) (*os.File, error) {
	return nil, errors.New("verified no-follow executable open is unsupported on this platform")
}

func statIdentity(info os.FileInfo) (device, inode uint64, uid, gid uint32, err error) {
	return 0, 0, 0, 0, errors.New("stable executable stat identity is unsupported on this platform")
}

func commandContext(ctx context.Context, expected Identity, args ...string) (*exec.Cmd, func() error, error) {
	return nil, nil, errors.New("verified descriptor execution is unsupported on this platform")
}
