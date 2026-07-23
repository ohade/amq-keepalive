//go:build darwin || linux

package executable

import (
	"fmt"
	"os"
	"syscall"
)

func openNoFollow(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open executable %q without following symlinks: %w", path, err)
	}
	return os.NewFile(uintptr(fd), path), nil
}

func statIdentity(info os.FileInfo) (device, inode uint64, uid, gid uint32, err error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("stat identity is unavailable for %q", info.Name())
	}
	return uint64(stat.Dev), uint64(stat.Ino), stat.Uid, stat.Gid, nil
}
