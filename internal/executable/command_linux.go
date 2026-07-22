//go:build linux

package executable

import (
	"context"
	"os/exec"
)

func commandContext(ctx context.Context, expected Identity, args ...string) (*exec.Cmd, func(), error) {
	file, err := OpenVerified(expected)
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.CommandContext(ctx, "/proc/self/fd/3", args...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, file)
	return cmd, func() { _ = file.Close() }, nil
}
