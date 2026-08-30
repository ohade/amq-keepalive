//go:build !darwin && !linux

package amq

import "syscall"

func detachedWakeSysProcAttr() *syscall.SysProcAttr {
	return nil
}
