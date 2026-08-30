//go:build darwin || linux

package amq

import "testing"

func TestDetachedWakeStartsInNewSession(t *testing.T) {
	attributes := detachedWakeSysProcAttr()
	if attributes == nil || !attributes.Setsid {
		t.Fatalf("detachedWakeSysProcAttr() = %#v, want Setsid", attributes)
	}
}
