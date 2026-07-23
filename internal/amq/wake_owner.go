package amq

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

const WakeOwnerEnvironment = "AMQ_WAKE_OWNER"

type wakeOwner struct {
	PID          int    `json:"pid"`
	ProcessStart string `json:"process_start,omitempty"`
	BootID       string `json:"boot_id,omitempty"`
	SessionID    int    `json:"session_id,omitempty"`
}

// WakeOwnerFromEnvironment captures the opaque owner token established by
// deferred coop exec. The exact process identity fields are required so AMQ
// can distinguish a live owner from PID reuse.
func WakeOwnerFromEnvironment() (string, error) {
	raw := strings.TrimSpace(os.Getenv(WakeOwnerEnvironment))
	if raw == "" {
		return "", errors.New("AMQ_WAKE_OWNER is required for managed wake attachment")
	}
	if err := ValidateWakeOwner(raw); err != nil {
		return "", err
	}
	return raw, nil
}

func ValidateWakeOwner(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return errors.New("wake owner is required")
	}
	var owner wakeOwner
	if err := json.Unmarshal([]byte(raw), &owner); err != nil {
		return fmt.Errorf("parse %s: %w", WakeOwnerEnvironment, err)
	}
	if owner.PID <= 0 {
		return errors.New("wake owner pid must be > 0")
	}
	if strings.TrimSpace(owner.ProcessStart) == "" {
		return errors.New("wake owner process start is required")
	}
	if strings.TrimSpace(owner.BootID) == "" {
		return errors.New("wake owner boot id is required")
	}
	if strings.ContainsRune(owner.ProcessStart, 0) {
		return errors.New("wake owner process start contains NUL")
	}
	if strings.ContainsRune(owner.BootID, 0) {
		return errors.New("wake owner boot id contains NUL")
	}
	if owner.SessionID < 0 {
		return errors.New("wake owner session id must be >= 0")
	}
	return nil
}

func environmentWithWakeOwner(base []string, owner string) ([]string, error) {
	if err := ValidateWakeOwner(owner); err != nil {
		return nil, err
	}
	prefix := WakeOwnerEnvironment + "="
	env := make([]string, 0, len(base)+1)
	for _, entry := range base {
		if !strings.HasPrefix(entry, prefix) {
			env = append(env, entry)
		}
	}
	return append(env, prefix+strings.TrimSpace(owner)), nil
}
