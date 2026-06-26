package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	SchemaVersion = 1

	StateAttached State = "attached"
	StateActive   State = "active"
	StateDetached State = "detached"
	StateStale    State = "stale"
)

type State string

type Entry struct {
	ID                     string    `json:"id"`
	Root                   string    `json:"root"`
	BaseRoot               string    `json:"base_root,omitempty"`
	SessionName            string    `json:"session_name,omitempty"`
	Agent                  string    `json:"agent"`
	Adapter                string    `json:"adapter"`
	Target                 string    `json:"target"`
	State                  State     `json:"state"`
	LastAttach             time.Time `json:"last_attach,omitempty"`
	LastSeenBySupervisor   time.Time `json:"last_seen_by_supervisor,omitempty"`
	FailureCount           int       `json:"failure_count,omitempty"`
	BackoffUntil           time.Time `json:"backoff_until,omitempty"`
	LastError              string    `json:"last_error,omitempty"`
	LastSupervisorDecision string    `json:"last_supervisor_decision,omitempty"`
}

type File struct {
	SchemaVersion int     `json:"schema_version"`
	Entries       []Entry `json:"entries"`
}

type Store struct {
	Path string
	Now  func() time.Time
}

func New(path string) *Store {
	return &Store{Path: path, Now: time.Now}
}

func DefaultPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".amq-keepalive", "registry.json"), nil
}

func EntryID(root, agent, adapterName, target string) string {
	sum := sha256.Sum256([]byte(root + "\x00" + agent + "\x00" + adapterName + "\x00" + target))
	return hex.EncodeToString(sum[:])
}

func (s *Store) Load() (File, error) {
	if s.Path == "" {
		return File{}, errors.New("registry path is required")
	}
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return File{SchemaVersion: SchemaVersion}, nil
	}
	if err != nil {
		return File{}, err
	}
	var file File
	if err := json.Unmarshal(data, &file); err != nil {
		return File{}, err
	}
	if file.SchemaVersion == 0 {
		file.SchemaVersion = SchemaVersion
	}
	if file.SchemaVersion != SchemaVersion {
		return File{}, fmt.Errorf("unsupported registry schema version %d", file.SchemaVersion)
	}
	sortEntries(file.Entries)
	return file, nil
}

func (s *Store) Save(file File) error {
	if s.Path == "" {
		return errors.New("registry path is required")
	}
	file.SchemaVersion = SchemaVersion
	sortEntries(file.Entries)

	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".registry-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(file); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		return err
	}
	return os.Chmod(s.Path, 0o600)
}

func (s *Store) Upsert(entry Entry) (Entry, error) {
	now := s.now()
	if entry.Root == "" {
		return Entry{}, errors.New("entry root is required")
	}
	if entry.Agent == "" {
		return Entry{}, errors.New("entry agent is required")
	}
	if entry.Adapter == "" {
		return Entry{}, errors.New("entry adapter is required")
	}
	if entry.Target == "" {
		return Entry{}, errors.New("entry target is required")
	}
	if entry.ID == "" {
		entry.ID = EntryID(entry.Root, entry.Agent, entry.Adapter, entry.Target)
	}
	if entry.State == "" {
		entry.State = StateAttached
	}
	if entry.LastAttach.IsZero() {
		entry.LastAttach = now
	}

	file, err := s.Load()
	if err != nil {
		return Entry{}, err
	}
	replaced := false
	for i := range file.Entries {
		if file.Entries[i].ID == entry.ID {
			file.Entries[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		file.Entries = append(file.Entries, entry)
	}
	return entry, s.Save(file)
}

func (s *Store) UpdateEntry(entry Entry) error {
	file, err := s.Load()
	if err != nil {
		return err
	}
	for i := range file.Entries {
		if file.Entries[i].ID == entry.ID {
			file.Entries[i] = entry
			return s.Save(file)
		}
	}
	return fmt.Errorf("registry entry %q not found", entry.ID)
}

func (s *Store) Forget(id string) (bool, error) {
	file, err := s.Load()
	if err != nil {
		return false, err
	}
	next := file.Entries[:0]
	removed := false
	for _, entry := range file.Entries {
		if entry.ID == id {
			removed = true
			continue
		}
		next = append(next, entry)
	}
	file.Entries = next
	return removed, s.Save(file)
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ID < entries[j].ID
	})
}
