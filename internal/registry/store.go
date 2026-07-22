package registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	SchemaVersion = 2
	// MaxGCRootBatchMembers is a persistence-layer safety bound as well as a
	// coordinator policy. A malformed or direct caller cannot durably freeze an
	// unbounded retirement batch.
	MaxGCRootBatchMembers = 8
	MaxGCRootAttempts     = 5
	GCRootAttemptWindow   = time.Minute

	StateAttached State = "attached"
	StateActive   State = "active"
	StateDetached State = "detached"
	StateStale    State = "stale"
	StateRetired  State = "retired"

	TransitionNone          TransitionPhase = ""
	TransitionReserved      TransitionPhase = "reserved"
	TransitionRetirePending TransitionPhase = "retire_pending"
	TransitionOldRetired    TransitionPhase = "old_retired"
)

type State string

type TransitionPhase string

// WakeOwner is deliberately a value-only type. Entry values are optimistic
// compare-and-swap tokens, so lifecycle metadata must remain comparable.
type WakeOwner struct {
	PID          int    `json:"pid"`
	ProcessStart string `json:"process_start"`
	BootID       string `json:"boot_id"`
	SessionID    int    `json:"session_id,omitempty"`
}

func (o WakeOwner) Strong() bool {
	return o.PID > 0 && strings.TrimSpace(o.ProcessStart) != "" && strings.TrimSpace(o.BootID) != "" && o.SessionID >= 0
}

type WakeBinding struct {
	Generation   string `json:"generation"`
	TargetDigest string `json:"target_digest"`
}

func (b WakeBinding) Complete() bool {
	return strings.TrimSpace(b.Generation) != "" && strings.TrimSpace(b.TargetDigest) != ""
}

type GCRootBatchPhase string

const (
	GCRootBatchPreflight GCRootBatchPhase = "preflight"
	GCRootBatchRetiring  GCRootBatchPhase = "retiring"
)

// GCRootBatchMember is an immutable snapshot of one listener selected for a
// root-wide GC batch. Keeping this outside Entry means a crash followed by a
// new registration cannot silently change the set or exact wake binding which
// a resumed batch is authorized to retire.
type GCRootBatchMember struct {
	EntryID          string      `json:"entry_id"`
	Root             string      `json:"root"`
	Agent            string      `json:"agent"`
	Adapter          string      `json:"adapter"`
	Target           string      `json:"target"`
	WakeOwnerPresent bool        `json:"wake_owner_present"`
	WakeOwner        WakeOwner   `json:"wake_owner"`
	WakeBinding      WakeBinding `json:"wake_binding"`
}

func (m GCRootBatchMember) Matches(entry Entry) bool {
	return entry.ID == m.EntryID && entry.Root == m.Root && entry.Agent == m.Agent &&
		entry.Adapter == m.Adapter && entry.Target == m.Target &&
		entry.WakeOwnerPresent == m.WakeOwnerPresent && entry.WakeOwner == m.WakeOwner &&
		entry.WakeBinding == m.WakeBinding
}

type GCRootBatch struct {
	ID            string              `json:"id"`
	CanonicalRoot string              `json:"canonical_root"`
	StartedAt     time.Time           `json:"started_at"`
	Phase         GCRootBatchPhase    `json:"phase"`
	Members       []GCRootBatchMember `json:"members"`
}

type GCRootAttempt struct {
	CanonicalRoot string    `json:"canonical_root"`
	StartedAt     time.Time `json:"started_at"`
}

// ReattachTransition records the old exact wake which a new reservation may
// have to retire. It intentionally contains no pointers, maps, or slices.
type ReattachTransition struct {
	Phase          TransitionPhase `json:"phase,omitempty"`
	Revision       int64           `json:"revision,omitempty"`
	OldID          string          `json:"old_id,omitempty"`
	OldRoot        string          `json:"old_root,omitempty"`
	OldAgent       string          `json:"old_agent,omitempty"`
	OldAdapter     string          `json:"old_adapter,omitempty"`
	OldTarget      string          `json:"old_target,omitempty"`
	OldBaseline    string          `json:"old_baseline_file,omitempty"`
	OldBaselineSum string          `json:"old_baseline_digest,omitempty"`
	OldOwnerSet    bool            `json:"old_owner_present,omitempty"`
	OldOwner       WakeOwner       `json:"old_owner,omitempty"`
	OldBinding     WakeBinding     `json:"old_binding,omitempty"`
}

func (t ReattachTransition) Active() bool { return t.Phase != TransitionNone }

type Entry struct {
	ID                     string             `json:"id"`
	Root                   string             `json:"root"`
	BaseRoot               string             `json:"base_root,omitempty"`
	SessionName            string             `json:"session_name,omitempty"`
	Agent                  string             `json:"agent"`
	Adapter                string             `json:"adapter"`
	Target                 string             `json:"target"`
	BaselineFile           string             `json:"baseline_file,omitempty"`
	BaselineDigest         string             `json:"baseline_digest,omitempty"`
	State                  State              `json:"state"`
	LastAttach             time.Time          `json:"last_attach,omitempty"`
	LastSeenBySupervisor   time.Time          `json:"last_seen_by_supervisor,omitempty"`
	FailureCount           int                `json:"failure_count,omitempty"`
	BackoffUntil           time.Time          `json:"backoff_until,omitempty"`
	NextHealthCheck        time.Time          `json:"next_health_check,omitempty"`
	DetachedSince          time.Time          `json:"detached_since,omitempty"`
	LastError              string             `json:"last_error,omitempty"`
	LastSupervisorDecision string             `json:"last_supervisor_decision,omitempty"`
	LegacyUnbound          bool               `json:"legacy_unbound,omitempty"`
	WakeOwnerPresent       bool               `json:"wake_owner_present,omitempty"`
	WakeOwner              WakeOwner          `json:"wake_owner,omitempty"`
	WakeBinding            WakeBinding        `json:"wake_binding,omitempty"`
	OwnerGoneSince         time.Time          `json:"owner_gone_since,omitempty"`
	RetiredAt              time.Time          `json:"retired_at,omitempty"`
	RetirementOutcome      string             `json:"retirement_outcome,omitempty"`
	RetirementReason       string             `json:"retirement_reason,omitempty"`
	GCFailureCount         int                `json:"gc_failure_count,omitempty"`
	GCBackoffUntil         time.Time          `json:"gc_backoff_until,omitempty"`
	GCQuarantinedAt        time.Time          `json:"gc_quarantined_at,omitempty"`
	GCQuarantineReason     string             `json:"gc_quarantine_reason,omitempty"`
	LastGCRootBatchAt      time.Time          `json:"last_gc_root_batch_at,omitempty"`
	LastGCDecision         string             `json:"last_gc_decision,omitempty"`
	LastGCReason           string             `json:"last_gc_reason,omitempty"`
	Transition             ReattachTransition `json:"reattach_transition,omitempty"`
}

type File struct {
	SchemaVersion  int             `json:"schema_version"`
	Entries        []Entry         `json:"entries"`
	GCRootBatches  []GCRootBatch   `json:"gc_root_batches,omitempty"`
	GCRootAttempts []GCRootAttempt `json:"gc_root_attempts,omitempty"`
}

type Store struct {
	Path string
	Now  func() time.Time
}

var ErrCorrupt = errors.New("registry file is corrupt")
var ErrTargetOwned = errors.New("adapter target is already owned")
var ErrAmbiguousSession = errors.New("multiple live registry entries exist for one root and agent")

type EntryUpdate struct {
	Before Entry
	After  Entry
}

type UpdateResult struct {
	Updated int
	Skipped int
}

// GCRootBatchAbandonOutcome is one frozen member's exact disposition during
// the explicit stuck-batch escape. Every non-retired member must be
// quarantined; only a check-only lifecycle result may supply a retired After.
type GCRootBatchAbandonOutcome struct {
	Before     Entry
	After      Entry
	Quarantine bool
}

type GCRootBatchAbandonResult struct {
	Quarantined []string
	Retired     []string
}

var processLocks sync.Map
var registrationLocks sync.Map

var saveAbandonedGCRootBatch = func(store *Store, file File) error {
	return store.saveUnlocked(file)
}

var removeMigrationTemp = os.Remove

type registrationSemaphore struct {
	token chan struct{}
}

func newRegistrationSemaphore() *registrationSemaphore {
	semaphore := &registrationSemaphore{token: make(chan struct{}, 1)}
	semaphore.token <- struct{}{}
	return semaphore
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
	var file File
	err := s.withLock(func() error {
		loaded, err := s.loadUnlocked()
		file = loaded
		return err
	})
	return file, err
}

// LoadPreview reads and validates the registry without creating lock files,
// backups, directories, or a migrated registry. Schema v1 is upgraded only in
// the returned in-memory value so callers such as GC dry-run remain genuinely
// non-mutating.
func (s *Store) LoadPreview() (File, error) {
	if s.Path == "" {
		return File{}, errors.New("registry path is required")
	}
	file, _, _, err := s.readRegistryUnlocked()
	if err == nil {
		_, err = normalizeFutureGCRootAttempts(&file, s.now())
	}
	return file, err
}

// WithRegistrationLock serializes the complete attach/reattach transaction
// across processes without blocking ordinary registry readers. Callers hold
// this lease from their fresh target inventory and ownership preflight through
// wake readiness and the final registry commit, so a racing claimant cannot
// start a wake before discovering the winner.
func (s *Store) WithRegistrationLock(fn func() error) error {
	return s.WithRegistrationLockContext(context.Background(), fn)
}

func (s *Store) WithRegistrationLockContext(ctx context.Context, fn func() error) error {
	if s.Path == "" {
		return errors.New("registry path is required")
	}
	if fn == nil {
		return errors.New("registration transaction is required")
	}
	path, err := filepath.Abs(s.Path + ".registration.lock")
	if err != nil {
		return err
	}
	semaphoreValue, _ := registrationLocks.LoadOrStore(path, newRegistrationSemaphore())
	semaphore := semaphoreValue.(*registrationSemaphore)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-semaphore.token:
	}
	defer func() { semaphore.token <- struct{}{} }()

	if err := ensureRegistryDir(filepath.Dir(path)); err != nil {
		return err
	}
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := lock.Chmod(0o600); err != nil {
		return err
	}
	for {
		acquired, err := flockTryExclusive(lock)
		if err != nil {
			return err
		}
		if acquired {
			break
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer flockRelease(lock)
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}

func (s *Store) loadUnlocked() (File, error) {
	return s.loadUnlockedValidated(nil)
}

// loadUnlockedValidated lets a write operation reject an unsafe registry
// shape before even schema migration changes durable state.
func (s *Store) loadUnlockedValidated(validate func(File) error) (File, error) {
	file, data, migrateV1, err := s.readRegistryUnlocked()
	if err != nil {
		return File{}, err
	}
	repairedAttempts, err := normalizeFutureGCRootAttempts(&file, s.now())
	if err != nil {
		return File{}, err
	}
	if err := validateRegistryFile(file); err != nil {
		return File{}, fmt.Errorf("%w %q after GC attempt normalization: %w", ErrCorrupt, s.Path, err)
	}
	if validate != nil {
		if err := validate(file); err != nil {
			return File{}, err
		}
	}
	if migrateV1 {
		if err := s.backupV1Registry(data); err != nil {
			return File{}, fmt.Errorf("back up registry schema v1: %w", err)
		}
	}
	if migrateV1 || repairedAttempts {
		if err := s.saveUnlocked(file); err != nil {
			return File{}, fmt.Errorf("persist registry normalization: %w", err)
		}
	}
	return file, nil
}

// normalizeFutureGCRootAttempts converts an impossible future rolling-window
// ledger into a bounded fail-closed window starting now. Persisting this repair
// lets the rate limit self-recover after one minute instead of remaining wedged
// until an arbitrary future timestamp. A future timestamp which authorizes an
// active frozen batch is never rewritten; that coordinator is quarantined for
// explicit recovery because its immutable start identity includes the time.
func normalizeFutureGCRootAttempts(file *File, now time.Time) (bool, error) {
	activeAttempts := make(map[string]struct{}, len(file.GCRootBatches))
	for _, batch := range file.GCRootBatches {
		key := batch.CanonicalRoot + "\x00" + batch.StartedAt.UTC().Format(time.RFC3339Nano)
		activeAttempts[key] = struct{}{}
	}
	changed := false
	seenClampedRoots := make(map[string]struct{})
	next := make([]GCRootAttempt, 0, len(file.GCRootAttempts))
	for _, attempt := range file.GCRootAttempts {
		if !attempt.StartedAt.After(now) {
			next = append(next, attempt)
			continue
		}
		key := attempt.CanonicalRoot + "\x00" + attempt.StartedAt.UTC().Format(time.RFC3339Nano)
		if _, active := activeAttempts[key]; active {
			return false, fmt.Errorf("future-dated active GC root batch attempt for %q at %s is quarantined", attempt.CanonicalRoot, attempt.StartedAt.UTC().Format(time.RFC3339Nano))
		}
		changed = true
		if _, duplicate := seenClampedRoots[attempt.CanonicalRoot]; duplicate {
			continue
		}
		seenClampedRoots[attempt.CanonicalRoot] = struct{}{}
		attempt.StartedAt = now
		next = append(next, attempt)
	}
	if changed {
		file.GCRootAttempts = next
	}
	return changed, nil
}

func (s *Store) readRegistryUnlocked() (File, []byte, bool, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return File{SchemaVersion: SchemaVersion}, nil, false, nil
	}
	if err != nil {
		return File{}, nil, false, err
	}
	var file File
	if err := json.Unmarshal(data, &file); err != nil {
		return File{}, nil, false, fmt.Errorf("%w %q: %w", ErrCorrupt, s.Path, err)
	}
	if file.SchemaVersion == 0 {
		file.SchemaVersion = 1
	}
	migrateV1 := file.SchemaVersion == 1
	if file.SchemaVersion == 1 {
		for i := range file.Entries {
			file.Entries[i].LegacyUnbound = true
			file.Entries[i].WakeOwnerPresent = false
			file.Entries[i].WakeOwner = WakeOwner{}
			file.Entries[i].WakeBinding = WakeBinding{}
		}
		file.SchemaVersion = SchemaVersion
	}
	if file.SchemaVersion != SchemaVersion {
		return File{}, nil, false, fmt.Errorf("unsupported registry schema version %d", file.SchemaVersion)
	}
	sortEntries(file.Entries)
	if err := validateRegistryFile(file); err != nil {
		return File{}, nil, false, fmt.Errorf("%w %q: %w", ErrCorrupt, s.Path, err)
	}
	return file, data, migrateV1, nil
}

func (s *Store) backupV1Registry(data []byte) error {
	path := s.Path + ".v1.bak"
	if err := validateExistingV1Backup(path, data); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".registry-v1-backup-*.tmp")
	if err != nil {
		return err
	}
	tmpName := file.Name()
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Publishing a fully synced temporary file with link(2) is atomic and
	// refuses to overwrite an existing backup. A racing publisher either wins
	// with its complete file or verifies the already-published complete value.
	if err := os.Link(tmpName, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := validateExistingV1Backup(path, data); err != nil {
			return err
		}
	}
	// The linked backup is the durable migration prerequisite. Sync its
	// directory before treating publication as complete; removal of the
	// now-unreferenced temporary name is only housekeeping and must not turn a
	// successfully published backup into a false migration failure.
	if err := syncDir(dir); err != nil {
		return err
	}
	ok = true
	_ = removeMigrationTemp(tmpName)
	return nil
}

func validateExistingV1Backup(path string, data []byte) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("existing migration backup %q is not a secure 0600 regular file", path)
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("existing migration backup %q does not match the schema v1 registry", path)
	}
	return nil
}

func (s *Store) Save(file File) error {
	if s.Path == "" {
		return errors.New("registry path is required")
	}
	return s.withLock(func() error {
		return s.saveUnlocked(file)
	})
}

func (s *Store) saveUnlocked(file File) error {
	file.SchemaVersion = SchemaVersion
	if err := validateRegistryFile(file); err != nil {
		return fmt.Errorf("refusing invalid registry state: %w", err)
	}
	sortEntries(file.Entries)

	dir := filepath.Dir(s.Path)
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
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		return err
	}
	if err := os.Chmod(s.Path, 0o600); err != nil {
		return err
	}
	return syncDir(dir)
}

func (s *Store) Upsert(entry Entry) (Entry, error) {
	prepared, err := s.prepareEntry(entry)
	if err != nil {
		return Entry{}, err
	}

	err = s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		if owner, ok := conflictingTargetOwner(file.Entries, prepared, false); ok {
			return targetOwnedError(prepared, owner)
		}
		occupied := entryIDs(file.Entries)
		replaced := false
		for i := range file.Entries {
			if file.Entries[i].ID != prepared.ID {
				continue
			}
			if file.Entries[i].State == StateRetired && prepared.State != StateRetired {
				file.Entries[i].ID = nextRetiredArchiveID(file.Entries[i], occupied)
				continue
			}
			if replaced {
				return fmt.Errorf("registry contains duplicate entry id %q", prepared.ID)
			}
			file.Entries[i] = prepared
			replaced = true
		}
		if !replaced {
			file.Entries = append(file.Entries, prepared)
		}
		return s.saveUnlocked(file)
	})
	return prepared, err
}

func (s *Store) ReplaceSessionAdapter(entry Entry) (Entry, []Entry, error) {
	prepared, err := s.prepareEntry(entry)
	if err != nil {
		return Entry{}, nil, err
	}

	var removed []Entry
	err = s.withLock(func() error {
		file, err := s.loadUnlockedValidated(func(file File) error {
			return validateSingleLiveSessionEntry(file.Entries, prepared.Root, prepared.Agent)
		})
		if err != nil {
			return err
		}
		if owner, ok := conflictingTargetOwner(file.Entries, prepared, true); ok {
			return targetOwnedError(prepared, owner)
		}
		next := make([]Entry, 0, len(file.Entries)+1)
		livePrevious := make([]Entry, 0, 1)
		occupied := entryIDs(file.Entries)
		var revision int64
		for _, existing := range file.Entries {
			// AMQ permits one wake process per root and agent. Reattach therefore
			// replaces the old registration even when the terminal adapter changed.
			if existing.Root == prepared.Root && existing.Agent == prepared.Agent && existing.State != StateRetired {
				removed = append(removed, existing)
				livePrevious = append(livePrevious, existing)
				if existing.Transition.Revision > revision {
					revision = existing.Transition.Revision
				}
				continue
			}
			if existing.State == StateRetired && existing.ID == prepared.ID {
				existing.ID = nextRetiredArchiveID(existing, occupied)
			}
			next = append(next, existing)
		}
		if len(livePrevious) == 1 {
			old := livePrevious[0]
			if old.Transition.Active() {
				prepared.Transition = old.Transition
				prepared.Transition.Revision = revision + 1
			} else {
				prepared.Transition = ReattachTransition{
					Phase: TransitionReserved, Revision: revision + 1,
					OldID: old.ID, OldRoot: old.Root, OldAgent: old.Agent,
					OldAdapter: old.Adapter, OldTarget: old.Target,
					OldBaseline: old.BaselineFile, OldBaselineSum: old.BaselineDigest,
					OldOwnerSet: old.WakeOwnerPresent, OldOwner: old.WakeOwner,
					OldBinding: old.WakeBinding,
				}
			}
		}
		next = append(next, prepared)
		file.Entries = next
		return s.saveUnlocked(file)
	})
	return prepared, removed, err
}

// RestoreSessionAdapterIfUnchanged rolls back a pre-wake reattach reservation
// only while that exact inactive row is still authoritative. It restores the
// prior live root/agent row atomically while retained retirement history stays
// in place. A concurrent change wins and keeps the recoverable reservation
// instead of being overwritten.
func (s *Store) RestoreSessionAdapterIfUnchanged(expected Entry, previous []Entry) (bool, error) {
	if expected.ID == "" {
		return false, errors.New("expected reservation id is required")
	}
	restored := false
	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		index := -1
		for i, entry := range file.Entries {
			if entry.State != StateRetired && entry.Root == expected.Root && entry.Agent == expected.Agent && entry.ID != expected.ID {
				return nil
			}
			if entry.ID == expected.ID {
				if entry != expected {
					return nil
				}
				index = i
			}
		}
		if index < 0 {
			return nil
		}
		next := make([]Entry, 0, len(file.Entries)-1+len(previous))
		next = append(next, file.Entries[:index]...)
		next = append(next, file.Entries[index+1:]...)
		for _, entry := range previous {
			if entry.State != StateRetired {
				next = append(next, entry)
			}
		}
		file.Entries = next
		if err := s.saveUnlocked(file); err != nil {
			return err
		}
		restored = true
		return nil
	})
	return restored, err
}

// CheckTargetAvailable is the read-only preflight used before a reattach
// touches AMQ. ReplaceSessionAdapter repeats the check under its write lock, so
// a racing owner still fails closed before registry mutation.
func (s *Store) CheckTargetAvailable(entry Entry, ignoreSameRootAgent bool) error {
	prepared, err := s.prepareEntry(entry)
	if err != nil {
		return err
	}
	return s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		if owner, ok := conflictingTargetOwner(file.Entries, prepared, ignoreSameRootAgent); ok {
			return targetOwnedError(prepared, owner)
		}
		return nil
	})
}

func (s *Store) prepareEntry(entry Entry) (Entry, error) {
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
	return entry, nil
}

func (s *Store) UpdateEntry(entry Entry) error {
	return s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for i := range file.Entries {
			if file.Entries[i].ID == entry.ID {
				file.Entries[i] = entry
				return s.saveUnlocked(file)
			}
		}
		return fmt.Errorf("registry entry %q not found", entry.ID)
	})
}

// UpdateEntries applies a supervisor pass under one lock and one atomic save.
// Each update is an optimistic compare-and-swap: a concurrent attach,
// reattach, forget, or supervisor pass wins over a stale snapshot instead of
// being overwritten or resurrected.
func (s *Store) UpdateEntries(updates []EntryUpdate) (UpdateResult, error) {
	var result UpdateResult
	seen := make(map[string]struct{}, len(updates))
	for _, update := range updates {
		if update.Before.ID == "" || update.After.ID == "" {
			return result, errors.New("batch update entry id is required")
		}
		if update.Before.ID != update.After.ID {
			return result, fmt.Errorf("batch update changes entry id from %q to %q", update.Before.ID, update.After.ID)
		}
		if _, ok := seen[update.Before.ID]; ok {
			return result, fmt.Errorf("batch update contains duplicate entry %q", update.Before.ID)
		}
		seen[update.Before.ID] = struct{}{}
	}
	if len(updates) == 0 {
		return result, nil
	}

	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		byID := make(map[string]int, len(file.Entries))
		for i := range file.Entries {
			byID[file.Entries[i].ID] = i
		}
		for _, update := range updates {
			i, ok := byID[update.Before.ID]
			if !ok || file.Entries[i] != update.Before {
				result.Skipped++
				continue
			}
			if update.Before == update.After {
				continue
			}
			file.Entries[i] = update.After
			result.Updated++
		}
		if result.Updated == 0 {
			return nil
		}
		return s.saveUnlocked(file)
	})
	return result, err
}

// StartGCRootBatch atomically freezes the exact listener membership and marks
// the selected root as having consumed a GC-window slot. expected must contain
// every non-retired listener row in the canonical root selected by the caller.
// The optimistic comparisons prevent a registration change from being folded
// into the frozen batch.
func (s *Store) StartGCRootBatch(batch GCRootBatch, expected []Entry) (File, error) {
	if err := validateGCRootBatch(batch); err != nil {
		return File{}, err
	}
	if len(expected) != len(batch.Members) {
		return File{}, fmt.Errorf("GC root batch expected %d rows for %d members", len(expected), len(batch.Members))
	}
	expectedByID := make(map[string]Entry, len(expected))
	for _, entry := range expected {
		if entry.ID == "" {
			return File{}, errors.New("GC root batch expected entry id is required")
		}
		if _, exists := expectedByID[entry.ID]; exists {
			return File{}, fmt.Errorf("GC root batch contains duplicate expected entry %q", entry.ID)
		}
		expectedByID[entry.ID] = entry
	}
	for _, member := range batch.Members {
		entry, ok := expectedByID[member.EntryID]
		if !ok || !member.Matches(entry) {
			return File{}, fmt.Errorf("GC root batch member %q does not match its expected entry", member.EntryID)
		}
	}

	var updated File
	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		if len(file.GCRootBatches) != 0 {
			return errors.New("a GC root batch is already active")
		}
		byID := make(map[string]int, len(file.Entries))
		for i := range file.Entries {
			byID[file.Entries[i].ID] = i
		}
		for _, expectedEntry := range expected {
			i, ok := byID[expectedEntry.ID]
			if !ok || file.Entries[i] != expectedEntry {
				return fmt.Errorf("GC root batch entry %q changed before membership could be frozen", expectedEntry.ID)
			}
			file.Entries[i].LastGCRootBatchAt = batch.StartedAt
		}
		attempts, err := appendGCRootAttempt(file.GCRootAttempts, batch.CanonicalRoot, batch.StartedAt)
		if err != nil {
			return err
		}
		file.GCRootAttempts = attempts
		file.GCRootBatches = append(file.GCRootBatches, batch)
		if err := s.saveUnlocked(file); err != nil {
			return err
		}
		updated = file
		return nil
	})
	return updated, err
}

// RecordGCRootAttempt atomically persists a non-starting root attempt such as
// an oversized-root refusal together with its per-entry diagnostics. The
// top-level rolling ledger survives a subsequent reattach which replaces the
// live row carrying LastGCRootBatchAt.
func (s *Store) RecordGCRootAttempt(canonicalRoot string, startedAt time.Time, updates []EntryUpdate) (File, error) {
	if strings.TrimSpace(canonicalRoot) == "" || startedAt.IsZero() {
		return File{}, errors.New("GC root attempt canonical root and start time are required")
	}
	seen := make(map[string]struct{}, len(updates))
	for _, update := range updates {
		if update.Before.ID == "" || update.Before.ID != update.After.ID {
			return File{}, errors.New("GC root attempt update requires one unchanged entry id")
		}
		if _, exists := seen[update.Before.ID]; exists {
			return File{}, fmt.Errorf("GC root attempt contains duplicate update %q", update.Before.ID)
		}
		seen[update.Before.ID] = struct{}{}
	}
	var updated File
	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		byID := make(map[string]int, len(file.Entries))
		for i := range file.Entries {
			byID[file.Entries[i].ID] = i
		}
		for _, update := range updates {
			i, ok := byID[update.Before.ID]
			if !ok || file.Entries[i] != update.Before {
				return fmt.Errorf("GC root attempt entry %q changed before its marker could be recorded", update.Before.ID)
			}
			file.Entries[i] = update.After
		}
		attempts, err := appendGCRootAttempt(file.GCRootAttempts, canonicalRoot, startedAt)
		if err != nil {
			return err
		}
		file.GCRootAttempts = attempts
		if err := s.saveUnlocked(file); err != nil {
			return err
		}
		updated = file
		return nil
	})
	return updated, err
}

func appendGCRootAttempt(existing []GCRootAttempt, canonicalRoot string, startedAt time.Time) ([]GCRootAttempt, error) {
	windowStart := startedAt.Add(-GCRootAttemptWindow)
	next := make([]GCRootAttempt, 0, MaxGCRootAttempts)
	recentRoots := make(map[string]struct{}, MaxGCRootAttempts)
	for _, attempt := range existing {
		if attempt.StartedAt.Before(windowStart) {
			continue
		}
		if attempt.StartedAt.After(startedAt) {
			attempt.StartedAt = startedAt
		}
		if _, duplicate := recentRoots[attempt.CanonicalRoot]; duplicate {
			continue
		}
		recentRoots[attempt.CanonicalRoot] = struct{}{}
		next = append(next, attempt)
	}
	if _, duplicate := recentRoots[canonicalRoot]; duplicate {
		return nil, fmt.Errorf("canonical root %q already consumed a GC attempt in the rolling window", canonicalRoot)
	}
	if len(recentRoots) >= MaxGCRootAttempts {
		return nil, fmt.Errorf("hard maximum of %d distinct GC roots in the rolling window is exhausted", MaxGCRootAttempts)
	}
	next = append(next, GCRootAttempt{CanonicalRoot: canonicalRoot, StartedAt: startedAt})
	sort.Slice(next, func(i, j int) bool {
		if next[i].StartedAt.Equal(next[j].StartedAt) {
			return next[i].CanonicalRoot < next[j].CanonicalRoot
		}
		return next[i].StartedAt.Before(next[j].StartedAt)
	})
	return next, nil
}

func validateGCRootBatch(batch GCRootBatch) error {
	if strings.TrimSpace(batch.ID) == "" || strings.TrimSpace(batch.CanonicalRoot) == "" || batch.StartedAt.IsZero() {
		return errors.New("GC root batch id, canonical root, and start time are required")
	}
	if batch.Phase != GCRootBatchPreflight && batch.Phase != GCRootBatchRetiring {
		return fmt.Errorf("GC root batch %q has invalid phase %q", batch.ID, batch.Phase)
	}
	if len(batch.Members) == 0 {
		return fmt.Errorf("GC root batch %q has no members", batch.ID)
	}
	if len(batch.Members) > MaxGCRootBatchMembers {
		return fmt.Errorf("GC root batch %q has %d members; hard maximum is %d", batch.ID, len(batch.Members), MaxGCRootBatchMembers)
	}
	canonicalRoot, err := canonicalRegistryRoot(batch.CanonicalRoot)
	if err != nil || canonicalRoot != batch.CanonicalRoot {
		return fmt.Errorf("GC root batch %q canonical root %q is invalid", batch.ID, batch.CanonicalRoot)
	}
	seen := make(map[string]struct{}, len(batch.Members))
	agents := make(map[string]struct{}, len(batch.Members))
	for _, member := range batch.Members {
		if member.EntryID == "" || member.Root == "" || member.Agent == "" || member.Adapter == "" || member.Target == "" {
			return fmt.Errorf("GC root batch %q has an incomplete member", batch.ID)
		}
		if _, exists := seen[member.EntryID]; exists {
			return fmt.Errorf("GC root batch %q contains duplicate member %q", batch.ID, member.EntryID)
		}
		memberRoot, err := canonicalRegistryRoot(member.Root)
		if err != nil || memberRoot != canonicalRoot {
			return fmt.Errorf("GC root batch %q member %q is outside canonical root %q", batch.ID, member.EntryID, canonicalRoot)
		}
		if !member.WakeOwnerPresent || !member.WakeOwner.Strong() || !member.WakeBinding.Complete() {
			return fmt.Errorf("GC root batch %q member %q lacks a strong owner-bound wake binding", batch.ID, member.EntryID)
		}
		if _, duplicate := agents[member.Agent]; duplicate {
			return fmt.Errorf("GC root batch %q contains multiple live members for agent %q", batch.ID, member.Agent)
		}
		agents[member.Agent] = struct{}{}
		seen[member.EntryID] = struct{}{}
	}
	return nil
}

func validateRegistryFile(file File) error {
	if len(file.GCRootBatches) > 1 {
		return fmt.Errorf("registry contains %d active GC root batches", len(file.GCRootBatches))
	}
	if len(file.GCRootAttempts) > MaxGCRootAttempts {
		return fmt.Errorf("registry contains %d GC root attempts; hard maximum is %d", len(file.GCRootAttempts), MaxGCRootAttempts)
	}
	attemptKeys := make(map[string]struct{}, len(file.GCRootAttempts))
	for _, attempt := range file.GCRootAttempts {
		root, err := canonicalRegistryRoot(attempt.CanonicalRoot)
		if err != nil || root != attempt.CanonicalRoot || attempt.StartedAt.IsZero() {
			return fmt.Errorf("registry contains an invalid GC root attempt for %q", attempt.CanonicalRoot)
		}
		key := root + "\x00" + attempt.StartedAt.UTC().Format(time.RFC3339Nano)
		if _, duplicate := attemptKeys[key]; duplicate {
			return fmt.Errorf("registry contains duplicate GC root attempt %q", key)
		}
		attemptKeys[key] = struct{}{}
	}

	byID := make(map[string]Entry, len(file.Entries))
	for _, entry := range file.Entries {
		if _, duplicate := byID[entry.ID]; duplicate {
			return fmt.Errorf("registry contains duplicate entry id %q", entry.ID)
		}
		quarantineReasonPresent := strings.TrimSpace(entry.GCQuarantineReason) != ""
		if entry.GCQuarantinedAt.IsZero() == quarantineReasonPresent {
			return fmt.Errorf("registry entry %q has incomplete GC quarantine metadata", entry.ID)
		}
		if entry.State == StateRetired && !entry.GCQuarantinedAt.IsZero() {
			return fmt.Errorf("registry entry %q is both retired and GC-quarantined", entry.ID)
		}
		byID[entry.ID] = entry
	}

	for _, batch := range file.GCRootBatches {
		if err := validateGCRootBatch(batch); err != nil {
			return err
		}
		attemptKey := batch.CanonicalRoot + "\x00" + batch.StartedAt.UTC().Format(time.RFC3339Nano)
		if _, ok := attemptKeys[attemptKey]; !ok {
			return fmt.Errorf("GC root batch %q lacks its durable rolling-window attempt", batch.ID)
		}
		members := make(map[string]GCRootBatchMember, len(batch.Members))
		for _, member := range batch.Members {
			entry, ok := byID[member.EntryID]
			if !ok || !member.Matches(entry) {
				return fmt.Errorf("GC root batch %q member %q does not match the current registry row", batch.ID, member.EntryID)
			}
			if entry.State != StateRetired && (!entry.GCQuarantinedAt.IsZero() || entry.LegacyUnbound || entry.Transition.Active() || !entry.WakeOwnerPresent || !entry.WakeOwner.Strong() || !entry.WakeBinding.Complete()) {
				return fmt.Errorf("GC root batch %q current member %q is not transition-free and strongly owner-bound", batch.ID, member.EntryID)
			}
			members[member.EntryID] = member
		}
		for _, entry := range file.Entries {
			if entry.State == StateRetired {
				continue
			}
			root, err := canonicalRegistryRoot(entry.Root)
			if err != nil {
				return err
			}
			if root == batch.CanonicalRoot {
				if _, frozen := members[entry.ID]; !frozen {
					return fmt.Errorf("GC root batch %q omits current listener %q from its frozen membership", batch.ID, entry.ID)
				}
			}
		}
	}
	return nil
}

func canonicalRegistryRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", errors.New("root is empty")
	}
	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", err
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(real), nil
	}
	return filepath.Clean(abs), nil
}

// AdvanceGCRootBatch moves an immutable batch from preflight to retirement.
// The exact phase comparison makes a duplicate/replayed transition harmless.
func (s *Store) AdvanceGCRootBatch(id string, from, to GCRootBatchPhase) (bool, error) {
	advanced := false
	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for i := range file.GCRootBatches {
			batch := &file.GCRootBatches[i]
			if batch.ID != id {
				continue
			}
			if batch.Phase == to {
				return nil
			}
			if batch.Phase != from {
				return fmt.Errorf("GC root batch %q phase is %q, want %q", id, batch.Phase, from)
			}
			batch.Phase = to
			if err := s.saveUnlocked(file); err != nil {
				return err
			}
			advanced = true
			return nil
		}
		return fmt.Errorf("GC root batch %q not found", id)
	})
	return advanced, err
}

// FinishGCRootBatch removes only the durable coordinator record. Per-entry
// LastGCRootBatchAt markers remain as the rolling-window ledger.
func (s *Store) FinishGCRootBatch(id string) (bool, error) {
	finished := false
	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for i, batch := range file.GCRootBatches {
			if batch.ID != id {
				continue
			}
			file.GCRootBatches = append(file.GCRootBatches[:i], file.GCRootBatches[i+1:]...)
			if err := s.saveUnlocked(file); err != nil {
				return err
			}
			finished = true
			return nil
		}
		return nil
	})
	return finished, err
}

// AbandonGCRootBatch atomically verifies the exact frozen batch and current
// member rows, applies their check-only outcomes, quarantines every unresolved
// member, and removes the coordinator. A save failure leaves the on-disk batch
// and all rows unchanged.
func (s *Store) AbandonGCRootBatch(expected GCRootBatch, outcomes []GCRootBatchAbandonOutcome, now time.Time, reason string) (GCRootBatchAbandonResult, error) {
	var result GCRootBatchAbandonResult
	if err := validateGCRootBatch(expected); err != nil {
		return result, err
	}
	if now.IsZero() || strings.TrimSpace(reason) == "" {
		return result, errors.New("GC root batch abandon time and reason are required")
	}
	if len(outcomes) != len(expected.Members) {
		return result, fmt.Errorf("GC root batch %q abandon has %d outcomes for %d members", expected.ID, len(outcomes), len(expected.Members))
	}
	outcomesByID := make(map[string]GCRootBatchAbandonOutcome, len(outcomes))
	for _, outcome := range outcomes {
		if outcome.Before.ID == "" || outcome.Before.ID != outcome.After.ID {
			return result, errors.New("GC root batch abandon outcome requires one unchanged entry id")
		}
		if _, duplicate := outcomesByID[outcome.Before.ID]; duplicate {
			return result, fmt.Errorf("GC root batch abandon contains duplicate outcome %q", outcome.Before.ID)
		}
		outcomesByID[outcome.Before.ID] = outcome
	}

	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		if len(file.GCRootBatches) != 1 || !sameGCRootBatch(file.GCRootBatches[0], expected) {
			return fmt.Errorf("GC root batch %q changed before explicit abandonment", expected.ID)
		}
		byID := make(map[string]int, len(file.Entries))
		for index := range file.Entries {
			byID[file.Entries[index].ID] = index
		}
		for _, member := range expected.Members {
			outcome, ok := outcomesByID[member.EntryID]
			if !ok || !member.Matches(outcome.Before) || !member.Matches(outcome.After) {
				return fmt.Errorf("GC root batch %q abandon outcome %q changed frozen identity", expected.ID, member.EntryID)
			}
			index, ok := byID[member.EntryID]
			if !ok || file.Entries[index] != outcome.Before {
				return fmt.Errorf("GC root batch %q member %q changed before explicit abandonment", expected.ID, member.EntryID)
			}
			updated := outcome.After
			if updated.State != StateRetired {
				if !outcome.Quarantine {
					return fmt.Errorf("GC root batch %q unresolved member %q is not quarantined", expected.ID, member.EntryID)
				}
				updated.GCQuarantinedAt = now.UTC()
				updated.GCQuarantineReason = reason
				updated.LastGCDecision = "skipped"
				updated.LastGCReason = "operator_abandoned_batch"
				result.Quarantined = append(result.Quarantined, member.EntryID)
			} else {
				if outcome.Quarantine {
					return fmt.Errorf("GC root batch %q retired member %q cannot also be quarantined", expected.ID, member.EntryID)
				}
				result.Retired = append(result.Retired, member.EntryID)
			}
			file.Entries[index] = updated
		}
		file.GCRootBatches = nil
		if err := saveAbandonedGCRootBatch(s, file); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return GCRootBatchAbandonResult{}, err
	}
	sort.Strings(result.Quarantined)
	sort.Strings(result.Retired)
	return result, nil
}

func sameGCRootBatch(left, right GCRootBatch) bool {
	return left.ID == right.ID && left.CanonicalRoot == right.CanonicalRoot &&
		left.StartedAt.Equal(right.StartedAt) && left.Phase == right.Phase &&
		slices.Equal(left.Members, right.Members)
}

func (s *Store) Forget(id string) (bool, error) {
	removed, err := s.ForgetMany([]string{id})
	return removed == 1, err
}

// ForgetIfUnchanged removes exactly the entry that was inspected. A concurrent
// reattach or supervisor update causes a safe skip instead of deleting newer
// state.
func (s *Store) ForgetIfUnchanged(expected Entry) (bool, error) {
	if expected.ID == "" {
		return false, errors.New("expected entry id is required")
	}
	removed := false
	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		for i, entry := range file.Entries {
			if entry.ID != expected.ID {
				continue
			}
			if entry != expected {
				return nil
			}
			file.Entries = append(file.Entries[:i], file.Entries[i+1:]...)
			removed = true
			return s.saveUnlocked(file)
		}
		return nil
	})
	return removed, err
}

func (s *Store) ForgetMany(ids []string) (int, error) {
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			wanted[id] = struct{}{}
		}
	}
	removed := 0
	err := s.withLock(func() error {
		file, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		found := 0
		for _, entry := range file.Entries {
			if _, ok := wanted[entry.ID]; ok {
				found++
			}
		}
		if found != len(wanted) {
			return fmt.Errorf("found %d of %d registry entries requested for removal", found, len(wanted))
		}
		next := file.Entries[:0]
		for _, entry := range file.Entries {
			if _, ok := wanted[entry.ID]; ok {
				removed++
				continue
			}
			next = append(next, entry)
		}
		file.Entries = next
		return s.saveUnlocked(file)
	})
	return removed, err
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

func validateSingleLiveSessionEntry(entries []Entry, root, agent string) error {
	count := 0
	for _, entry := range entries {
		if entry.State != StateRetired && entry.Root == root && entry.Agent == agent {
			count++
		}
	}
	if count > 1 {
		return fmt.Errorf("%w: root=%q agent=%q count=%d", ErrAmbiguousSession, root, agent, count)
	}
	return nil
}

func entryIDs(entries []Entry) map[string]struct{} {
	ids := make(map[string]struct{}, len(entries)+1)
	for _, entry := range entries {
		ids[entry.ID] = struct{}{}
	}
	return ids
}

// nextRetiredArchiveID assigns history a durable identity distinct from the
// live tuple ID. The ID is derived once from the exact retired snapshot and is
// never recomputed when later GC observations update that archival row.
func nextRetiredArchiveID(entry Entry, occupied map[string]struct{}) string {
	payload, _ := json.Marshal(entry) // Entry contains only JSON-supported value fields.
	for sequence := 0; ; sequence++ {
		material := make([]byte, 0, len(payload)+32)
		material = append(material, "retired-archive-v1\x00"...)
		material = append(material, payload...)
		material = append(material, fmt.Sprintf("\x00%d", sequence)...)
		sum := sha256.Sum256(material)
		id := "retired-" + hex.EncodeToString(sum[:])
		if _, exists := occupied[id]; exists {
			continue
		}
		occupied[id] = struct{}{}
		return id
	}
}

func conflictingTargetOwner(entries []Entry, candidate Entry, ignoreSameRootAgent bool) (Entry, bool) {
	candidateTarget := canonicalStoredTarget(candidate.Adapter, candidate.Target)
	for _, existing := range entries {
		if existing.State == StateRetired {
			continue
		}
		if existing.ID == candidate.ID {
			continue
		}
		if ignoreSameRootAgent && existing.Root == candidate.Root && existing.Agent == candidate.Agent {
			continue
		}
		if existing.Adapter == candidate.Adapter && canonicalStoredTarget(existing.Adapter, existing.Target) == candidateTarget {
			return existing, true
		}
	}
	return Entry{}, false
}

func canonicalStoredTarget(adapterName, target string) string {
	target = strings.TrimSpace(target)
	if adapterName == "cmux" {
		// UUIDs are case-insensitive. This also protects new canonical writers
		// from legacy registry rows which persisted lower-case cmux targets.
		return strings.ToLower(target)
	}
	return target
}

func targetOwnedError(candidate, owner Entry) error {
	return fmt.Errorf(
		"%w: adapter=%q target=%q requested_by=%s@%s existing_owner=%s@%s existing_id=%s",
		ErrTargetOwned,
		candidate.Adapter,
		candidate.Target,
		candidate.Agent,
		candidate.Root,
		owner.Agent,
		owner.Root,
		owner.ID,
	)
}

func (s *Store) withLock(fn func() error) error {
	if s.Path == "" {
		return errors.New("registry path is required")
	}
	path, err := filepath.Abs(s.Path)
	if err != nil {
		return err
	}
	mutexValue, _ := processLocks.LoadOrStore(path, &sync.Mutex{})
	mutex := mutexValue.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Dir(s.Path)
	if err := ensureRegistryDir(dir); err != nil {
		return err
	}

	lock, err := os.OpenFile(s.Path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := lock.Chmod(0o600); err != nil {
		return err
	}
	if err := flockExclusive(lock); err != nil {
		return err
	}
	defer flockRelease(lock)

	return fn()
}

func ensureRegistryDir(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
