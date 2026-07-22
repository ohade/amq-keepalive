package amq

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

var ErrAlreadyRunning = errors.New("amq wake already running")
var ErrWakeReadinessUncertain = errors.New("amq wake readiness is uncertain; child was left unsignaled")

const defaultWakeReadyTimeout = 10 * time.Second
const defaultCommandTimeout = 5 * time.Second
const staleWakeReadyMarkerAge = 24 * time.Hour
const maxWakeBaselineBytes = 64 * 1024
const maxLifecycleResultBytes = 64 * 1024
const maxCLIOutputBytes = 64 * 1024

const CapabilityWakeGCV1 = "wake_gc_v1"

type Env struct {
	SchemaVersion int               `json:"schema_version"`
	AMQVersion    string            `json:"amq_version"`
	Root          string            `json:"root"`
	BaseRoot      string            `json:"base_root"`
	SessionName   string            `json:"session_name"`
	InSession     bool              `json:"in_session"`
	Me            string            `json:"me"`
	Project       string            `json:"project"`
	RootSource    string            `json:"root_source"`
	Peers         map[string]string `json:"peers"`
	Shell         string            `json:"shell"`
	Capabilities  []string          `json:"capabilities,omitempty"`
}

func (e Env) HasCapability(capability string) bool {
	return slices.Contains(e.Capabilities, capability)
}

type WakeOwner struct {
	PID          int    `json:"pid"`
	ProcessStart string `json:"process_start"`
	BootID       string `json:"boot_id"`
	SessionID    int    `json:"session_id,omitempty"`
}

func (o WakeOwner) Strong() bool {
	return o.PID > 0 && strings.TrimSpace(o.ProcessStart) != "" && strings.TrimSpace(o.BootID) != "" && o.SessionID >= 0
}

func ParseWakeOwner(raw string) (WakeOwner, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return WakeOwner{}, errors.New("AMQ_WAKE_OWNER is missing")
	}
	if len(raw) > 4096 {
		return WakeOwner{}, errors.New("AMQ_WAKE_OWNER exceeds 4096 bytes")
	}
	var owner WakeOwner
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&owner); err != nil {
		return WakeOwner{}, fmt.Errorf("AMQ_WAKE_OWNER is invalid: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return WakeOwner{}, errors.New("AMQ_WAKE_OWNER contains trailing JSON data")
	}
	if !owner.Strong() {
		return WakeOwner{}, errors.New("AMQ_WAKE_OWNER lacks a strong pid/process-start/boot identity")
	}
	if strings.ContainsRune(owner.ProcessStart, 0) || strings.ContainsRune(owner.BootID, 0) {
		return WakeOwner{}, errors.New("AMQ_WAKE_OWNER contains NUL")
	}
	return owner, nil
}

func WakeOwnerFromEnvironment() (WakeOwner, error) {
	if detail := strings.TrimSpace(os.Getenv("AMQ_WAKE_OWNER_ERROR")); detail != "" {
		return WakeOwner{}, fmt.Errorf("AMQ_WAKE_OWNER_ERROR: %s", detail)
	}
	return ParseWakeOwner(os.Getenv("AMQ_WAKE_OWNER"))
}

type WakeBinding struct {
	Generation   string `json:"generation"`
	TargetDigest string `json:"target_digest"`
}

func (b WakeBinding) Complete() bool {
	return strings.TrimSpace(b.Generation) != "" && strings.TrimSpace(b.TargetDigest) != ""
}

type WakeRepairResult struct {
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (r WakeRepairResult) Text() string {
	return strings.TrimSpace(strings.Join([]string{r.Status, r.Reason, r.Message, r.Error}, " "))
}

type StartWakeRequest struct {
	Root           string
	Me             string
	InjectVia      string
	Adapter        string
	Target         string
	BaselineFile   string
	BaselineDigest string
	Timeout        time.Duration
	Owner          WakeOwner
}

type WakeCommandResult struct {
	Schema              int    `json:"schema,omitempty"`
	Status              string `json:"status"`
	ReasonCode          string `json:"reason_code,omitempty"`
	Reason              string `json:"reason,omitempty"`
	Root                string `json:"root,omitempty"`
	Agent               string `json:"agent,omitempty"`
	CurrentGeneration   string `json:"current_generation,omitempty"`
	CurrentTargetDigest string `json:"current_target_digest,omitempty"`
	CurrentWakeMode     string `json:"current_wake_mode,omitempty"`
	Generation          string `json:"generation,omitempty"`
	TargetDigest        string `json:"target_digest,omitempty"`
}

type WakeStartError struct {
	Result WakeCommandResult
	Cause  error
}

func (e *WakeStartError) Error() string {
	if e.Result.ReasonCode != "" {
		return fmt.Sprintf("amq wake failed: status=%s reason_code=%s", e.Result.Status, e.Result.ReasonCode)
	}
	return fmt.Sprintf("amq wake failed: %v", e.Cause)
}

func (e *WakeStartError) Unwrap() error { return e.Cause }

type RetireWakeRequest struct {
	Root             string
	Me               string
	InjectVia        string
	Adapter          string
	Target           string
	Generation       string
	TargetDigest     string
	RequireOwnerGone bool
	Check            bool
	Timeout          time.Duration
}

type RetireWakeResult = WakeCommandResult

type CLI struct {
	Path string
}

func NewCLI(path string) CLI {
	if path == "" {
		path = "amq"
	}
	return CLI{Path: path}
}

func (c CLI) Env(ctx context.Context) (Env, error) {
	commandCtx, cancel := context.WithTimeout(ctx, defaultCommandTimeout)
	defer cancel()
	stdout, stderr, err := c.run(commandCtx, "env", "--json")
	if err != nil {
		if commandCtx.Err() != nil {
			return Env{}, fmt.Errorf("amq env timed out or was canceled: %w", commandCtx.Err())
		}
		return Env{}, fmt.Errorf("amq env failed: %w: %s", err, strings.TrimSpace(stderr))
	}
	var env Env
	if err := decodeStrictJSON(stdout, &env); err != nil {
		return Env{}, fmt.Errorf("parse amq env: %w", err)
	}
	if env.SchemaVersion != 1 {
		return Env{}, fmt.Errorf("parse amq env: unsupported schema_version %d", env.SchemaVersion)
	}
	return env, nil
}

func (c CLI) RepairWake(ctx context.Context, root, me string) (WakeRepairResult, error) {
	args := []string{"wake", "repair", "-json"}
	if root != "" {
		args = append(args, "-root", root)
	}
	if me != "" {
		args = append(args, "-me", me)
	}
	stdout, stderr, err := c.run(ctx, args...)
	result, parseErr := parseWakeRepair(stdout)
	if parseErr != nil {
		if err != nil {
			return WakeRepairResult{
				Status: "error",
				Error:  strings.TrimSpace(strings.Join([]string{err.Error(), stderr}, ": ")),
			}, err
		}
		return WakeRepairResult{}, parseErr
	}
	if result.Error == "" && len(stderr) > 0 {
		result.Error = strings.TrimSpace(stderr)
	}
	return result, err
}

func (c CLI) StartWake(ctx context.Context, req StartWakeRequest) (WakeBinding, error) {
	if req.InjectVia == "" {
		return WakeBinding{}, errors.New("inject-via executable is required")
	}
	if req.Adapter == "" {
		return WakeBinding{}, errors.New("adapter is required")
	}
	if req.Target == "" {
		return WakeBinding{}, errors.New("target is required")
	}
	if !req.Owner.Strong() {
		return WakeBinding{}, errors.New("strong wake owner is required")
	}
	baselineFile := strings.TrimSpace(req.BaselineFile)
	if (baselineFile == "") != (strings.TrimSpace(req.BaselineDigest) == "") {
		return WakeBinding{}, errors.New("baseline file and digest must be set together")
	}
	if baselineFile != "" {
		digest, err := BaselineDigest(baselineFile)
		if err != nil {
			return WakeBinding{}, fmt.Errorf("validate wake baseline: %w", err)
		}
		if digest != req.BaselineDigest {
			return WakeBinding{}, errors.New("wake baseline digest changed since registration")
		}
	}

	args := []string{"wake"}
	_, readyFile, err := newWakeReadyPath()
	if err != nil {
		return WakeBinding{}, err
	}
	_, resultFile, err := newWakeReadyPath()
	if err != nil {
		return WakeBinding{}, err
	}
	defer func() { _ = os.Remove(resultFile) }()

	if req.Root != "" {
		args = append(args, "-root", req.Root)
	}
	if req.Me != "" {
		args = append(args, "-me", req.Me)
	}
	if baselineFile != "" {
		args = append(args, "--baseline-file", baselineFile, "--replace-existing-baseline")
	} else {
		args = append(args, "--baseline-existing")
	}
	args = append(args,
		"-inject-via", req.InjectVia,
		"-inject-arg", "inject",
		"-inject-arg", req.Adapter,
		"-inject-arg", req.Target,
		"--accept-existing-wake",
		"--require-owner",
		"-ready-file", readyFile,
		"--result-file", resultFile,
	)

	// The wake process is intentionally longer-lived than this invocation (and
	// than the supervisor which launched it). Do not use exec.CommandContext:
	// its cancellation goroutine would kill an already-ready wake when the
	// supervisor receives SIGTERM. After spawning, cancellation never signals
	// the child; a durable registry reservation lets a later pass converge.
	cmd := exec.Command(c.Path, args...)
	ownerJSON, err := json.Marshal(req.Owner)
	if err != nil {
		return WakeBinding{}, fmt.Errorf("marshal wake owner: %w", err)
	}
	cmd.Env = setExactEnvironment(os.Environ(), "AMQ_WAKE_OWNER", string(ownerJSON), "AMQ_WAKE_OWNER_ERROR")
	configureWakeProcess(cmd)
	if err := ctx.Err(); err != nil {
		_ = os.Remove(readyFile)
		return WakeBinding{}, err
	}
	if err := cmd.Start(); err != nil {
		_ = os.Remove(readyFile)
		return WakeBinding{}, err
	}
	done := make(chan wakeProcessResult, 1)
	go func() {
		err := cmd.Wait()
		binding, readyErr := readWakeBinding(readyFile)
		ready := readyErr == nil
		_ = os.Remove(readyFile)
		done <- wakeProcessResult{Err: err, Ready: ready, Binding: binding, ReadyErr: readyErr}
	}()
	binding, processDone, err := waitForWakeReady(ctx, done, readyFile, req.Timeout)
	if err != nil {
		// Readiness wins a cancellation/timeout race. Once AMQ has published the
		// ready file, ownership of the long-lived process has transferred to AMQ
		// and this caller must never kill it.
		if readyBinding, readyErr := readWakeBinding(readyFile); readyErr == nil {
			_ = os.Remove(readyFile)
			return readyBinding, nil
		}
		if !processDone {
			select {
			case result := <-done:
				processDone = true
				if result.Ready {
					return result.Binding, nil
				}
			default:
			}
		}
		if !processDone {
			// Never signal a spawned wake from helper cancellation. It may be
			// between lock acquisition and atomic ready-file publication; killing
			// here could terminate an established or accepted wake. The durable
			// registry reservation lets a later supervisor pass converge safely.
			return WakeBinding{}, fmt.Errorf("%w: %w", ErrWakeReadinessUncertain, err)
		}
		if result, parseErr := readWakeCommandResult(resultFile); parseErr == nil {
			return WakeBinding{}, &WakeStartError{Result: result, Cause: err}
		}
		return WakeBinding{}, err
	}
	// The shared readiness directory must survive AMQ's post-rename directory
	// fsync, but the marker itself is no longer needed after acknowledgement.
	_ = os.Remove(readyFile)
	return binding, nil
}

func (c CLI) RetireWake(ctx context.Context, req RetireWakeRequest) (RetireWakeResult, error) {
	if req.Root == "" || req.Me == "" || req.InjectVia == "" || req.Adapter == "" || req.Target == "" ||
		req.Generation == "" || req.TargetDigest == "" {
		return RetireWakeResult{}, errors.New("exact root, agent, injector, adapter, target, generation, and target digest are required")
	}
	args := []string{"wake", "retire", "--json", "--root", req.Root, "--me", req.Me,
		"--inject-via", req.InjectVia,
		"--inject-arg", "inject", "--inject-arg", req.Adapter, "--inject-arg", req.Target,
		"--if-generation", req.Generation, "--if-target-digest", req.TargetDigest,
	}
	if req.RequireOwnerGone {
		args = append(args, "--require-owner-gone")
	}
	if req.Check {
		args = append(args, "--check")
	}
	timeout := req.Timeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stdout, stderr, runErr := c.run(commandCtx, args...)
	result, parseErr := parseRetireResult(stdout)
	if parseErr != nil {
		if runErr != nil {
			return RetireWakeResult{}, fmt.Errorf("amq wake retire failed without valid JSON: %w", runErr)
		}
		return RetireWakeResult{}, parseErr
	}
	if err := validateRetireEcho(req, result); err != nil {
		return result, err
	}
	if commandCtx.Err() != nil {
		return result, commandCtx.Err()
	}
	if runErr != nil {
		return result, &WakeStartError{Result: result, Cause: fmt.Errorf("amq wake retire: %w: %s", runErr, strings.TrimSpace(stderr))}
	}
	if result.Status == "refused" || result.Status == "error" {
		return result, &WakeStartError{Result: result, Cause: errors.New("amq wake retire returned a failure status with a zero exit code")}
	}
	return result, nil
}

// BaselineDigest validates the stable file properties needed by keepalive and
// returns the digest persisted in the registry. AMQ performs the authoritative
// root, agent, owner, and manifest validation when wake starts.
func BaselineDigest(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "." || !filepath.IsAbs(path) {
		return "", errors.New("wake baseline path must be absolute")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return "", errors.New("wake baseline must be a regular file, not a symlink")
	}
	if before.Mode().Perm() != 0o600 {
		return "", fmt.Errorf("wake baseline mode is %o, want 0600", before.Mode().Perm())
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	after, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !os.SameFile(before, after) {
		return "", errors.New("wake baseline changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxWakeBaselineBytes+1))
	if err != nil {
		return "", err
	}
	if len(data) > maxWakeBaselineBytes {
		return "", fmt.Errorf("wake baseline exceeds %d bytes", maxWakeBaselineBytes)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func newWakeReadyPath() (string, string, error) {
	cacheDir := strings.TrimSpace(os.Getenv("AMQ_KEEPALIVE_CACHE_DIR"))
	if cacheDir == "" {
		var err error
		cacheDir, err = os.UserCacheDir()
		if err != nil {
			return "", "", fmt.Errorf("resolve user cache directory for wake readiness: %w", err)
		}
	}
	dir := filepath.Join(cacheDir, "amq-keepalive", "readiness")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", fmt.Errorf("create wake readiness directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", "", fmt.Errorf("inspect wake readiness directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", "", fmt.Errorf("wake readiness path %q must be a real directory", dir)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", "", fmt.Errorf("secure wake readiness directory: %w", err)
	}
	scavengeStaleWakeReadyMarkers(dir, time.Now())
	placeholder, err := os.CreateTemp(dir, "wake-*")
	if err != nil {
		return "", "", fmt.Errorf("reserve wake readiness path: %w", err)
	}
	path := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		_ = os.Remove(path)
		return "", "", fmt.Errorf("close wake readiness placeholder: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return "", "", fmt.Errorf("prepare wake readiness destination: %w", err)
	}
	return dir, path, nil
}

func scavengeStaleWakeReadyMarkers(dir string, now time.Time) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "wake-") {
			continue
		}
		info, err := entry.Info()
		if err != nil || now.Sub(info.ModTime()) < staleWakeReadyMarkerAge {
			continue
		}
		_ = os.Remove(filepath.Join(dir, entry.Name()))
	}
}

type wakeProcessResult struct {
	Err      error
	Ready    bool
	Binding  WakeBinding
	ReadyErr error
}

func (c CLI) run(ctx context.Context, args ...string) ([]byte, string, error) {
	cmd := exec.CommandContext(ctx, c.Path, args...)
	cmd.Env = environmentWithout(os.Environ(), "AMQ_WAKE_OWNER", "AMQ_WAKE_OWNER_ERROR")
	stdout := newBoundedBuffer(maxCLIOutputBytes)
	stderr := newBoundedBuffer(maxCLIOutputBytes)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if stdout.Exceeded() || stderr.Exceeded() {
		err = errors.Join(err, fmt.Errorf("amq command output exceeds %d bytes", maxCLIOutputBytes))
	}
	return stdout.Bytes(), stderr.String(), err
}

type boundedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
}

func newBoundedBuffer(limit int) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	written := len(data)
	remaining := b.limit - b.buffer.Len()
	if remaining < len(data) {
		b.exceeded = true
		if remaining < 0 {
			remaining = 0
		}
		data = data[:remaining]
	}
	_, _ = b.buffer.Write(data)
	return written, nil
}

func (b *boundedBuffer) Bytes() []byte  { return b.buffer.Bytes() }
func (b *boundedBuffer) String() string { return b.buffer.String() }
func (b *boundedBuffer) Exceeded() bool { return b.exceeded }

func parseWakeRepair(data []byte) (WakeRepairResult, error) {
	var result WakeRepairResult
	if len(bytes.TrimSpace(data)) == 0 {
		return WakeRepairResult{}, errors.New("empty amq wake repair output")
	}
	if err := json.Unmarshal(data, &result); err != nil {
		var raw map[string]any
		if rawErr := json.Unmarshal(data, &raw); rawErr != nil {
			return WakeRepairResult{}, err
		}
		result.Status = stringField(raw, "status")
		result.Reason = stringField(raw, "reason")
		result.Message = stringField(raw, "message")
		result.Error = stringField(raw, "error")
	}
	result.Status = strings.TrimSpace(result.Status)
	return result, nil
}

func waitForWakeReady(ctx context.Context, done <-chan wakeProcessResult, readyFile string, timeout time.Duration) (WakeBinding, bool, error) {
	if timeout <= 0 {
		timeout = defaultWakeReadyTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		if wakeReadyFileExists(readyFile) {
			binding, err := readWakeBinding(readyFile)
			return binding, false, err
		}
		select {
		case result := <-done:
			if result.Ready {
				return result.Binding, true, nil
			}
			if wakeReadyFileExists(readyFile) {
				binding, err := readWakeBinding(readyFile)
				return binding, true, err
			}
			if result.Err == nil {
				if result.ReadyErr != nil {
					return WakeBinding{}, true, fmt.Errorf("amq wake exited without valid readiness: %w", result.ReadyErr)
				}
				return WakeBinding{}, true, errors.New("amq wake exited before becoming ready")
			}
			return WakeBinding{}, true, fmt.Errorf("amq wake exited before becoming ready: %w", result.Err)
		case <-ctx.Done():
			return WakeBinding{}, false, ctx.Err()
		case <-timer.C:
			return WakeBinding{}, false, fmt.Errorf("timed out after %s waiting for amq wake readiness", timeout)
		case <-ticker.C:
		}
	}
}

func readWakeBinding(path string) (WakeBinding, error) {
	var ready struct {
		Schema       int    `json:"schema"`
		Generation   string `json:"generation"`
		TargetDigest string `json:"target_digest"`
	}
	if err := readSecureJSONFile(path, &ready); err != nil {
		return WakeBinding{}, err
	}
	binding := WakeBinding{Generation: strings.TrimSpace(ready.Generation), TargetDigest: strings.TrimSpace(ready.TargetDigest)}
	if ready.Schema != 1 || !binding.Complete() {
		return WakeBinding{}, errors.New("wake readiness must contain schema 1 and a complete generation/digest binding")
	}
	return binding, nil
}

func readWakeCommandResult(path string) (WakeCommandResult, error) {
	var result WakeCommandResult
	if err := readSecureJSONFile(path, &result); err != nil {
		return WakeCommandResult{}, err
	}
	result.Status = strings.TrimSpace(result.Status)
	result.ReasonCode = strings.TrimSpace(result.ReasonCode)
	if result.Schema != 1 || result.Status != "failed" || result.ReasonCode == "" {
		return WakeCommandResult{}, errors.New("wake failure result must contain schema 1, status failed, and a reason_code")
	}
	switch result.ReasonCode {
	case "existing_wake_blocking", "invalid_owner", "unverified_wake", "invalid_baseline", "internal_failure":
	default:
		return WakeCommandResult{}, fmt.Errorf("wake failure result has unknown reason_code %q", result.ReasonCode)
	}
	return result, nil
}

// ValidateExistingWakeBlocker binds a structured start failure to the exact
// owner-bound wake which a reattach transaction is considering for retirement.
// A reason code alone is not proof that the persisted old generation is the
// blocker reported by AMQ.
func ValidateExistingWakeBlocker(root, agent string, binding WakeBinding, result WakeCommandResult) error {
	if result.Schema != 1 || result.Status != "failed" || result.ReasonCode != "existing_wake_blocking" {
		return errors.New("wake failure does not describe an existing-wake blocker")
	}
	if !binding.Complete() {
		return errors.New("persisted wake binding is incomplete")
	}
	wantRoot, err := canonicalPath(root)
	if err != nil {
		return fmt.Errorf("canonicalize expected blocker root: %w", err)
	}
	gotRoot, err := canonicalPath(result.Root)
	if err != nil || gotRoot != wantRoot || result.Agent != agent {
		return errors.New("wake blocker root/agent identity mismatch")
	}
	if result.CurrentWakeMode != "owner_bound" {
		return fmt.Errorf("wake blocker mode %q is not owner_bound", result.CurrentWakeMode)
	}
	if result.CurrentGeneration != binding.Generation || result.CurrentTargetDigest != binding.TargetDigest {
		return errors.New("wake blocker generation/digest mismatch")
	}
	return nil
}

func readSecureJSONFile(path string, destination any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("lifecycle result %q must be a 0600 regular file", path)
	}
	if info.Size() > maxLifecycleResultBytes {
		return fmt.Errorf("lifecycle result exceeds %d bytes", maxLifecycleResultBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) {
		return errors.New("lifecycle result changed while opening")
	}
	decoder := json.NewDecoder(io.LimitReader(file, maxLifecycleResultBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("lifecycle result contains trailing JSON data")
	}
	return nil
}

func parseRetireResult(data []byte) (RetireWakeResult, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return RetireWakeResult{}, errors.New("empty amq wake retire output")
	}
	var result RetireWakeResult
	if err := decodeStrictJSON(data, &result); err != nil {
		return RetireWakeResult{}, fmt.Errorf("parse amq wake retire JSON: %w", err)
	}
	result.Status = strings.TrimSpace(result.Status)
	result.ReasonCode = strings.TrimSpace(result.ReasonCode)
	if result.Schema != 1 {
		return RetireWakeResult{}, fmt.Errorf("amq wake retire response has unsupported schema %d", result.Schema)
	}
	if result.Status == "" || result.ReasonCode == "" {
		return RetireWakeResult{}, errors.New("amq wake retire response lacks status or reason_code")
	}
	switch result.Status {
	case "eligible":
		if result.ReasonCode != "owner_gone" {
			return RetireWakeResult{}, errors.New("eligible retirement response must use reason_code owner_gone")
		}
	case "retired":
		if result.ReasonCode != "retired_exact" {
			return RetireWakeResult{}, errors.New("retired response must use reason_code retired_exact")
		}
	case "already_retired":
		if result.ReasonCode != "tombstone_match" {
			return RetireWakeResult{}, errors.New("already_retired response must use reason_code tombstone_match")
		}
	case "superseded":
		if result.ReasonCode != "generation_superseded" {
			return RetireWakeResult{}, errors.New("superseded response must use reason_code generation_superseded")
		}
	case "error":
		if result.ReasonCode != "internal_error" {
			return RetireWakeResult{}, errors.New("error response must use reason_code internal_error")
		}
	case "refused":
	default:
		return RetireWakeResult{}, fmt.Errorf("amq wake retire response has unknown status %q", result.Status)
	}
	return result, nil
}

func decodeStrictJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON response contains trailing data")
	}
	return nil
}

func validateRetireEcho(req RetireWakeRequest, result RetireWakeResult) error {
	wantRoot, err := canonicalPath(req.Root)
	if err != nil {
		return err
	}
	gotRoot, err := canonicalPath(result.Root)
	if err != nil || gotRoot != wantRoot || result.Agent != req.Me {
		return errors.New("amq wake retire response root/agent identity mismatch")
	}
	if result.Generation != req.Generation || result.TargetDigest != req.TargetDigest {
		return errors.New("amq wake retire response generation/digest mismatch")
	}
	if result.Status == "superseded" && !SupersededProvesReplacement(req, result) {
		return errors.New("amq wake retire superseded response does not prove a different current wake")
	}
	return nil
}

func SupersededProvesReplacement(req RetireWakeRequest, result RetireWakeResult) bool {
	if result.Status != "superseded" || result.ReasonCode != "generation_superseded" || result.CurrentGeneration == "" {
		return false
	}
	switch result.CurrentWakeMode {
	case "owner_bound":
		return result.CurrentGeneration != req.Generation && result.CurrentTargetDigest != "" &&
			result.CurrentTargetDigest != req.TargetDigest
	case "raw":
		return result.CurrentGeneration != req.Generation && result.CurrentTargetDigest == ""
	default:
		return false
	}
}

func canonicalPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("canonical path is empty")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return real, nil
	}
	return abs, nil
}

func environmentWithout(base []string, names ...string) []string {
	blocked := make(map[string]struct{}, len(names))
	for _, name := range names {
		blocked[name] = struct{}{}
	}
	result := make([]string, 0, len(base))
	for _, item := range base {
		name, _, _ := strings.Cut(item, "=")
		if _, ok := blocked[name]; !ok {
			result = append(result, item)
		}
	}
	return result
}

func setExactEnvironment(base []string, name, value string, clear ...string) []string {
	names := append([]string{name}, clear...)
	return append(environmentWithout(base, names...), name+"="+value)
}

func wakeReadyFileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func stringField(raw map[string]any, key string) string {
	value, ok := raw[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}
