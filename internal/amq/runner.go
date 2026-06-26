package amq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

var ErrAlreadyRunning = errors.New("amq wake already running")

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
	Root      string
	Me        string
	InjectVia string
	Adapter   string
	Target    string
}

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
	stdout, stderr, err := c.run(ctx, "env", "--json")
	if err != nil {
		return Env{}, fmt.Errorf("amq env failed: %w: %s", err, strings.TrimSpace(stderr))
	}
	var env Env
	if err := json.Unmarshal(stdout, &env); err != nil {
		return Env{}, fmt.Errorf("parse amq env: %w", err)
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

func (c CLI) StartWake(ctx context.Context, req StartWakeRequest) error {
	if req.InjectVia == "" {
		return errors.New("inject-via executable is required")
	}
	if req.Adapter == "" {
		return errors.New("adapter is required")
	}
	if req.Target == "" {
		return errors.New("target is required")
	}

	args := []string{"wake"}
	if req.Root != "" {
		args = append(args, "-root", req.Root)
	}
	if req.Me != "" {
		args = append(args, "-me", req.Me)
	}
	args = append(args,
		"-inject-via", req.InjectVia,
		"-inject-arg", "inject",
		"-inject-arg", req.Adapter,
		"-inject-arg", req.Target,
	)

	cmd := exec.CommandContext(ctx, c.Path, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already") {
			return ErrAlreadyRunning
		}
		return err
	}
	if cmd.Process != nil {
		return cmd.Process.Release()
	}
	return nil
}

func (c CLI) run(ctx context.Context, args ...string) ([]byte, string, error) {
	cmd := exec.CommandContext(ctx, c.Path, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.String(), err
}

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

func stringField(raw map[string]any, key string) string {
	value, ok := raw[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}
