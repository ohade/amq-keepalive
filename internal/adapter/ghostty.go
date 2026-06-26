package adapter

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

type CommandRunner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

type Ghostty struct {
	Runner CommandRunner
}

func (Ghostty) Name() string {
	return "ghostty"
}

func (g Ghostty) Discover(ctx context.Context) (string, error) {
	if err := requireDarwin(); err != nil {
		return "", err
	}
	out, err := g.runner().Run(ctx, "osascript", "-e", ghosttyDiscoverScript)
	if err != nil {
		return "", fmt.Errorf("discover Ghostty window: %w: %s", err, strings.TrimSpace(string(out)))
	}
	target := strings.TrimSpace(string(out))
	if target == "" {
		return "", errors.New("discover Ghostty window: empty window title")
	}
	return target, nil
}

func (g Ghostty) Probe(ctx context.Context, target string) error {
	if err := requireDarwin(); err != nil {
		return err
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return errors.New("ghostty adapter target is required")
	}
	out, err := g.runner().Run(ctx, "osascript", "-e", ghosttyProbeScript, target)
	if err != nil {
		return fmt.Errorf("probe Ghostty target %q: %w: %s", target, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (g Ghostty) Inject(ctx context.Context, target string, payload string) error {
	if err := requireDarwin(); err != nil {
		return err
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return errors.New("ghostty adapter target is required")
	}
	out, err := g.runner().Run(ctx, "osascript", "-e", ghosttyInjectScript, target, payload)
	if err != nil {
		return fmt.Errorf("inject into Ghostty target %q: %w: %s", target, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (g Ghostty) runner() CommandRunner {
	if g.Runner != nil {
		return g.Runner
	}
	return ExecRunner{}
}

func requireDarwin() error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("ghostty adapter requires macOS, got %s", runtime.GOOS)
	}
	return nil
}

const ghosttyDiscoverScript = `
tell application "System Events"
	if not (exists process "Ghostty") then error "Ghostty is not running"
	tell process "Ghostty"
		if (count of windows) is 0 then error "Ghostty has no windows"
		set targetWindow to missing value
		repeat with candidateWindow in windows
			try
				if focused of candidateWindow is true then
					set targetWindow to candidateWindow
					exit repeat
				end if
			end try
		end repeat
		if targetWindow is missing value then set targetWindow to window 1
		return name of targetWindow
	end tell
end tell
`

const ghosttyProbeScript = `
on run argv
	set targetTitle to item 1 of argv
	tell application "System Events"
		if not (exists process "Ghostty") then error "Ghostty is not running"
		tell process "Ghostty"
			if (count of windows) is 0 then error "Ghostty has no windows"
			set matchCount to 0
			repeat with candidateWindow in windows
				if name of candidateWindow is targetTitle then set matchCount to matchCount + 1
			end repeat
			if matchCount is 1 then return "ok"
			if matchCount is 0 then error "no unique Ghostty target: no window titled: " & targetTitle
			error "ambiguous Ghostty target: " & matchCount & " windows titled: " & targetTitle
		end tell
	end tell
end run
`

const ghosttyInjectScript = `
on run argv
	set targetTitle to item 1 of argv
	set payload to item 2 of argv
	set oldClipboard to missing value
	tell application "System Events"
		if not (exists process "Ghostty") then error "Ghostty is not running"
		tell process "Ghostty"
			if (count of windows) is 0 then error "Ghostty has no windows"
			set matchCount to 0
			repeat with candidateWindow in windows
				if name of candidateWindow is targetTitle then set matchCount to matchCount + 1
			end repeat
			if matchCount is 0 then error "no unique Ghostty target: no window titled: " & targetTitle
			if matchCount is greater than 1 then error "ambiguous Ghostty target: " & matchCount & " windows titled: " & targetTitle
		end tell
	end tell
	try
		set oldClipboard to the clipboard
	end try
	set the clipboard to payload
	try
		tell application "Ghostty" to activate
		delay 0.05
		tell application "System Events"
			if not (exists process "Ghostty") then error "Ghostty is not running"
			tell process "Ghostty"
				set frontmost to true
				set targetWindow to missing value
				set matchCount to 0
				repeat with candidateWindow in windows
					if name of candidateWindow is targetTitle then
						set matchCount to matchCount + 1
						set targetWindow to candidateWindow
					end if
				end repeat
				if matchCount is 0 then error "no unique Ghostty target: no window titled: " & targetTitle
				if matchCount is greater than 1 then error "ambiguous Ghostty target: " & matchCount & " windows titled: " & targetTitle
				perform action "AXRaise" of targetWindow
			end tell
			delay 0.05
			keystroke "v" using command down
			delay 0.03
			key code 36
		end tell
	on error errMsg number errNum
		if oldClipboard is not missing value then set the clipboard to oldClipboard
		error errMsg number errNum
	end try
	if oldClipboard is not missing value then set the clipboard to oldClipboard
end run
`
