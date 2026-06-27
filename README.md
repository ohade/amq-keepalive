# amq-keepalive

`amq-keepalive` is a standalone helper for keeping AMQ wake delivery attached to
the terminal or adapter a user explicitly registered.

M0 is intentionally small:

- one static Go binary;
- a private registry under `~/.amq-keepalive/`;
- explicit `attach`, `reattach`, `supervise`, `inject`, `doctor`, and `forget`
  commands;
- a fake `file` adapter for deterministic tests;
- supervisor logic that only talks to AMQ through the public `amq` CLI.

It does not parse AMQ mailbox, lock, presence, or target files. AMQ state is
observed and repaired only through `amq wake repair`, `amq wake`, and
`amq env --json`.

## Current Scope

The implemented surface now includes the M0 registry/supervisor proof and the
first M1 macOS pieces:

- `ghostty` adapter using Ghostty's native macOS AppleScript interface;
- `install-launchd` / `uninstall` for a user LaunchAgent supervisor;
- `install-hook` for registering the SessionStart reattach hook in Claude Code
  and/or Codex.

The Ghostty adapter target contract is `ghostty:terminal:<id>`. `attach
--adapter ghostty` discovers the focused terminal in the selected tab of the
front Ghostty window when `--target` is omitted. Probe and inject fail closed
unless that id resolves to exactly one Ghostty terminal. Injection uses Ghostty's
native `input text` and `send key "enter"` AppleScript commands; it does not use
window titles, System Events, focus stealing, or the clipboard.

Old title targets are intentionally rejected. Re-run `reattach --adapter ghostty`
from the current session to register a fresh terminal-id target.

## Example

```sh
go build ./cmd/amq-keepalive

./amq-keepalive attach \
  --adapter file \
  --target /tmp/amq-keepalive-inbox.txt \
  --no-start

./amq-keepalive supervise --once

./amq-keepalive doctor
```

Ghostty attach:

```sh
./amq-keepalive attach --adapter ghostty
```

Session-start reattach:

```sh
./amq-keepalive reattach --adapter ghostty
```

`reattach` is the reboot-safe path: it discovers the current adapter target,
replaces any prior entry for the same AMQ root, agent, and adapter, and then
starts wake for the fresh target. Startup waits for AMQ's readiness marker before
reporting success, so a refused or already-running wake is not silently accepted.
This keeps the registry from accumulating stale terminal ids after a session is
recreated.

Supported hook install:

```sh
./amq-keepalive install-hook --agent both
```

`install-hook` writes an executable wrapper to
`~/.amq-keepalive/hooks/amq-keepalive-session-start.sh`, backs up existing
config files before changing them, and appends a SessionStart registration to
Claude Code's `~/.claude/settings.json` and/or Codex's `~/.codex/hooks.json`.
It is idempotent: running it again does not duplicate the hook. Use
`--agent claude` or `--agent codex` to target only one agent, and `--dry-run` to
print the exact snippets without writing files.

The wrapper bounds the actual `reattach` work with
`AMQ_KEEPALIVE_TIMEOUT_SECONDS` (default: 10). If Ghostty discovery, probing, or
AMQ wake startup hangs, the hook logs the timeout and still returns `{}` so
agent startup continues.

Manual Claude Code SessionStart hook snippet:

```json
{
  "matcher": "*",
  "hooks": [
    {
      "type": "command",
      "command": "AMQ_KEEPALIVE_BIN='/absolute/path/to/amq-keepalive' AMQ_KEEPALIVE_TIMEOUT_SECONDS='10' '/absolute/path/to/amq-keepalive-session-start.sh'",
      "timeout": 15,
      "statusMessage": "Reattaching AMQ wake..."
    }
  ]
}
```

Manual Codex SessionStart hook snippet:

```json
{
  "hooks": [
    {
      "command": "AMQ_KEEPALIVE_BIN='/absolute/path/to/amq-keepalive' AMQ_KEEPALIVE_TIMEOUT_SECONDS='10' '/absolute/path/to/amq-keepalive-session-start.sh'",
      "timeout": 15000,
      "type": "command"
    }
  ]
}
```

Set `AMQ_KEEPALIVE_BIN=/absolute/path/to/amq-keepalive` if the binary is not on
`PATH`. The hook also supports `AMQ_KEEPALIVE_ADAPTER`, `AMQ_KEEPALIVE_TARGET`,
`AMQ_KEEPALIVE_REGISTRY`, `AMQ_KEEPALIVE_AMQ`, `AMQ_KEEPALIVE_SELF`,
`AMQ_KEEPALIVE_ROOT`, `AMQ_KEEPALIVE_BASE_ROOT`, `AMQ_KEEPALIVE_SESSION`,
`AMQ_KEEPALIVE_ME`, `AMQ_KEEPALIVE_TIMEOUT_SECONDS`, and
`AMQ_KEEPALIVE_NO_START=1` for test/dry-run wiring. Leave
`AMQ_KEEPALIVE_SELF` unset unless you need to force a specific absolute injector
path. The hook always prints `{}` so agent startup continues even if reattach
fails; failures are logged to `~/.amq-keepalive/session-start.log`.

LaunchAgent install:

```sh
./amq-keepalive install-launchd
```

For a dry plist write without loading the service:

```sh
./amq-keepalive install-launchd --no-load --plist /tmp/com.ohade.amq-keepalive.plist
```

For AMQ wake integration, `supervise` starts AMQ with this binary as the
`--inject-via` executable. AMQ appends the message payload as the final argument,
and `amq-keepalive inject <adapter> <target> <payload>` hands it to the adapter.

## Boundaries

- The tool does not parse AMQ mailbox, lock, presence, or target files.
- The tool does not launch or resurrect terminal sessions.
- Adapter targets should use an explicit scheme shape:
  `<adapter>:<scheme>:<value>`. The M1.5 Ghostty scheme is
  `ghostty:terminal:<id>`.
- If a registered Ghostty terminal id cannot be found, the entry is marked
  `detached` until the user runs `reattach --adapter ghostty` again or the
  SessionStart hook reattaches the recreated session.
- Reboot survival comes from reattaching on session start, not from assuming a
  terminal id survives process or machine restart. The recreated session
  registers its current target.
- `reattach` updates the registry target and starts wake for that target when no
  valid wake is already running. If an old wake process is still live with a
  previous target, AMQ currently has no safe retarget command, so reattach fails
  closed and logs the failure instead of pretending the old process was updated.
  Stale/dead wake locks are handled by starting wake for the fresh target.
- `install-launchd` installs only a per-user LaunchAgent for this supervisor.
