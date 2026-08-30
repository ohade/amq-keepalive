# amq-keepalive

`amq-keepalive` is a standalone helper for keeping AMQ wake delivery attached to
the terminal or adapter a user explicitly registered.

M0 is intentionally small:

- one static Go binary;
- a private registry under `~/.amq-keepalive/`;
- explicit `attach`, `reattach`, `supervise`, `inject`, `doctor`,
  `retire-session`, and `forget` commands;
- a fake `file` adapter for deterministic tests;
- supervisor logic that only talks to AMQ through the public `amq` CLI.

It does not parse AMQ mailbox, lock, presence, or target files. AMQ state is
established through target-aware `amq wake` calls and discovered through
`amq env --json`.

## Current Scope

The implemented surface now includes the M0 registry/supervisor proof and the
first M1 macOS pieces:

- `ghostty` adapter using Ghostty's native macOS AppleScript interface;
- `cmux` adapter targeting the exact `CMUX_SURFACE_ID` through cmux's JSON RPC;
- `install-launchd` / `uninstall` for a user LaunchAgent supervisor;
- `install-hook` for registering the SessionStart reattach hook in Claude Code
  and/or Codex.

The Ghostty adapter target contract is `ghostty:terminal:<id>`. `attach
--adapter ghostty` discovers the focused terminal in the selected tab of the
front Ghostty window when `--target` is omitted. Probe and inject fail closed
unless that id resolves to exactly one Ghostty terminal. Injection uses Ghostty's
native `input text` and `send key "enter"` AppleScript commands; it does not use
window titles, System Events, focus stealing, or the clipboard. Trailing CR/LF
characters are trimmed before the explicit Enter key is sent, so newline-ended
AMQ payloads do not double-submit.

Old title targets are intentionally rejected. Re-run `reattach --adapter ghostty`
from the current session to register a fresh terminal-id target.

The cmux adapter target contract is `cmux:surface:<uuid>`. When `--target` is
omitted it reads the exact UUID from `CMUX_SURFACE_ID`; short refs such as
`surface:2` are rejected because they can drift. Probe and injection use
`cmux rpc` with JSON-encoded parameters, so literal `\n`, `\r`, `\t`, leading
dashes, quotes, and other message-derived text are not interpreted by the
friendly CLI's text unescaper. Injection sends the exact text, waits 150ms for
cmux's input queue to settle, and sends Enter as a separate `surface.send_key`
RPC.
UUIDs are canonicalized to uppercase before registration so the same surface
cannot produce a false AMQ target mismatch through casing alone.

The cmux CLI is resolved from `AMQ_KEEPALIVE_CMUX`,
`CMUX_BUNDLED_CLI_PATH`, `PATH`, or the standard system/user cmux application
bundle locations. The LaunchAgent PATH also includes the system app's bundled
CLI directory.

## Example

```sh
make build

./bin/amq-keepalive attach \
  --adapter file \
  --target /tmp/amq-keepalive-inbox.txt \
  --no-start

./bin/amq-keepalive supervise --once

./bin/amq-keepalive doctor
```

## Version reporting

`amq-keepalive -v`, `amq-keepalive --version`, and `amq-keepalive version`
each print the build version as one line. `make build` stamps that version from
`git describe --tags --always --dirty` and writes the binary to
`./bin/amq-keepalive`.

The equivalent direct build command is:

```sh
go build \
  -ldflags "-X github.com/ohade/amq-keepalive/internal/app.Version=$(git describe --tags --always --dirty)" \
  -o ./bin/amq-keepalive \
  ./cmd/amq-keepalive
```

A plain `go build` has no explicit stamp. In that case the version command
falls back to Go's embedded build information: the module version followed by
`vcs.revision` and `vcs.modified` when those settings are available. If Go has
no embedded build information, it reports `dev`.

Ghostty attach:

```sh
./amq-keepalive attach --adapter ghostty
```

Session-start reattach:

```sh
./amq-keepalive reattach --adapter cmux
```

`reattach` is the reboot-safe path: it discovers the current adapter target and
ensures wake uses it before persisting any registry change. Only after success
does it atomically replace prior entries for the same AMQ root and agent across
all adapters. Startup passes AMQ's internal
`--accept-existing-wake` readiness contract: a live wake is accepted only when
its `--inject-via` executable and fixed arguments exactly match. A differing
target fails closed while the prior registry entry remains untouched. A timeout
or process termination before wake readiness likewise cannot expose a tentative
new target to the supervisor. This keeps the registry from accumulating stale
terminal ids or claiming a cmux attachment while Ghostty is still live. Use
`--wake-ready-timeout` on `attach`, `reattach`, or `supervise` to adjust the
readiness wait; the default is 10 seconds. This requires an AMQ build that
supports `--accept-existing-wake` target verification. Managed wake startup
uses a fresh OS session so a short-lived launcher or hook cannot deliver
`SIGHUP` after readiness. Callers creating a wake outside their own co-op
ownership boundary must remove `AMQ_WAKE_OWNER`; callers running inside the
matching co-op agent retain it.

For a launcher recreating a terminal, add `--retire-detached`. This opt-in path
looks up the prior registration for the same AMQ root and agent. If its adapter
target is independently proven gone, it first asks AMQ's target-aware wake start
to converge on the new exact target. An already-absent lock starts directly. A
live old wake rejects that start without mutation, after which `amq wake retire`
must revalidate the saved process and injector identity before one bounded retry.
If the old wake exits during that handoff, the retry safely acquires the now-free
lock. Registry replacement still happens only after wake readiness. A live old
target, ambiguous probe, missing registry identity, or unresolved retirement/start
mismatch fails closed; no active wake is retargeted.

Safe detached-session retirement:

```sh
./amq-keepalive retire-session \
  --root "$HOME/.agent-mail/dashboard" \
  --adapter cmux \
  --agents codex,claude
```

`retire-session` is the inverse lifecycle path for a terminal workspace that
was deleted while its AMQ wakes remained alive. Before touching AMQ it requires
exactly one registry entry per requested agent and independently proves every
registered cmux surface is missing. It reads AMQ's schema-2 wake classification,
uses `amq wake recover-owner` for an owner-bound claim, and otherwise asks
`amq wake retire` to verify the live process identity, unchanged lock, injector
executable, adapter, and exact surface target before signaling. A proven-missing
wake is already retired. Only successfully recovered, retired, or already-absent
entries are removed from the keepalive registry; the AMQ session directory,
mailbox history, and saved wake target are preserved for fresh agents to reuse.
Any ambiguous probe, target mismatch, unverified lock, or ownership race fails
closed. Older AMQ versions without schema-2 wake checks retain the exact
`wake retire` path.

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

The wrapper auto-selects `cmux` when `CMUX_SURFACE_ID` is present and otherwise
falls back to `ghostty`; `AMQ_KEEPALIVE_ADAPTER` remains an explicit override.
It bounds the actual `reattach` work with `AMQ_KEEPALIVE_TIMEOUT_SECONDS`
(default: 10). The hook passes a shorter inner AMQ readiness timeout (8 seconds
by default) so the process has bounded time to report failure before the outer
watchdog fires. If adapter discovery, probing, or AMQ wake startup hangs, the hook
logs the adapter and exact target with the timeout and still returns `{}` so
agent startup continues. Invalid or non-positive timeout overrides are normalized
back to the default. The hook also bounds the initial stdin read so a host that
leaves stdin open cannot stall startup before the reattach watchdog begins.

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
`AMQ_KEEPALIVE_CMUX`,
`AMQ_KEEPALIVE_REGISTRY`, `AMQ_KEEPALIVE_AMQ`, `AMQ_KEEPALIVE_SELF`,
`AMQ_KEEPALIVE_ROOT`, `AMQ_KEEPALIVE_BASE_ROOT`, `AMQ_KEEPALIVE_SESSION`,
`AMQ_KEEPALIVE_ME`, `AMQ_KEEPALIVE_TIMEOUT_SECONDS`,
`AMQ_KEEPALIVE_WAKE_TIMEOUT_MILLISECONDS`, and
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

`install-launchd` and `uninstall` refuse to overwrite or remove an existing
plist unless it already looks like an `amq-keepalive supervise` LaunchAgent with
the requested label. This protects unrelated user LaunchAgents when `--plist` is
customized.

For AMQ wake integration, `supervise` starts AMQ with this binary as the
`--inject-via` executable. AMQ appends the message payload as the final argument,
and `amq-keepalive inject <adapter> <target> <payload>` hands it to the adapter.

## Boundaries

- The tool does not parse AMQ mailbox, lock, presence, or target files.
- The tool does not launch or resurrect terminal sessions.
- `retire-session` delegates wake-lock classification, owner recovery, identity
  verification, and signaling to `amq wake check`, `amq wake recover-owner`, and
  `amq wake retire`; keepalive still does not parse AMQ lock or target files.
- Adapter targets should use an explicit scheme shape:
  `<adapter>:<scheme>:<value>`. The supported terminal schemes are
  `ghostty:terminal:<id>` and `cmux:surface:<uuid>`.
- If a registered Ghostty terminal id cannot be found, the entry is marked
  `detached` until the user runs `reattach --adapter ghostty` again or the
  SessionStart hook reattaches the recreated session.
- Reboot survival comes from reattaching on session start, not from assuming a
  terminal id survives process or machine restart. The recreated session
  registers its current target.
- `reattach` never retargets a live wake implicitly. A matching live target is
  verified, while a differing live target fails closed and logs the mismatch
  without changing the registry. With explicit `--retire-detached`, only a saved
  target proven gone enters bounded recovery: AMQ's target-aware start handles a
  missing lock directly, while a blocking live old wake must be identity-checked
  and retired before one retry. Stale/dead wake locks are otherwise replaced by
  AMQ's target-aware start using the registry target; keepalive does not resurrect
  an older saved adapter through `amq wake repair` first.
- Supervisor failures emit a transition-only warning with root, agent, adapter,
  target, failure count, and error; repeated checks during the same backoff do
  not spam stderr.
- `install-launchd` installs only a per-user LaunchAgent for this supervisor.
- Custom `--registry` parent directories keep their existing permissions; files
  created by this tool are still written with private registry/lock modes.
