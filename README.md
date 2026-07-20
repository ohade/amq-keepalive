# amq-keepalive

`amq-keepalive` is a standalone helper for keeping AMQ wake delivery attached to
the terminal or adapter a user explicitly registered.

M0 is intentionally small:

- one static Go binary;
- a private registry under `~/.amq-keepalive/`;
- explicit `attach`, `reattach`, `supervise`, `inject`, `doctor`,
  `gc`, `retire-session`, and `forget` commands;
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
`surface:2` are rejected because they can drift. Existence probing uses the
global `cmux rpc system.tree` inventory, shared once across every due cmux entry
in a supervisor pass. This avoids per-entry child processes and does not mistake
a valid but unreadable terminal for a detached surface. Injection uses JSON RPC
with encoded parameters, so literal `\n`, `\r`, `\t`, leading dashes, quotes,
and other message-derived text are not interpreted by the friendly CLI's text
unescaper. It sends the exact text, waits 150ms for cmux's input queue to settle,
and sends Enter as a separate `surface.send_key` RPC.
UUIDs are canonicalized to uppercase before registration so the same surface
cannot produce a false AMQ target mismatch through casing alone. cmux ownership
is keyed by the canonical `/dev/tty*` reported by `system.tree`, and a target is
usable only when exactly one live surface UUID maps to that TTY. Missing TTYs or
multiple UUID aliases fail closed during attach, supervision, and every
injection; no text is sent until a fresh tree proves the target unambiguous.

The cmux CLI is resolved from `AMQ_KEEPALIVE_CMUX`,
`CMUX_BUNDLED_CLI_PATH`, `PATH`, or the standard system/user cmux application
bundle locations. The LaunchAgent PATH also includes the system app's bundled
CLI directory.

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
./amq-keepalive reattach --adapter cmux
```

`reattach` is the reboot-safe path. It discovers and inventories the current
adapter target, obtains a cross-process registration lease, and atomically
persists an inactive `attached` reservation before touching AMQ. That reservation
replaces prior entries for the same AMQ root and agent across all adapters and
makes a crash or late readiness recoverable by the next supervisor pass. Startup
then uses AMQ's internal
`--accept-existing-wake` readiness contract: a live wake is accepted only when
its `--inject-via` executable and fixed arguments exactly match. A differing
target fails closed. A definite pre-readiness exit restores the prior rows; a
timeout or cancellation after spawn leaves the reservation in place because the
unsignaled AMQ child may still publish readiness. On success the same reservation
is marked active, so there is no post-readiness commit window that can orphan a
live wake. Use
`--wake-ready-timeout` on `attach`, `reattach`, or `supervise` to adjust the
readiness wait; the default is 10 seconds. This requires an AMQ build that
supports `--accept-existing-wake` target verification.

For a launcher recreating a terminal, add `--retire-detached`. This opt-in path
looks up the prior registration for the same AMQ root and agent. If its adapter
target is independently proven gone, it first asks AMQ's target-aware wake start
to converge on the new exact target. An already-absent lock starts directly. If
a live old wake blocks the exact-target start, this release stops: destructive
retirement is hard-gated until AMQ exposes a positively verifiable
identity-safe-retire capability from issue #235. No `amq wake retire` subprocess
is invoked, no active wake is retargeted, and the prior registry rows are restored
after the definite start failure.

Safe detached-session retirement:

```sh
./amq-keepalive retire-session \
  --root "$HOME/.agent-mail/dashboard" \
  --adapter cmux \
  --agents codex,claude
```

`retire-session` currently performs only its fail-closed preflight. It requires
exactly one registry entry per requested agent and independently proves every
registered cmux surface is missing, then returns the #235 capability error without
signaling AMQ or changing the registry. It preserves the wakes and rows so callers
can fall back to a new room until the upstream identity-safe retirement capability
lands.

Detached registry cleanup is preview-first:

```sh
./amq-keepalive gc
```

`gc` defaults to a read-only JSON dry run and requires 24 hours of continuously
proven detachment. Target collisions, live cmux TTY aliases, transient probe
failures, and recent or legacy entries without a `detached_since` timestamp are
excluded. `gc --apply` is deliberately hard-gated by the same #235 capability
boundary and invokes neither AMQ nor registry mutation. Use the JSON candidates
for review only; do not treat them as proof that a wake process is safe to signal.

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
The supervisor loop defaults to one minute, while healthy wake checks run at
most every five minutes. Missing targets back off from five minutes to one hour.
Registry changes from a pass are compare-and-swapped in one locked atomic save.
Continuous mode emits no per-pass JSON; `supervise --once` retains the structured
result. SIGINT and SIGTERM cancel the loop cleanly for launchd replacement.
Spawned wake processes run in a separate Unix session with null stdio, so they do
not retain a terminal surface or the LaunchAgent's unrotated log and do not receive
the supervisor's terminal-generated signals. Registration-lease waits are
context-cancelable, and default signal handling is restored after the first signal.

## Boundaries

- The tool does not parse AMQ mailbox, lock, presence, or target files.
- The tool does not launch or resurrect terminal sessions.
- All destructive wake-retirement paths are disabled until AMQ #235 exposes a
  positive identity-safe-retire capability. Production code contains no
  `wake retire` execution path in this release.
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
  without signaling it. With explicit `--retire-detached`, a saved target proven
  gone may attempt exact-target convergence; a blocking old wake hits the #235
  hard gate with no retirement or retry. Stale/dead wake locks are otherwise
  replaced by AMQ's target-aware start using the registry target; keepalive does
  not resurrect an older saved adapter through `amq wake repair` first.
- A normalized `(adapter, target)` can have only one registry owner. Legacy
  collisions fail closed for every claimant; the supervisor never picks a winner.
  For cmux, the stronger rule is exactly one live surface UUID per canonical TTY.
  A collision discovered after wakes already exist prevents injection and re-arm
  but does not auto-retire those wakes; preserve the wakes and rows and let callers
  fall back to a new room.
- Only `ErrTargetNotFound` marks an entry detached. Transport, permission, parse,
  and cmux internal errors are ambiguous and use transient backoff instead.
- Supervisor failures emit a transition-only warning with root, agent, adapter,
  target, failure count, and error; recovery is logged once, and repeated checks
  of the same condition do not spam stderr.
- `install-launchd` installs only a per-user LaunchAgent for this supervisor.
- Custom `--registry` parent directories keep their existing permissions; files
  created by this tool are still written with private registry/lock modes.
