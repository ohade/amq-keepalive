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
its `--inject-via` executable, ordered fixed arguments, owner, persisted floor,
and generation-bound catch-up acknowledgement match. A differing target fails
closed. A definite pre-readiness exit restores the prior rows; a
timeout or cancellation after spawn leaves the reservation in place because the
unsignaled AMQ child may still publish readiness. On success the same reservation
is marked active, so there is no post-readiness commit window that can orphan a
live wake. Use
`--wake-ready-timeout` on `attach`, `reattach`, or `supervise` to adjust the
readiness wait; the default is 10 seconds. This requires an AMQ build that
supports `--accept-existing-wake` target verification and
exact baseline manifests. Readiness-marker maintenance scans and removes only a
bounded number per start; cleanup failures are aggregated as warnings and do
not turn an otherwise-positive ready acknowledgement into a false failure.
Normal `attach` and `reattach` also require a strict
`amq env --json` response advertising `wake_gc_v1` before any registry
reservation or wake start; `--no-start` remains the explicit register-only
escape hatch. If the capability probe fails, messages remain queued and no
managed wake state is changed. Legacy starts without a registered floor use
`--baseline-existing` once; AMQ materializes that snapshot into the saved target
and reuses it across wake downtime. SessionStart reattach instead receives
`AMQ_WAKE_BASELINE_FILE` from `amq coop exec --defer-wake` and passes the exact
file to AMQ. If a live same-transport/same-owner wake has an older exact floor,
AMQ performs its generation- and identity-safe baseline rotation; different
injectors, arguments, owners, or unverified wakes are never signaled. Wake keeps
floor messages unread, emits no receipts, and notifies arrivals after the floor.

For a launcher recreating a terminal, `reattach` enables `--retire-detached` by
default. It first tries the new exact wake. Retirement is considered only when
the structured start failure names the persisted old root, agent, owner-bound
mode, generation, and target digest as the blocker. Keepalive then rechecks the
`wake_gc_v1` capability, performs a generation-bound owner-gone check, records a
durable `retire_pending` transition, and asks AMQ to retire only that exact old
wake. A refusal, mismatched echo, live owner, ambiguous result, timeout, or
replacement generation fails closed and preserves the inactive reservation for
supervisor recovery. A positive retirement is recorded before the new wake is
retried, so a crash cannot restore the retired row or retarget another wake.

Safe detached-session retirement:

```sh
./amq-keepalive retire-session \
  --root "$HOME/.agent-mail/dashboard" \
  --adapter cmux \
  --agents codex,claude > /tmp/amq-retire-preview.json

plan_id="$(jq -r .plan_id /tmp/amq-retire-preview.json)"
./amq-keepalive retire-session \
  --root "$HOME/.agent-mail/dashboard" \
  --adapter cmux \
  --agents codex,claude \
  --apply \
  --confirm-plan "$plan_id"
```

The first invocation is genuinely read-only: it does not create a lock or
backup, change the registry, or call AMQ. The deterministic `plan_id` binds the
canonical registry and collaboration root, explicit sorted agents, exact legacy
row digests and normalized missing targets, canonical AMQ and keepalive
executables, their SHA-256 content and stable stat/ownership/mode identities,
and lifecycle timeout. Use exactly the same options for apply; any row, target,
path, executable content/metadata, or timeout drift invalidates the token before
a signal. `--agents` must name every non-retired listener at that canonical root;
an omitted same-adapter, mixed-adapter, or live sibling refuses the whole plan.
Every manual preflight and mutation reopens and rehashes both files.
Linux executes AMQ from the verified descriptor; macOS executes a private
fsynced snapshot copied from that descriptor because Darwin rejects executable
`/dev/fd` paths.

Apply repeats target-absence proof under the registration lock, preflights every
agent through AMQ's explicit manual-retirement contract, and durably saves every
generation/digest-bound intent before sending the first signal. A static
refusal therefore sends no retirement signals. Each successful result is saved
immediately as a retained retired row without deleting mailbox, target, or
baseline data. If keepalive crashes after AMQ records its tombstone but before
the registry save, the pending intent blocks reattach and automatic GC for that
root; repeating the exact confirmed command accepts only AMQ's matching
idempotent receipt and cannot signal that wake twice. Missing, raw, unverified,
changed, or mismatched wakes remain unresolved and are reported loudly.

An absent `.wake.lock` is eligible only after two independent checks. Keepalive
first proves through the selected adapter that the external target is absent.
AMQ separately proves that no managed wake lock exists and that its securely
saved target and presence evidence exactly match the requested transport. AMQ
returns a stable synthetic generation/digest binding for its evidence;
keepalive persists its distinct
`manual_absent_eligible` phase and accepts only the paired
`manual_absent_retired` mutation, including the same exact idempotent result on
crash replay. Lock-backed retirement keeps its existing exact tombstone replay.
This is evidence
that no managed wake exists, not permission to guess: a live or ambiguous
adapter target remains blocked by keepalive, while a changed saved target,
malformed AMQ state, or any binding drift fails closed in AMQ. This explicit
command does not weaken owner-bound automatic GC or make legacy retirement
automatic.

A legacy `.wake.lock` with a blank generation or target digest is not usable as
an exact preflight binding. AMQ returns `manual_legacy_lock_unbound` and keeps
the row unresolved. Only after `amq doctor --ops` proves that lock stale may an
operator run `amq doctor --ops --fix-wake-locks`, then retry the explicit
missing-lock retirement flow; keepalive never performs that cleanup itself.

While any member remains pending, the complete canonical root is frozen:
sibling registration/reconciliation, retention purges, and root GC artifacts
remain byte-equivalent. Only enrollment in the same exact plan or an exact
pending-to-receipt transition can be saved.

Detached registry cleanup is preview-first:

```sh
./amq-keepalive gc
```

`gc` defaults to a genuinely read-only JSON preview: even a schema-v1 registry
is migrated only in memory, without lock files, backups, mode changes, or other
filesystem writes. `gc --apply` requires `wake_gc_v1`, an exact owner-bound wake
binding, two positive AMQ owner-gone observations at least five minutes apart,
and a lifecycle timeout no longer than five seconds. Legacy/unbound,
incomplete, transitioning, live, refused, ambiguous, or unsupported rows are
never retired. Retired rows remain as immutable diagnostic history for at least
24 hours and are later removed only by exact compare-and-swap.

Automatic cleanup uses the same policy through `supervise --auto-gc`. One pass
selects one canonical AMQ collaboration root, ordered by oldest proven
`owner_gone_since` and then canonical path. Every non-retired listener in that
root is frozen into a durable batch with its exact identity and wake binding.
The batch preflights every member before the first mutation; any live,
legacy/unbound, incomplete, refused, superseded, or unexpected member blocks the
whole new batch with zero retire calls. An all-eligible batch retires at most
eight frozen listeners, saving each result immediately. A crash after AMQ's
tombstone therefore resumes by replaying the same exact request and accepting
only the matching tombstone. New registrations for that root wait while the
batch is active.

The hard throughput envelope is five distinct canonical roots per rolling
minute, persisted in the registry across supervisor restarts. A partial batch
resumes after five seconds without consuming a new-root slot; after a completed
root, another eligible root may use the same five-second catch-up cadence while
capacity remains. Roots with more than eight listener rows touch no AMQ and get
a bounded diagnostic backoff. These limits are internal and have no CLI flags
that can weaken them.

If a durable coordinator is stuck, an operator can escape it only by repeating
the exact batch id twice:

```sh
./amq-keepalive gc \
  --abandon-batch '<exact-batch-id>' \
  --confirm-abandon-batch '<exact-batch-id>'
```

The command verifies the frozen id, phase, membership, and every current row in
one atomic registry save. A retiring batch first performs exact check-only AMQ
reconciliation when `wake_gc_v1` is available; it never issues a retire
mutation. Positively inactive generations become retired rows. All unresolved
rows are quarantined from automatic GC and the coordinator is removed. If AMQ
capability discovery is unavailable, the same double-confirmed command is a
registry-only escape that prints `unresolved_amq_state: true`; it does not claim
that any wake was retired. A fresh explicit attach/reattach replaces the live
row and clears its quarantine.

The supervisor keeps the cross-process registration lease while one root batch
runs. That preserves frozen membership and excludes a racing reattach. The pass
ends immediately after the batch's bounded maximum of sixteen lifecycle calls
for eight listeners; it never performs unrelated wake starts under the same
lease. A five-second follow-up pass handles unrelated rows. At the five-second
command ceiling, the worst batch lease is therefore about 80 seconds plus local
persistence, rather than that batch time plus an unbounded reconciliation tail.

Before enabling automatic GC against an existing user registry:

1. stop or disable the LaunchAgent and copy the private registry file as a
   rollback backup;
2. run `gc` without `--apply` and review every identity/binding decision;
3. canary one disposable owner-bound session with a five-minute grace, verify
   its queued mailbox remains intact, and confirm only its exact wake and row
   retire;
4. enable `supervise --auto-gc` only after that canary. Before the first AMQ
   retirement mutation, rollback may stop the daemon, restore
   `--auto-gc=false`, and restore the saved registry. After any retirement has
   succeeded, never restore a stale pre-retirement registry: keep the new
   lifecycle implementation installed and replay or reconcile the exact
   pending batch to a durable receipt. That point is roll-forward-only because
   AMQ process state cannot be recreated by copying registry bytes.

Retired rows are diagnostic evidence for at least 24 hours, not permanent audit
history. They are purged only after retention by an exact compare-and-swap.

AMQ and keepalive also have an executable producer/consumer contract check:

```sh
AMQ_SOURCE=/path/to/agent-message-queue sh ./verify-amq-contract.sh
```

The script requires an explicit AMQ source checkout, builds that exact source,
and runs the real producer/consumer contract. Release and cross-repository CI
jobs should invoke this command directly; a missing or unbuildable source is a
hard failure, never a silent skip. The consumer accepts additive unknown JSON
fields for forward compatibility while still rejecting missing required
fields, duplicate keys at any nesting depth, unknown enum values, trailing JSON,
and unsupported schemas.
Schema-v2 registries require an explicit non-null top-level `entries` array;
writers emit `[]` for an empty registry. Schema-v1 migration and additive
unknown fields remain supported.

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
It gives stdin parsing plus inner reattach/readiness work one deterministic
`AMQ_KEEPALIVE_TIMEOUT_SECONDS` budget (default: 10), then allows the combined
process wrapper a fixed one-second scheduler/startup grace. This remains below
the installed host hook's additional five-second hard-timeout margin. The hook
reserves time for verification to observe the exact `notifier_live`
acknowledgement. Invalid timeout overrides are normalized, open
stdin cannot stall startup, and timeout cleanup never signals AMQ's detached wake
grandchild. Ordinary non-AMQ launches stay quiet. An intended deferred-wake launch
that lacks a valid baseline or exact acknowledgement emits a visible
`AMQ wake unavailable; messages remain queued` SessionStart warning. While the
shared deadline still has budget, cmux also gets a best-effort exact-surface
notification; agent startup continues even if that optional notification cannot
be delivered.

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
      "timeout": 15,
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
path. `AMQ_WAKE_BASELINE_FILE` and the sanitized `AMQ_WAKE_BASELINE_ERROR` are
normally supplied by `amq coop exec --defer-wake`. Success prints `{}`; intended
AMQ launches print the warning JSON on failure. Details are logged to
`~/.amq-keepalive/session-start.log`.

LaunchAgent install:

```sh
./amq-keepalive install-launchd
```

Pass `--auto-gc` to persist the owner-bound GC policy in the LaunchAgent. The
minimum five-minute owner-gone grace, minimum 24-hour retired-row retention, and
maximum five-second lifecycle timeout are validated both at the CLI and inside
the collector.

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
Ordinary registry observations from a pass are compare-and-swapped in one locked
atomic save. Root-batch retirement results are saved one member at a time under
the same registration lease so crash recovery can replay the exact unfinished
member without widening batch membership.
Continuous mode emits no per-pass JSON; `supervise --once` retains the structured
result. SIGINT and SIGTERM cancel the loop cleanly for launchd replacement.
Spawned wake processes run in a separate Unix session with null stdio, so they do
not retain a terminal surface or the LaunchAgent's unrotated log and do not receive
the supervisor's terminal-generated signals. Registration-lease waits are
context-cancelable, and default signal handling is restored after the first signal.
Any failed supervisor pass retries after five seconds, including failures before
a durable GC coordinator can be discovered. Transition diagnostics are part of
the observable contract: a failed stderr write is returned instead of silently
discarding the diagnostic.

`doctor` is registry-read-only: it uses the preview loader and creates no lock,
backup, or migrated registry. Its JSON includes `active_gc_batch_id` and
`active_gc_batch_phase` when a durable coordinator is present.

## Boundaries

- The tool does not parse AMQ mailbox, lock, presence, or target files.
- The tool does not launch or resurrect terminal sessions.
- Keepalive invokes `amq wake retire` only for an exact owner-bound generation
  after the capability, identity, owner-gone, and durable-transition gates
  described above. Terminal presence or target disappearance is never used as
  authority to retire a wake.
- Adapter targets should use an explicit scheme shape:
  `<adapter>:<scheme>:<value>`. The supported terminal schemes are
  `ghostty:terminal:<id>` and `cmux:surface:<uuid>`.
- If a registered Ghostty terminal id cannot be found, the entry is marked
  `detached` until the user runs `reattach --adapter ghostty` again or the
  SessionStart hook reattaches the recreated session.
- Reboot survival comes from reattaching on session start, not from assuming a
  terminal id survives process or machine restart. The recreated session
  registers its current target.
- `reattach` never retargets a live wake to another terminal implicitly. A
  matching live target is verified; the narrow SessionStart path may rotate only
  its baseline generation when transport and owner are exact. A differing live
  target can retire only when the structured blocker exactly matches the
  persisted old generation and AMQ independently proves its owner gone. Stale or
  ambiguous blockers remain queued for later recovery; keepalive does not invoke
  `amq wake repair` first.
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
