# amq-keepalive

`amq-keepalive` is a standalone helper for keeping AMQ wake delivery attached to
the terminal or adapter a user explicitly registered.

M0 is intentionally small:

- one static Go binary;
- a private registry under `~/.amq-keepalive/`;
- explicit `attach`, `supervise`, `inject`, `doctor`, and `forget` commands;
- a fake `file` adapter for deterministic tests;
- supervisor logic that only talks to AMQ through the public `amq` CLI.

It does not parse AMQ mailbox, lock, presence, or target files. AMQ state is
observed and repaired only through `amq wake repair`, `amq wake`, and
`amq env --json`.

## Current Scope

The implemented surface now includes the M0 registry/supervisor proof and the
first M1 macOS pieces:

- `ghostty` adapter using Ghostty's native macOS AppleScript interface;
- `install-launchd` / `uninstall` for a user LaunchAgent supervisor.

The Ghostty adapter target contract is `ghostty:terminal:<id>`. `attach
--adapter ghostty` discovers the focused terminal in the selected tab of the
front Ghostty window when `--target` is omitted. Probe and inject fail closed
unless that id resolves to exactly one Ghostty terminal. Injection uses Ghostty's
native `input text` and `send key "enter"` AppleScript commands; it does not use
window titles, System Events, focus stealing, or the clipboard.

Old title targets are intentionally rejected. Re-run `attach --adapter ghostty`
to register a terminal-id target.

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
  `detached` until the user runs `attach --adapter ghostty` again.
- Ghostty terminal-id persistence across full Ghostty quit/relaunch and machine
  reboot has not been proven in this non-destructive test pass. Treat a missing
  id after restart as an expected stale-target condition: `doctor` will show the
  detached entry and last error, and reattach creates a fresh terminal-id entry.
- `install-launchd` installs only a per-user LaunchAgent for this supervisor.
