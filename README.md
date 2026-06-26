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

- `ghostty` adapter using macOS Accessibility/System Events;
- `install-launchd` / `uninstall` for a user LaunchAgent supervisor.

The Ghostty adapter targets an existing Ghostty window by title. `attach
--adapter ghostty` can discover the focused Ghostty window title when `--target`
is omitted. Probe and inject fail closed unless that title matches exactly one
Ghostty window. Injection activates Ghostty, raises the matching window, pastes
the AMQ payload, and presses Return. This requires macOS Accessibility
permission for the built binary or the terminal app running it.

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
- If a registered Ghostty window cannot be found, or if the title is ambiguous,
  the entry is marked detached until the user runs `attach` again.
- Title-based Ghostty targeting is the M1 compatibility path. A future adapter
  should prefer a durable unique Ghostty terminal identifier or a tool-controlled
  unique title marker if Ghostty does not expose suitable IPC.
- Ghostty injection uses the macOS clipboard briefly for paste delivery. The
  previous clipboard value is restored, but a user copy during that short window
  can still be overwritten.
- `install-launchd` installs only a per-user LaunchAgent for this supervisor.
