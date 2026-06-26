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

M0 proves the registry and supervisor contract. It does not install launchd jobs
and does not include a Ghostty/macOS terminal adapter yet. `install-launchd` and
`uninstall` are present as explicit M1 placeholders.

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

For AMQ wake integration, `supervise` starts AMQ with this binary as the
`--inject-via` executable. AMQ appends the message payload as the final argument,
and `amq-keepalive inject <adapter> <target> <payload>` hands it to the adapter.
