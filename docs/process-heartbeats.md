# Per-process heartbeats

pi-msg observes `pi-processes` lifecycle events and periodically wakes a settled
agent with elapsed time and the last 40 output lines. Presence remains **dnd**
while processes run. Reports due together are coalesced; the bridge defers them
until an active turn settles. No chat reply is required for expected work.

By default the interval is ten minutes, or `PI_MSG_PROCESS_HEARTBEAT_MS` if set
(milliseconds; use a positive, finite, timer-safe value). The default coalescing
flush is two seconds (`PI_MSG_PROCESS_HEARTBEAT_FLUSH_MS` for testing).

## Opt-in loading

Without opt-in, existing extension loading, tools, and default heartbeat behavior
remain unchanged. To expose a per-start override, **replace** the existing
pi-processes package entry in the relevant Pi `settings.json` with:

```json
{
  "packages": [
    {
      "source": "npm:@aliou/pi-processes@0.12.0",
      "extensions": ["-extensions/processes/index.ts"]
    }
  ]
}
```

Keep other package entries. The exact negative filter disables **only** the
primary entry. Do not use `extensions: []`, disable the entire package, or add
`skills: []`: the logs/dock extensions and all skills should remain enabled.
Remove any separate `-e`/`extensions` entry loading that same primary factory.
Pi's actual package resolver was tested with this negative-only filter.

In the environment inherited by pi-msg and its Pi child, set:

```sh
export PI_MSG_PROCESS_EXTENSION=/absolute/path/to/node_modules/@aliou/pi-processes/extensions/processes/index.ts
```

Use the installed package's actual absolute path (for example under a persona's
`npm/node_modules`), not a package name, URL, relative path, or shell command.
This is a trusted code-loading path, not a sandbox. pi-msg already loads its
embedded companion; do not additionally load the companion or primary extension.
Apply both settings together to a disposable test profile first. No deployed
settings, Nix configuration, or installed dependencies are modified by this
feature. Removing opt-in requires restoring the normal primary package entry.

## Tool behavior

With opt-in, the original `process` tool gains an optional **top-level** field:

```json
{
  "action": "start",
  "name": "build",
  "command": "make build",
  "heartbeatSeconds": 30
}
```

- Omitted: retain the environment/default interval.
- `0`: disable **periodic pi-msg heartbeats only** for this process. Upstream
  exit, failure, kill, and log-match notifications, output collection, and
  process presence are unchanged. Use the original `notify` options normally.
- Integer `1` through `2147483`: override the interval in seconds. The maximum
  keeps milliseconds below Node's signed 32-bit timer ceiling.
- Negative, fractional, non-number, non-finite, or oversized values are rejected
  before upstream execution. An explicit field on any action other than
  `start` is rejected, including `update`; changing an existing process's
  interval is not supported.

Each process has an independent timer anchored to its original start time, not
its tool-result time. A successful start replaces the default schedule using
`result.details.process.id`. Missed boundaries after an event-loop stall are
skipped, not replayed in a catch-up flood. Ended processes and shutdown cancel
pending schedules/reports; a rapid exit before the start returns cannot revive
a stale timer. Clearing finished entries retains live processes' timers.
Intervals are session-local, not persisted or restored across Pi restarts.
There is no new command prefix or bridge configuration field.

## API seam and compatibility

The companion dynamically imports the **unmodified upstream default extension
factory** and invokes it once with a proxy of Pi's `ExtensionAPI`. Only the
registration of the full `process` `ToolDefinition` is intercepted. The schema
is extended without mutating the original, and execution delegates to the
original `execute`, stripping only `heartbeatSeconds`. All other options,
arguments (including abort signal, update callback, and context), invocation
receiver, results/errors, renderers, and schema/prompt metadata are retained.
All other upstream factory registrations use Pi normally. This is not a process
manager fork, a replacement spawn implementation, or a `command:start` wrapper;
`getAllTools()` exposes metadata, not executable implementations.

The scheduling observer uses the upstream `processes:started`/`ended`/`changed`
bus events and `processes:request:list`/`combined_output` queries. Assumptions:
a top-level object tool schema, start events with `id`/`startTime`, successful
start results with `details.process.id`, and opaque unique process IDs. Recheck
these seams when upgrading upstream. Bad paths/factories/schema shapes fail
extension loading visibly; Pi can otherwise continue running, so inspect load
errors rather than assuming opt-in succeeded.

Validated on Linux with Pi **0.84.4**, both installed and freshly downloaded
upstream **0.12.0**, using Pi's real resource resolver and TypeScript loader.
The filter preserved logs, dock, and skills; one authoritative manager answered;
the optional schema was exposed; positive/zero/default starts and their original
log-match/success notifications worked. macOS was not smoke-tested; upstream
is POSIX-only.

At validation, npm latest was **0.13.2**, but the **0.13.x** line targets Pi
**1.0.0** and is not compatible with this Pi runtime: its notification cleanup
calls the return values of `pi.on()` as disposers, whereas Pi 0.84.4 returns
`void`. A real 0.13.2 loader smoke reached shutdown and failed there with
`TypeError: dispose is not a function`; published 0.13.0/0.13.1 have the same
requirement. Thus 0.12.0 is the latest compatible release checked, not a claim
that all future versions work. Do not upgrade a live Pi to test this adapter.

## Verification

Focused tests need no Pi installation, model, credentials, or XMPP:

```sh
nix develop --command bash -c 'go test ./... && make test-companion && go vet ./... && go build -o /tmp/pi-msg-heartbeat ./cmd/pi-msg'
```

The companion tests exercise independent/default/zero intervals, validation
before side effects, full delegation, replacement/anchoring, coalescing,
missed-boundary handling, rapid exit, end/clear/shutdown, and in-flight cleanup.

For a real-loader check, pass package roots (not entry files):

```sh
nix develop --command node scripts/process-heartbeat-smoke.mjs \
  /absolute/path/to/@earendil-works/pi-coding-agent \
  /absolute/path/to/@aliou/pi-processes
```

The probe creates disposable HOME/agent/cwd state, resolves the exact package
filter, loads the actual companion plus retained upstream entries, starts three
three-second shell processes, and captures notifications/relay calls locally.
It invokes no LLM and opens no XMPP connection. It does not install dependencies;
prepare alternate upstream versions only in `/tmp` (e.g. `npm pack`, extract,
then `npm install --omit=dev --ignore-scripts --legacy-peer-deps` in that scratch
package). Leave live `node_modules` and persona settings untouched.

### Disposable droid/XMPP smoke (operator-owned)

Use an authorized temporary droid profile/account and a freshly built binary;
do not restart production agents. Enable the above filter/env only on the
adapted droid. Optionally keep an unadapted droid as a loading/behavior control.
Set `PI_MSG_PROCESS_HEARTBEAT_MS=30000` on the adapted droid, then ask it to start
three concurrent named processes lasting about 40 seconds: `heartbeatSeconds:8`,
omitted, and `0`, with explicit `notify` log-match and success settings. Have
it end its turn rather than sleep/poll. Check the saved tool calls/results,
bridge/session logs, and XMPP presence:

1. The 8-second process wakes first; omitted waits until about 30 seconds
   (plus flush/turn-settlement delay); zero never contributes a periodic report.
2. All three still emit upstream log-match and success notifications and keep
   process presence busy until exit. Distinguish these from pi-msg periodic
   `[pi-msg: process ... has been running ...]` prompts.
3. Clearing finished entries while one process remains live must not suppress
   its next heartbeat. Invalid overrides/explicit overrides on `list` or
   `update` must fail without spawning/updating anything.
4. Exit or stop before the next boundary produces no stale heartbeat; shutdown
   or session replacement leaves no old-instance timers/relays.
5. The control still exposes the original tool, loads one manager, and starts
   processes normally without the new field.

Stop the disposable droids and test driver after collecting evidence. Never
upload their credential-bearing profiles or raw authentication files.
