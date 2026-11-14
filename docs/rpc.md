# Pi RPC stdout transport

`pi-msg` reads Pi's stdout as LF-delimited JSON objects. CRLF is accepted;
carriage returns inside JSON strings are not delimiters. Empty records are
ignored, and a final record without a newline is accepted, as before.

## Record size and failure policy

The supported maximum is **64 MiB (67,108,864 bytes) per JSON record**, excluding
LF and an optional preceding CR. This is a per-record limit, not a session or
individual image limit. Pi's `agent_end` aggregates messages, including image
payloads, so it can be much larger than any individual message. Buffers grow on
demand rather than reserving 64 MiB for every client; decoding also requires
memory for the resulting event.

A record exceeding this bound, or any stdout read/framing error, is **terminal**:

- Record/read failure diagnostics identify the transport and limit/error, never
  include the record's text, prompts, or image data.
- The transport rejects new writes, closes stdin to release blocked writers,
  and cancels pending requests. Pi is killed and reaped, rather than left alive
  blocked on stdout with no reader.
- `ExitErr()` retains the original framing/read failure, not the consequent
  process-kill error. `Done()` and `Events()` close after the child is reaped and
  stdout routing has stopped. The bridge's existing unexpected-exit path reports
  the error and shuts down; this is **not** marked as an intentional `Stop()`.
- Malformed JSON records remain skippable, preserving existing behavior; the
  diagnostic now reports only the byte count, not a private payload excerpt.

EOF also shuts down the transport and closes stdin. A child that keeps running
without stdout is killed after a three-second grace period. Intentional `Stop()`
still sends SIGINT with the same three-second hard-kill fallback, and is safe to
call concurrently. Writes are serialized separately from lifecycle state so a
blocked stdin write cannot prevent termination.

Only the teardown goroutine closes the public channels, after joining the
stdout reader. If a descendant keeps stdout open after Pi exits, teardown
closes that inherited pipe after a three-second drain grace period. Routing can
be cancelled on child exit or shutdown even if the 64-event queue is full. Buffered events remain available, but shutdown may drop
undelivered events rather than hang indefinitely waiting for a consumer. Normal
live-client event order, UI relays, and response correlation are unchanged;
readable final responses are retained when the child exits. No
inbox acknowledgment/replay policy changes are made: losing the transport is not
an `agent_settled` event or a successful prompt acknowledgment.

## Regression coverage and incident attribution

`internal/pimsg/rpc_reader_test.go` uses isolated test-binary children and real
kernel pipes, without calling Pi, a model provider, XMPP, or the owner. Coverage
includes a small successful UI relay followed by a 14 MiB image-heavy aggregate,
settlement, a new prompt, and a correlated request; a frame exceeding 64 MiB
while both a request and stdin writer are pending; injected IO failure; concurrent
shutdown/exit/failure; EOF with a still-live child; an inherited open stdout
writer after child exit; a full event queue at child exit; and exact-limit and
LF/CRLF/string framing checks.

[zpm/pi-msg#165](https://github.com/zachpmanson/pi-msg/issues/165) followed a stalled Falco session. The deployed 8 MiB scanner silently
ignored its terminal error, a confirmed defect reproduced offline. An independent
reconstruction produced an approximately 14 MB aggregate, but the incident's
original stdout wire was not captured. Oversized `agent_end` is a **probable**
explanation, not a proven observation of that particular run. Raising the limit
alone would not fix silent reader death; terminal error handling and lifecycle
regressions are essential.
