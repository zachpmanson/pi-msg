package pimsg

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// Event is a single JSON object emitted by `pi --mode rpc` on stdout (an event
// or a response). Fields are accessed loosely since the RPC surface is wide
// and mostly pass-through.
type Event map[string]any

// Type returns the event's "type" field, or "".
func (e Event) Type() string { s, _ := e["type"].(string); return s }

// Str returns string field k, or "".
func (e Event) Str(k string) string { s, _ := e[k].(string); return s }

// Bool returns bool field k, or false.
func (e Event) Bool(k string) bool { b, _ := e[k].(bool); return b }

// F64 returns float64 field k (JSON numbers decode as float64), or 0.
func (e Event) F64(k string) float64 { f, _ := e[k].(float64); return f }

// Obj returns object field k as an Event, or nil.
func (e Event) Obj(k string) Event {
	if m, ok := e[k].(map[string]any); ok {
		return Event(m)
	}
	return nil
}

// Arr returns array field k as []any, or nil.
func (e Event) Arr(k string) []any {
	if a, ok := e[k].([]any); ok {
		return a
	}
	return nil
}

// success reports whether a response event indicates success.
func (e Event) success() bool { return e.Bool("success") }

// errText returns a response's error string, defaulting to "unknown".
func (e Event) errText() string {
	if s := e.Str("error"); s != "" {
		return s
	}
	return "unknown"
}

// maxRPCLine bounds a single stdout line. pi can emit large assistant/tool
// payloads on one line, well beyond bufio.Scanner's 64 KiB default.
const maxRPCLine = 64 << 20 // 64 MiB, excluding LF and optional preceding CR

// RPCClient spawns and talks to `pi --mode rpc`: it frames stdout as JSONL,
// writes one JSON command per line to stdin, and correlates request/response
// pairs by a generated id. Non-response events are delivered on Events().
type RPCClient struct {
	bin     string
	model   string
	cwd     string
	extPath string // optional companion extension to load via `-e`
	// sessionPath, when set, resumes that session file on launch via
	// `--session`. Set before Start().
	sessionPath string
	env         []string     // extra environment ("KEY=value") for the pi process
	stderr      func(string) // optional per-line stderr sink

	events   chan Event
	done     chan struct{} // closed once pi is reaped and stdout routing has stopped
	halt     chan struct{} // cancels routing/requests when transport shuts down
	stopCh   chan struct{} // closed by Stop to signal intentional shutdown
	stopOnce sync.Once
	writeMu  sync.Mutex // serialize writes without blocking lifecycle state/pipe close

	mu              sync.Mutex
	cmd             *exec.Cmd
	stdin           io.WriteCloser
	nextID          int
	pending         map[string]chan Event
	closed          bool
	readerCancelled bool // wait forcibly closed stdout after child exit
	exitErr         error
}

// NewRPCClient constructs a client. bin defaults to "pi" when empty. extPath,
// when non-empty, is a Pi extension file loaded via `-e`.
func NewRPCClient(bin, model, cwd, extPath string, stderr func(string)) *RPCClient {
	if bin == "" {
		bin = "pi"
	}
	return &RPCClient{
		bin:     bin,
		model:   model,
		cwd:     cwd,
		extPath: extPath,
		stderr:  stderr,
		events:  make(chan Event, 64),
		done:    make(chan struct{}),
		halt:    make(chan struct{}),
		stopCh:  make(chan struct{}),
		pending: make(map[string]chan Event),
	}
}

// Events is the stream of non-response events from pi. It is closed when pi
// exits.
func (c *RPCClient) Events() <-chan Event { return c.events }

// Done is closed once the pi process has exited.
func (c *RPCClient) Done() <-chan struct{} { return c.done }

// ExitErr returns the stdout transport failure, or otherwise the process exit
// error, if any (valid after Done closes).
func (c *RPCClient) ExitErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exitErr
}

// Start spawns pi and begins reading its output. The reader goroutines run
// until the process exits, at which point Events() and Done() are closed.
func (c *RPCClient) Start() error {
	args := []string{"--mode", "rpc"}
	if c.model != "" {
		args = append(args, "--model", c.model)
	}
	if c.extPath != "" {
		args = append(args, "-e", c.extPath)
	}
	if c.sessionPath != "" {
		args = append(args, "--session", c.sessionPath)
	}
	cmd := exec.Command(c.bin, args...)
	cmd.Dir = c.cwd
	cmd.Env = append(os.Environ(), c.env...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("pi stdin: %w", err)
	}
	// Own stdout separately from exec.Cmd: Wait must not close it before the
	// reader has drained the final events. Wait also cancels blocked routing.
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		return fmt.Errorf("pi stdout: %w", err)
	}
	cmd.Stdout = stdoutWriter
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		return fmt.Errorf("pi stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		return fmt.Errorf("starting %s: %w", c.bin, err)
	}

	_ = stdoutWriter.Close()

	c.mu.Lock()
	c.cmd = cmd
	c.stdin = stdin
	c.mu.Unlock()

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer stdout.Close()
		c.readStdout(stdout)
	}()
	go c.readStderr(stderr)
	go c.wait(readerDone, stdout)
	return nil
}

// shutTransport cancels requests/routing and closes stdin without holding the
// state lock during IO. In particular, a blocked writer cannot prevent teardown.
// Only stdout failures set exitErr here; wait preserves them over a kill error.
func (c *RPCClient) shutTransport(err error) {
	c.mu.Lock()
	if err != nil && c.exitErr == nil {
		c.exitErr = err
	}
	first := !c.closed
	c.closed = true
	stdin := c.stdin
	if first {
		close(c.halt)
	}
	c.mu.Unlock()
	if first && stdin != nil {
		_ = stdin.Close()
	}
}

// wait is the sole channel closer. It cancels blocked routing on process exit,
// then joins the reader so no sender can race with closing events.
func (c *RPCClient) wait(readerDone <-chan struct{}, stdout io.ReadCloser) {
	err := c.cmd.Wait()
	c.shutTransport(nil)
	// A descendant may inherit stdout and keep the pipe open after Pi exits.
	// Allow normal draining, but never wait indefinitely for that inherited FD.
	t := time.NewTimer(3 * time.Second)
	select {
	case <-readerDone:
	case <-t.C:
		c.mu.Lock()
		c.readerCancelled = true
		c.mu.Unlock()
		_ = stdout.Close()
		<-readerDone
	}
	t.Stop()
	c.mu.Lock()
	if c.exitErr == nil {
		c.exitErr = err
	}
	c.pending = make(map[string]chan Event)
	c.mu.Unlock()
	close(c.done)
	close(c.events)
}

// killAfterGrace bounds shutdown even if the child ignores EOF or SIGINT.
func (c *RPCClient) killAfterGrace() {
	t := time.NewTimer(3 * time.Second)
	defer t.Stop()
	select {
	case <-c.done:
	case <-t.C:
		c.kill()
	}
}

func (c *RPCClient) kill() {
	c.mu.Lock()
	cmd := c.cmd
	c.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func (c *RPCClient) stdoutFailed(err error) {
	c.shutTransport(err)
	c.kill()
	if c.stderr != nil {
		c.stderr(err.Error())
	}
}

// readStdout frames stdout as LF-delimited JSONL (ScanLines also accepts CRLF).
// Malformed JSON remains skippable, but framing/IO failures are terminal: never
// leave a live child blocked on stdout after its only reader has stopped.
func (c *RPCClient) readStdout(r io.Reader) {
	sc := bufio.NewScanner(r)
	// Reserve room for CRLF so a record of exactly maxRPCLine is supported.
	sc.Buffer(make([]byte, 0, 64*1024), maxRPCLine+2)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) > maxRPCLine {
			c.stdoutFailed(fmt.Errorf("pi stdout record exceeds %d-byte limit", maxRPCLine))
			return
		}
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			if c.stderr != nil {
				c.stderr(fmt.Sprintf("unparseable stdout JSON record (%d bytes)", len(line)))
			}
			continue
		}
		if !c.route(ev) {
			return
		}
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
			c.mu.Lock()
			cancelled := c.readerCancelled
			c.mu.Unlock()
			if cancelled {
				return // wait closed an inherited pipe after the process exited.
			}
		}
		c.stdoutFailed(fmt.Errorf("pi stdout framing/read failed (record limit %d bytes): %w", maxRPCLine, err))
		return
	}
	// EOF also ends the transport, even if the child has not exited yet.
	// Closing stdin allows normal exit; bound a child that keeps running.
	c.shutTransport(nil)
	go c.killAfterGrace()
}

// route delivers a response to its waiter, or forwards other events to the
// events channel.
func (c *RPCClient) route(ev Event) bool {
	if ev.Type() == "response" {
		if id := ev.Str("id"); id != "" {
			c.mu.Lock()
			ch, ok := c.pending[id]
			if ok {
				delete(c.pending, id)
			}
			c.mu.Unlock()
			if ok {
				ch <- ev
				return true
			}
		}
	}
	// Preserve already-readable final events on a normal child exit when
	// there is queue space. Only a blocked delivery needs cancellation.
	select {
	case c.events <- ev:
		return true
	default:
	}
	select {
	case c.events <- ev:
		return true
	case <-c.halt:
		return false
	}
}

// readStderr forwards pi's stderr line-by-line to the sink.
func (c *RPCClient) readStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	// Preserve the diagnostic stream's existing limit; only stdout carries
	// image-heavy aggregate RPC events.
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for sc.Scan() {
		if line := sc.Text(); line != "" && c.stderr != nil {
			c.stderr(line)
		}
	}
}

// write marshals a command and writes it as one line to pi's stdin.
func (c *RPCClient) write(cmd map[string]any) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	stdin, closed := c.stdin, c.closed
	c.mu.Unlock()
	if closed || stdin == nil {
		return errors.New("pi not running")
	}
	_, err = stdin.Write(b)
	return err
}

// Send writes a fire-and-forget command (no response correlation).
func (c *RPCClient) Send(cmd map[string]any) {
	if err := c.write(cmd); err != nil && c.stderr != nil {
		c.stderr("send failed: " + err.Error())
	}
}

// Request sends a command with a generated id and waits for the matching
// response, or fails on timeout / pi exit.
func (c *RPCClient) Request(ctx context.Context, cmd map[string]any, timeout time.Duration) (Event, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("pi not running")
	}
	c.nextID++
	id := fmt.Sprintf("r%d", c.nextID)
	ch := make(chan Event, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	cmd["id"] = id
	if err := c.write(cmd); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ev := <-ch:
		return ev, nil
	case <-timer.C:
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("timeout waiting for %v", cmd["type"])
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	case <-c.done:
		// stdout may have delivered the final response immediately before
		// teardown completed. Do not lose it to select's random choice.
		select {
		case ev := <-ch:
			return ev, nil
		default:
		}
		c.mu.Lock()
		delete(c.pending, id)
		err := c.exitErr
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("pi exited")
	}
}

// --- typed command helpers (mirror the TS RpcClient surface) ---

// Prompt sends a user message. streaming selects "steer" behavior when a run
// is already in flight; pass "" otherwise.
func (c *RPCClient) Prompt(message, streamingBehavior string) {
	cmd := map[string]any{"type": "prompt", "message": message}
	if streamingBehavior != "" {
		cmd["streamingBehavior"] = streamingBehavior
	}
	c.Send(cmd)
}

// promptWithID reports pipe failures synchronously but leaves acceptance on the
// ordered Events stream. It must not wait for a response inside an RPC handler.
func (c *RPCClient) promptWithID(id, message, streamingBehavior string) error {
	cmd := map[string]any{"id": id, "type": "prompt", "message": message}
	if streamingBehavior != "" {
		cmd["streamingBehavior"] = streamingBehavior
	}
	return c.write(cmd)
}

func (c *RPCClient) NewSession(ctx context.Context) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "new_session"}, 30*time.Second)
}

func (c *RPCClient) Compact(ctx context.Context, customInstructions string) (Event, error) {
	cmd := map[string]any{"type": "compact"}
	if customInstructions != "" {
		cmd["customInstructions"] = customInstructions
	}
	return c.Request(ctx, cmd, 120*time.Second)
}

func (c *RPCClient) SetThinkingLevel(ctx context.Context, level string) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "set_thinking_level", "level": level}, 30*time.Second)
}

func (c *RPCClient) SetModel(ctx context.Context, provider, modelID string) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "set_model", "provider": provider, "modelId": modelID}, 30*time.Second)
}

func (c *RPCClient) GetAvailableModels(ctx context.Context) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "get_available_models"}, 30*time.Second)
}

// GetState returns current session state (session file path, id, name, model).
func (c *RPCClient) GetState(ctx context.Context) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "get_state"}, 30*time.Second)
}

// ExportHTML renders the current session to a static HTML file at
// outputPath and returns the path the exporter actually wrote. Mirrors pi's
// `export_html` RPC command (the CLI /export equivalent), so /export is
// deterministic and never requires an agent turn.
func (c *RPCClient) ExportHTML(ctx context.Context, outputPath string) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "export_html", "outputPath": outputPath}, 60*time.Second)
}

// GetSessionStats returns token/cost/message tallies for the current session.
func (c *RPCClient) GetSessionStats(ctx context.Context) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "get_session_stats"}, 30*time.Second)
}

// SetSessionName sets the current session's display name.
func (c *RPCClient) SetSessionName(ctx context.Context, name string) (Event, error) {
	return c.Request(ctx, map[string]any{"type": "set_session_name", "name": name}, 30*time.Second)
}

func (c *RPCClient) Abort() { c.Send(map[string]any{"type": "abort"}) }

// ClearQueue removes pi's queued steering and follow-up messages and returns
// their text under data.steering / data.followUp. Requires pi >= 0.84.4; older
// pi answers with an unknown-command failure, which callers must tolerate.
//
// Pi's documented order is clear_queue BEFORE abort: `abort` on its own leaves
// the queue intact, so a steer that landed mid-run would start a fresh run the
// instant the aborted one stops.
func (c *RPCClient) ClearQueue(ctx context.Context) (Event, error) {
	// Short timeout on purpose: this sits in front of /abort, which must feel
	// immediate. Pi reads its stdin continuously, so a live pi answers well
	// inside this. An unresponsive one costs the drain, not the abort.
	return c.Request(ctx, map[string]any{"type": "clear_queue"}, 3*time.Second)
}

// CancelUI declines a pi UI request dialog (nobody is at the TUI to answer).
func (c *RPCClient) CancelUI(id string) {
	c.Send(map[string]any{"type": "extension_ui_response", "id": id, "cancelled": true})
}

// RespondUIRelay completes a companion-extension tool-action relay with a
// string result: "ok" on success, or a failure reason the extension surfaces
// to the model as the tool's error. The relay rides ui.select (not confirm)
// because confirm's response is boolean-only — the boolean said *that* the
// action failed but never *why*, so a server rejection (e.g. an upload slot
// refused as "too large: 207387434 bytes") never reached the model (issue #34).
func (c *RPCClient) RespondUIRelay(id, result string) {
	c.Send(map[string]any{"type": "extension_ui_response", "id": id, "value": result})
}

// Stop signals intentional shutdown and terminates the pi process.
func (c *RPCClient) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.shutTransport(nil)
		c.mu.Lock()
		cmd := c.cmd
		c.mu.Unlock()
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
			go c.killAfterGrace()
		}
	})
}

// StoppedIntentionally reports whether Stop was called (so the bridge can
// distinguish a requested shutdown from an unexpected pi crash).
func (c *RPCClient) StoppedIntentionally() bool {
	select {
	case <-c.stopCh:
		return true
	default:
		return false
	}
}
