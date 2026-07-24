package main

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

// Obj returns object field k as an Event, or nil.
func (e Event) Obj(k string) Event {
	if m, ok := e[k].(map[string]any); ok {
		return Event(m)
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
const maxRPCLine = 8 << 20 // 8 MiB

// RPCClient spawns and talks to `pi --mode rpc`: it frames stdout as JSONL,
// writes one JSON command per line to stdin, and correlates request/response
// pairs by a generated id. Non-response events are delivered on Events().
type RPCClient struct {
	bin    string
	model  string
	cwd    string
	stderr func(string) // optional per-line stderr sink

	events chan Event
	done   chan struct{} // closed once pi exits
	stopCh chan struct{} // closed by Stop to signal intentional shutdown

	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	nextID  int
	pending map[string]chan Event
	closed  bool
	exitErr error
}

// NewRPCClient constructs a client. bin defaults to "pi" when empty.
func NewRPCClient(bin, model, cwd string, stderr func(string)) *RPCClient {
	if bin == "" {
		bin = "pi"
	}
	return &RPCClient{
		bin:     bin,
		model:   model,
		cwd:     cwd,
		stderr:  stderr,
		events:  make(chan Event, 64),
		done:    make(chan struct{}),
		stopCh:  make(chan struct{}),
		pending: make(map[string]chan Event),
	}
}

// Events is the stream of non-response events from pi. It is closed when pi
// exits.
func (c *RPCClient) Events() <-chan Event { return c.events }

// Done is closed once the pi process has exited.
func (c *RPCClient) Done() <-chan struct{} { return c.done }

// ExitErr returns the process exit error, if any (valid after Done closes).
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
	cmd := exec.Command(c.bin, args...)
	cmd.Dir = c.cwd
	cmd.Env = os.Environ()

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("pi stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("pi stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("pi stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting %s: %w", c.bin, err)
	}

	c.mu.Lock()
	c.cmd = cmd
	c.stdin = stdin
	c.mu.Unlock()

	go c.readStdout(stdout)
	go c.readStderr(stderr)
	go c.wait()
	return nil
}

// wait reaps the process and tears down: it closes done, fails pending
// waiters (via done), and closes the events channel.
func (c *RPCClient) wait() {
	err := c.cmd.Wait()

	c.mu.Lock()
	c.closed = true
	c.exitErr = err
	_ = c.stdin.Close()
	c.mu.Unlock()

	close(c.done)   // unblocks any pending Request waiters
	close(c.events) // signals the bridge loop that pi is gone
}

// readStdout frames stdout as JSONL and routes each object.
func (c *RPCClient) readStdout(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxRPCLine)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			if c.stderr != nil {
				n := len(line)
				if n > 200 {
					n = 200
				}
				c.stderr("unparseable stdout line: " + string(line[:n]))
			}
			continue
		}
		c.route(ev)
	}
}

// route delivers a response to its waiter, or forwards other events to the
// events channel.
func (c *RPCClient) route(ev Event) {
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
				return
			}
		}
	}
	select {
	case c.events <- ev:
	case <-c.done:
	}
}

// readStderr forwards pi's stderr line-by-line to the sink.
func (c *RPCClient) readStderr(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxRPCLine)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.stdin == nil {
		return errors.New("pi not running")
	}
	_, err = c.stdin.Write(b)
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

func (c *RPCClient) Abort() { c.Send(map[string]any{"type": "abort"}) }

// CancelUI declines a pi UI request dialog (nobody is at the TUI to answer).
func (c *RPCClient) CancelUI(id string) {
	c.Send(map[string]any{"type": "extension_ui_response", "id": id, "cancelled": true})
}

// Stop signals intentional shutdown and terminates the pi process.
func (c *RPCClient) Stop() {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	c.mu.Lock()
	cmd := c.cmd
	c.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(os.Interrupt)
		// Fall back to a hard kill shortly after, in case pi ignores SIGINT.
		go func() {
			t := time.NewTimer(3 * time.Second)
			defer t.Stop()
			select {
			case <-c.done:
			case <-t.C:
				_ = cmd.Process.Kill()
			}
		}()
	}
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
