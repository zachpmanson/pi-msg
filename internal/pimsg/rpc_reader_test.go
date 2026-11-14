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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Runs only in an isolated test-binary child; no Pi, provider, or XMPP calls.
func TestRPCPipeChild(t *testing.T) {
	mode := os.Getenv("PI_MSG_RPC_TEST_CHILD")
	if mode == "" {
		return
	}
	out := json.NewEncoder(os.Stdout)
	in := bufio.NewReader(os.Stdin)
	read := func() Event {
		line, err := in.ReadBytes('\n')
		if err != nil {
			os.Exit(0)
		}
		var ev Event
		if json.Unmarshal(line, &ev) != nil {
			os.Exit(2)
		}
		return ev
	}
	switch mode {
	case "large":
		_ = out.Encode(Event{"type": "extension_ui_request", "method": "select", "id": "relay-1"})
		reply := read()
		if reply.Type() != "extension_ui_response" || reply.Str("id") != "relay-1" || reply.Str("value") != "ok" {
			os.Exit(3)
		}
		// Each image-bearing message is <3 MiB, but agent_end aggregates 14 MiB.
		var messages []any
		for i := 0; i < 7; i++ {
			messages = append(messages, Event{"role": "toolResult", "content": []any{Event{"type": "image", "data": strings.Repeat("x", 2<<20)}}})
		}
		_ = out.Encode(Event{"type": "agent_end", "messages": messages})
		_ = out.Encode(Event{"type": "agent_settled"})
	case "overflow":
		_ = read() // A correlated request will remain pending.
		_ = out.Encode(Event{"type": "ready"})
		// Consume just one byte of a second large command, leaving its writer
		// blocked on the real stdin pipe while stdout exceeds the limit.
		_, _ = in.ReadByte()
		_ = out.Encode(Event{"type": "writer_blocked"})
		_, _ = io.WriteString(os.Stdout, `{"type":"agent_end","private":"`)
		chunk := strings.Repeat("s", 1<<20)
		for i := 0; i < 80; i++ {
			_, _ = io.WriteString(os.Stdout, chunk)
		}
		// Do not consume stdin or exit voluntarily: the reader must kill us.
		select {}
	case "flood":
		for i := 0; i < 256; i++ {
			_ = out.Encode(Event{"type": "agent_start"})
		}
		os.Exit(0)
	case "eof":
		_ = out.Encode(Event{"type": "ready"})
		_ = os.Stdout.Close()
		time.Sleep(time.Hour) // Stay alive with no stdout consumer until killed.
	case "idle":
		_ = out.Encode(Event{"type": "ready"})
		for {
			_ = read()
		}
	}
	for {
		cmd := read()
		if cmd.Type() == "prompt" {
			_ = out.Encode(Event{"type": "message_start", "message": Event{"role": "user"}})
		}
		if id := cmd.Str("id"); id != "" {
			_ = out.Encode(Event{"type": "response", "id": id, "success": true})
		}
	}
}

func rpcPipeClient(t *testing.T, mode string, sink func(string)) *RPCClient {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Start normally, including real kernel pipes. Ignore Pi's CLI arguments.
	path := filepath.Join(t.TempDir(), "fake-pi")
	script := fmt.Sprintf("#!/bin/sh\nexec '%s' -test.run '^TestRPCPipeChild$'\n", strings.ReplaceAll(exe, "'", "'\\''"))
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	c := NewRPCClient(path, "", "", "", sink)
	c.env = []string{"PI_MSG_RPC_TEST_CHILD=" + mode}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop(); rpcAwaitDone(t, c) })
	return c
}

func rpcAwaitDone(t *testing.T, c *RPCClient) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(30 * time.Second):
		t.Fatal("transport did not terminate and reap child")
	}
	if c.cmd.ProcessState == nil {
		t.Fatal("Done closed without reaping child")
	}
}

func rpcNextEvent(t *testing.T, c *RPCClient, typ string) Event {
	t.Helper()
	select {
	case ev, ok := <-c.Events():
		if !ok || ev.Type() != typ {
			t.Fatalf("event = %v, open=%v, want %s; exit=%v", ev.Type(), ok, typ, c.ExitErr())
		}
		return ev
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", typ)
		return nil
	}
}

func TestRPCPipeLargeAggregateAndNextPrompt(t *testing.T) {
	c := rpcPipeClient(t, "large", nil)
	ev := rpcNextEvent(t, c, "extension_ui_request")
	c.RespondUIRelay(ev.Str("id"), "ok")
	aggregate := rpcNextEvent(t, c, "agent_end")
	if len(aggregate.Arr("messages")) != 7 {
		t.Fatal("lost image messages")
	}
	for _, message := range aggregate.Arr("messages") {
		image := Event(message.(map[string]any)).Arr("content")[0].(map[string]any)
		if len(image["data"].(string)) != 2<<20 {
			t.Fatal("truncated image")
		}
	}
	rpcNextEvent(t, c, "agent_settled")
	c.Prompt("next prompt", "")
	rpcNextEvent(t, c, "message_start")
	res, err := c.GetState(context.Background())
	if err != nil || !res.success() || res.Str("id") == "" {
		t.Fatalf("subsequent correlated request: %v, %v", res, err)
	}
	t.Log("real-pipe UI relay, 14 MiB aggregate, settlement, next prompt and correlated request succeeded")
}

func TestRPCPipeOversizeTerminatesPendingRequestAndWriter(t *testing.T) {
	var mu sync.Mutex
	var logs []string
	c := rpcPipeClient(t, "overflow", func(s string) { mu.Lock(); logs = append(logs, s); mu.Unlock() })
	requestDone := make(chan error, 1)
	go func() {
		_, err := c.Request(context.Background(), map[string]any{"type": "get_state"}, time.Minute)
		requestDone <- err
	}()
	rpcNextEvent(t, c, "ready")
	writerDone := make(chan error, 1)
	go func() { writerDone <- c.write(map[string]any{"type": "prompt", "message": strings.Repeat("w", 8<<20)}) }()
	rpcNextEvent(t, c, "writer_blocked")
	rpcAwaitDone(t, c)
	for name, ch := range map[string]<-chan error{"request": requestDone, "writer": writerDone} {
		select {
		case err := <-ch:
			if err == nil {
				t.Fatalf("%s succeeded after failure", name)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s stuck after transport failure", name)
		}
	}
	if err := c.ExitErr(); err == nil || !strings.Contains(err.Error(), "67108864") || !strings.Contains(err.Error(), "stdout") {
		t.Fatalf("lost framing error: %v", err)
	}
	if c.StoppedIntentionally() {
		t.Fatal("reader failure suppressed crash reporting")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logs) != 1 || strings.Contains(logs[0], "private") || strings.Contains(logs[0], strings.Repeat("s", 20)) {
		t.Fatalf("missing or unsafe failure diagnostic: %v", logs)
	}
	if _, ok := <-c.Events(); ok {
		t.Fatal("Events remained open")
	}
	t.Logf("terminal framing failure reaped child and unblocked request/writer: %v", c.ExitErr())
}

// Inject at the reader IO boundary, retaining a real child for kill/reap.
func rpcInjectedReaderClient(t *testing.T) (*RPCClient, *io.PipeWriter) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	c := NewRPCClient("", "", "", "", nil)
	cmd := exec.Command(exe, "-test.run", "^TestRPCPipeChild$")
	cmd.Env = append(os.Environ(), "PI_MSG_RPC_TEST_CHILD=idle")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c.cmd, c.stdin = cmd, stdin
	// Ensure the child is running before racing its normal stdin-EOF exit
	// against Stop and an error from its injected stdout reader.
	if _, err := bufio.NewReader(stdout).ReadString('\n'); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	_ = stdout.Close()
	r, w := io.Pipe()
	readerDone := make(chan struct{})
	go func() { defer close(readerDone); defer r.Close(); c.readStdout(r) }()
	go c.wait(readerDone, r)
	t.Cleanup(func() { c.Stop(); rpcAwaitDone(t, c) })
	return c, w
}

func TestRPCInjectedReadErrorIsTerminal(t *testing.T) {
	c, w := rpcInjectedReaderClient(t)
	requestDone := make(chan error, 1)
	go func() {
		_, err := c.GetState(context.Background())
		requestDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		pending := len(c.pending)
		c.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request never became pending")
		}
		time.Sleep(time.Millisecond)
	}
	injected := errors.New("injected stdout IO failure")
	_ = w.CloseWithError(injected)
	rpcAwaitDone(t, c)
	if !errors.Is(c.ExitErr(), injected) {
		t.Fatalf("ExitErr = %v, want original IO failure", c.ExitErr())
	}
	select {
	case err := <-requestDone:
		if err == nil {
			t.Fatal("pending request succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("request stuck")
	}
}

type rpcNotifyingWriter struct {
	io.WriteCloser
	written chan struct{}
}

func (w rpcNotifyingWriter) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	close(w.written)
	return n, err
}

func TestRPCFinalResponseSurvivesChildExit(t *testing.T) {
	c, w := rpcInjectedReaderClient(t)
	written := make(chan struct{})
	c.mu.Lock()
	c.stdin = rpcNotifyingWriter{c.stdin, written}
	c.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		res, err := c.GetState(context.Background())
		if err == nil && !res.success() {
			err = errors.New("final response was not successful")
		}
		result <- err
	}()
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not write")
	}
	_ = c.stdin.Close() // Child exits, while stdout's final records remain unread.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not exit")
		}
		time.Sleep(time.Millisecond)
	}
	_, err := io.WriteString(w, "{\"type\":\"agent_settled\"}\n{\"type\":\"response\",\"id\":\"r1\",\"success\":true}\n")
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	rpcAwaitDone(t, c)
	if err := <-result; err != nil {
		t.Fatalf("lost final response at child exit: %v", err)
	}
	rpcNextEvent(t, c, "agent_settled")
}

func TestRPCConcurrentStopExitAndReaderFailure(t *testing.T) {
	for i := 0; i < 12; i++ {
		c, w := rpcInjectedReaderClient(t)
		failure := errors.New("injected concurrent reader failure")
		var wg sync.WaitGroup
		for j := 0; j < 8; j++ {
			wg.Add(1)
			go func() { defer wg.Done(); c.Stop() }()
		}
		wg.Add(2)
		go func() { defer wg.Done(); c.Send(map[string]any{"type": "exit"}) }()
		go func() { defer wg.Done(); _ = w.CloseWithError(failure) }()
		wg.Wait()
		rpcAwaitDone(t, c)
		if !errors.Is(c.ExitErr(), failure) {
			t.Fatalf("lost reader failure during shutdown: %v", c.ExitErr())
		}
	}
}

func TestRPCChildExitUnblocksFullEventQueue(t *testing.T) {
	c := rpcPipeClient(t, "flood", nil)
	// Deliberately do not consume Events: process exit must cancel routing.
	rpcAwaitDone(t, c)
	for range c.Events() {
	}
	if err := c.ExitErr(); err != nil {
		t.Fatalf("normal process exit reported failure: %v", err)
	}
}

func TestRPCWaitBoundsInheritedStdout(t *testing.T) {
	c, w := rpcInjectedReaderClient(t)
	defer w.Close()
	// The real child exits normally on stdin EOF, but the injected stdout
	// writer remains open, just like a descendant's inherited pipe descriptor.
	c.shutTransport(nil)
	rpcAwaitDone(t, c)
	if err := c.ExitErr(); err != nil {
		t.Fatalf("teardown pipe close replaced successful process exit: %v", err)
	}
}

func TestRPCStdoutEOFDoesNotLeaveLiveChild(t *testing.T) {
	c := rpcPipeClient(t, "eof", nil)
	rpcNextEvent(t, c, "ready")
	rpcAwaitDone(t, c)
	if c.ExitErr() == nil || c.StoppedIntentionally() {
		t.Fatalf("unexpected EOF child exit: %v intentional=%v", c.ExitErr(), c.StoppedIntentionally())
	}
}

func TestRPCJSONLFramingAndRedactedMalformedRecord(t *testing.T) {
	var logs []string
	c := NewRPCClient("", "", "", "", func(s string) { logs = append(logs, s) })
	// JSON-escaped CR/LF, literal Unicode line separator, and spaces in a string
	// must survive. CRLF/empty records are accepted; bare CR is not a delimiter.
	input := "\r\n{private-payload}\n" + `{"type":"text","value":"a\r\nb c"}` + "\r\n" +
		`{"type":"bad"}` + "\r" + `{"type":"not-a-new-record"}` + "\n" + `{"type":"agent_settled"}`
	c.readStdout(strings.NewReader(input))
	if got := (<-c.Events()).Str("value"); got != "a\r\nb c" {
		t.Fatalf("string corrupted: %q", got)
	}
	if got := (<-c.Events()).Type(); got != "agent_settled" {
		t.Fatalf("framing changed: %q", got)
	}
	if len(logs) != 2 || strings.Contains(strings.Join(logs, " "), "private-payload") {
		t.Fatalf("unsafe/missing malformed JSON diagnostic: %v", logs)
	}
}

func TestRPCExactLineLimit(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n", ""} {
		t.Run(fmt.Sprintf("ending-%q", ending), func(t *testing.T) {
			prefix := `{"type":"agent_end","data":"`
			suffix := `"}`
			// Stream the source to avoid retaining another full-size input copy
			// alongside the scanner buffer and decoded event under -race.
			parts := []io.Reader{strings.NewReader(prefix)}
			chunk := strings.Repeat("x", 64<<10)
			for remaining := maxRPCLine - len(prefix) - len(suffix); remaining > 0; {
				n := min(remaining, len(chunk))
				parts = append(parts, strings.NewReader(chunk[:n]))
				remaining -= n
			}
			parts = append(parts, strings.NewReader(suffix+ending))
			c := NewRPCClient("", "", "", "", nil)
			c.readStdout(io.MultiReader(parts...))
			if c.ExitErr() != nil || len(c.events) != 1 {
				t.Fatalf("exact-limit record rejected: events=%d err=%v", len(c.events), c.ExitErr())
			}
		})
	}
}
