// Command pi-msg bridges the Pi coding agent (`pi --mode rpc`) to XMPP, so the
// agent can be driven from a chat client. See README.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "[pi-msg] %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Invocation-time initial prompt: spawn a fresh, on-demand persona with this
	// task as its very first prompt (beltino#18 doer flow). --command is an
	// alias; both are explicit "stateless spawn" requests, so the saved session
	// is never resumed and the text fires once at startup.
	promptFlag := flag.String("prompt", "", "initial task prompt for a fresh on-demand spawn (delivered as the persona's first prompt; forces a fresh session)")
	commandFlag := flag.String("command", "", "alias for --prompt")
	flag.Parse()

	cfg, err := loadConfig(configPath())
	if err != nil {
		if errors.Is(err, errNoConfig) {
			return fmt.Errorf("%w — nothing to do. See README for setup", err)
		}
		return err
	}
	acct, err := resolveAccount(cfg, os.Getenv("PI_MSG_ACCOUNT"))
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	debug := os.Getenv("PI_MSG_DEBUG") != ""
	b := NewBridge(acct, debug)
	b.initialPrompt = *promptFlag
	if b.initialPrompt == "" {
		b.initialPrompt = *commandFlag
	}
	if b.initialPrompt != "" {
		b.log("info", "initial prompt set via CLI (--prompt/--command)")
	}
	return b.Run(ctx)
}
