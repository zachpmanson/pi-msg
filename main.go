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
	"strings"
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
	// --check validates and reports, and never connects. It exists because the
	// config format is strict and the failure mode is a fleet-wide one: every
	// bridge restarts together, and a rejected config takes all of them down.
	// Being able to run the parser against the real config first turns that into
	// a pre-flight check instead of an outage.
	checkFlag := flag.Bool("check", false, "validate the config, print the resolved account and its room rules, then exit without connecting")
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
	if *checkFlag {
		printAccountSummary(acct)
		return nil
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

// printAccountSummary is the --check report: the resolved account and, room by
// room, the addressing rules that will actually be in force. Room rules are the
// part worth printing — a room silently missing, or carrying an unexpected
// trigger, is the whole reason to run a pre-flight check.
func printAccountSummary(a ResolvedAccount) {
	out := os.Stdout
	fmt.Fprintf(out, "config: %s\n", configPath())
	fmt.Fprintf(out, "account: %s\n", a.Name)
	fmt.Fprintf(out, "jid: %s\n", a.JID)
	fmt.Fprintf(out, "owner: %s\n", a.Owner)
	fmt.Fprintf(out, "service: %s\n", a.Service)
	fmt.Fprintf(out, "resource: %s\n", a.Resource)
	if a.Workdir != "" {
		fmt.Fprintf(out, "workdir: %s\n", a.Workdir)
	}
	if a.Model != "" {
		fmt.Fprintf(out, "model: %s\n", a.Model)
	}
	fmt.Fprintf(out, "nick: %s\n", a.Nick)
	fmt.Fprintf(out, "mam backfill: %t\n", a.MAM)
	if len(a.Rooms) == 0 {
		fmt.Fprintf(out, "rooms: none (1:1 mode)\n")
	} else {
		fmt.Fprintf(out, "rooms: %d\n", len(a.Rooms))
		for _, r := range a.Rooms {
			fmt.Fprintf(out, "  %s  trigger=%q reactions=%t\n", r, a.TriggerFor(r), a.ReactionsFor(r))
		}
	}
	if a.ErrorRoom != "" {
		fmt.Fprintf(out, "error room (write-only): %s\n", a.ErrorRoom)
	}
	fmt.Fprintf(out, "tools: %s\n", strings.Join(toolNames(a), ", "))
}
