// Package pimsg contains the XMPP bridge and its command-line runner.
package pimsg

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
)

// Run parses command-line flags and runs the bridge.
func Run() error {
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
	checkFlag := flag.Bool("check", false, "validate the config, print every account and its room rules, then exit without connecting")
	flag.Parse()

	cfg, err := loadConfig(configPath())
	if err != nil {
		if errors.Is(err, errNoConfig) {
			return fmt.Errorf("%w — nothing to do. See README for setup", err)
		}
		return err
	}
	if *checkFlag {
		// EVERY account, not just the selected one: the parse is strict and the
		// failure it guards against is fleet-wide, so a pre-flight that only
		// checked one account would pass while a sibling account's room silently
		// failed to resolve (#106 review).
		return checkAllAccounts(cfg)
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

// checkAllAccounts is the --check report: every account, resolved, with its
// room rules. It returns an error if any account fails to resolve, so a
// pre-flight run is a single command rather than one per account — and a broken
// sibling account cannot pass unnoticed.
func checkAllAccounts(cfg *Config) error {
	out := os.Stdout
	fmt.Fprintf(out, "config: %s\n", configPath())
	names := accountNames(cfg)
	sort.Strings(names)
	var failed []string
	for i, name := range names {
		if i > 0 {
			fmt.Fprintln(out)
		}
		acct, err := resolveAccount(cfg, name)
		if err != nil {
			// Report every failure, not just the first: fixing them one deploy at a
			// time is exactly what a pre-flight is meant to avoid.
			fmt.Fprintf(out, "account %s: INVALID: %v\n", name, err)
			failed = append(failed, name)
			continue
		}
		printAccountSummary(acct)
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d account(s) invalid: %s", len(failed), strings.Join(failed, ", "))
	}
	fmt.Fprintf(out, "\n%d account(s) valid\n", len(names))
	return nil
}

// printAccountSummary is the --check report for one account: the resolved account
// and, room by room, the addressing rules that will actually be in force. Room
// rules are the part worth printing — a room silently missing, or carrying an
// unexpected trigger, is the whole reason to run a pre-flight check.
func printAccountSummary(a ResolvedAccount) {
	out := os.Stdout
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
