// Command pi-msg bridges the Pi coding agent (`pi --mode rpc`) to XMPP, so the
// agent can be driven from a chat client. See README.md.
package main

import (
	"context"
	"errors"
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
	return NewBridge(acct, debug).Run(ctx)
}
