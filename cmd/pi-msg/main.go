// Command pi-msg bridges the Pi coding agent to XMPP.
package main

import (
	"fmt"
	"os"

	"github.com/zachpmanson/pi-msg/internal/pimsg"
)

func main() {
	if err := pimsg.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "[pi-msg] %v\n", err)
		os.Exit(1)
	}
}
