package pimsg

import (
	_ "embed"
	"fmt"
	"os"
)

// xmppToolsExt is the companion Pi extension source, embedded into the binary
// so there's nothing to install separately. pi-msg writes it to a temp file at
// startup and launches pi with `-e <that file>`. See internal/pimsg/piext/xmpp-tools.ts.
//
//go:embed piext/xmpp-tools.ts
var xmppToolsExt string

// relayPrefix marks a pi extension `notify` as a pi-msg action relay rather
// than a genuine user notification. Kept in sync with RELAY_PREFIX in
// piext/xmpp-tools.ts.
const relayPrefix = "pi-msg-relay:"

// writeTempExtension materialises the embedded companion extension to a temp
// `.ts` file (jiti keys off the extension) and returns its path. The caller is
// responsible for removing it.
func writeTempExtension() (string, error) {
	f, err := os.CreateTemp("", "pi-msg-ext-*.ts")
	if err != nil {
		return "", fmt.Errorf("creating extension temp file: %w", err)
	}
	if _, err := f.WriteString(xmppToolsExt); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", fmt.Errorf("writing extension: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("closing extension: %w", err)
	}
	return f.Name(), nil
}
