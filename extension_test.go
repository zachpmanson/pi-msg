package main

import (
	"os"
	"strings"
	"testing"
)

// TestExtensionBeforeAgentStartWiring guards the one link between the account's
// beforeAgentStartText and the prompt the agent actually reads: bridge.go
// exports PI_MSG_BEFORE_AGENT_START_TEXT and the embedded extension reads it on
// before_agent_start. Either half vanishing silently disables the feature, so
// assert both halves here.
func TestExtensionBeforeAgentStartWiring(t *testing.T) {
	if !strings.Contains(xmppToolsExt, "PI_MSG_BEFORE_AGENT_START_TEXT") {
		t.Error("embedded extension does not read PI_MSG_BEFORE_AGENT_START_TEXT")
	}
	if !strings.Contains(xmppToolsExt, "before_agent_start") {
		t.Error("embedded extension no longer hooks before_agent_start")
	}
}

func TestWriteTempExtension(t *testing.T) {
	path, err := writeTempExtension()
	if err != nil {
		t.Fatalf("writeTempExtension: %v", err)
	}
	defer os.Remove(path)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read extension: %v", err)
	}
	if string(raw) != xmppToolsExt {
		t.Error("temp extension does not match the embedded source")
	}
}
