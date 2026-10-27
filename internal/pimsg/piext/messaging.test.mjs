// Exercise the real companion tool's relay-result handling without Pi or XMPP.
import { test } from "node:test";
import assert from "node:assert/strict";
import { stripTypeScriptTypes } from "node:module";
import { readFileSync } from "node:fs";

const source = stripTypeScriptTypes(readFileSync(new URL("./xmpp-tools.ts", import.meta.url), "utf8"))
	.replace('import { Type } from "typebox";', 'const Type = new Proxy({}, { get: () => (value) => value });');
const { default: xmppTools } = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);

function fixture(t, result) {
	const previous = { ...process.env };
	process.env.PI_MSG_TOOLS = "messaging,file";
	delete process.env.PI_MSG_PROCESS_EXTENSION;
	t.after(() => {
		for (const name of ["PI_MSG_TOOLS", "PI_MSG_PROCESS_EXTENSION"]) {
			if (previous[name] === undefined) delete process.env[name];
			else process.env[name] = previous[name];
		}
	});
	t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
	const hooks = new Map(), tools = [], relays = [];
	xmppTools({
		events: { on: () => () => {} },
		on: (name, fn) => hooks.set(name, fn),
		registerTool: (tool) => tools.push(tool),
	});
	hooks.get("session_start")({}, {
		sessionManager: { getSessionId: () => "test-messaging" },
		ui: { select: async (title) => {
			relays.push(JSON.parse(title.slice("pi-msg-relay:".length)));
			return result;
		} },
	});
	t.after(() => hooks.get("session_shutdown")());
	return { tool: tools.find((tool) => tool.name === "send_message"), fileTool: tools.find((tool) => tool.name === "send_file"), relays };
}

test("denied send throws the bridge's actionable error, not a successful tool result", async (t) => {
	const denial = 'send_message: "stranger@x" is not an allowed destination. Allowed 1:1 JIDs: ["zach@x"]. Allowed room JIDs: ["team@muc.x"]. Room-scoped members/mention names (NOT DM destinations): ["team@muc.x": ["alice"]]. Send to the room JID and mention the member in text. allowArbitraryJid=false: other destinations are not permitted. No message was sent.';
	const { tool, relays } = fixture(t, denial);
	await assert.rejects(tool.execute("call-1", { to: "stranger@x", text: "hello" }), {
		message: "pi-msg could not send the message: " + denial,
	});
	assert.deepEqual(relays, [{ action: "send_message", to: "stranger@x", text: "hello", replyTo: "" }]);
});

test("permitted send returns success and the delivered stanza ID", async (t) => {
	const { tool } = fixture(t, "sent:stanza-1");
	const result = await tool.execute("call-1", { to: "zach@x", text: "hello" });
	assert.equal(result.content[0].text, "Sent to zach@x with stanza-id stanza-1.");
	assert.equal(result.details.stanza_id, "stanza-1");
});

test("noop remains successful without pretending to deliver a message", async (t) => {
	const { tool } = fixture(t, "noop");
	const result = await tool.execute("call-1", { to: "noop", text: "" });
	assert.equal(result.content[0].text, "No message sent.");
	assert.equal(result.details.stanza_id, undefined);
});

test("tool contract distinguishes DM permissions from room-scoped mention targets", (t) => {
	const { tool } = fixture(t, "noop");
	assert.match(tool.description, /configured owner \(1:1\) or a configured room \(groupchat\)/);
	assert.match(tool.description, /Room members are only mention targets within their room, NOT DM destinations/);
	assert.match(tool.description, /Send to the room JID and mention the member in text/);
	assert.match(tool.promptGuidelines.join(" "), /`sender:` identifies the author, not an authorized DM destination/);
	assert.match(tool.promptGuidelines.join(" "), /Room membership never grants DM permission/);
	assert.match(tool.promptGuidelines.join(" "), /`allowArbitraryJid: true` permits other valid peer JIDs/);
	assert.match(tool.parameters.to.description, /configured owner \(1:1\) or configured room \(groupchat\)/);
	assert.match(tool.parameters.to.description, /room members are mention targets, NOT DM destinations/);
	assert.match(tool.parameters.to.description, /other peer JIDs require allowArbitraryJid=true/);
	assert.match(tool.parameters.text.description, /send to the room JID and mention the member here/);
	assert.doesNotMatch([tool.description, ...tool.promptGuidelines, tool.parameters.to.description].join(" "), /or a known room occupant|and known occupants|or sender JID/);
});

test("file tool contract limits destinations to the configured owner and rooms", (t) => {
	const { fileTool } = fixture(t, "noop");
	assert.match(fileTool.description, /only the configured owner or configured rooms are allowed, even when `allowArbitraryJid` is true/);
	assert.match(fileTool.description, /Room membership does not authorize a direct file transfer/);
	assert.match(fileTool.parameters.to.description, /Configured owner or configured room JID only/);
	assert.match(fileTool.parameters.to.description, /room members and arbitrary peers are not file destinations/);
});
