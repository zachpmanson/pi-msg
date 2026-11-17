// Exercise the real companion tool's relay-result handling without Pi or XMPP.
import { test } from "node:test";
import assert from "node:assert/strict";
import { stripTypeScriptTypes } from "node:module";
import { readFileSync } from "node:fs";

const source = stripTypeScriptTypes(readFileSync(new URL("./xmpp-tools.ts", import.meta.url), "utf8"))
	.replace('import { Type } from "typebox";', 'const Type = new Proxy({}, { get: () => (value) => value });');
const { default: xmppTools } = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);

function fixture(t, result, arbitraryPeers = false) {
	const previous = { ...process.env };
	process.env.PI_MSG_TOOLS = "messaging,file,messages,reaction";
	process.env.PI_MSG_CAPABILITIES = JSON.stringify({ owner: "owner@x", rooms: ["team@muc.x"], arbitraryPeers });
	delete process.env.PI_MSG_PROCESS_EXTENSION;
	t.after(() => {
		for (const name of ["PI_MSG_TOOLS", "PI_MSG_PROCESS_EXTENSION", "PI_MSG_CAPABILITIES"]) {
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
	return { tools, hooks, tool: tools.find((tool) => tool.name === "send_message"), fileTool: tools.find((tool) => tool.name === "send_file"), relays };
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

for (const arbitrary of [false, true]) {
 test(`effective destination schemas (arbitrary peers ${arbitrary})`, (t) => {
	const { tools, tool, fileTool } = fixture(t, "noop", arbitrary);
	const read = tools.find(t => t.name === "read_messages");
	for (const description of [tool.parameters.to.description, read.parameters.target.description]) {
		assert.match(description, /owner@x/);
		assert.match(description, /team@muc.x/);
		assert.match(description, arbitrary ? /also other valid peer JIDs, not unconfigured rooms/ : /only; room membership does not authorize DMs/);
	}
	for (const tool of tools) {
		assert.equal(tool.promptGuidelines, undefined);
		assert.doesNotMatch(JSON.stringify(tool), /allowArbitraryJid/);
	}
	assert.match(fileTool.parameters.to.description, /Configured owner or configured room JID only/);
	assert.match(fileTool.parameters.to.description, /arbitrary peers are not file destinations/);
	assert.match(fileTool.description, /do not repost it in the receiving XMPP conversation/);
	assert.match(tool.parameters.to.description, /noop to send nothing/);
	assert.match(tool.parameters.reply_to.description, /normally omit.*same conversation.*never an archive cursor/);
	assert.match(tool.parameters.reply_timeout_seconds.description, /Positive finite.*omit for no watch.*no automatic external follow-up.*handled silently.*not broadcasts.*replace.*shutdown/);
	assert.match(read.description, /oldest first.*\[we sent\].*\[id …\].*\[stanza …\]/);
	t.diagnostic(JSON.stringify(tools.map(({ name, description, parameters }) => ({ name, description, parameters }))));
 });
}

test("generic identity is rendered on every turn without fleet membership", async t => {
 const old = process.env.PI_MSG_ACCOUNT;
 process.env.PI_MSG_ACCOUNT = "sample";
 t.after(() => { if (old === undefined) delete process.env.PI_MSG_ACCOUNT; else process.env.PI_MSG_ACCOUNT = old; });
 const { hooks } = fixture(t, "noop");
 for (let turn = 0; turn < 2; turn++) {
  const result = await hooks.get("before_agent_start")({ systemPrompt: "base" });
  assert.match(result.systemPrompt, /^You are sample\.\n\nbase/);
  assert.doesNotMatch(result.systemPrompt, /Zach|fleet/);
 }
});
