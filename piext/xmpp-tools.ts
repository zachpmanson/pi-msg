// xmpp-tools.ts — companion Pi extension for pi-msg.
//
// pi-msg runs the agent as `pi --mode rpc -e <this file>`. It owns the XMPP
// connection; the agent (Pi) is a separate process. A registered tool's handler
// therefore can't touch the socket directly — so it relays the action to pi-msg
// over the RPC extension-UI channel and blocks for the result.
//
// Relay transport: `ctx.ui.select(title, ["ok"])`. In RPC mode this emits an
// `extension_ui_request` (method "select") on stdout and blocks until the
// client sends back `extension_ui_response {value}`. We smuggle a JSON action
// through the sentinel-prefixed `title`; pi-msg recognises the sentinel,
// performs the real XMPP action, and answers `value: "ok"` on success or a
// failure reason on error. select is used (not confirm) because its response
// carries a *string* back to the extension, so the failure reason (e.g. a
// server refusing an upload as too large) reaches the LLM as the tool error
// instead of a bare boolean (pi-msg issue #34).
//
// Which tools are registered is chosen by pi-msg via the PI_MSG_TOOLS env var
// (comma-separated); this mirrors the account's config (e.g. send_reaction is
// gated on the `reactions` opt-in). This is the "structured tool call instead
// of in-band text" path from issue #8 / docs/subagents.md. Routing (`to:`)
// intentionally stays prompt-injected; only discrete side-effect actions move
// to tools.
//
// Types are erased by jiti at load time, so the `import type` never resolves at
// runtime; only the value import (`typebox`) is resolved, against Pi's own deps.

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

// Marks a confirm as a pi-msg action relay rather than a real user dialog.
// Kept in sync with relayPrefix in extension.go.
const RELAY_PREFIX = "pi-msg-relay:";

// Minimal structural type for the one UI method we use, so we don't depend on
// the exact exported type name.
type RelayUI = {
	confirm(title: string, message?: string): Promise<boolean>;
	select(title: string, options: string[]): Promise<string | undefined>;
};

export default function xmppTools(pi: ExtensionAPI) {
	// Captured on session_start; used by tool handlers to reach pi-msg.
	let ui: RelayUI | undefined;

	// --- Background-process presence tracking ---
	//
	// The pi-processes extension broadcasts every process lifecycle change on
	// the shared bus (processes:started / processes:ended, via its
	// event-bridge hook) and serves synchronous list queries on
	// processes:request:list. pi-msg shows dnd while a background process
	// runs, so we relay the manager's current process count over the same
	// sentinel channel as the tools. Every change triggers a fresh query of
	// the authoritative manager (never delta arithmetic, so counts can't
	// drift), and the count is re-seeded on every session_start to cover
	// processes that predate this extension instance — the registry is
	// in-memory and dies with the pi process anyway.
	const PROCESS_STARTED = "processes:started";
	const PROCESS_ENDED = "processes:ended";
	const PROCESS_LIST = "processes:request:list";

	// queryProcessCount asks the pi-processes manager for its current process
	// list over the in-process request channel (its reply is synchronous).
	// Falls back to 0 if pi-processes isn't loaded or doesn't answer.
	function queryProcessCount(): Promise<number> {
		return new Promise((resolve) => {
			let settled = false;
			const done = (n: number) => {
				if (!settled) {
					settled = true;
					resolve(n);
				}
			};
			pi.events.emit(PROCESS_LIST, {
				reply: (processes: unknown) => done(Array.isArray(processes) ? processes.length : 0),
			});
			setTimeout(() => done(0), 500);
		});
	}

	async function refreshProcessCount() {
		if (!ui) return;
		try {
			const count = await queryProcessCount();
			await relay("process_count", { count });
		} catch {
			// Best-effort: presence is cosmetic; a dropped relay is harmless.
		}
	}

	pi.events.on(PROCESS_STARTED, () => {
		void refreshProcessCount();
	});
	pi.events.on(PROCESS_ENDED, () => {
		void refreshProcessCount();
	});

	pi.on("session_start", (_event, ctx) => {
		ui = ctx.ui as unknown as RelayUI;
		// Deferred seed of the background-process count. This must NOT happen
		// synchronously in the hook: pi's RPC mode attaches its stdin reader
		// only after session_start hooks resolve, so blocking on a dialog
		// (relay → ui.select) during session_start can never complete — the
		// response can't be read — and any stdin traffic arriving meanwhile
		// makes pi exit cleanly (crashloop). Deferring past bootstrap turns
		// the seed into a normal post-start relay, exactly like the tool
		// relays. Started/ended listeners below cover live changes; this only
		// covers processes already tracked before the first relay.
		setTimeout(() => {
			void refreshProcessCount();
		}, 2000);
	});

	// Inject the agent's identity ($PI_MSG_ACCOUNT) at the top of every system
	// prompt so it's the first thing the agent reads. Prevents identity confusion
	// in multi-persona fleets where several agents share the same project context.
	pi.on("before_agent_start", async (event) => {
		const account = process.env.PI_MSG_ACCOUNT;
		if (!account) return;
		return {
			systemPrompt: `You are **${account}**. This is your identity in Zach\'s fleet.

${event.systemPrompt}`,
		};
	});

	// Which tools to register, chosen by pi-msg via PI_MSG_TOOLS (comma list).
	// Unset (e.g. running the extension standalone) enables both.
	const raw = process.env.PI_MSG_TOOLS;
	const enabled =
		raw === undefined ? new Set(["file", "reaction"]) : new Set(raw.split(",").map((s) => s.trim()));

	// relay hands an action to pi-msg and blocks for its string result: "ok" on
	// success, or a failure reason that becomes the tool error the model sees.
	async function relay(action: string, args: Record<string, unknown>): Promise<string> {
		if (!ui) {
			throw new Error("no relay channel to pi-msg (session not started)");
		}
		return (await ui.select(RELAY_PREFIX + JSON.stringify({ action, ...args }), ["ok"])) ?? "relay cancelled (no response from pi-msg)";
	}

	if (enabled.has("reaction")) {
		pi.registerTool({
			name: "send_reaction",
			label: "React (XMPP)",
			description:
				"React to a chat message with a single emoji over XMPP (XEP-0444). By default reacts to the most recent incoming message; pass messageId to target an arbitrary message by its stanza ID.",
			promptSnippet: "React to a chat message with an emoji",
			promptGuidelines: [
				"Use send_reaction to acknowledge a message with one emoji (e.g. 👀 for seen, ✅ for done).",
				"To react to a specific message, include its stanza ID as messageId. The from-JID is resolved from the message history cache; if that fails you may also supply the from-JID explicitly.",
			],
			parameters: Type.Object({
				emoji: Type.String({ description: "A single emoji, e.g. 👀 or ✅" }),
				messageId: Type.Optional(Type.String({ description: "Optional XMPP stanza ID of the target message; omitting targets the most recent incoming message" })),
				from: Type.Optional(Type.String({ description: "Optional full JID of the target message's author; resolved automatically from message history cache when messageId is provided" })),
			}),
			async execute(_toolCallId, params) {
				const p = params as { emoji?: string; messageId?: string; from?: string };
				const emoji = String(p.emoji ?? "").trim();
				if (!emoji) {
					throw new Error("emoji is required");
				}
				const args: Record<string, unknown> = { emoji };
				if (p.messageId) {
					args.messageId = p.messageId;
				}
				if (p.from) {
					args.from = p.from;
				}
				const result = await relay("react", args);
				if (result !== "ok") {
					throw new Error("pi-msg could not send the reaction: " + result);
				}
				return {
					content: [{ type: "text", text: `Reacted with ${emoji}.` }],
					details: { emoji, ...(p.messageId ? { messageId: p.messageId } : {}) },
				};
			},
		});
	}

	if (enabled.has("file")) {
		pi.registerTool({
			name: "send_file",
			label: "Send file (XMPP)",
			description:
				"Upload a local file and deliver it to the human over XMPP (XEP-0363 HTTP Upload). The path must be absolute and readable on this host. Defaults to the current conversation; pass `to` to target a specific allowed JID. Returns the share URL of the uploaded file, but DO NOT repeat the URL in the XMPP chat itself — the recipient can already see the file there. The URL is only for reuse in other places (e.g. a GitHub PR description).",
			promptSnippet: "Send a local file (log, diff, image) to the human over chat",
			promptGuidelines: [
				"Use send_file to deliver a real local file to the human; give an absolute path. It is for files, not for pasting text.",
				"The tool result includes the share URL — reuse it (e.g. in a PR description or follow-up message) instead of describing the file.",
			],
			parameters: Type.Object({
				path: Type.String({ description: "Absolute path to a local file on this host" }),
				to: Type.Optional(Type.String({ description: "Destination JID; defaults to the current conversation" })),
			}),
			async execute(_toolCallId, params) {
				const p = params as { path?: string; to?: string };
				const path = String(p.path ?? "").trim();
				if (!path) {
					throw new Error("path is required");
				}
				// The relay returns the XEP-0363 share URL on success, or a failure
				// reason (not a URL) on error — so a result that is a URL is the
				// uploaded link, anything else is the failure reason.
				const result = await relay("file", { path, to: p.to ?? "" });
				const url = /^https?:\/\//.test(result) ? result : "";
				if (!url) {
					throw new Error(`pi-msg could not send the file ${path}: ${result}`);
				}
				return {
					content: [{ type: "text", text: `Sent file ${path}. Share URL: ${url}` }],
					details: { path, url },
				};
			},
		});
	}
}
