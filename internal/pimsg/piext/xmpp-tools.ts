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
// Message sending and conversation history are explicit tools in every account
// mode. The bridge selects this extension's tool set through PI_MSG_TOOLS.
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
	const PROCESS_CHANGED = "processes:changed";

	// Live process statuses, mirroring pi-processes' LIVE_STATUSES: the manager
	// keeps finished/killed entries in its registry until cleared, so counting
	// every entry would report dnd for processes that are no longer running.
	const LIVE_STATUSES = new Set(["running", "terminating", "terminate_timeout"]);

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
				reply: (processes: unknown) => {
					if (!Array.isArray(processes)) {
						done(0);
						return;
					}
					const live = processes.filter(
						(p) => p !== null && typeof p === "object" && LIVE_STATUSES.has((p as { status?: string }).status ?? ""),
					).length;
					done(live);
				},
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

	pi.events.on(PROCESS_STARTED, (info: unknown) => {
		void refreshProcessCount();
		// Arm the per-process heartbeat schedule anchored to this process's
		// start time (the payload is the full ProcessInfo incl. startTime).
		const id = String((info as { id?: unknown })?.id ?? "");
		const startTime = (info as { startTime?: unknown })?.startTime;
		if (id && typeof startTime === "number") {
			cancelHeartbeat(id); // safety: never double-schedule an id
			heartbeatSched.set(id, { startTime, boundary: 1 });
			scheduleNextBoundary(id);
		}
	});
	pi.events.on(PROCESS_ENDED, (info: unknown) => {
		void refreshProcessCount();
		// A process that ended must not fire a stale heartbeat, and its
		// pending report (if any) is dropped with it.
		const id = String((info as { id?: unknown })?.id ?? "");
		if (id) cancelHeartbeat(id);
	});
	// processes:changed fires on clear/rename — anything that alters the list
	// without a start/end (e.g. the agent clearing finished entries), so the
	// presence corrects itself instead of staying dnd on a stale count. When
	// the manager clears the whole registry (reason "cleared") we drop every
	// pending schedule; on started/ended it just mirrors the events above and
	// does nothing heartbeat-specific.
	pi.events.on(PROCESS_CHANGED, (payload: unknown) => {
		void refreshProcessCount();
		if ((payload as { reason?: unknown })?.reason === "cleared") {
			for (const id of [...heartbeatSched.keys()]) cancelHeartbeat(id);
			heartbeatDue.clear();
		}
	});

	// --- Long-running-process heartbeats ---
	//
	// While background processes run, pi-msg keeps the agent's presence dnd so
	// it doesn't drift away — but a process that runs for hours is invisible
	// once the agent has settled. Rather than a wall-clock poll (which misses a
	// 12-min run, as discovered 2026-09-04), each process's heartbeat is
	// *scheduled*: on processes:started we arm a timer at the 10-minute
	// boundary after start, and re-arm on each fire (10m, 20m, 30m, …). Each
	// timer just marks the process "due"; a short coalescing flusher drains
	// everything that is due-and-live into a single relayed heartbeat, so N
	// simultaneous boundaries collapse into one wakeup. The bridge decides
	// when delivery is safe (never mid-run) and does the prompt injection;
	// this side only gathers and relays.
	const PROCESS_COMBINED_OUTPUT = "processes:request:combined_output";
	const HEARTBEAT_INTERVAL_MS = Number(process.env.PI_MSG_PROCESS_HEARTBEAT_MS ?? 600_000);
	const HEARTBEAT_FLUSH_MS = Number(process.env.PI_MSG_PROCESS_HEARTBEAT_FLUSH_MS ?? 2_000);
	const HEARTBEAT_TAIL_LINES = 40;

	// Per-process scheduled boundary timers, keyed by process id. `boundary`
	// is the next 10-minute boundary index to fire (1 = 10m after start); it
	// increments each time a boundary comes due so the cadence compounds for
	// the life of the process.
	interface HeartbeatSched {
		startTime: number;
		boundary: number;
		timer?: ReturnType<typeof setTimeout>;
	}
	const heartbeatSched = new Map<string, HeartbeatSched>();
	// Process ids whose next boundary has come due and are awaiting the next
	// flush. Set membership is how the flusher knows what to relay.
	const heartbeatDue = new Set<string>();

	// scheduleNextBoundary arms (or re-arms) the timer for a process's next
	// 10-minute boundary, relative to its start time. On fire it marks the
	// process due and schedules the following boundary.
	function scheduleNextBoundary(id: string) {
		const s = heartbeatSched.get(id);
		if (!s) return;
		const nextAt = s.startTime + s.boundary * HEARTBEAT_INTERVAL_MS;
		const delay = Math.max(0, nextAt - Date.now());
		s.timer = setTimeout(() => {
			heartbeatDue.add(id);
			s.boundary += 1;
			scheduleNextBoundary(id); // arm the next boundary
		}, delay);
	}

	// cancelHeartbeat stops a process's schedule and drops any pending report.
	// Safe to call for an id with no schedule.
	function cancelHeartbeat(id: string) {
		const s = heartbeatSched.get(id);
		if (s?.timer) clearTimeout(s.timer);
		heartbeatSched.delete(id);
		heartbeatDue.delete(id);
	}

	// queryProcesses asks the pi-processes manager for the full process list
	// (same channel as queryProcessCount, but returns the entries).
	function queryProcesses(): Promise<Array<Record<string, unknown>>> {
		return new Promise((resolve) => {
			let settled = false;
			const done = (list: Array<Record<string, unknown>>) => {
				if (!settled) {
					settled = true;
					resolve(list);
				}
			};
			pi.events.emit(PROCESS_LIST, {
				reply: (processes: unknown) => {
					if (!Array.isArray(processes)) {
						done([]);
					return;
				}
				done(processes.filter((p) => p !== null && typeof p === "object") as Array<Record<string, unknown>>);
			},
			});
			setTimeout(() => done([]), 500);
		});
	}

	// fetchProcessTail pulls the tail of a process's combined output via the
	// manager's synchronous query channel. Returns "" when there is no output.
	function fetchProcessTail(id: string): Promise<string> {
		return new Promise((resolve) => {
			let settled = false;
			const done = (text: string) => {
				if (!settled) {
					settled = true;
					resolve(text);
				}
			};
			pi.events.emit(PROCESS_COMBINED_OUTPUT, {
				id,
				tailLines: HEARTBEAT_TAIL_LINES,
				reply: (lines: unknown) => {
					if (!Array.isArray(lines)) {
						done("");
						return;
					}
					done(
						lines
							.map((l) =>
								l !== null && typeof l === "object" && typeof (l as { text?: unknown }).text === "string"
									? (l as { text: string }).text
									: "",
							)
							.filter((t) => t.length > 0)
							.join("\n"),
					);
				},
			});
			setTimeout(() => done(""), 500);
		});
	}

	// flushHeartbeats is the coalescing drain: it relays a single
	// process_heartbeat for every process that has come due since the last
	// flush and is still live. Due-but-ended processes are dropped (their
	// cancel on ended already removed them, so this is a belt-and-braces
	// liveness check against the authoritative manager).
	async function flushHeartbeats() {
		if (!ui) return;
		if (heartbeatDue.size === 0) return;
		try {
			const processes = await queryProcesses();
			const now = Date.now();
			const byId = new Map<string, Record<string, unknown>>();
			for (const p of processes) {
				if (LIVE_STATUSES.has(String(p.status ?? ""))) {
					const id = String(p.id ?? "");
					if (id) byId.set(id, p);
				}
			}
			const reports: Array<Record<string, unknown>> = [];
			for (const id of heartbeatDue) {
				heartbeatDue.delete(id); // drain: each due mark is relayed at most once
				const p = byId.get(id);
				if (!p) continue;
				const startTime = p.startTime;
				const elapsedMs = typeof startTime === "number" ? now - startTime : 0;
				reports.push({
					id,
					name: String(p.name ?? id),
					command: String(p.command ?? ""),
					elapsedSecs: Math.round(elapsedMs / 1000),
					tail: await fetchProcessTail(id),
				});
			}
			if (reports.length === 0) return;
			await relay("process_heartbeat", { processes: reports });
		} catch {
			// Best-effort: a dropped heartbeat is just a missed status check.
		}
	}

	// The coalescing flusher interval. Registered at session_start: setInterval
	// is non-blocking, so it is safe under the rule that no relay/dialog may
	// run synchronously in that hook.
	let flushTimer: ReturnType<typeof setInterval> | undefined;
	function startFlusher() {
		if (flushTimer) return;
		flushTimer = setInterval(() => {
			void flushHeartbeats();
		}, HEARTBEAT_FLUSH_MS);
	}

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
		startFlusher();
	});

	// Inject the agent's identity ($PI_MSG_ACCOUNT) at the top of every system
	// prompt so it's the first thing the agent reads. Prevents identity confusion
	// in multi-persona fleets where several agents share the same project context.
	//
	// beforeAgentStartText (PI_MSG_BEFORE_AGENT_START_TEXT) is appended *after*
	// the identity line, so identity stays the first thing read. before_agent_start
	// fires on every turn, so the text is re-applied throughout a long session
	// rather than fading like a one-off instruction — the same property the
	// equivalent Claude Code UserPromptSubmit hook relies on. Empty means no
	// injection (and the system prompt is then byte-identical to no-extension
	// behaviour).
	//
	// A shell-command variant (beforeAgentStartHook, run via pi.exec) is not
	// built yet — see zpm/pi-msg#103.
	pi.on("before_agent_start", async (event) => {
		const account = process.env.PI_MSG_ACCOUNT;
		const text = process.env.PI_MSG_BEFORE_AGENT_START_TEXT ?? "";
		if (!account && !text) return;
		let systemPrompt = event.systemPrompt;
		if (account) {
			systemPrompt = `You are **${account}**. This is your identity in Zach\'s fleet.

${systemPrompt}`;
		}
		if (text) {
			systemPrompt = `${systemPrompt}\n\n${text}`;
		}
		return { systemPrompt };
	});

	// Which tools to register, chosen by pi-msg via PI_MSG_TOOLS (comma list).
	// Unset (e.g. running the extension standalone) enables both.
	const raw = process.env.PI_MSG_TOOLS;
	const enabled =
		raw === undefined ? new Set(["file", "reaction", "messages", "messaging"]) : new Set(raw.split(",").map((s) => s.trim()));

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

	if (enabled.has("messaging")) {
		pi.registerTool({
			name: "send_message",
			label: "Send chat message (XMPP)",
			description:
				"Send a message explicitly to the owner, a joined room, or a known room occupant. Other valid JIDs are allowed only when account config `allowArbitraryJid` is true. Final assistant text is internal to the harness and is never sent automatically. Use `reply_to` with a `[stanza …]` message ID to thread a reply; never use an archive `[id …]` cursor.",
			promptSnippet: "Send a message to an XMPP conversation",
			promptGuidelines: [
				"Use send_message for every outbound chat message; a final assistant response is not delivered to chat.",
				"Choose `to` from the conversation JID (`from:`) or sender JID (`sender:`); use the owner JID to message the owner.",
				"Use `reply_to` only when replying to a specific stanza; a message ID from read_messages is valid. Multiple recipients require multiple calls.",
				"Only the owner, joined rooms, and known occupants are allowed by default. `allowArbitraryJid: true` permits other valid JIDs.",
			],
			parameters: Type.Object({
				to: Type.String({ description: "Destination JID: owner, joined room, or known occupant; arbitrary JIDs require allowArbitraryJid=true" }),
				text: Type.String({ description: "Message body to send" }),
				reply_to: Type.Optional(Type.String({ description: "Optional stanza ID from an incoming message or read_messages result to thread this reply under" })),
			}),
			async execute(_toolCallId, params) {
				const p = params as { to?: string; text?: string; reply_to?: string };
				const to = String(p.to ?? "").trim();
				const text = String(p.text ?? "").trim();
				if (!to) throw new Error("to is required");
				if (!text) throw new Error("text is required");
				const result = await relay("send_message", { to, text, replyTo: p.reply_to ?? "" });
				if (!result.startsWith("sent:")) {
					throw new Error("pi-msg could not send the message: " + result);
				}
				return {
					content: [{ type: "text", text: `Message sent to ${to}.` }],
					details: { to, reply_to: p.reply_to ?? "", result },
				};
			},
		});
	}

	if (enabled.has("messages")) {
		pi.registerTool({
			name: "read_messages",
			label: "Read conversation history (XMPP)",
			description:
				"Read an XMPP room or 1:1 chat from its XEP-0313 archive. Defaults to the newest 30 (max 100) messages; `since` and `before` narrow/page the window. By default, only the owner JID and configured rooms are readable; account config `allowArbitraryJid` can permit other peers. Returns messages oldest first with `[id …]` as the archive pagination cursor and `[stanza …]` as the message ID for send_message's `reply_to`. Includes sent lines marked `[we sent]`. Reading does not send a message.",
			promptSnippet: "Read room or 1:1 conversation history",
			promptGuidelines: [
				"Use read_messages when you need earlier context or a room conversation you were not addressed in.",
				"Pass a room or peer JID as `target`. With no target, a single-room account defaults to that room; a 1:1 account defaults to the owner.",
				"By default, only the owner and configured rooms are readable. Other peers require `allowArbitraryJid: true` in account config.",
				"Pass `since` (RFC 3339 timestamp or relative age like 2h) to bound the read, and `before` using an archive `[id …]` cursor to page older.",
				"To reply, call send_message; use `[stanza …]` as its optional `reply_to`, never `[id …]`.",
				"read_messages only reads. Use send_message to send a chat message; final assistant text is internal to the harness.",
			],
			parameters: Type.Object({
				target: Type.Optional(Type.String({ description: "Owner or joined room JID; omitted defaults to the sole room or, for 1:1 accounts, the owner" })),

				limit: Type.Optional(Type.Number({ description: "How many messages to fetch (default 30, max 100)" })),
				since: Type.Optional(
					Type.String({
						description:
							"Lower bound on the window: an RFC 3339 timestamp (e.g. 2026-09-28T19:30:00+10:00) or a relative age (e.g. 2h, 90m). Omit for no lower bound.",
					}),
				),
				before: Type.Optional(Type.String({ description: "Archive cursor from `[id …]` on a previous read; returns strictly older messages. Do not use a `[stanza …]` message ID here." })),

			}),
			async execute(_toolCallId, params) {
				const p = params as { target?: string; limit?: number; since?: string; before?: string };
				const args: Record<string, unknown> = { target: p.target ?? "" };
				if (typeof p.limit === "number" && Number.isFinite(p.limit)) {
					args.limit = Math.trunc(p.limit);
				}
				if (typeof p.since === "string" && p.since.trim()) {
					args.since = p.since.trim();
				}
				if (typeof p.before === "string" && p.before.trim()) {
					args.before = p.before.trim();
				}
				const result = await relay("read_messages", args);
				if (!result.startsWith("[pi-msg: read_messages:")) {
					throw new Error("read_messages failed: " + result);
				}
				return {
					content: [{ type: "text", text: result }],
					details: { target: p.target ?? "", limit: args.limit, since: args.since, before: args.before },
				};
			},
		});
	}

	if (enabled.has("file")) {
		pi.registerTool({
			name: "send_file",
			label: "Send file (XMPP)",
			description:
				"Upload a local file and deliver it to the human over XMPP (XEP-0363 HTTP Upload). The path must be absolute and readable on this host. Defaults to the current conversation; pass `to` to target a specific allowed JID. Sending a file does not count as a chat reply; use send_message if a response is needed. Returns the share URL of the uploaded file, but DO NOT repeat the URL in the XMPP chat itself — the recipient can already see the file there. The URL is only for reuse in other places (e.g. a GitHub PR description).",
			promptSnippet: "Send a local file (log, diff, image) to the human over chat",
			promptGuidelines: [
				"Use send_file to deliver a real local file to the human; give an absolute path. It is for files, not for pasting text, and it does not replace send_message when a chat reply is required.",
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
