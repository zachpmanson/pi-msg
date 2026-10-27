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

import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

// Marks a confirm as a pi-msg action relay rather than a real user dialog.
// Kept in sync with relayPrefix in extension.go.
const RELAY_PREFIX = "pi-msg-relay:";

// Minimal structural type for the one UI method we use, so we don't depend on
// the exact exported type name.
type RelayUI = {
	confirm(title: string, message?: string): Promise<boolean>;
	select(title: string, options: string[], config?: { timeout: number }): Promise<string | undefined>;
};

export default function xmppTools(pi: ExtensionAPI) {
	// Captured on session_start; used by tool handlers to reach pi-msg.
	let ui: RelayUI | undefined;

	// --- Subagent presence tracking ---
	// pi-subagents publishes async-started/async-complete on the shared bus.
	// Its RPC status API returns prose, not a structured fleet snapshot. Read
	// its lifecycle artifacts instead (never prompts or tool output), using
	// exact session ownership. A bounded reconciliation tick covers missed
	// events, chain transitions, reloads, and runners killed without an event.
	// This is read-only: the plugin remains responsible for repairing artifacts.
	const scope = typeof process.getuid === "function" ? `uid-${process.getuid()}` : undefined;
	const subagentRoot = scope ? join(tmpdir(), `pi-subagents-${scope}`, "async-subagent-runs") : undefined;
	let subagentSession: string | undefined;
	let subagentTimer: ReturnType<typeof setInterval> | undefined;
	let subagentSeed: ReturnType<typeof setTimeout> | undefined;
	let subagentEpoch = 0;
	let subagentSending = false;
	let subagentDirty = false;
	let lastSubagentCount: number | undefined;
	// Event paths also cover runs stored outside the default artifact root.
	const subagentPaths = new Set<string>();
	const subagentStarting = new Map<string, { pid: number; until: number }>();

	function subagentCount(): number {
		if (!subagentSession) return 0;
		const paths = new Set(subagentPaths);
		if (subagentRoot) {
			try {
				for (const dir of readdirSync(subagentRoot, { withFileTypes: true })) {
					if (dir.isDirectory()) paths.add(join(subagentRoot, dir.name));
				}
			} catch { /* plugin absent, or no runs yet */ }
		}
		let count = 0;
		for (const dir of paths) {
			try {
				const status = JSON.parse(readFileSync(join(dir, "status.json"), "utf8"));
				subagentStarting.delete(dir);
				if (status?.sessionId !== subagentSession || status.isNested === true) continue;
				if (status.state !== "running" && status.state !== "queued") continue;
				// Match pi-subagents' stale-alive-PID ceiling: a reused PID
				// cannot keep an abandoned artifact busy forever.
				const updated = status.lastUpdate ?? status.startedAt;
				if (typeof updated !== "number" || Date.now() - updated > 86_400_000) continue;
				// Missing PIDs get only the runner's bootstrap grace, never an
				// indefinite busy state. EPERM means alive but not signalable.
				if (Number.isInteger(status.pid) && status.pid > 0) {
					try { process.kill(status.pid, 0); }
					catch (error) { if ((error as NodeJS.ErrnoException).code !== "EPERM") continue; }
				} else if (!(typeof status.startedAt === "number" && Date.now() - status.startedAt < 1000)) {
					continue;
				}
				const steps = Array.isArray(status.steps) ? status.steps : [];
				const running = steps.filter((step: { status?: string } | null) => step?.status === "running").length;
				// Pending future chain stages are not concurrent children. Keep
				// one unit of work visible between stages/while queued.
				count += Math.max(1, running);
			} catch {
				// The started event can precede the runner's first status write.
				// Keep that work visible immediately, but only for bootstrap.
				const starting = subagentStarting.get(dir);
				if (starting && Date.now() < starting.until) {
					try { process.kill(starting.pid, 0); count++; }
					catch (error) { if ((error as NodeJS.ErrnoException).code === "EPERM") count++; }
				} else {
					subagentStarting.delete(dir);
				}
			}
		}
		return count;
	}

	async function refreshSubagents() {
		if (!ui || !subagentSession) return;
		subagentDirty = true;
		if (subagentSending) return;
		subagentSending = true;
		const epoch = subagentEpoch;
		try {
			while (subagentDirty && ui && epoch === subagentEpoch) {
				subagentDirty = false;
				const count = subagentCount();
				if (count === lastSubagentCount) continue;
				const result = await relay("subagent_count", { count }, 1500);
				if (epoch === subagentEpoch && result === "ok") lastSubagentCount = count;
			}
		} catch { /* retry on next lifecycle event/reconciliation tick */ }
		finally { if (epoch === subagentEpoch) subagentSending = false; }
	}

	const subagentUnsubscribes = ["subagent:async-started", "subagent:async-complete"].map((event) =>
		pi.events.on(event, (payload: unknown) => {
			const info = payload as { sessionId?: string; asyncDir?: string; pid?: number } | null;
			if (!subagentSession || !info || info.sessionId !== subagentSession) return;
			if (typeof info.asyncDir === "string") {
				subagentPaths.add(info.asyncDir);
				if (event === "subagent:async-started" && typeof info.pid === "number" && Number.isInteger(info.pid) && info.pid > 0) {
					subagentStarting.set(info.asyncDir, { pid: info.pid, until: Date.now() + 1000 });
				} else {
					subagentStarting.delete(info.asyncDir);
				}
			}
			void refreshSubagents();
		}),
	);

	pi.on("session_shutdown", () => {
		subagentEpoch++;
		if (subagentTimer) clearInterval(subagentTimer);
		if (subagentSeed) clearTimeout(subagentSeed);
		if (flushTimer) clearInterval(flushTimer);
		flushTimer = undefined;
		for (const id of [...heartbeatSched.keys()]) cancelHeartbeat(id);
		for (const unsubscribe of processUnsubscribes) unsubscribe();
		for (const unsubscribe of subagentUnsubscribes) unsubscribe();
		subagentPaths.clear();
		subagentStarting.clear();
		subagentSession = undefined;
		// Send before dropping the old UI; don't block session replacement.
		void relay("subagent_count", { count: 0 }, 1500).catch(() => {});
		ui = undefined;
	});

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

	const processUnsubscribes = [pi.events.on(PROCESS_STARTED, (info: unknown) => {
		void refreshProcessCount();
		// Arm the per-process heartbeat schedule anchored to this process's
		// start time (the payload is the full ProcessInfo incl. startTime).
		const id = String((info as { id?: unknown })?.id ?? "");
		const startTime = (info as { startTime?: unknown })?.startTime;
		if (id && typeof startTime === "number") {
			cancelHeartbeat(id); // safety: never double-schedule an id
			heartbeatSched.set(id, { startTime, intervalMs: HEARTBEAT_INTERVAL_MS, boundary: 1 });
			scheduleNextBoundary(id);
		}
	}),
	pi.events.on(PROCESS_ENDED, (info: unknown) => {
		void refreshProcessCount();
		// A process that ended must not fire a stale heartbeat, and its
		// pending report (if any) is dropped with it.
		const id = String((info as { id?: unknown })?.id ?? "");
		if (id) cancelHeartbeat(id);
	}),
	// clear removes finished entries, not live processes. Reconcile only this
	// snapshot so a concurrent start cannot lose its schedule.
	pi.events.on(PROCESS_CHANGED, (payload: unknown) => {
		void refreshProcessCount();
		if ((payload as { reason?: unknown })?.reason === "cleared") {
			const snapshot = new Map(heartbeatSched);
			void queryProcesses().then((processes) => {
				const live = new Set(processes.filter((p) => LIVE_STATUSES.has(String(p.status))).map((p) => String(p.id)));
				for (const [id, schedule] of snapshot) {
					if (!live.has(id) && heartbeatSched.get(id) === schedule) cancelHeartbeat(id);
				}
			});
		}
	})];

	// --- Long-running-process heartbeats ---
	//
	// While background processes run, pi-msg keeps the agent's presence dnd so
	// it doesn't drift away — but a process that runs for hours is invisible
	// once the agent has settled. Rather than a wall-clock poll (which misses a
	// 12-min run, as discovered 2026-09-04), each process's heartbeat is
	// *scheduled*: on processes:started we arm a timer at the 10-minute
	// boundary after start by default (overridable per start), and re-arm. Each
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
	// is the next interval boundary index to fire (1 = one interval); it
	// increments each time a boundary comes due so the cadence compounds for
	// the life of the process.
	interface HeartbeatSched {
		startTime: number;
		boundary: number;
		intervalMs: number;
		timer?: ReturnType<typeof setTimeout>;
	}
	const heartbeatSched = new Map<string, HeartbeatSched>();
	// Process ids whose next boundary has come due and are awaiting the next
	// flush. Set membership is how the flusher knows what to relay.
	const heartbeatDue = new Set<string>();

	// scheduleNextBoundary arms (or re-arms) the timer for a process's next
	// interval boundary, relative to its start time. On fire it marks the
	// process due and schedules the following boundary.
	function scheduleNextBoundary(id: string) {
		const s = heartbeatSched.get(id);
		if (!s) return;
		const nextAt = s.startTime + s.boundary * s.intervalMs;
		const delay = Math.max(0, nextAt - Date.now());
		s.timer = setTimeout(() => {
			if (heartbeatSched.get(id) !== s) return;
			heartbeatDue.add(id);
			// Skip missed boundaries after event-loop stalls, keeping the anchor.
			s.boundary = Math.max(s.boundary + 1, Math.floor((Date.now() - s.startTime) / s.intervalMs) + 1);
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

	// Opt-in seam: initialize the original factory once and intercept its full
	// ToolDefinition. getAllTools() is metadata; command:start is not the tool.
	async function installProcessAdapter(entry: string): Promise<void> {
		if (!isAbsolute(entry)) throw new Error("PI_MSG_PROCESS_EXTENSION must be an absolute primary extension entry path");
		const module = await import(entry);
		if (typeof module.default !== "function") throw new Error("PI_MSG_PROCESS_EXTENSION must export a default extension factory");
		let registered = false;
		const adapter = new Proxy(pi, {
			get(target, property) {
				if (property !== "registerTool") return Reflect.get(target, property, target);
				return (tool: ToolDefinition) => {
					if (tool.name !== "process") return target.registerTool(tool);
					if (registered) throw new Error("upstream registered process more than once");
					if (tool.parameters.type !== "object" || !tool.parameters.properties) {
						throw new Error("unsupported upstream process schema: expected a top-level object");
					}
					registered = true;
					const maxSeconds = Math.floor(2_147_483_647 / 1000);
					target.registerTool({
						...tool,
						parameters: {
							...tool.parameters,
							properties: {
								...tool.parameters.properties,
								heartbeatSeconds: Type.Optional(Type.Integer({
									minimum: 0, maximum: maxSeconds,
									description: "Only for start: periodic pi-msg heartbeat seconds. Omit for the environment/default interval; 0 disables periodic heartbeats only. Exit and log-match notifications are unchanged.",
								})),
							},
						},
						async execute(...args) {
							const params = args[1] as Record<string, unknown>;
							const explicit = Object.prototype.hasOwnProperty.call(params, "heartbeatSeconds");
							const seconds = params.heartbeatSeconds;
							if (explicit) {
								if (params.action !== "start") throw new Error("heartbeatSeconds is supported only for process start");
								if (typeof seconds !== "number" || !Number.isInteger(seconds) || seconds < 0 || seconds > maxSeconds) {
									throw new Error(`heartbeatSeconds must be an integer from 0 to ${maxSeconds}`);
								}
								// Strip only our field; forward every other argument unchanged.
								const { heartbeatSeconds: _, ...upstreamParams } = params;
								args[1] = upstreamParams;
							}
							const result = await tool.execute.apply(this, args);
							if (explicit) {
								const id = String((result.details as { process?: { id?: unknown } } | undefined)?.process?.id ?? "");
								const schedule = heartbeatSched.get(id);
								// ended/shutdown can precede this resolution. Never resurrect
								// a removed schedule from a stale successful start result.
								if (schedule) {
									cancelHeartbeat(id);
									if (seconds !== 0) {
										const intervalMs = (seconds as number) * 1000;
										heartbeatSched.set(id, {
											startTime: schedule.startTime, intervalMs,
											boundary: Math.floor(Math.max(0, Date.now() - schedule.startTime) / intervalMs) + 1,
										});
										scheduleNextBoundary(id);
									}
								}
							}
							return result;
						},
					});
				};
			},
		});
		await module.default(adapter);
		if (!registered) throw new Error("upstream factory did not register the process tool");
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
	let heartbeatFlushing = false;
	async function flushHeartbeats() {
		if (!ui || heartbeatFlushing) return;
		if (heartbeatDue.size === 0) return;
		heartbeatFlushing = true;
		const epoch = subagentEpoch;
		// Snapshot generations and drain before any await. A later boundary
		// belongs to the next flush, and a replacement is not due merely
		// because its predecessor was in this batch.
		const due = [...heartbeatDue].map((id) => ({ id, schedule: heartbeatSched.get(id) }));
		for (const { id } of due) heartbeatDue.delete(id);
		try {
			const processes = await queryProcesses();
			if (epoch !== subagentEpoch) return;
			const now = Date.now();
			const byId = new Map<string, Record<string, unknown>>();
			for (const p of processes) {
				if (LIVE_STATUSES.has(String(p.status ?? ""))) {
					const id = String(p.id ?? "");
					if (id) byId.set(id, p);
				}
			}
			const reports: Array<Record<string, unknown>> = [];
			const reportedSchedules = new Map<string, HeartbeatSched>();
			for (const { id, schedule } of due) {
				if (!schedule || heartbeatSched.get(id) !== schedule) continue;
				const p = byId.get(id);
				if (!p) { cancelHeartbeat(id); continue; }
				const tail = await fetchProcessTail(id);
				if (!schedule || heartbeatSched.get(id) !== schedule || epoch !== subagentEpoch) continue;
				const startTime = p.startTime;
				const elapsedMs = typeof startTime === "number" ? now - startTime : 0;
				reportedSchedules.set(id, schedule);
				reports.push({
					id,
					name: String(p.name ?? id),
					command: String(p.command ?? ""),
					elapsedSecs: Math.round(elapsedMs / 1000),
					tail,
				});
			}
			const liveReports = reports.filter((p) => heartbeatSched.get(String(p.id)) === reportedSchedules.get(String(p.id)));
			if (liveReports.length === 0 || epoch !== subagentEpoch) return;
			await relay("process_heartbeat", { processes: liveReports });
		} catch {
			// Best-effort: a dropped heartbeat is just a missed status check.
		} finally { heartbeatFlushing = false; }
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
		subagentSession = ctx.sessionManager.getSessionId() ?? undefined;
		subagentSending = false;
		lastSubagentCount = undefined;
		// Same bootstrap constraint as process counts: no dialog in this hook.
		subagentSeed = setTimeout(() => {
			void refreshSubagents();
			subagentTimer = setInterval(() => void refreshSubagents(), 2000);
		}, 2000);
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
	async function relay(action: string, args: Record<string, unknown>, timeout?: number): Promise<string> {
		if (!ui) {
			throw new Error("no relay channel to pi-msg (session not started)");
		}
		return (await ui.select(RELAY_PREFIX + JSON.stringify({ action, ...args }), ["ok"], timeout === undefined ? undefined : { timeout })) ?? "relay cancelled (no response from pi-msg)";
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
				"Send a message explicitly to the configured owner (1:1) or a configured room (groupchat). Room members are only mention targets within their room, NOT DM destinations. Send to the room JID and mention the member in text. Other valid peer JIDs are allowed only when account config `allowArbitraryJid` is true. Final assistant text is internal to the harness and is never sent automatically. `reply_to` is optional: omit it for ordinary replies; use a `[stanza …]` message ID when explicitly threading or when it materially helps associate a follow-up. Never use an archive `[id …]` cursor. Set `reply_timeout_seconds` only to opt into a one-shot internal reminder when no qualifying reply arrives; omission creates no watch.",
			promptSnippet: "Send a message to an XMPP conversation",
			promptGuidelines: [
				"Use send_message for every outbound chat message; a final assistant response is not delivered to chat.",
				"Choose `to` from the conversation JID (`from:`) or use the owner JID to message the owner. `sender:` identifies the author, not an authorized DM destination. To address a room member, send to the room JID and mention the member in text.",
				"Use `reply_to` when you want to thread a reply to a specific stanza or threading materially helps associate a follow-up; otherwise omit it. A message ID from read_messages is valid. Multiple recipients require multiple calls.",
				"An optional positive finite `reply_timeout_seconds` arms a non-blocking bridge watch after sending. DMs from the peer satisfy it; rooms require a direct ping to you or a threaded reply to this stanza, not general chatter or broadcasts. The reminder may be handled silently. One watch per conversation; a later watched send replaces it. Watches are process-local and cleared on shutdown.",
				"Only the configured owner and configured rooms are allowed by default. Room membership never grants DM permission. `allowArbitraryJid: true` permits other valid peer JIDs.",
			],
			parameters: Type.Object({
				to: Type.String({ description: "Destination JID: configured owner (1:1) or configured room (groupchat); room members are mention targets, NOT DM destinations; other peer JIDs require allowArbitraryJid=true" }),
				text: Type.String({ description: "Message body to send; when addressing a room member, send to the room JID and mention the member here" }),
				reply_to: Type.Optional(Type.String({ description: "Optional stanza ID from an incoming message or read_messages result to thread this reply under" })),
				reply_timeout_seconds: Type.Optional(Type.Number({ exclusiveMinimum: 0, description: "Opt-in one-shot internal reminder if no qualifying reply arrives after this many seconds; omit for no watch" })),
			}),
			async execute(_toolCallId, params) {
				const p = params as { to?: string; text?: string; reply_to?: string; reply_timeout_seconds?: number };
				const to = String(p.to ?? "").trim();
				const text = String(p.text ?? "").trim();
				if (!to) throw new Error("to is required");
				if (to !== "noop" && !text) throw new Error("text is required");
				const timeout = p.reply_timeout_seconds;
				if (timeout !== undefined && (typeof timeout !== "number" || !Number.isFinite(timeout) || timeout <= 0)) {
					throw new Error("reply_timeout_seconds must be a finite positive number of seconds");
				}
				const result = await relay("send_message", { to, text, replyTo: p.reply_to ?? "", replyTimeoutSeconds: timeout });
				if (result === "noop" && to === "noop") {
					return { content: [{ type: "text", text: "No message sent." }], details: { to, result } };
				}
				if (!result.startsWith("sent:")) {
					throw new Error("pi-msg could not send the message: " + result);
				}
				const stanzaId = result.slice("sent:".length);
				return {
					content: [{ type: "text", text: `Sent to ${to} with stanza-id ${stanzaId}.` + (timeout === undefined ? "" : ` Reply watch active (${timeout} seconds).`) }],
					details: { to, reply_to: p.reply_to ?? "", stanza_id: stanzaId, reply_timeout_seconds: timeout, reply_watch_active: timeout !== undefined, result },
				};
			},
		});
	}

	if (enabled.has("messages")) {
		pi.registerTool({
			name: "read_messages",
			label: "Read conversation history (XMPP)",
			description:
				"Read an XMPP room or 1:1 chat from its XEP-0313 archive. `target` is required: pass the owner JID, a configured room JID, or an allowed peer JID. Defaults to the newest 30 (max 100) messages; `since` and `before` narrow/page the window. By default, only the owner JID and configured rooms are readable; account config `allowArbitraryJid` can permit other peers. Returns messages oldest first with `[id …]` as the archive pagination cursor and `[stanza …]` as the message ID for send_message's `reply_to`. Includes sent lines marked `[we sent]`. Reading does not send a message.",
			promptSnippet: "Read room or 1:1 conversation history",
			promptGuidelines: [
				"Use read_messages when you need earlier context or a room conversation you were not addressed in.",
				"Pass the conversation JID as the required `target` on every call. Use the owner JID for 1:1 history or a configured room JID for room history; other peers require `allowArbitraryJid: true`.",
				"By default, only the owner and configured rooms are readable. Other peers require `allowArbitraryJid: true` in account config.",
				"Pass `since` (RFC 3339 timestamp or relative age like 2h) to bound the read, and `before` using an archive `[id …]` cursor to page older.",
				"To thread a reply when useful, call send_message with `[stanza …]` as its optional `reply_to`; otherwise omit it. Never use `[id …]` for `reply_to`.",
				"read_messages only reads. Use send_message to send a chat message; final assistant text is internal to the harness.",
			],
			parameters: Type.Object({
				target: Type.String({ description: "Required conversation JID: owner, configured room, or allowed peer JID" }),

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
				const p = params as { target: string; limit?: number; since?: string; before?: string };
				const args: Record<string, unknown> = { target: p.target };
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
					details: { target: p.target, limit: args.limit, since: args.since, before: args.before },
				};
			},
		});
	}

	if (enabled.has("file")) {
		pi.registerTool({
			name: "send_file",
			label: "Send file (XMPP)",
			description:
				"Upload a local file and deliver it to the human over XMPP (XEP-0363 HTTP Upload). The path must be absolute and readable on this host. Defaults to the current conversation; only the configured owner or configured rooms are allowed, even when `allowArbitraryJid` is true. Room membership does not authorize a direct file transfer. Sending a file does not count as a chat reply; use send_message if a response is needed. Returns the share URL of the uploaded file, but DO NOT repeat the URL in the XMPP chat itself — the recipient can already see the file there. The URL is only for reuse in other places (e.g. a GitHub PR description).",
			promptSnippet: "Send a local file (log, diff, image) to the human over chat",
			promptGuidelines: [
				"Use send_file to deliver a real local file to the human; give an absolute path. It is for files, not for pasting text, and it does not replace send_message when a chat reply is required.",
				"The tool result includes the share URL — reuse it (e.g. in a PR description or follow-up message) instead of describing the file.",
			],
			parameters: Type.Object({
				path: Type.String({ description: "Absolute path to a local file on this host" }),
				to: Type.Optional(Type.String({ description: "Configured owner or configured room JID only; defaults to the current conversation; room members and arbitrary peers are not file destinations" })),
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
	const processEntry = process.env.PI_MSG_PROCESS_EXTENSION;
	if (processEntry) return installProcessAdapter(processEntry);

}
