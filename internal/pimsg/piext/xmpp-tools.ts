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
import { readdirSync, readFileSync, openSync, closeSync, fsyncSync, writeFileSync, renameSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";

// Marks a confirm as a pi-msg action relay rather than a real user dialog.
// Kept in sync with relayPrefix in extension.go.
const RELAY_PREFIX = "pi-msg-relay:";

// Minimal structural type for the one UI method we use, so we don't depend on
// the exact exported type name.
type RelayUI = {
	confirm(title: string, message?: string): Promise<boolean>;
	select(title: string, options: string[], config?: { timeout: number }): Promise<string | undefined>;
};

// Bridge-supplied runtime facts, captured at normal extension registration time.
// This describes capabilities only; all enforcement remains in the Go bridge.
function conversationPermissions(): string {
	const config = JSON.parse(process.env.PI_MSG_CAPABILITIES ?? "{}");
	const owner = config.owner ? `owner ${config.owner}` : "configured owner";
	const rooms = Array.isArray(config.rooms)
		? (config.rooms.length ? ` or configured rooms (${config.rooms.join(", ")})` : "")
		: " or configured rooms";
	return `Normally ${owner}${rooms}${config.arbitraryPeers === true ? "; also other valid peer JIDs, not unconfigured rooms" : "; room membership does not authorize DMs"}.`;
}

export default function xmppTools(pi: ExtensionAPI) {
	const permissions = conversationPermissions();
	// Private bridge command: materialize with public native APIs, then switch
	// exactly once. Building before switch leaves the old runtime usable on
	// preparation failure and prevents an Agent/SessionManager UUID split.
	const idleRequest = process.env.PI_MSG_IDLE_REQUEST;
	if (idleRequest && isAbsolute(idleRequest)) {
		pi.registerCommand("pi-msg-idle-reset", {
			handler: async (token, ctx) => {
				const sync = (path: string) => { const fd = openSync(path, "r"); try { fsyncSync(fd); } finally { closeSync(fd); } };
				const request = JSON.parse(readFileSync(idleRequest, "utf8"));
				const outcome = (value: object) => {
					const receipt = idleRequest + ".receipt";
					writeFileSync(receipt + ".tmp", JSON.stringify({ token, sourceID: request.sourceID, ...value }), { mode: 0o600 });
					sync(receipt + ".tmp"); renameSync(receipt + ".tmp", receipt); sync(dirname(receipt));
				};
				const check = () => {
					const current = JSON.parse(readFileSync(idleRequest, "utf8"));
					const state = JSON.parse(readFileSync(idleRequest.slice(0, -".request".length), "utf8"));
					if (!token || token !== request.token || current.token !== token || current.hash !== request.hash || current.seed !== request.seed || String(state.Generation) !== request.generation || state.Attempt?.Token !== token || state.Attempt?.Phase !== "resetting")
						throw new Error("idle reset authority changed");
				};
				let switchStarted = false, switched = false;
				let target: {id:string;file:string;seedHash:string} | undefined;
				try {
					const source = () => {
						check();
						if (!ctx.isIdle() || ctx.hasPendingMessages() || "id:" + ctx.sessionManager.getSessionId() !== request.sourceID || ctx.sessionManager.getSessionFile() !== request.sourceFile)
							throw new Error("idle reset source is not exact and idle");
						if (createHash("sha256").update(readFileSync(request.sourceFile)).digest("hex") !== request.hash)
							throw new Error("idle reset snapshot changed");
					};
					source();
					const { SessionManager } = await import("@earendil-works/pi-coding-agent");
					source();
					if (!ctx.model || !request.thinking || ctx.model.provider !== request.provider || ctx.model.id !== request.model || ctx.thinkingLevel !== request.thinking) throw new Error("idle reset model/thinking missing");
					const sm = SessionManager.create(ctx.cwd, ctx.sessionManager.getSessionDir());
					const file = sm.getSessionFile();
					if (!file || !isAbsolute(file) || file === request.sourceFile) throw new Error("no distinct native session path");
					// Pi 0.84.4 defers persistence until assistant output. Native
					// existing-empty-file initialization writes its own header/new
					// UUID. No JSONL edits, fabricated assistant or provider call.
					const fd = openSync(file, "wx", 0o600); closeSync(fd);
					sm.setSessionFile(file);
					sm.appendModelChange(ctx.model.provider, ctx.model.id);
					sm.appendThinkingLevelChange(request.thinking);
					sm.appendCustomMessageEntry("pi-msg-idle-seed", request.seed, false, { token, sourceID: request.sourceID, sourceFile: request.sourceFile });
					sync(file); sync(dirname(file));
					const id = "id:" + sm.getSessionId();
					source();
					target={id,file,seedHash:createHash("sha256").update(request.seed).digest("hex")};
					switchStarted=true;
					const result = await ctx.switchSession(file, {
						withSession(fresh) {
							switched=true;
							let stale=false;try{check()}catch{stale=true}
							if ("id:" + fresh.sessionManager.getSessionId() !== id || fresh.sessionManager.getSessionFile() !== file || id === request.sourceID || !fresh.isIdle() || fresh.hasPendingMessages() || fresh.model?.provider !== request.provider || fresh.model?.id !== request.model || fresh.thinkingLevel !== request.thinking)
								throw new Error("idle reset final identity/idle mismatch");
							outcome({ status: "success", cancelled: false, stale, id, file, seedHash: createHash("sha256").update(request.seed).digest("hex") });
						},
					});
					if (result.cancelled) outcome({ status: "cancelled", cancelled: true, sourceAuthoritative:true });
				} catch (error) {
					outcome({ status: switched ? "switched-failed" : "failed", sourceAuthoritative:!switchStarted, ...(switched ? target : {}), error: String(error) });
					throw error;
				}
			},
		});
	}
	// Captured on session_start; used by tool handlers to reach pi-msg.
	let ui: RelayUI | undefined;
	let sessionIdentity = "";

	// --- Subagent presence tracking ---
	// pi-subagents publishes async-started/async-complete on the shared bus.
	// Its RPC status API returns prose, not a structured fleet snapshot. Read
	// its lifecycle artifacts instead (never prompts or tool output), using
	// exact session ownership. A bounded reconciliation tick covers missed
	// events, chain transitions, reloads, and runners killed without an event.
	// This is read-only: the plugin remains responsible for repairing artifacts.
	const scope = typeof process.getuid === "function" ? `uid-${process.getuid()}` : undefined;
	const subagentRoot = scope ? join(tmpdir(), `pi-subagents-${scope}`, "async-subagent-runs") : undefined;
	// Current plugin builds own runs by session file; older builds use the UUID.
	// Both must be exact, nonempty identities from the current session manager.
	const subagentSessions = new Set<string>();
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
		if (subagentSessions.size === 0) return 0;
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
				// isNested records routing capability, even for top-level runs.
				// Paths come from the top-level directory scan or an exactly
				// current-session event; status ownership must still match.
				if (!subagentSessions.has(status?.sessionId)) continue;
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
		if (!ui || subagentSessions.size === 0) return;
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
			if (typeof info?.sessionId !== "string" || !subagentSessions.has(info.sessionId)) return;
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
		subagentSessions.clear();
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
			const epoch = subagentEpoch;
            const identity = sessionIdentity;
            const count = await queryProcessCount();
            if (epoch !== subagentEpoch) return;
			await relay("process_count", { count, sessionIdentity: identity });
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
        const id = ctx.sessionManager.getSessionId();
        const file = ctx.sessionManager.getSessionFile?.();
        sessionIdentity = id ? `id:${id}` : file ? `file:${file}` : "";
		// A replacement may start without unloading this extension instance.
		subagentEpoch++;
		if (subagentTimer) clearInterval(subagentTimer);
		if (subagentSeed) clearTimeout(subagentSeed);
		subagentPaths.clear();
		subagentStarting.clear();
		subagentSessions.clear();
		for (const identity of [ctx.sessionManager.getSessionFile?.(), ctx.sessionManager.getSessionId()]) {
			if (typeof identity === "string" && identity.length > 0) subagentSessions.add(identity);
		}
		ui = ctx.ui as unknown as RelayUI;
		subagentSending = false;
		subagentDirty = false;
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
			systemPrompt = `You are ${account}.

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
		return (await ui.select(RELAY_PREFIX + JSON.stringify({ action, ...((action === "process_count" || action === "subagent_count" || action === "process_heartbeat") ? { sessionIdentity } : {}), ...args }), ["ok"], timeout === undefined ? undefined : { timeout })) ?? "relay cancelled (no response from pi-msg)";
	}

	if (enabled.has("reaction")) {
		pi.registerTool({
			name: "send_reaction",
			label: "React (XMPP)",
			description: "Add an emoji reaction to an XMPP message.",
			promptSnippet: "React to a chat message with an emoji",
			parameters: Type.Object({
				emoji: Type.String({ description: "A single emoji, e.g. 👀 or ✅" }),
				messageId: Type.Optional(Type.String({ description: "Optional XMPP stanza ID of the target message; omitting targets the most recent incoming message" })),
				from: Type.Optional(Type.String({ description: "Optional author JID fallback when automatic resolution from message history fails" })),
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
			description: "Send one XMPP chat message to one destination. Multiple messages or recipients require separate calls.",
			promptSnippet: "Send a message to an XMPP conversation",
			parameters: Type.Object({
				to: Type.String({ description: `Destination JID: ${permissions} Use noop to send nothing.` }),
				text: Type.String({ description: "Message body to send" }),
				reply_to: Type.Optional(Type.String({ description: "Optional threading; normally omit. Use an incoming/history stanza ID from the same conversation, never an archive cursor." })),
				allow_wake: Type.Optional(Type.Boolean({ description: "Default false. Explicitly allow waking addressed pi-msg peers currently away or xa. Without it the entire send fails; inspect claim/release history or remove unnecessary addressing before deciding to wake. Not a destination-permission override." })),
				reply_timeout_seconds: Type.Optional(Type.Number({ exclusiveMinimum: 0, description: "Positive finite seconds; omit for no watch. One-shot internal reminder, no automatic external follow-up; may be handled silently. DMs qualify from the peer; rooms require direct address or a reply to the watched stanza, not broadcasts/general chatter. One watch per conversation; later watched sends replace it; shutdown discards watches." })),
			}),
			async execute(_toolCallId, params) {
				const p = params as { to?: string; text?: string; reply_to?: string; reply_timeout_seconds?: number; allow_wake?: boolean };
				const to = String(p.to ?? "").trim();
				const text = String(p.text ?? "").trim();
				if (!to) throw new Error("to is required");
				if (to !== "noop" && !text) throw new Error("text is required");
				const timeout = p.reply_timeout_seconds;
				if (timeout !== undefined && (typeof timeout !== "number" || !Number.isFinite(timeout) || timeout <= 0)) {
					throw new Error("reply_timeout_seconds must be a finite positive number of seconds");
				}
				if (p.allow_wake !== undefined && typeof p.allow_wake !== "boolean") {
					throw new Error("allow_wake must be a boolean");
				}
				const result = await relay("send_message", { to, text, replyTo: p.reply_to ?? "", replyTimeoutSeconds: timeout, allowWake: p.allow_wake });
				if (result === "noop" && to === "noop") {
					return { content: [{ type: "text", text: "No message sent." }], details: { to, result } };
				}
				if (!result.startsWith("sent:")) {
					throw new Error("pi-msg could not send the message: " + result);
				}
				const stanzaId = result.slice("sent:".length);
				return {
					content: [{ type: "text", text: `Sent to ${to} with stanza-id ${stanzaId}.` + (timeout === undefined ? "" : ` Reply watch active (${timeout} seconds).`) }],
					details: { to, reply_to: p.reply_to ?? "", stanza_id: stanzaId, allow_wake: p.allow_wake ?? false, reply_timeout_seconds: timeout, reply_watch_active: timeout !== undefined, result },
				};
			},
		});
	}

	if (enabled.has("messages")) {
		pi.registerTool({
			name: "read_messages",
			label: "Read conversation history (XMPP)",
			description: "Read XMPP history without sending anything. Results are oldest first; sent lines are marked [we sent]. [id …] is an archive cursor; [stanza …] is a message ID.",
			promptSnippet: "Read room or 1:1 conversation history",
			parameters: Type.Object({
				target: Type.String({ description: `Required conversation JID: ${permissions}` }),

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
			description: "Upload and deliver a file; not a substitute for a required chat reply. The returned URL can be reused elsewhere, but do not repost it in the receiving XMPP conversation.",
			promptSnippet: "Send a local file (log, diff, image) to the human over chat",
			parameters: Type.Object({
				path: Type.String({ description: "Absolute readable path to a local file on this host" }),
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
