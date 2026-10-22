// Full companion + real dynamic factory import, fake upstream tool and clock.
import { after, test } from "node:test";
import assert from "node:assert/strict";
import { stripTypeScriptTypes } from "node:module";
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const source = stripTypeScriptTypes(readFileSync(new URL("./xmpp-tools.ts", import.meta.url), "utf8"))
	.replace('import { Type } from "typebox";', `const Type = {
		Optional: (s) => s, Integer: (s) => ({ type: "integer", ...s }),
		String: (s) => ({ type: "string", ...s }), Number: (s) => ({ type: "number", ...s }),
		Object: (properties) => ({ type: "object", properties }),
	};`);
// A file URL preserves Node's absolute-path resolution for import(entry).
const moduleDir = mkdtempSync(join(tmpdir(), "pi-msg-heartbeat-module-"));
const modulePath = join(moduleDir, "companion.mjs");
writeFileSync(modulePath, source);
after(() => rmSync(moduleDir, { recursive: true, force: true }));
const { default: xmppTools } = await import(pathToFileURL(modulePath).href);
const drain = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };

async function fixture(t, { optIn = true, interval = 5000, defaultInterval = false } = {}) {
	const dir = mkdtempSync(join(tmpdir(), "pi-msg-heartbeat-test-"));
	const entry = join(dir, "upstream.mjs");
	const key = Symbol.for(dir);
	writeFileSync(entry, `export default (pi) => globalThis[Symbol.for(${JSON.stringify(dir)})](pi);`);
	const env = { ...process.env };
	delete process.env.PI_MSG_PROCESS_EXTENSION;
	if (optIn) process.env.PI_MSG_PROCESS_EXTENSION = entry;
	if (defaultInterval) delete process.env.PI_MSG_PROCESS_HEARTBEAT_MS;
	else process.env.PI_MSG_PROCESS_HEARTBEAT_MS = String(interval);
	process.env.PI_MSG_PROCESS_HEARTBEAT_FLUSH_MS = "100";
	process.env.PI_MSG_TOOLS = "";
	t.mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"], now: 100000 });
	const hooks = new Map(), events = new Map(), tools = [], relays = [], calls = [], processes = new Map();
	const on = (map, name, fn) => {
		if (!map.has(name)) map.set(name, new Set());
		map.get(name).add(fn);
		return () => map.get(name).delete(fn);
	};
	const bus = {
		on: (name, fn) => on(events, name, fn),
		emit: (name, value) => { for (const fn of events.get(name) ?? []) fn(value); },
	};
	bus.on("processes:request:list", ({ reply }) => reply([...processes.values()]));
	let tailReply = ({ reply }) => reply([{ text: "tail line" }]);
	bus.on("processes:request:combined_output", (request) => tailReply(request));
	const ctx = {
		cwd: dir, sessionManager: { getSessionId: () => "test-session" },
		ui: { select: async (title) => { relays.push(JSON.parse(title.slice("pi-msg-relay:".length))); return "ok"; } },
	};
	let serial = 0, initializations = 0, behavior;
	function start(id = `opaque-${++serial}`, overrides = {}) {
		const info = { id, name: "job", command: "sleep 100", startTime: Date.now(), status: "running", ...overrides };
		processes.set(id, info);
		bus.emit("processes:started", info);
		return info;
	}
	function end(id) {
		const info = processes.get(id);
		if (info) info.status = "finished";
		bus.emit("processes:ended", { id });
	}
	const upstream = {
		name: "process", label: "Process", description: "original description", promptSnippet: "snippet",
		promptGuidelines: ["guideline"], renderCall() {}, renderResult() {}, customMetadata: { preserved: true },
		parameters: { type: "object", properties: { action: { type: "string" }, notify: { type: "object" } }, required: ["action"], additionalProperties: false, title: "upstream schema" },
		async execute(...args) {
			calls.push({ args, receiver: this });
			if (behavior) return behavior(...args);
			if (args[1].action === "start") return { content: [{ type: "text", text: "started" }], details: { action: "start", process: start() } };
			return { content: [], details: { action: args[1].action } };
		},
	};
	globalThis[key] = (api) => {
		initializations++;
		assert.equal(api.events, bus);
		api.registerCommand("original", { handler() {} });
		api.registerTool(upstream);
		api.registerTool({ name: "other", execute() {} });
	};
	const pi = { events: bus, on: (name, fn) => on(hooks, name, fn), registerTool: (tool) => tools.push(tool), registerCommand() {} };
	const result = xmppTools(pi);
	if (optIn) assert.ok(result instanceof Promise);
	else assert.equal(result, undefined, "ordinary factory remains synchronous");
	await result;
	const hook = (name) => { for (const fn of hooks.get(name) ?? []) fn({}, ctx); };
	hook("session_start");
	t.after(() => {
		hook("session_shutdown");
		delete globalThis[key];
		for (const name of ["PI_MSG_PROCESS_EXTENSION", "PI_MSG_PROCESS_HEARTBEAT_MS", "PI_MSG_PROCESS_HEARTBEAT_FLUSH_MS", "PI_MSG_TOOLS"]) {
			if (env[name] === undefined) delete process.env[name]; else process.env[name] = env[name];
		}
		rmSync(dir, { recursive: true, force: true });
	});
	const tick = async (ms) => { t.mock.timers.tick(ms); await drain(); };
	return {
		bus, pi, ctx, tools, upstream, calls, processes, relays, start, end, tick, hook,
		tool: tools.find((tool) => tool.name === "process"),
		get initializations() { return initializations; },
		set behavior(fn) { behavior = fn; }, set tailReply(fn) { tailReply = fn; },
		heartbeats: () => relays.filter((r) => r.action === "process_heartbeat"),
	};
}

const execute = (f, params, ...rest) => f.tool.execute("call-id", params, ...rest);
const ids = (f) => f.heartbeats().at(-1)?.processes.map((p) => p.id).sort();

test("no opt-in is synchronous, does not import upstream, retains default event timers", async (t) => {
	const f = await fixture(t, { optIn: false, defaultInterval: true });
	assert.equal(f.initializations, 0);
	assert.equal(f.tools.length, 0);
	f.start("default");
	await f.tick(599900);
	assert.equal(f.heartbeats().length, 0);
	await f.tick(100);
	await f.tick(100);
	assert.deepEqual(ids(f), ["default"]);
});

test("adapter initializes once, preserves schema/metadata/renderers and full execution contract", async (t) => {
	const f = await fixture(t);
	assert.equal(f.initializations, 1);
	assert.deepEqual(f.tools.map((tool) => tool.name), ["process", "other"]);
	for (const field of ["label", "description", "promptSnippet", "promptGuidelines", "renderCall", "renderResult", "customMetadata"]) assert.equal(f.tool[field], f.upstream[field], field);
	for (const field of ["required", "additionalProperties", "title"]) assert.equal(f.tool.parameters[field], f.upstream.parameters[field]);
	assert.equal(f.tool.parameters.properties.notify, f.upstream.parameters.properties.notify);
	assert.equal(f.upstream.parameters.properties.heartbeatSeconds, undefined, "no schema mutation");
	assert.deepEqual({ ...f.tool.parameters.properties.heartbeatSeconds, description: undefined }, { type: "integer", minimum: 0, maximum: 2147483, description: undefined });
	const signal = new AbortController().signal, updates = [], update = (p) => updates.push(p);
	const result = { content: [{ type: "text", text: "untouched" }], details: { unknown: true }, usage: { test: 1 } };
	f.behavior = async (...args) => { args[3](result); return result; };
	const params = { action: "start", command: "arbitrary command", cwd: "/some/path", notify: { logMatches: [{ pattern: "ready" }], onExit: "turn" }, heartbeatSeconds: 3, futureOption: { nested: true } };
	const receiver = { caller: true };
	assert.equal(await f.tool.execute.call(receiver, "call", params, signal, update, f.ctx), result);
	assert.equal(f.calls[0].receiver, receiver);
	assert.deepEqual(f.calls[0].args, ["call", { ...params, heartbeatSeconds: undefined }, signal, update, f.ctx].map((v, i) => i === 1 ? Object.fromEntries(Object.entries(v).filter(([k]) => k !== "heartbeatSeconds")) : v));
	assert.equal(f.calls[0].args[1].notify, params.notify);
	assert.equal(params.heartbeatSeconds, 3, "input not mutated");
	assert.deepEqual(updates, [result]);
	const omitted = { action: "list", futureOption: true };
	await execute(f, omitted, signal, update, f.ctx);
	assert.equal(f.calls[1].args[1], omitted, "omitted field preserves exact params identity");
	const error = new Error("original spawn error");
	f.behavior = () => { throw error; };
	await assert.rejects(execute(f, { action: "start", heartbeatSeconds: 1 }), (e) => e === error);
	const controller = new AbortController(); controller.abort();
	f.behavior = (_id, _params, s) => { assert.equal(s, controller.signal); s.throwIfAborted(); };
	await assert.rejects(execute(f, { action: "start", heartbeatSeconds: 1 }, controller.signal), (e) => e === controller.signal.reason);
});

test("rejects invalid/unsupported explicit values before upstream side effects", async (t) => {
	const f = await fixture(t);
	for (const heartbeatSeconds of [undefined, null, "1", true, -1, 0.5, NaN, Infinity, 2147484, Number.MAX_SAFE_INTEGER]) {
		await assert.rejects(execute(f, { action: "start", heartbeatSeconds }), /heartbeatSeconds must be an integer/);
	}
	for (const action of ["list", "stop", "output", "write", "update", "clear", "unknown"]) {
		await assert.rejects(execute(f, { action, heartbeatSeconds: 0 }), /only for process start/);
	}
	assert.equal(f.calls.length, 0);
	await execute(f, { action: "start", heartbeatSeconds: 2147483 });
	await f.tick(1000);
	assert.equal(f.heartbeats().length, 0, "upper bound does not overflow to an immediate timer");
});

test("independent per-ID intervals, omission/environment, zero and coalescing", async (t) => {
	const f = await fixture(t);
	await execute(f, { action: "start", heartbeatSeconds: 1 });
	await execute(f, { action: "start", heartbeatSeconds: 2 });
	await execute(f, { action: "start", heartbeatSeconds: 0 });
	await execute(f, { action: "start" });
	await f.tick(1000); await f.tick(100);
	assert.deepEqual(ids(f), ["opaque-1"]);
	await f.tick(900); await f.tick(100);
	assert.deepEqual(ids(f), ["opaque-1", "opaque-2"], "simultaneous boundaries coalesce");
	assert.equal(f.heartbeats().length, 2);
	assert.equal(f.heartbeats()[0].processes[0].tail, "tail line");
	await f.tick(2900); await f.tick(100);
	// A coarse clock jump can put new due marks behind an in-flight batch.
	// They must all be reported, not necessarily in that same generation batch.
	assert.deepEqual([...new Set(f.heartbeats().slice(2).flatMap((r) => r.processes.map((p) => p.id)))].sort(), ["opaque-1", "opaque-2", "opaque-4"]);
	assert.ok(f.heartbeats().every((r) => !r.processes.some((p) => p.id === "opaque-3")));
	assert.equal(f.calls[2].args[1].heartbeatSeconds, undefined, "zero doesn't change upstream notification options");
});

test("replacement is anchored to start, drops pending default due, and skips catch-up floods", async (t) => {
	const f = await fixture(t, { interval: 1000 });
	let release;
	f.behavior = async () => { const p = f.start("delayed"); await new Promise((r) => { release = r; }); return { content: [], details: { process: p } }; };
	const pending = execute(f, { action: "start", heartbeatSeconds: 2 });
	// Hold the old default's output query while the start result replaces it.
	let releaseTail;
	f.tailReply = ({ reply }) => { releaseTail = reply; };
	await f.tick(1500);
	release(); await pending;
	releaseTail([{ text: "stale default" }]); await drain();
	f.tailReply = ({ reply }) => reply([{ text: "new interval" }]);
	assert.equal(f.heartbeats().length, 0);
	await f.tick(499);
	assert.equal(f.heartbeats().length, 0);
	await f.tick(1); await f.tick(100);
	assert.deepEqual(ids(f), ["delayed"], "2s boundary is from start, not result resolution");
	await f.tick(20000); await f.tick(100);
	assert.equal(f.heartbeats().length, 2, "stall coalesces to one report");
	await f.tick(100);
	assert.equal(f.heartbeats().length, 2, "no catch-up timer flood");
});

test("a later batch member cannot borrow a replacement schedule while an earlier tail awaits", async (t) => {
	const f = await fixture(t, { interval: 1000 });
	let releaseStart;
	f.behavior = async (_id, params) => {
		const p = f.start(params.name, { name: params.name });
		if (params.name === "B") await new Promise((resolve) => { releaseStart = resolve; });
		return { content: [], details: { process: p } };
	};
	await execute(f, { action: "start", name: "A" });
	const pending = execute(f, { action: "start", name: "B", heartbeatSeconds: 10 });
	let releaseTail;
	const tails = [];
	f.tailReply = ({ id, reply }) => {
		tails.push(id);
		if (id === "A") releaseTail = reply;
		else reply([{ text: id }]);
	};
	await f.tick(1100);
	assert.deepEqual(tails, ["A"]);
	releaseStart(); await pending;
	releaseTail([{ text: "A" }]); await drain();
	assert.deepEqual(ids(f), ["A"], "B's new 10s schedule was not due");
	assert.deepEqual(tails, ["A"], "skip B's replaced generation before fetching its tail");
	f.tailReply = ({ id, reply }) => reply([{ text: id }]);
	await f.tick(8900); await f.tick(100);
	assert.ok(ids(f).includes("B"), "B still reports at its own anchored boundary");
	assert.equal(f.heartbeats().filter((r) => r.processes.some((p) => p.id === "B")).length, 1);
});

test("boundaries arriving during an in-flight batch survive for the next flush", async (t) => {
	const f = await fixture(t, { interval: 1000 });
	let releaseStart;
	f.behavior = async (_id, params) => {
		const p = f.start(params.name, { name: params.name });
		if (params.name === "B") await new Promise((resolve) => { releaseStart = resolve; });
		return { content: [], details: { process: p } };
	};
	await execute(f, { action: "start", name: "A" });
	const pending = execute(f, { action: "start", name: "B", heartbeatSeconds: 2 });
	let releaseTail;
	f.tailReply = ({ id, reply }) => {
		if (id === "A") releaseTail = reply;
		else reply([{ text: id }]);
	};
	await f.tick(1100);
	releaseStart(); await pending;
	await f.tick(1000); // B's new generation becomes due while A is held.
	releaseTail([{ text: "A" }]); await drain();
	assert.deepEqual(ids(f), ["A"], "old batch does not report B's replacement");
	f.tailReply = ({ id, reply }) => reply([{ text: id }]);
	await f.tick(100);
	assert.deepEqual(ids(f), ["A", "B"], "new due marks were not consumed by the old batch");
});

test("rapid exit and shutdown before a start resolves never resurrect schedules", async (t) => {
	const f = await fixture(t);
	let release;
	f.behavior = async () => { const p = f.start("rapid"); await new Promise((r) => { release = r; }); return { content: [], details: { process: p } }; };
	const pending = execute(f, { action: "start", heartbeatSeconds: 1 });
	f.end("rapid"); release(); await pending;
	await f.tick(10000);
	assert.equal(f.heartbeats().length, 0);
	const late = execute(f, { action: "start", heartbeatSeconds: 1 });
	f.hook("session_shutdown"); release(); await late;
	f.start("after-shutdown");
	await f.tick(10000);
	assert.equal(f.heartbeats().length, 0);
	assert.equal(f.processes.size, 2, "cleanup doesn't stop or alter upstream processes");
});

test("end/clear remove finished schedules but clearing finished entries retains live timers", async (t) => {
	const f = await fixture(t);
	await execute(f, { action: "start", heartbeatSeconds: 1 });
	await execute(f, { action: "start", heartbeatSeconds: 1 });
	f.end("opaque-1"); f.processes.delete("opaque-1");
	f.bus.emit("processes:changed", { reason: "cleared" }); await drain();
	await f.tick(1000); await f.tick(100);
	assert.deepEqual(ids(f), ["opaque-2"]);
	f.processes.delete("opaque-2"); // clear/drop without a prior ended event
	f.bus.emit("processes:changed", { reason: "cleared" }); await drain();
	await f.tick(10000);
	assert.equal(f.heartbeats().length, 1);
});

test("in-flight tails serialize flushes and discard ended/shutdown reports", async (t) => {
	const f = await fixture(t);
	await execute(f, { action: "start", heartbeatSeconds: 1 });
	await execute(f, { action: "start", heartbeatSeconds: 1 });
	let release, queries = 0;
	f.tailReply = ({ id, reply }) => { queries++; if (id === "opaque-2") release = reply; else reply([{ text: "first" }]); };
	await f.tick(1000); await f.tick(100);
	assert.equal(queries, 2);
	await f.tick(200);
	assert.equal(queries, 2, "no overlapping drain while waiting for output");
	f.end("opaque-1"); release([{ text: "second" }]); await drain();
	assert.deepEqual(ids(f), ["opaque-2"], "ended while another tail was fetched");
	await f.tick(700); await f.tick(100);
	f.hook("session_shutdown"); release([{ text: "late" }]); await drain();
	assert.equal(f.heartbeats().length, 1, "old flush may not relay after shutdown");
});

test("ended before coalescing flush and absent-from-manager processes do not report", async (t) => {
	const f = await fixture(t);
	await execute(f, { action: "start", heartbeatSeconds: 1 });
	await f.tick(1000);
	f.end("opaque-1"); await f.tick(100);
	assert.equal(f.heartbeats().length, 0);
	await execute(f, { action: "start", heartbeatSeconds: 1 });
	f.processes.clear();
	await f.tick(1000); await f.tick(100);
	assert.equal(f.heartbeats().length, 0);
});

test("invalid opt-in paths/factories fail visibly rather than replacing execution", async (t) => {
	const dir = mkdtempSync(join(tmpdir(), "pi-msg-invalid-factory-"));
	const previous = process.env.PI_MSG_PROCESS_EXTENSION;
	const previousTools = process.env.PI_MSG_TOOLS;
	process.env.PI_MSG_TOOLS = "";
	t.after(() => {
		if (previous === undefined) delete process.env.PI_MSG_PROCESS_EXTENSION; else process.env.PI_MSG_PROCESS_EXTENSION = previous;
		if (previousTools === undefined) delete process.env.PI_MSG_TOOLS; else process.env.PI_MSG_TOOLS = previousTools;
		rmSync(dir, { recursive: true, force: true });
	});
	const pi = { events: { on: () => () => {}, emit() {} }, on() {}, registerTool() {} };
	process.env.PI_MSG_PROCESS_EXTENSION = "relative/index.ts";
	await assert.rejects(xmppTools(pi), /must be an absolute/);
	const cases = [
		["export const notAFactory = true;", /default extension factory/],
		["export default () => {};", /did not register/],
		['export default (pi) => pi.registerTool({name:"process",parameters:{type:"array"}});', /unsupported upstream process schema/],
		['export default (pi) => {const tool={name:"process",parameters:{type:"object",properties:{}}};pi.registerTool(tool);pi.registerTool(tool);};', /more than once/],
	];
	for (const [index, [source, pattern]] of cases.entries()) {
		const entry = join(dir, `invalid-${index}.mjs`);
		writeFileSync(entry, source);
		process.env.PI_MSG_PROCESS_EXTENSION = entry;
		await assert.rejects(xmppTools(pi), pattern);
	}
});

test("adapter omission retains ten-minute default and unrelated actions leave schedules alone", async (t) => {
	const f = await fixture(t, { defaultInterval: true });
	await execute(f, { action: "start" });
	for (const action of ["list", "output", "write", "update", "stop", "clear"]) {
		await execute(f, { action, id: "opaque-1", watches: { mode: "clear" } });
	}
	await f.tick(599900);
	assert.equal(f.heartbeats().length, 0);
	await f.tick(100); await f.tick(100);
	assert.deepEqual(ids(f), ["opaque-1"]);
});
