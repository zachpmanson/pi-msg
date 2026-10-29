// Exercise the embedded companion itself, without Pi, an LLM, or XMPP.
// Node 22.13+ strips types; only the unrelated schema builder is stubbed.
import { test } from "node:test";
import assert from "node:assert/strict";
import { stripTypeScriptTypes } from "node:module";
import { readFileSync, writeFileSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const source = stripTypeScriptTypes(readFileSync(new URL("./xmpp-tools.ts", import.meta.url), "utf8"))
	.replace('import { Type } from "typebox";', 'const Type = new Proxy({}, { get: () => () => ({}) });');
const { default: xmppTools } = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);

const SESSION_ID = "7ef36f22-23c6-4dd9-a04b-1202a5b64dc2";
const OTHER_SESSION_ID = "3a164681-6bc4-467f-9da2-67ef49948a9d";
const SESSION_FILE = `/home/agent/.pi/agent/sessions/--home-agent-project--/2026-09-28T19-30-00-000Z_${SESSION_ID}.jsonl`;
const OTHER_SESSION_FILE = `/home/agent/.pi/agent/sessions/--home-agent-project--/2026-09-28T20-30-00-000Z_${OTHER_SESSION_ID}.jsonl`;
const CHILD_SESSION_FILE = `${SESSION_FILE.slice(0, -6)}/1ad41c56/run-0/session.jsonl`;

function fixture(t) {
	const previousProcessExtension = process.env.PI_MSG_PROCESS_EXTENSION;
	delete process.env.PI_MSG_PROCESS_EXTENSION;
	t.after(() => {
		if (previousProcessExtension === undefined) delete process.env.PI_MSG_PROCESS_EXTENSION;
		else process.env.PI_MSG_PROCESS_EXTENSION = previousProcessExtension;
	});
	const dir = mkdtempSync(join(tmpdir(), "pi-msg-subagent-test-"));
	const previousTmp = process.env.TMPDIR;
	process.env.TMPDIR = dir;
	t.after(() => {
		if (previousTmp === undefined) delete process.env.TMPDIR;
		else process.env.TMPDIR = previousTmp;
		rmSync(dir, { recursive: true, force: true });
	});
	t.mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"], now: 100000 });
	t.mock.method(process, "kill", (pid) => {
		if (pid === 999999) throw Object.assign(new Error("dead"), { code: "ESRCH" });
		if (pid === 999998) throw Object.assign(new Error("denied"), { code: "EPERM" });
		return true;
	});
	const events = new Map();
	const hooks = new Map();
	const relays = [];
	const root = join(dir, `pi-subagents-uid-${process.getuid()}`, "async-subagent-runs");
	const bus = {
		on(name, handler) {
			if (!events.has(name)) events.set(name, new Set());
			events.get(name).add(handler);
			return () => events.get(name).delete(handler);
		},
		emit(name, value) {
			for (const handler of events.get(name) ?? []) handler(value);
		},
	};
	// Keep unrelated process tracking synchronous and empty.
	bus.on("processes:request:list", ({ reply }) => reply([]));
	const ctx = {
		sessionManager: { getSessionFile: () => SESSION_FILE, getSessionId: () => SESSION_ID },
		ui: { select: async (title) => { relays.push(JSON.parse(title.slice("pi-msg-relay:".length))); return "ok"; } },
	};
	function install() {
		hooks.clear();
		xmppTools({ events: bus, on: (name, fn) => hooks.set(name, fn), registerTool() {} });
	}
	install();
	const start = () => hooks.get("session_start")({}, ctx);
	const shutdown = () => hooks.get("session_shutdown")({}, ctx);
	function write(id, overrides = {}, base = root) {
		const path = join(base, id);
		mkdirSync(path, { recursive: true });
		writeFileSync(join(path, "status.json"), JSON.stringify({
			runId: id, sessionId: SESSION_FILE, state: "running", pid: 42,
			startedAt: Date.now(), steps: [{ status: "running", agent: "worker" }], ...overrides,
		}));
		return path;
	}
	const emit = (id, event = "subagent:async-started", sessionId = SESSION_FILE) =>
		bus.emit(event, { sessionId, pid: 42, asyncDir: join(root, id), task: "SECRET PROMPT", goal: "SECRET GOAL" });
	const counts = () => relays.filter((r) => r.action === "subagent_count");
	const latest = () => counts().at(-1)?.count;
	const drain = async () => { for (let i = 0; i < 10; i++) await Promise.resolve(); };
	const tick = async (ms = 2000) => { t.mock.timers.tick(ms); await drain(); };
	t.after(shutdown);
	return { root, bus, ctx, write, emit, counts, latest, drain, tick, start, shutdown, install };
}

test("session-owned parallel children aggregate, then complete/fail/stop/pause cleanly", async (t) => {
	const f = fixture(t);
	f.write("a");
	f.write("other", { sessionId: OTHER_SESSION_FILE });
	f.write("unowned", { sessionId: undefined });
	f.write("nested", { isNested: true, sessionId: CHILD_SESSION_FILE }, join(f.root, "..", "nested-subagent-runs", "a"));
	f.start();
	assert.equal(f.latest(), undefined, "bootstrap must not block on an RPC dialog");
	await f.tick();
	assert.equal(f.latest(), 1);
	f.write("b", { steps: [{ status: "running" }, { status: "running" }, { status: "pending" }] });
	f.emit("b");
	await f.drain();
	assert.equal(f.latest(), 3);
	f.write("a", { state: "complete" });
	f.emit("a", "subagent:async-complete");
	await f.drain();
	assert.equal(f.latest(), 2);
	f.write("b", { steps: [{ status: "failed" }, { status: "running" }] });
	await f.tick();
	assert.equal(f.latest(), 1);
	for (const state of ["paused", "stopped", "failed", "complete"]) {
		f.write("b", { state });
		await f.tick();
		assert.equal(f.latest(), 0, state);
		f.write("b");
		await f.tick();
		assert.equal(f.latest(), 1);
	}
	assert.ok(f.counts().every((r) => Object.keys(r).sort().join() === "action,count"), "no task/role/output leaks");
});

test("file-owned started event is immediately visible before the first artifact, with bounded bootstrap grace", async (t) => {
	const f = fixture(t);
	f.start();
	f.emit("starting");
	await f.drain();
	assert.equal(f.latest(), 1, "parent can settle before the runner writes status");
	await f.tick();
	assert.equal(f.latest(), 0, "failed bootstrap must not remain busy");
	f.write("starting");
	await f.tick();
	assert.equal(f.latest(), 1, "delayed valid artifact should self-heal");
});

test("routing-enabled root stays busy beyond bootstrap and recovers after reload", async (t) => {
	const f = fixture(t);
	const id = "f8ba129c-4cf0-4943-a71e-e51a5f4b4b81";
	f.start();
	f.emit(id);
	await f.drain();
	assert.equal(f.latest(), 1, "started event covers bootstrap");
	const running = {
		lifecycleArtifactVersion: 2, mode: "single", isNested: true,
		lastUpdate: Date.now(), currentStep: 0, chainStepCount: 1,
		steps: [{ agent: "presence-sleeper", status: "running", sessionFile: CHILD_SESSION_FILE }],
	};
	f.write(id, running);
	for (let i = 0; i < 3; i++) {
		await f.tick();
		assert.equal(f.latest(), 1, "root must not become idle after bootstrap expires");
	}
	f.shutdown();
	await f.drain();
	assert.equal(f.latest(), 0);
	f.install();
	f.start(); // no lifecycle event: recover solely from top-level artifacts
	await f.tick();
	assert.equal(f.latest(), 1, "isNested is routing capability, not root exclusion");
	f.write(id, { ...running, state: "complete" });
	f.emit(id, "subagent:async-complete");
	await f.drain();
	assert.equal(f.latest(), 0, "root completion must still clear presence");
});

test("current-session event paths count routing-enabled roots but foreign nested runs stay isolated", async (t) => {
	const f = fixture(t);
	const nestedRoot = join(f.root, "..", "nested-subagent-runs", "root-run");
	const nestedPath = f.write("nested", { isNested: true, sessionId: CHILD_SESSION_FILE }, nestedRoot);
	// Even if a child-owned artifact appears in the scanned directory, it is foreign.
	f.write("foreign-child", { isNested: true, sessionId: CHILD_SESSION_FILE });
	f.start();
	await f.tick();
	assert.equal(f.latest(), 0, "nested artifacts must not leak through the root scan");
	for (const asyncDir of [nestedPath, join(nestedRoot, "starting")]) {
		f.bus.emit("subagent:async-started", { sessionId: CHILD_SESSION_FILE, pid: 42, asyncDir });
		await f.drain();
		assert.equal(f.latest(), 0, "foreign child events must not count artifacts or bootstrap");
	}
	const customRoot = join(f.root, "..", "custom-async-runs");
	const customPath = f.write("root", { isNested: true }, customRoot);
	await f.tick();
	assert.equal(f.latest(), 0, "custom directories are not scanned");
	f.bus.emit("subagent:async-started", { sessionId: SESSION_FILE, pid: 42, asyncDir: customPath });
	await f.drain();
	assert.equal(f.latest(), 1, "exact current-session event supplies root provenance");
	await f.tick();
	assert.equal(f.latest(), 1, "event-supplied root persists beyond bootstrap");
	f.write("root", { isNested: true, state: "complete" }, customRoot);
	f.bus.emit("subagent:async-complete", { sessionId: SESSION_FILE, asyncDir: customPath });
	await f.drain();
	assert.equal(f.latest(), 0);
});

test("chain gaps stay busy without counting future stages; dead/deleted/invalid artifacts clear", async (t) => {
	const f = fixture(t);
	f.start();
	f.write("chain", { steps: [{ status: "complete" }, { status: "pending" }, { status: "pending" }] });
	await f.tick();
	assert.equal(f.latest(), 1);
	f.write("chain", { pid: 999999 });
	await f.tick();
	assert.equal(f.latest(), 0, "lost runner must not stay busy");
	f.write("chain", { pid: 999998 });
	await f.tick();
	assert.equal(f.latest(), 1, "EPERM is not evidence the runner died");
	f.write("chain", { startedAt: Date.now() - 86_400_001 });
	await f.tick();
	assert.equal(f.latest(), 0, "reused/live PID cannot retain an abandoned artifact forever");
	f.write("chain", { pid: undefined });
	f.emit("chain");
	await f.drain();
	assert.equal(f.latest(), 1, "PID-less bootstrap grace");
	await f.tick();
	assert.equal(f.latest(), 0, "missing PID expires");
	f.write("chain");
	await f.tick();
	assert.equal(f.latest(), 1);
	writeFileSync(join(f.root, "chain", "status.json"), "partial");
	await f.tick();
	assert.equal(f.latest(), 0);
	f.write("chain");
	await f.tick();
	assert.equal(f.latest(), 1, "partial read self-heals");
	rmSync(join(f.root, "chain"), { recursive: true });
	await f.tick();
	assert.equal(f.latest(), 0);
});

test("reload re-seeds file-owned live artifacts; session replacement and shutdown discard stale callbacks", async (t) => {
	const f = fixture(t);
	f.write("live");
	f.start();
	await f.tick();
	assert.equal(f.latest(), 1);
	f.shutdown();
	await f.drain();
	assert.equal(f.latest(), 0);
	const length = f.counts().length;
	f.emit("live");
	await f.tick(20000);
	assert.equal(f.counts().length, length, "old timer or event handler survived shutdown");
	f.install();
	f.start();
	await f.tick();
	assert.equal(f.latest(), 1, "reload lost an existing child");
	f.shutdown();
	f.install();
	f.ctx.sessionManager.getSessionFile = () => OTHER_SESSION_FILE;
	f.ctx.sessionManager.getSessionId = () => OTHER_SESSION_ID;
	f.start();
	await f.tick();
	assert.equal(f.latest(), 0, "new session inherited old work");
	f.emit("live");
	await f.drain();
	assert.equal(f.latest(), 0, "foreign event leaked into session");
});

test("UUID-owned events and artifacts remain compatible alongside file-owned runs", async (t) => {
	const f = fixture(t);
	f.start();
	f.emit("uuid-starting", "subagent:async-started", SESSION_ID);
	await f.drain();
	assert.equal(f.latest(), 1, "UUID event must bridge bootstrap even when a session file exists");
	f.write("uuid-starting", { sessionId: SESSION_ID });
	f.write("file-owned");
	await f.tick();
	assert.equal(f.latest(), 2, "both exact identities may own separate child slots");
	f.write("uuid-starting", { sessionId: SESSION_ID, state: "complete" });
	f.emit("uuid-starting", "subagent:async-complete", SESSION_ID);
	await f.drain();
	assert.equal(f.latest(), 1);
	f.shutdown();
	f.install();
	delete f.ctx.sessionManager.getSessionFile;
	f.write("uuid-recovered", { sessionId: SESSION_ID });
	f.start();
	await f.tick();
	assert.equal(f.latest(), 1, "UUID-only session managers still recover their artifacts, not file-owned ones");
});

test("foreign and missing owners never match by path fragments or UUID substrings", async (t) => {
	const f = fixture(t);
	f.start();
	await f.tick();
	assert.equal(f.latest(), 0);
	const foreignOwners = [
		OTHER_SESSION_FILE, OTHER_SESSION_ID, undefined, null, "",
		SESSION_FILE.split("/").at(-1), SESSION_FILE.slice(1),
		SESSION_FILE.replace("/sessions/", "/sessions/../sessions/"),
		`/other/sessions/${SESSION_FILE.split("/").at(-1)}`,
		`${SESSION_FILE}.other`, `prefix-${SESSION_ID}`,
	];
	for (const [i, sessionId] of foreignOwners.entries()) {
		// No artifact: a wrongly accepted event would count via bootstrap grace.
		f.bus.emit("subagent:async-started", { sessionId, pid: 42, asyncDir: join(f.root, `event-${i}`) });
		await f.drain();
		assert.equal(f.latest(), 0, `foreign event: ${sessionId}`);
		f.write(`disk-${i}`, { sessionId });
	}
	await f.tick();
	assert.equal(f.latest(), 0, "foreign/unowned disk statuses must also stay isolated");
});

test("session_start replacement clears old file/UUID aliases and bootstrap paths", async (t) => {
	const f = fixture(t);
	f.start();
	f.write("old-file");
	f.write("old-uuid", { sessionId: SESSION_ID });
	f.emit("old-starting");
	await f.drain();
	assert.equal(f.latest(), 3);
	f.ctx.sessionManager.getSessionFile = () => OTHER_SESSION_FILE;
	f.ctx.sessionManager.getSessionId = () => OTHER_SESSION_ID;
	f.start(); // no shutdown/reinstall: same extension instance, different session
	f.emit("new-starting", "subagent:async-started", OTHER_SESSION_FILE);
	await f.drain();
	assert.equal(f.latest(), 1, "old artifacts and bootstrap paths survived replacement");
	for (const oldOwner of [SESSION_FILE, SESSION_ID]) {
		f.emit("stale-starting", "subagent:async-started", oldOwner);
		await f.drain();
		assert.equal(f.latest(), 1, "replacement retained an old ownership alias");
	}
	await f.tick();
	assert.equal(f.latest(), 0, "only the replacement's expired bootstrap should remain");
});

test("missing or empty current identities cannot attribute unowned work", async (t) => {
	const f = fixture(t);
	for (const identity of [undefined, null, ""]) {
		f.ctx.sessionManager.getSessionFile = () => identity;
		f.ctx.sessionManager.getSessionId = () => identity;
		f.write("unowned", { sessionId: identity });
		f.start();
		f.bus.emit("subagent:async-started", { sessionId: identity, pid: 42, asyncDir: join(f.root, "unowned-starting") });
		await f.tick();
		assert.ok(f.counts().every((r) => r.count === 0), "missing identities must not become ownership aliases");
	}
});

test("coalesces lifecycle events during a delayed relay and retries cancelled deliveries", async (t) => {
	const f = fixture(t);
	f.start();
	await f.tick();
	let answer;
	const attempted = [];
	f.ctx.ui.select = (title) => {
		const r = JSON.parse(title.slice("pi-msg-relay:".length));
		if (r.action !== "subagent_count") return Promise.resolve("ok");
		attempted.push(r.count);
		return new Promise((resolve) => { answer = resolve; });
	};
	f.write("a");
	f.emit("a");
	f.write("b");
	f.emit("b");
	await f.drain();
	answer("ok");
	await f.drain();
	assert.deepEqual(attempted, [1, 2], "second snapshot must follow the first");
	answer(undefined); // simulate transport timeout/cancel
	await f.drain();
	f.ctx.ui.select = async (title) => {
		const r = JSON.parse(title.slice("pi-msg-relay:".length));
		if (r.action === "subagent_count") attempted.push(r.count);
		return "ok";
	};
	await f.tick(); // cancelled snapshot is retried, not cached as delivered
	assert.deepEqual(attempted, [1, 2, 2]);
});

test("absent plugin stays idle, and pending old relays cannot survive replacement", async (t) => {
	const f = fixture(t);
	f.start();
	await f.tick();
	assert.equal(f.latest(), 0);
	const count = f.counts().length;
	await f.tick();
	assert.equal(f.counts().length, count, "unchanged idle snapshots should not send presence traffic");
	let release;
	const sent = [];
	f.ctx.ui.select = (title) => {
		const r = JSON.parse(title.slice("pi-msg-relay:".length));
		if (r.action === "subagent_count") {
			sent.push(r.count);
			if (r.count === 1) return new Promise((resolve) => { release = resolve; });
		}
		return Promise.resolve("ok");
	};
	f.write("a");
	f.emit("a");
	f.write("b");
	f.emit("b"); // dirty snapshot queued while count=1 is in flight
	f.shutdown();
	release("ok");
	await f.drain();
	assert.deepEqual(sent, [1, 0], "old dirty snapshot was relayed after shutdown");
});
