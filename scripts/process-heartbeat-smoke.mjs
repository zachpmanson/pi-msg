// Compatibility probe: actual Pi resource filter/TS loader + upstream factory.
// No model, XMPP connection, live settings writes, or dependency installation.
// Usage: node scripts/process-heartbeat-smoke.mjs /abs/pi-package /abs/pi-processes-package
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve, relative } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { setTimeout as wait } from "node:timers/promises";

assert.equal(process.argv.length, 4, "pass absolute Pi and pi-processes package roots");
const piRoot = resolve(process.argv[2]), upstreamRoot = resolve(process.argv[3]);
const companion = resolve(dirname(fileURLToPath(import.meta.url)), "../internal/pimsg/piext/xmpp-tools.ts");
const scratch = mkdtempSync(join(tmpdir(), "pi-msg-loader-smoke-"));
const agentDir = join(scratch, "agent");
mkdirSync(agentDir);
// Set before importing Pi or upstream: all config and log paths are disposable.
process.env.HOME = scratch;
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_MSG_PROCESS_EXTENSION = join(upstreamRoot, "extensions/processes/index.ts");
process.env.PI_MSG_PROCESS_HEARTBEAT_FLUSH_MS = "100";
delete process.env.PI_MSG_PROCESS_HEARTBEAT_MS;
process.env.PI_MSG_TOOLS = "";
process.chdir(scratch);
const fromPi = (path) => import(pathToFileURL(join(piRoot, "dist", path)).href);
let loaded, ctx, runtime, evidence;
try {
	const { DefaultPackageManager } = await fromPi("core/package-manager.js");
	const { SettingsManager } = await fromPi("core/settings-manager.js");
	const settingsManager = SettingsManager.inMemory({ packages: [{
		source: upstreamRoot, extensions: ["-extensions/processes/index.ts"],
	}] });
	const pm = new DefaultPackageManager({ cwd: scratch, agentDir, settingsManager });
	const resources = await pm.resolve(() => { throw new Error("probe must not install dependencies"); });
	const extensions = resources.extensions.filter((r) => r.enabled).map((r) => r.path);
	const manifest = JSON.parse(readFileSync(join(upstreamRoot, "package.json"), "utf8"));
	const retained = extensions.map((path) => relative(upstreamRoot, path)).sort();
	assert.ok(!extensions.includes(process.env.PI_MSG_PROCESS_EXTENSION), "primary must be excluded");
	for (const entry of manifest.pi.extensions.filter((p) => p !== "./extensions/processes/index.ts")) {
		assert.ok(extensions.includes(resolve(upstreamRoot, entry)), `filter lost ${entry}`);
	}
	assert.ok(resources.skills.some((r) => r.enabled), "omitting skills must preserve skills");
	const { loadExtensions, createExtensionRuntime } = await fromPi("core/extensions/loader.js");
	const { createEventBus } = await fromPi("core/event-bus.js");
	const bus = createEventBus();
	const listRegistrations = [];
	const originalOn = bus.on;
	bus.on = (name, handler) => {
		if (name === "processes:request:list") listRegistrations.push(handler);
		return originalOn(name, handler);
	};
	runtime = createExtensionRuntime();
	const notifications = [], relays = [];
	runtime.sendMessage = (message, options) => notifications.push({ message, options });
	runtime.sendUserMessage = () => {}; // capture-only host: never run an agent/model
	runtime.appendEntry = () => {};
	loaded = await loadExtensions([companion, ...extensions], scratch, bus, runtime);
	assert.deepEqual(loaded.errors, [], "real loader errors");
	const tools = loaded.extensions.flatMap((extension) => [...extension.tools.values()].map((entry) => entry.definition));
	assert.equal(tools.filter((tool) => tool.name === "process").length, 1, "duplicate tool registration");
	assert.equal(listRegistrations.length, 1, "duplicate manager initialized");
	const tool = tools.find((tool) => tool.name === "process");
	const schema = tool.parameters.properties.heartbeatSeconds;
	assert.equal(schema.type, "integer");
	assert.equal(schema.maximum, 2147483);
	assert.ok(!tool.parameters.required?.includes("heartbeatSeconds"));
	assert.equal(typeof tool.renderCall, "function");
	assert.equal(typeof tool.renderResult, "function");
	ctx = {
		cwd: scratch, hasUI: false, mode: "rpc", isIdle: () => true,
		sessionManager: { getSessionId: () => "loader-smoke", getSessionFile: () => undefined, getEntries: () => [], getBranch: () => [] },
		ui: { notify() {}, setStatus() {}, setWidget() {}, select: async (title) => {
			assert.ok(title.startsWith("pi-msg-relay:"));
			relays.push(JSON.parse(title.slice("pi-msg-relay:".length))); return "ok";
		} },
	};
	for (const extension of loaded.extensions) {
		for (const handler of extension.handlers.get("session_start") ?? []) await handler({ reason: "startup" }, ctx);
	}
	const run = (params) => tool.execute("smoke-call", params, new AbortController().signal, () => {}, ctx);
	for (const params of [{ action: "start", name: "must-not-spawn", command: "false", heartbeatSeconds: -1 }, { action: "clear", heartbeatSeconds: 0 }]) {
		await assert.rejects(run(params), /heartbeatSeconds/);
	}
	const running = [];
	for (const [name, heartbeatSeconds] of [["enabled", 1], ["disabled", 0], ["default", undefined]]) {
		const result = await run({ action: "start", name, command: "printf 'ready\\n'; sleep 3", cwd: scratch,
			notify: { onSuccess: "turn", logMatches: [{ pattern: "ready", on: "turn" }] },
			...(heartbeatSeconds === undefined ? {} : { heartbeatSeconds }),
		});
		assert.equal(result.details.action, "start");
		assert.equal(typeof result.details.process.id, "string");
		running.push(result.details.process.id);
	}
	let snapshots = [];
	bus.emit("processes:request:list", { reply: (processes) => snapshots.push(processes) });
	assert.equal(snapshots.length, 1, "more than one authoritative manager replied");
	assert.equal(snapshots[0].length, 3);
	assert.equal((await run({ action: "list" })).details.action, "list");
	assert.equal((await run({ action: "output", id: running[0] })).details.action, "output");
	await wait(4200);
	const heartbeats = relays.filter((r) => r.action === "process_heartbeat");
	assert.ok(heartbeats.length >= 1, "no positive interval heartbeat");
	assert.ok(heartbeats.every((r) => r.processes.every((p) => p.id === running[0])), "zero/default emitted an unexpected heartbeat");
	for (const id of running) {
		assert.ok(notifications.some(({ message }) => message.details?.processId === id && message.details?.kind === "log_match"), `lost log match for ${id}`);
		assert.ok(notifications.some(({ message }) => message.details?.processId === id && message.details?.kind === "success"), `lost exit for ${id}`);
	}
	await run({ action: "clear" });
	evidence = { pi: JSON.parse(readFileSync(join(piRoot, "package.json"))).version,
		upstream: manifest.version, retainedExtensions: retained, skillsPreserved: true, managers: listRegistrations.length,
		schema: "optional integer 0..2147483", processes: running.length, heartbeats: heartbeats.length,
		logMatchesAndExits: "all three processes, including heartbeatSeconds:0", result: "passed" };
} finally {
	if (loaded && ctx) {
		for (const extension of loaded.extensions) {
			for (const handler of extension.handlers.get("session_shutdown") ?? []) {
				try { await handler({ reason: "quit" }, ctx); }
				catch (error) { console.error("upstream shutdown failed:", error); process.exitCode = 1; }
			}
		}
	}
	runtime?.invalidate();
	process.chdir(tmpdir());
	rmSync(scratch, { recursive: true, force: true });
}
if (evidence && !process.exitCode) console.log(JSON.stringify(evidence, null, 2));
