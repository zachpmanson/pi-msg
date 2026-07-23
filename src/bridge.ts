#!/usr/bin/env node
import { loadConfig, resolveAccount, configPath } from "./config.ts";
import { XmppBridge } from "./xmpp.ts";
import { RpcClient, type RpcEvent } from "./rpc.ts";

const DEBUG = !!process.env.PI_MSG_DEBUG;

function log(msg: string): void {
	// eslint-disable-next-line no-console
	console.error(`[pi-msg] ${msg}`);
}

/** Pull the plain-text portion out of an assistant message's content. */
function extractText(content: unknown): string {
	if (typeof content === "string") return content.trim();
	if (!Array.isArray(content)) return "";
	return content
		.filter((c): c is { type: string; text: string } =>
			!!c && typeof c === "object" && (c as { type?: unknown }).type === "text" && typeof (c as { text?: unknown }).text === "string",
		)
		.map((c) => c.text)
		.join("\n")
		.trim();
}

async function main(): Promise<void> {
	const config = loadConfig();
	if (!config) {
		log(`no config at ${configPath()} — nothing to do. See README for setup.`);
		process.exit(1);
	}
	let account;
	try {
		account = resolveAccount(config);
	} catch (err) {
		log((err as Error).message);
		process.exit(1);
	}

	let streaming = false;
	let shuttingDown = false;
	let repliedThisRun = false;
	let typingTimer: ReturnType<typeof setInterval> | null = null;

	function startTyping(): void {
		void xmpp.chatState("composing");
		if (typingTimer) return;
		// Chat clients auto-clear "typing…" after ~30s; refresh to keep it lit.
		typingTimer = setInterval(() => void xmpp.chatState("composing"), 20000);
		typingTimer.unref?.();
	}

	function stopTyping(): void {
		if (typingTimer) {
			clearInterval(typingTimer);
			typingTimer = null;
		}
		void xmpp.chatState("active");
	}

	const xmpp = new XmppBridge(account, {
		onMessage: (body) => void handleChat(body),
		log: (message, level) => {
			if (level === "info" && !DEBUG) return;
			log(`${level}: ${message}`);
		},
	});

	const rpc = new RpcClient({
		model: account.model,
		cwd: account.workdir,
		onEvent: (ev) => void handleRpcEvent(ev),
		onExit: (code) => void onPiExit(code),
		onStderr: (line) => {
			if (DEBUG) log(`pi stderr: ${line}`);
		},
	});

	async function handleRpcEvent(ev: RpcEvent): Promise<void> {
		switch (ev.type) {
			case "agent_start":
				streaming = true;
				repliedThisRun = false;
				startTyping();
				void xmpp.setPresence("working…");
				break;
			case "agent_settled":
				streaming = false;
				stopTyping();
				void xmpp.setPresence("listening");
				// The reply text + typing/presence already signal "done". Only nudge
				// if the run produced no message, so silence isn't mistaken for a hang.
				if (!repliedThisRun) await xmpp.send("✅ done (no reply) — your turn");
				break;
			case "message_end": {
				const message = ev.message as { role?: string; content?: unknown } | undefined;
				if (message?.role !== "assistant") return;
				const text = extractText(message.content);
				if (text) {
					await xmpp.send(text);
					repliedThisRun = true;
				}
				break;
			}
			case "extension_error":
				await xmpp.send(`⚠️ extension error: ${String(ev.error ?? "unknown")}`);
				break;
			case "extension_ui_request": {
				// No one is at the TUI to answer dialogs; cancel so pi doesn't block.
				const method = String(ev.method ?? "");
				if (["select", "confirm", "input", "editor"].includes(method) && typeof ev.id === "string") {
					rpc.cancelUi(ev.id);
					await xmpp.send(`⚠️ pi asked for input (${method}) — auto-dismissed (no interactive UI over chat).`);
				} else if (method === "notify" && ev.message) {
					if (DEBUG) await xmpp.send(`ℹ️ ${String(ev.message)}`);
				}
				break;
			}
			default:
				break;
		}
	}

	async function handleChat(text: string): Promise<void> {
		const t = text.trim();
		if (!t) return;

		if (t.startsWith("/")) {
			const spaceIdx = t.indexOf(" ");
			const name = (spaceIdx === -1 ? t.slice(1) : t.slice(1, spaceIdx)).toLowerCase();
			const arg = spaceIdx === -1 ? "" : t.slice(spaceIdx + 1).trim();
			switch (name) {
				case "new": {
					if (streaming) rpc.abort();
					const res = await rpc.newSession();
					await xmpp.send(res.success ? "🆕 new session ready" : `⚠️ /new failed: ${res.error ?? "unknown"}`);
					return;
				}
				case "compact": {
					const res = await rpc.compact(arg || undefined);
					await xmpp.send(res.success ? "🗜️ context compacted" : `⚠️ /compact failed: ${res.error ?? "unknown"}`);
					return;
				}
				case "think": {
					const res = await rpc.setThinkingLevel(arg);
					await xmpp.send(res.success ? `🧠 thinking level: ${arg}` : `⚠️ /think failed: ${res.error ?? "unknown"}`);
					return;
				}
				case "model": {
					await handleModel(arg);
					return;
				}
				case "abort":
				case "stop":
					rpc.abort();
					await xmpp.send("⛔ aborted");
					return;
				case "quit":
				case "exit":
					await shutdown("requested over chat");
					return;
				default:
					// Extension commands, /skill:name, and /template are handled by pi's prompt pipeline.
					rpc.prompt(t, streaming ? "steer" : undefined);
					startTyping();
					return;
			}
		}
		rpc.prompt(t, streaming ? "steer" : undefined);
		startTyping();
	}

	async function handleModel(arg: string): Promise<void> {
		if (!arg) {
			await xmpp.send("usage: /model <provider/id> or /model <search>");
			return;
		}
		if (arg.includes("/")) {
			const [provider, ...rest] = arg.split("/");
			const res = await rpc.setModel(provider!, rest.join("/"));
			await xmpp.send(res.success ? `🤖 model set: ${arg}` : `⚠️ /model failed: ${res.error ?? "unknown"}`);
			return;
		}
		// Fuzzy: fetch models and match by substring.
		const res = await rpc.getAvailableModels();
		const models = ((res.data as { models?: Array<{ provider: string; id: string }> })?.models) ?? [];
		const match = models.find((m) => `${m.provider}/${m.id}`.toLowerCase().includes(arg.toLowerCase()));
		if (!match) {
			await xmpp.send(`no model matches "${arg}". Try /model provider/id.`);
			return;
		}
		const set = await rpc.setModel(match.provider, match.id);
		await xmpp.send(set.success ? `🤖 model set: ${match.provider}/${match.id}` : `⚠️ /model failed: ${set.error ?? "unknown"}`);
	}

	async function onPiExit(code: number | null): Promise<void> {
		if (shuttingDown) return;
		await xmpp.send(`🔴 pi exited (code ${code ?? "?"}). Bridge shutting down.`);
		await xmpp.close();
		process.exit(code ?? 0);
	}

	async function shutdown(reason: string): Promise<void> {
		if (shuttingDown) return;
		shuttingDown = true;
		log(`shutting down: ${reason}`);
		try {
			await xmpp.send("🔴 session ended");
		} catch {
			// ignore
		}
		rpc.stop();
		await xmpp.close();
		process.exit(0);
	}

	process.on("SIGINT", () => void shutdown("SIGINT"));
	process.on("SIGTERM", () => void shutdown("SIGTERM"));

	// Bring up XMPP first so we can report problems, then start pi.
	await xmpp.connect();
	rpc.start();
	await xmpp.send("🟢 pi-msg bridge up. Chat to drive the agent; try /new, /compact, /model, /think, /abort, /quit.");
	log(`bridging account "${account.name}" (${account.jid}) to owner ${account.owner}`);
}

void main();
