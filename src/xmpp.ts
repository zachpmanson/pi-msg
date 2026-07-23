import { client, xml, type Client } from "@xmpp/client";
import SASLFactory from "saslmechanisms";
import type { ResolvedAccount } from "./config.ts";

/** An XMPP stanza element (the type `xml()` builds and `"stanza"` events emit). */
type Stanza = ReturnType<typeof xml>;

/**
 * Build an @xmpp/client, preferring SASL PLAIN over SCRAM-SHA-1.
 *
 * @xmpp/client bundles the `sasl-scram-sha-1` mechanism and, because it is
 * registered first, always selects it when a server offers it. That
 * implementation produces a client response ejabberd rejects with
 * "not-authorized - Response decoding failed", so auth fails against ejabberd
 * even with correct credentials. PLAIN works and is safe here: servers we
 * target require STARTTLS, so SASL only runs after the channel is encrypted.
 *
 * We drop SCRAM-SHA-1 for the duration of client construction (mechanisms are
 * registered synchronously inside `client()`), then restore the factory so the
 * rest of the process is untouched.
 */
function buildClient(options: Parameters<typeof client>[0]): Client {
	const proto = (SASLFactory as unknown as { prototype: { use: (...args: unknown[]) => unknown } }).prototype;
	const originalUse = proto.use;
	proto.use = function (this: unknown, mech: unknown) {
		if ((mech as { prototype?: { name?: string } })?.prototype?.name === "SCRAM-SHA-1") return this;
		return originalUse.apply(this, arguments as unknown as unknown[]);
	};
	try {
		return client(options);
	} finally {
		proto.use = originalUse;
	}
}

/** Split a JID into its bare form (localpart@domain), dropping any resource. */
export function bareJid(full: string): string {
	return full.split("/")[0]!.toLowerCase();
}

/** Soft cap for a single outgoing message body; longer text is split on
 * newline / word boundaries so servers don't reject oversized stanzas. */
const MAX_BODY = 3000;

function chunk(text: string, max = MAX_BODY): string[] {
	if (text.length <= max) return [text];
	const chunks: string[] = [];
	let rest = text;
	while (rest.length > max) {
		let cut = rest.lastIndexOf("\n", max);
		if (cut < max * 0.5) cut = rest.lastIndexOf(" ", max);
		if (cut < max * 0.5) cut = max;
		chunks.push(rest.slice(0, cut));
		rest = rest.slice(cut).replace(/^\s+/, "");
	}
	if (rest) chunks.push(rest);
	return chunks;
}

export interface XmppBridgeOptions {
	/** Called with the body of each direct message from the owner. */
	onMessage: (body: string) => void;
	/** Optional diagnostic sink (connection state, errors). */
	log?: (message: string, level: "info" | "warning" | "error") => void;
}

/**
 * Thin wrapper around @xmpp/client for a single account: connect + presence,
 * receive owner-only direct messages, and send chat messages to the owner.
 * Auto-reconnect is provided by the client bundle (@xmpp/reconnect).
 */
export class XmppBridge {
	private readonly account: ResolvedAccount;
	private readonly opts: XmppBridgeOptions;
	private xmpp: Client | null = null;
	private online = false;
	private presenceStatus = "listening";

	constructor(account: ResolvedAccount, opts: XmppBridgeOptions) {
		this.account = account;
		this.opts = opts;
	}

	private log(message: string, level: "info" | "warning" | "error" = "info"): void {
		this.opts.log?.(message, level);
	}

	/** Connect, authenticate, and announce presence. Resolves once online. */
	async connect(): Promise<void> {
		const [username, domain] = this.account.jid.split("@");
		if (!username || !domain) throw new Error(`pi-msg: invalid jid "${this.account.jid}"`);

		const xmpp = buildClient({
			service: this.account.service,
			domain,
			username,
			resource: this.account.resource,
			password: this.account.password,
		});
		this.xmpp = xmpp;

		xmpp.on("error", (err: Error) => this.log(`xmpp error: ${err.message}`, "error"));
		xmpp.on("offline", () => {
			this.online = false;
			this.log("xmpp offline", "warning");
		});
		xmpp.on("stanza", (stanza) => this.handleStanza(stanza));

		const owner = bareJid(this.account.owner);
		xmpp.on("online", async () => {
			this.online = true;
			// Announce available presence with the current status label. Fires on
			// every (re)connect, so the roster shows the bot online the whole time.
			await this.setPresence();
			this.log(`online as ${this.account.jid}/${this.account.resource}, relaying to ${owner}`, "info");
		});

		await xmpp.start();
	}

	private handleStanza(stanza: Stanza): void {
		if (!stanza.is("message")) return;
		const type = stanza.attrs.type as string | undefined;
		// Only direct 1:1 chat (or type-less) messages; skip groupchat/errors/headline.
		if (type && type !== "chat" && type !== "normal") return;
		const from = stanza.attrs.from as string | undefined;
		if (!from || bareJid(from) !== bareJid(this.account.owner)) return;
		const body = stanza.getChildText("body");
		if (!body || !body.trim()) return; // ignore chat-states, receipts, empty
		this.opts.onMessage(body);
	}

	/** Send a chat message to the owner. Splits long text across stanzas. */
	async send(text: string): Promise<void> {
		if (!this.xmpp || !this.online) {
			this.log("send skipped: not online", "warning");
			return;
		}
		for (const part of chunk(text)) {
			try {
				await this.xmpp.send(xml("message", { type: "chat", to: bareJid(this.account.owner) }, xml("body", {}, part)));
			} catch (err) {
				this.log(`send failed: ${(err as Error).message}`, "error");
				break;
			}
		}
	}

	/**
	 * Announce available presence with a status label (shown on the owner's
	 * roster, e.g. "listening" / "working…"). The label is remembered and
	 * re-sent on every reconnect. Call with no argument to re-assert the
	 * current label.
	 */
	async setPresence(status?: string): Promise<void> {
		if (status !== undefined) this.presenceStatus = status;
		if (!this.xmpp || !this.online) return;
		try {
			await this.xmpp.send(xml("presence", {}, xml("status", {}, this.presenceStatus)));
		} catch (err) {
			this.log(`presence failed: ${(err as Error).message}`, "warning");
		}
	}

	/**
	 * Send an XEP-0085 chat-state notification to the owner (the "typing…"
	 * indicator). "composing" shows typing; "active"/"paused" clears it.
	 * Sent as a bodyless <message>, so it never appears as a chat line.
	 */
	async chatState(state: "composing" | "active" | "paused"): Promise<void> {
		if (!this.xmpp || !this.online) return;
		try {
			await this.xmpp.send(
				xml(
					"message",
					{ type: "chat", to: bareJid(this.account.owner) },
					xml(state, { xmlns: "http://jabber.org/protocol/chatstates" }),
				),
			);
			this.log(`chatstate ${state} sent`, "info");
		} catch (err) {
			this.log(`chatstate failed: ${(err as Error).message}`, "warning");
		}
	}

	/** Disconnect cleanly. */
	async close(): Promise<void> {
		this.online = false;
		if (!this.xmpp) return;
		try {
			await this.xmpp.stop();
		} catch {
			// best-effort on shutdown
		}
		this.xmpp = null;
	}
}
