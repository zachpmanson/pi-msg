import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

/** One XMPP account the bridge can connect as. */
export interface Account {
	/** Bare JID of the bot account, e.g. "pi@chat.example.com". */
	jid: string;
	/** Password for the bot account. */
	password: string;
	/**
	 * JID of the human this account relays to. For v1 this is also the only
	 * JID whose incoming messages are injected into the agent (whitelist).
	 */
	owner: string;
	/**
	 * Connection endpoint. Defaults to "xmpp://<jid-domain>:5222".
	 * May also be a "wss://.../xmpp-websocket" URL.
	 */
	service?: string;
	/** XMPP resource (client-session label). Defaults to "pi-msg". */
	resource?: string;
	/** Mirror a one-line notice each time a tool starts. Default false. */
	toolActivity?: boolean;
	/** Model pattern to launch pi with (e.g. "anthropic/claude-sonnet-latest"). Optional. */
	model?: string;
	/** Working directory for the pi agent. Defaults to the process cwd. */
	workdir?: string;
}

/** On-disk config: an arbitrary number of named accounts. "default" is used
 * when no account is selected. */
export interface Config {
	accounts: Record<string, Account>;
}

/** A fully-resolved account ready to connect with (defaults applied). */
export interface ResolvedAccount {
	name: string;
	jid: string;
	password: string;
	owner: string;
	service: string;
	resource: string;
	toolActivity: boolean;
	model?: string;
	workdir?: string;
}

export const DEFAULT_ACCOUNT = "default";
export const DEFAULT_RESOURCE = "pi-msg";

/** Path to the config file: $PI_MSG_CONFIG or ~/.config/pi-msg/config.json. */
export function configPath(): string {
	return process.env.PI_MSG_CONFIG || join(homedir(), ".config", "pi-msg", "config.json");
}

/** Read and parse the config file. Returns null if it does not exist. */
export function loadConfig(path = configPath()): Config | null {
	let raw: string;
	try {
		raw = readFileSync(path, "utf8");
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
		throw new Error(`pi-msg: cannot read config at ${path}: ${(err as Error).message}`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch (err) {
		throw new Error(`pi-msg: config at ${path} is not valid JSON: ${(err as Error).message}`);
	}
	if (!parsed || typeof parsed !== "object" || typeof (parsed as Config).accounts !== "object") {
		throw new Error(`pi-msg: config at ${path} must have an "accounts" object`);
	}
	return parsed as Config;
}

/** Derive the default XMPP service endpoint from a bare JID's domain. */
function defaultService(jid: string): string {
	const domain = jid.split("@")[1] ?? jid;
	return `xmpp://${domain}:5222`;
}

/**
 * Select and validate an account. Selection order:
 *   $PI_MSG_ACCOUNT (if present in the file) -> "default".
 * Throws with a human-readable message on any misconfiguration; the caller is
 * expected to surface it via ctx.ui.notify and then no-op.
 */
export function resolveAccount(config: Config, requested = process.env.PI_MSG_ACCOUNT): ResolvedAccount {
	const names = Object.keys(config.accounts);
	if (names.length === 0) throw new Error("pi-msg: config has no accounts");

	const name = requested && config.accounts[requested] ? requested : DEFAULT_ACCOUNT;
	const account = config.accounts[name];
	if (!account) {
		if (requested) {
			throw new Error(`pi-msg: account "${requested}" not found and no "${DEFAULT_ACCOUNT}" account defined`);
		}
		throw new Error(`pi-msg: no "${DEFAULT_ACCOUNT}" account defined (set PI_MSG_ACCOUNT to one of: ${names.join(", ")})`);
	}

	const missing = (["jid", "password", "owner"] as const).filter((k) => !account[k]);
	if (missing.length > 0) {
		throw new Error(`pi-msg: account "${name}" is missing required field(s): ${missing.join(", ")}`);
	}

	return {
		name,
		jid: account.jid,
		password: account.password,
		owner: account.owner,
		service: account.service || defaultService(account.jid),
		resource: account.resource || DEFAULT_RESOURCE,
		toolActivity: account.toolActivity ?? false,
		model: account.model,
		workdir: account.workdir,
	};
}
