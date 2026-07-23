import { spawn, type ChildProcess } from "node:child_process";
import { StringDecoder } from "node:string_decoder";

/** A JSON event/response line emitted by `pi --mode rpc` on stdout. */
export interface RpcEvent {
	type: string;
	[key: string]: unknown;
}

export interface RpcResponse extends RpcEvent {
	type: "response";
	command: string;
	success: boolean;
	id?: string;
	error?: string;
	data?: unknown;
}

export interface RpcClientOptions {
	/** Path/name of the pi binary. Default "pi". */
	piBin?: string;
	/** Model pattern to pass via --model. */
	model?: string;
	/** Working directory for the agent. */
	cwd?: string;
	/** Called for every event/response that is not a reply to a pending request(). */
	onEvent: (event: RpcEvent) => void;
	/** Called when the pi process exits. */
	onExit: (code: number | null, signal: NodeJS.Signals | null) => void;
	/** Called for each stderr line from pi (diagnostics). */
	onStderr?: (line: string) => void;
}

/**
 * Minimal client for Pi's RPC mode. Spawns `pi --mode rpc`, frames stdout as
 * strict JSONL (split on \n only, tolerate trailing \r — NOT Node readline,
 * which also splits on U+2028/U+2029 and would corrupt JSON strings), and
 * writes one JSON command per line to stdin.
 */
export class RpcClient {
	private readonly opts: RpcClientOptions;
	private child: ChildProcess | null = null;
	private idCounter = 0;
	private readonly pending = new Map<string, (res: RpcResponse) => void>();

	constructor(opts: RpcClientOptions) {
		this.opts = opts;
	}

	start(): void {
		const args = ["--mode", "rpc"];
		if (this.opts.model) args.push("--model", this.opts.model);
		const child = spawn(this.opts.piBin ?? "pi", args, {
			cwd: this.opts.cwd,
			stdio: ["pipe", "pipe", "pipe"],
			env: process.env,
		});
		this.child = child;

		this.attachJsonlReader(child.stdout!, (line) => this.handleLine(line));
		this.attachLineReader(child.stderr!, (line) => this.opts.onStderr?.(line));

		child.on("exit", (code, signal) => {
			this.child = null;
			for (const resolve of this.pending.values()) {
				resolve({ type: "response", command: "?", success: false, error: "pi exited" });
			}
			this.pending.clear();
			this.opts.onExit(code, signal);
		});
	}

	private handleLine(line: string): void {
		if (!line.trim()) return;
		let event: RpcEvent;
		try {
			event = JSON.parse(line) as RpcEvent;
		} catch {
			this.opts.onStderr?.(`unparseable stdout line: ${line.slice(0, 200)}`);
			return;
		}
		if (event.type === "response") {
			const res = event as RpcResponse;
			if (res.id && this.pending.has(res.id)) {
				this.pending.get(res.id)!(res);
				this.pending.delete(res.id);
				return;
			}
		}
		this.opts.onEvent(event);
	}

	/** Fire-and-forget command. */
	send(command: Record<string, unknown>): void {
		if (!this.child?.stdin?.writable) return;
		this.child.stdin.write(JSON.stringify(command) + "\n");
	}

	/** Command with response correlation. Resolves when pi replies for this id. */
	request(command: Record<string, unknown>, timeoutMs = 30000): Promise<RpcResponse> {
		const id = `r${++this.idCounter}`;
		return new Promise<RpcResponse>((resolve) => {
			if (!this.child?.stdin?.writable) {
				resolve({ type: "response", command: String(command.type), success: false, error: "pi not running" });
				return;
			}
			const timer = setTimeout(() => {
				if (this.pending.delete(id)) {
					resolve({ type: "response", command: String(command.type), success: false, error: "timeout" });
				}
			}, timeoutMs);
			timer.unref?.();
			this.pending.set(id, (res) => {
				clearTimeout(timer);
				resolve(res);
			});
			this.child.stdin.write(JSON.stringify({ ...command, id }) + "\n");
		});
	}

	prompt(message: string, streamingBehavior?: "steer" | "followUp"): void {
		this.send(streamingBehavior ? { type: "prompt", message, streamingBehavior } : { type: "prompt", message });
	}

	newSession(): Promise<RpcResponse> {
		return this.request({ type: "new_session" });
	}

	compact(customInstructions?: string): Promise<RpcResponse> {
		return this.request(customInstructions ? { type: "compact", customInstructions } : { type: "compact" }, 120000);
	}

	setThinkingLevel(level: string): Promise<RpcResponse> {
		return this.request({ type: "set_thinking_level", level });
	}

	setModel(provider: string, modelId: string): Promise<RpcResponse> {
		return this.request({ type: "set_model", provider, modelId });
	}

	getAvailableModels(): Promise<RpcResponse> {
		return this.request({ type: "get_available_models" });
	}

	abort(): void {
		this.send({ type: "abort" });
	}

	/** Reply to an extension_ui_request dialog (we cancel; nobody's at the TUI). */
	cancelUi(id: string): void {
		this.send({ type: "extension_ui_response", id, cancelled: true });
	}

	stop(): void {
		if (this.child) this.child.kill("SIGTERM");
	}

	/** Strict JSONL reader per Pi's framing rules. */
	private attachJsonlReader(stream: NodeJS.ReadableStream, onLine: (line: string) => void): void {
		const decoder = new StringDecoder("utf8");
		let buffer = "";
		stream.on("data", (chunk: Buffer | string) => {
			buffer += typeof chunk === "string" ? chunk : decoder.write(chunk);
			let nl: number;
			while ((nl = buffer.indexOf("\n")) !== -1) {
				let line = buffer.slice(0, nl);
				buffer = buffer.slice(nl + 1);
				if (line.endsWith("\r")) line = line.slice(0, -1);
				onLine(line);
			}
		});
		stream.on("end", () => {
			buffer += decoder.end();
			if (buffer.length > 0) onLine(buffer.endsWith("\r") ? buffer.slice(0, -1) : buffer);
		});
	}

	/** Loose line reader for stderr diagnostics (framing doesn't matter here). */
	private attachLineReader(stream: NodeJS.ReadableStream, onLine: (line: string) => void): void {
		let buffer = "";
		stream.setEncoding?.("utf8");
		stream.on("data", (chunk: string) => {
			buffer += chunk;
			let nl: number;
			while ((nl = buffer.indexOf("\n")) !== -1) {
				const line = buffer.slice(0, nl);
				buffer = buffer.slice(nl + 1);
				if (line.trim()) onLine(line);
			}
		});
	}
}
