// Offline installed-Pi/TypeBox render probe, driven by TestRenderedContext.
// Explicit inputs only: no discovery of live settings, credentials or context.
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
const pkg = resolve(process.argv[2]);
const installed = path => import(pathToFileURL(resolve(pkg, path)).href);
const { loadExtensions } = await installed("dist/core/extensions/loader.js");
const { buildSystemPrompt } = await installed("dist/core/system-prompt.js");
const { ExtensionRunner } = await installed("dist/core/extensions/runner.js");
const { SessionManager } = await installed("dist/core/session-manager.js");
const version = JSON.parse(readFileSync(resolve(pkg, "package.json"), "utf8")).version;
const cwd = process.cwd();
const loaded = await loadExtensions([fileURLToPath(new URL("./xmpp-tools.ts", import.meta.url))], cwd);
if (loaded.errors.length) throw new Error(JSON.stringify(loaded.errors));
const tools = loaded.extensions.flatMap(extension => [...extension.tools.values()].map(tool => tool.definition));
const prompt = readFileSync(0, "utf8");
// Synthetic shared-context inputs exercise the same builder path as AGENTS/SOUL
// without reading or changing the deployment's shared files.
const contextFiles = [
  { path: "/virtual/AGENTS.md", content: "Render fixture shared agent instructions." },
  { path: "/virtual/SOUL.md", content: "Render fixture shared personality instructions." },
];
const options = {
  cwd, contextFiles, skills: [], selectedTools: tools.map(tool => tool.name),
  toolSnippets: Object.fromEntries(tools.filter(tool => tool.promptSnippet).map(tool => [tool.name, tool.promptSnippet])),
  promptGuidelines: tools.flatMap(tool => tool.promptGuidelines ?? []),
};
const baseSystem = buildSystemPrompt(options);
// No model registry is needed: this hook only changes the system prompt and no
// session_start/tool handler is run (so there is no relay, timer or provider).
const runner = new ExtensionRunner(loaded.extensions, loaded.runtime, cwd, SessionManager.inMemory(cwd), undefined);
runner.onError(error => { throw new Error(JSON.stringify(error)); });
const result = await runner.emitBeforeAgentStart(prompt, undefined, baseSystem, options);
console.log(JSON.stringify({ version, loader: "loadExtensions", builder: "buildSystemPrompt", baseSystem,
  system: result?.systemPrompt ?? baseSystem, prompt, contextFiles, promptGuidelines: options.promptGuidelines,
  tools: tools.map(({name, description, parameters, promptGuidelines}) => ({name, description, parameters, promptGuidelines})) }));
