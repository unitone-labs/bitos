#!/usr/bin/env node

// src/main.ts
import fs6 from "node:fs";
import os7 from "node:os";
import path7 from "node:path";
import { spawn } from "node:child_process";

// src/config.ts
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
var DEFAULT_GATEWAY = "https://bitos.dev";
function configDir() {
  const base = process.env["XDG_CONFIG_HOME"] ?? path.join(os.homedir(), ".config");
  return path.join(base, "bitos");
}
var configPath = () => path.join(configDir(), "config.json");
function loadConfig() {
  try {
    const raw = JSON.parse(fs.readFileSync(configPath(), "utf8"));
    return {
      gateway: typeof raw.gateway === "string" ? raw.gateway : DEFAULT_GATEWAY,
      ...typeof raw.token === "string" ? { token: raw.token } : {},
      ...typeof raw.address === "string" ? { address: raw.address } : {},
      ...typeof raw.apiKey === "string" ? { apiKey: raw.apiKey } : {},
      ...typeof raw.betaPassword === "string" ? { betaPassword: raw.betaPassword } : {}
    };
  } catch {
    return { gateway: DEFAULT_GATEWAY };
  }
}
function saveConfig(config) {
  fs.mkdirSync(configDir(), { recursive: true, mode: 448 });
  fs.writeFileSync(configPath(), `${JSON.stringify(config, null, 2)}
`, { mode: 384 });
}

// src/http.ts
var GatewayError = class extends Error {
  status;
  constructor(message, status) {
    super(message);
    this.status = status;
  }
};
function headers(config, extra) {
  return {
    ...config.token !== void 0 ? { authorization: `Bearer ${config.token}` } : {},
    ...config.betaPassword !== void 0 ? { "x-beta-password": config.betaPassword } : {},
    ...extra
  };
}
async function fail(res) {
  let detail = "";
  try {
    const body = await res.json();
    if (typeof body.error === "string") detail = body.error;
  } catch {
  }
  if (detail === "beta_locked") {
    throw new GatewayError(
      "This gateway is in private beta. Run: bitos config set beta-password <password>",
      res.status
    );
  }
  if (detail === "sign_in_required" || res.status === 401) {
    throw new GatewayError("Not signed in. Run: bitos login", res.status);
  }
  throw new GatewayError(detail !== "" ? detail : `HTTP ${res.status}`, res.status);
}
async function getJson(config, path8) {
  const res = await fetch(`${config.gateway}${path8}`, { headers: headers(config) });
  if (!res.ok) await fail(res);
  return await res.json();
}
async function postJson(config, path8, body) {
  const res = await fetch(`${config.gateway}${path8}`, {
    method: "POST",
    headers: headers(config, { "content-type": "application/json" }),
    body: JSON.stringify(body)
  });
  if (!res.ok) await fail(res);
  return await res.json();
}
async function postBytes(config, path8, bytes, extra) {
  const res = await fetch(`${config.gateway}${path8}`, {
    method: "POST",
    headers: headers(config, { "content-type": "application/octet-stream", ...extra }),
    body: bytes
  });
  if (!res.ok) await fail(res);
  return await res.json();
}
async function getBytes(config, path8) {
  const res = await fetch(`${config.gateway}${path8}`, { headers: headers(config) });
  if (!res.ok) await fail(res);
  return new Uint8Array(await res.arrayBuffer());
}
async function del(config, path8) {
  const res = await fetch(`${config.gateway}${path8}`, {
    method: "DELETE",
    headers: headers(config)
  });
  if (!res.ok) await fail(res);
}
async function* parseSse(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  for (; ; ) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf("\n\n")) !== -1) {
      const chunk = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      let event = "message";
      const data = [];
      for (const line of chunk.split("\n")) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
      }
      if (data.length > 0) yield { event, data: data.join("\n") };
    }
  }
}
async function streamTask(config, body, handlers = {}) {
  const res = await fetch(`${config.gateway}/api/tasks`, {
    method: "POST",
    headers: headers(config, { "content-type": "application/json", accept: "text/event-stream" }),
    body: JSON.stringify(body)
  });
  if (!res.ok) await fail(res);
  const ctype = res.headers.get("content-type") ?? "";
  if (res.body === null || !ctype.includes("text/event-stream")) {
    return await res.json();
  }
  for await (const msg of parseSse(res.body)) {
    let data;
    try {
      data = JSON.parse(msg.data);
    } catch {
      continue;
    }
    if (msg.event === "accepted") handlers.onAccepted?.(data);
    else if (msg.event === "step") handlers.onStep?.(data);
    else if (msg.event === "result") return data;
    else if (msg.event === "error") {
      return { status: "error", error: data.error ?? "task failed" };
    }
  }
  return { status: "error", error: "the stream ended without a result" };
}

// src/repl.ts
import readline from "node:readline";

// src/agent.ts
import os6 from "node:os";

// ../../packages/agent/src/tools.ts
import fs2 from "node:fs";
import os2 from "node:os";
import path2 from "node:path";
import { execFile } from "node:child_process";
var MAX_TOOL_OUTPUT = 12e3;
var MAX_FILE_READ = 6e4;
var COMMAND_TIMEOUT_MS = 12e4;
var DENIED = "DENIED by the user. Do not retry; ask what they want instead.";
function clip(text, max = MAX_TOOL_OUTPUT) {
  if (text.length <= max) return text;
  const head = text.slice(0, max);
  return `${head}
\u2026[${text.length - max} more characters truncated]`;
}
function resolvePath(cwd, target) {
  if (target === "~" || target.startsWith("~/")) return path2.join(os2.homedir(), target.slice(1));
  return path2.resolve(cwd, target);
}
function shown(cwd, abs) {
  const rel = path2.relative(cwd, abs);
  if (rel !== "" && !rel.startsWith("..") && !path2.isAbsolute(rel)) return rel;
  const home = os2.homedir();
  return abs.startsWith(`${home}/`) ? `~${abs.slice(home.length)}` : abs;
}
var str = (v, name) => {
  if (typeof v !== "string" || v === "") throw new Error(`${name} must be a non-empty string`);
  return v;
};
function isSecretFile(file) {
  const base = path2.basename(file);
  if (base === ".env") return true;
  if (!base.startsWith(".env.")) return false;
  return !/^\.env\.(example|sample|template|dist)$/.test(base);
}
var SECRET_DENIED = "REFUSED: that file holds secrets (.env). Ask the user for the one value you need instead of reading the file.";
var LOCAL_TOOLS = [
  {
    name: "read_file",
    description: "Read a UTF-8 text file anywhere on this machine (relative, absolute or ~ paths). Returns the content with line numbers.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string", description: "File path, relative to the working directory." }
      },
      required: ["path"]
    },
    mutating: false,
    run: async (args, ctx) => {
      const file = resolvePath(ctx.cwd, str(args["path"], "path"));
      if (isSecretFile(file)) return SECRET_DENIED;
      const text = fs2.readFileSync(file, "utf8");
      const numbered = text.split("\n").map((l, i) => `${String(i + 1).padStart(4)}  ${l}`).join("\n");
      return clip(numbered, MAX_FILE_READ);
    }
  },
  {
    name: "list_dir",
    description: "List a directory anywhere on this machine (names only; directories end with /). Paths may be relative, absolute or start with ~.",
    parameters: {
      type: "object",
      properties: { path: { type: "string", description: 'Directory, default "."' } }
    },
    mutating: false,
    run: async (args, ctx) => {
      const dir = resolvePath(ctx.cwd, typeof args["path"] === "string" && args["path"] !== "" ? args["path"] : ".");
      const entries = fs2.readdirSync(dir, { withFileTypes: true });
      return clip(
        entries.filter((e) => e.name !== "node_modules" && e.name !== ".git").map((e) => e.isDirectory() ? `${e.name}/` : e.name).sort().join("\n")
      );
    }
  },
  {
    name: "search",
    description: "Search file contents recursively with a regular expression (like grep -rn). Skips node_modules and .git.",
    parameters: {
      type: "object",
      properties: {
        pattern: { type: "string", description: "Regular expression." },
        path: { type: "string", description: 'Directory to search, default "."' }
      },
      required: ["pattern"]
    },
    mutating: false,
    run: async (args, ctx) => {
      const re = new RegExp(str(args["pattern"], "pattern"));
      const root = resolvePath(ctx.cwd, typeof args["path"] === "string" && args["path"] !== "" ? args["path"] : ".");
      const hits = [];
      const walk2 = (dir) => {
        for (const e of fs2.readdirSync(dir, { withFileTypes: true })) {
          if (e.name === "node_modules" || e.name === ".git" || e.name === "dist") continue;
          const full = path2.join(dir, e.name);
          if (e.isDirectory()) {
            walk2(full);
            continue;
          }
          if (!e.isFile() || isSecretFile(full) || fs2.statSync(full).size > 2e6) continue;
          let text;
          try {
            text = fs2.readFileSync(full, "utf8");
          } catch {
            continue;
          }
          const lines = text.split("\n");
          for (let i = 0; i < lines.length && hits.length < 200; i++) {
            if (re.test(lines[i])) hits.push(`${shown(ctx.cwd, full)}:${i + 1}: ${lines[i].trim()}`);
          }
          if (hits.length >= 200) return;
        }
      };
      walk2(root);
      return hits.length === 0 ? "(no matches)" : clip(hits.join("\n"));
    }
  },
  {
    name: "write_file",
    description: "Create or overwrite a file with the given content. Creates parent directories.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string" },
        content: { type: "string" }
      },
      required: ["path", "content"]
    },
    mutating: true,
    run: async (args, ctx) => {
      const file = resolvePath(ctx.cwd, str(args["path"], "path"));
      const content = typeof args["content"] === "string" ? args["content"] : "";
      const existed = fs2.existsSync(file);
      if (!await ctx.confirm("write_file", `${existed ? "overwrite" : "create"} ${shown(ctx.cwd, file)} (${content.length} chars)`)) {
        return DENIED;
      }
      fs2.mkdirSync(path2.dirname(file), { recursive: true });
      fs2.writeFileSync(file, content);
      return `wrote ${content.length} chars to ${shown(ctx.cwd, file)}`;
    }
  },
  {
    name: "edit_file",
    description: "Replace ONE exact occurrence of `old` with `new` in a file. Fails if `old` is missing or ambiguous \u2014 read the file first and copy the text exactly.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string" },
        old: { type: "string", description: "Exact text to replace (must occur exactly once)." },
        new: { type: "string" }
      },
      required: ["path", "old", "new"]
    },
    mutating: true,
    run: async (args, ctx) => {
      const file = resolvePath(ctx.cwd, str(args["path"], "path"));
      const before = str(args["old"], "old");
      const after = typeof args["new"] === "string" ? args["new"] : "";
      const text = fs2.readFileSync(file, "utf8");
      const count = text.split(before).length - 1;
      if (count === 0) return "ERROR: `old` not found in the file \u2014 re-read it and copy the text exactly.";
      if (count > 1) return `ERROR: \`old\` occurs ${count} times \u2014 include more surrounding lines to make it unique.`;
      const preview = `- ${before.split("\n")[0].slice(0, 70)}
+ ${after.split("\n")[0].slice(0, 70)}`;
      if (!await ctx.confirm("edit_file", `${shown(ctx.cwd, file)}
${preview}`)) {
        return DENIED;
      }
      fs2.writeFileSync(file, text.replace(before, after));
      return `edited ${shown(ctx.cwd, file)}`;
    }
  },
  {
    name: "run_command",
    description: "Run a shell command and return stdout+stderr (2 minute limit). Runs in the working directory unless `cwd` names another folder (absolute or ~). Use for tests, builds, git, package managers, and anything else on this machine.",
    parameters: {
      type: "object",
      properties: { command: { type: "string" }, cwd: { type: "string", description: "Folder to run in; defaults to the working directory." } },
      required: ["command"]
    },
    mutating: true,
    run: async (args, ctx) => {
      const command = str(args["command"], "command");
      const where2 = typeof args["cwd"] === "string" && args["cwd"] !== "" ? resolvePath(ctx.cwd, args["cwd"]) : ctx.cwd;
      const label = where2 === ctx.cwd ? command : `(in ${shown(ctx.cwd, where2)}) ${command}`;
      if (!await ctx.confirm("run_command", label)) {
        return DENIED;
      }
      ctx.note(`$ ${label}`);
      return new Promise((resolve) => {
        execFile(
          process.env["SHELL"] ?? "/bin/sh",
          ["-lc", command],
          { cwd: where2, timeout: COMMAND_TIMEOUT_MS, maxBuffer: 8 * 1024 * 1024 },
          (err, stdout, stderr) => {
            const code = err !== null && typeof err.code === "number" ? err.code : err !== null ? 1 : 0;
            const body = `${stdout}${stderr !== "" ? `
[stderr]
${stderr}` : ""}`;
            resolve(clip(`${body.trim()}
[exit ${code}]`));
          }
        );
      });
    }
  }
];

// ../../packages/agent/src/loop.ts
import fs5 from "node:fs";
import os5 from "node:os";
import path5 from "node:path";

// ../../packages/agent/src/rules.ts
import fs3 from "node:fs";
import os3 from "node:os";
import path3 from "node:path";
var PROJECT_RULE_FILES = ["AGENTS.md", "CLAUDE.md"];
var MAX_RULES_CHARS = 24e3;
function personalRuleFiles(home = os3.homedir(), env = process.env) {
  const config = env["XDG_CONFIG_HOME"] ?? path3.join(home, ".config");
  return [path3.join(config, "bitos", "AGENTS.md"), path3.join(home, ".claude", "CLAUDE.md")];
}
function readCapped(file) {
  let text;
  try {
    text = fs3.readFileSync(file, "utf8");
  } catch {
    return null;
  }
  if (text.trim() === "") return null;
  const truncated = text.length > MAX_RULES_CHARS;
  return { path: file, text: truncated ? `${text.slice(0, MAX_RULES_CHARS)}
[cut here \u2014 the file continues]` : text, truncated };
}
function findProjectRules(cwd) {
  let dir = path3.resolve(cwd);
  for (; ; ) {
    for (const name of PROJECT_RULE_FILES) {
      const found = readCapped(path3.join(dir, name));
      if (found !== null) return found;
    }
    const atRepoRoot = fs3.existsSync(path3.join(dir, ".git"));
    const parent = path3.dirname(dir);
    if (atRepoRoot || parent === dir) return null;
    dir = parent;
  }
}
function loadRules(cwd, home = os3.homedir(), env = process.env) {
  let personal = null;
  for (const file of personalRuleFiles(home, env)) {
    personal = readCapped(file);
    if (personal !== null) break;
  }
  return { project: findProjectRules(cwd), personal };
}
function rulesBlock(rules) {
  const parts = [];
  if (rules.project !== null) {
    parts.push(`Rules of this project, from ${rules.project.path} \u2014 follow them over your defaults:

${rules.project.text}`);
  }
  if (rules.personal !== null) {
    parts.push(`The user's own rules, from ${rules.personal.path}:

${rules.personal.text}`);
  }
  return parts.join("\n\n");
}

// ../../packages/agent/src/skills.ts
import fs4 from "node:fs";
import os4 from "node:os";
import path4 from "node:path";
var SKILL_FILE = "SKILL.md";
var PROJECT_SKILL_DIRS = [".bitos/skills", ".claude/skills", ".agents/skills"];
var MAX_SKILL_CHARS = 24e3;
function personalSkillDirs(home = os4.homedir(), env = process.env) {
  const config = env["XDG_CONFIG_HOME"] ?? path4.join(home, ".config");
  return [path4.join(config, "bitos", "skills"), path4.join(home, ".claude", "skills"), path4.join(home, ".agents", "skills")];
}
function parseSkillHeader(text) {
  const m = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/.exec(text);
  if (m === null) return null;
  const lines = m[1].split("\n");
  const fields = /* @__PURE__ */ new Map();
  for (let i = 0; i < lines.length; i++) {
    const kv = /^([A-Za-z_][\w-]*):\s*(.*)$/.exec(lines[i]);
    if (kv === null) continue;
    let value = kv[2].trim();
    if (/^[>|]-?$/.test(value)) {
      const block = [];
      while (i + 1 < lines.length && /^\s+\S/.test(lines[i + 1])) block.push(lines[++i].trim());
      value = block.join(" ");
    }
    fields.set(kv[1], value.replace(/^(['"])(.*)\1$/, "$2"));
  }
  const name = fields.get("name") ?? "";
  const description = fields.get("description") ?? "";
  return name === "" || description === "" ? null : { name, description };
}
function readDir(dir) {
  let names;
  try {
    names = fs4.readdirSync(dir);
  } catch {
    return [];
  }
  const out2 = [];
  for (const name of names.sort()) {
    const file = path4.join(dir, name, SKILL_FILE);
    let text;
    try {
      text = fs4.readFileSync(file, "utf8");
    } catch {
      continue;
    }
    const header = parseSkillHeader(text);
    if (header !== null && header.name === name) out2.push({ name, description: header.description.replace(/\s+/g, " "), file });
  }
  return out2;
}
function discoverSkills(cwd, home = os4.homedir(), env = process.env) {
  const seen = /* @__PURE__ */ new Map();
  const take = (refs) => {
    for (const r of refs) if (!seen.has(r.name)) seen.set(r.name, r);
  };
  let dir = path4.resolve(cwd);
  for (; ; ) {
    for (const sub of PROJECT_SKILL_DIRS) take(readDir(path4.join(dir, sub)));
    const atRepoRoot = fs4.existsSync(path4.join(dir, ".git"));
    const parent = path4.dirname(dir);
    if (atRepoRoot || parent === dir) break;
    dir = parent;
  }
  for (const d of personalSkillDirs(home, env)) take(readDir(d));
  return [...seen.values()].sort((a, b) => a.name.localeCompare(b.name));
}
function skillTool(skills) {
  if (skills.length === 0) return null;
  const listing = skills.map((s) => `- ${s.name}: ${s.description}`).join("\n");
  return {
    name: "skill",
    description: `Load one of the skills available here \u2014 a method or checklist written for this kind of task. Call it before starting work the skill covers; its text comes back for you to follow.

Available skills:
${listing}`,
    parameters: {
      type: "object",
      properties: { name: { type: "string", description: "The skill to load, from the list." } },
      required: ["name"]
    },
    mutating: false,
    run: async (args) => {
      const name = typeof args["name"] === "string" ? args["name"].trim() : "";
      const found = skills.find((s) => s.name === name);
      if (found === void 0) return `No skill named '${name}'. Available: ${skills.map((s) => s.name).join(", ")}`;
      const text = fs4.readFileSync(found.file, "utf8");
      const body = text.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n?/, "").trim();
      return clip(`Skill '${found.name}' (${found.file}):

${body}`, MAX_SKILL_CHARS);
    }
  };
}

// ../../packages/agent/src/loop.ts
var MAX_ROUNDS = 25;
var CONTEXT_LIMIT = 16e4;
var AgentError = class extends Error {
  status;
  constructor(message, status) {
    super(message);
    this.name = "AgentError";
    this.status = status;
  }
};
var REPEAT_LIMIT = 3;
var REPEAT_REFUSED = "REFUSED: the same call three times in a row. Change your approach, or ask the user what they want.";
function defaultSystemPrompt(cwd) {
  let git = "";
  try {
    if (fs5.existsSync(path5.join(cwd, ".git"))) git = " This directory is a git repository.";
  } catch {
  }
  return [
    "You are bitos, a coding and operations assistant running in the user's terminal, with inference from decentralized web3 networks.",
    `Working directory: ${cwd}.${git} Home: ${os5.homedir()}. Platform: ${os5.platform()} ${os5.arch()}.`,
    "You have tools to read, search, edit and write files and to run shell commands ANYWHERE on this",
    "machine: relative paths hang off the working directory, absolute paths and ~ reach the rest.",
    "Prefer the working directory unless the user points elsewhere or the task clearly lives elsewhere.",
    "Work like a careful engineer: look before you change (read_file/search first), make minimal",
    "edits with edit_file, verify with run_command (tests, typecheck, git diff), and report what you",
    "actually did. Never invent file contents or command output \u2014 call the tool.",
    "When the user declines a tool call, stop and ask; do not try the same thing another way.",
    "Answer in the user's language. Be concise; put code in fenced blocks."
  ].join(" ");
}
async function runTool(tools, call, ctx, events) {
  const tool = tools.find((t) => t.name === call.function.name);
  if (tool === void 0) return `ERROR: unknown tool ${call.function.name}`;
  let args = {};
  try {
    args = JSON.parse(call.function.arguments === "" ? "{}" : call.function.arguments);
  } catch {
    return "ERROR: arguments were not valid JSON";
  }
  events.onToolStart(tool.name, args);
  const started = Date.now();
  try {
    const result = await tool.run(args, ctx);
    events.onToolEnd(tool.name, result, Date.now() - started);
    return result;
  } catch (err) {
    const msg = `ERROR: ${err instanceof Error ? err.message : String(err)}`;
    events.onToolEnd(tool.name, msg, Date.now() - started);
    return msg;
  }
}
var Agent = class {
  constructor(transport, ctx, events, options = {}) {
    this.transport = transport;
    this.ctx = ctx;
    this.events = events;
    const skills = options.skills === void 0 ? discoverSkills(ctx.cwd) : options.skills ?? [];
    const loader = skillTool(skills);
    this.tools = [...options.tools ?? LOCAL_TOOLS, ...loader === null ? [] : [loader]];
    this.model = options.model ?? "bitos/code";
    this.maxRounds = options.maxRounds ?? MAX_ROUNDS;
    const rules = options.rules === void 0 ? loadRules(ctx.cwd) : options.rules;
    const block = rules === null ? "" : rulesBlock(rules);
    const prompt = options.systemPrompt ?? defaultSystemPrompt(ctx.cwd);
    this.messages = [{ role: "system", content: block === "" ? prompt : `${prompt}

${block}` }];
  }
  messages;
  tools;
  model;
  maxRounds;
  /** Cost across the session, from the gateway's usage (estimates when absent). */
  totalUsd = 0;
  /** The tools the model is offered, the skill loader included when there are skills. */
  get toolNames() {
    return this.tools.map((t) => t.name);
  }
  reset() {
    this.messages.length = 1;
    this.recent = [];
  }
  /** Signatures of the last calls, for the repeat guard. */
  recent = [];
  /**
   * A model that issues the same call three times running is stuck; the
   * third time the user is asked, and a no goes back as a refusal.
   */
  async guarded(call) {
    const signature = `${call.function.name}\0${call.function.arguments}`;
    const repeats = this.recent.filter((s) => s === signature).length;
    this.recent = [...this.recent.slice(-(REPEAT_LIMIT - 2)), signature];
    if (repeats >= REPEAT_LIMIT - 1) {
      const allowed = await this.ctx.confirm("repeat", `${call.function.name} for the ${repeats + 1}rd time with the same input`);
      if (!allowed) return REPEAT_REFUSED;
    }
    return runTool(this.tools, call, this.ctx, this.events);
  }
  /** One user turn: loops through tool calls until the model answers in prose. */
  async turn(input) {
    this.messages.push({ role: "user", content: input });
    let rounds = 0;
    let toolCalls = 0;
    for (; ; ) {
      if (rounds >= this.maxRounds) {
        return {
          text: `Stopped after ${this.maxRounds} rounds without a final answer \u2014 tell me how to continue.`,
          rounds,
          toolCalls
        };
      }
      rounds++;
      this.events.onThinking();
      const reply = await this.complete();
      const calls = reply.tool_calls ?? [];
      this.messages.push({ role: "assistant", content: reply.content, ...calls.length > 0 ? { tool_calls: calls } : {} });
      if (calls.length === 0) {
        return { text: reply.content ?? "", rounds, toolCalls };
      }
      for (const call of calls) {
        toolCalls++;
        const result = await this.guarded(call);
        this.messages.push({ role: "tool", content: result, tool_call_id: call.id });
      }
      this.trim();
    }
  }
  /** Keep the context bounded: the oldest tool results collapse to a stub. */
  trim() {
    let budget = 0;
    for (const m of this.messages) budget += (m.content ?? "").length;
    for (let i = 1; i < this.messages.length - 6 && budget > CONTEXT_LIMIT; i++) {
      const m = this.messages[i];
      if (m.role === "tool" && (m.content ?? "").length > 200) {
        budget -= (m.content ?? "").length - 40;
        m.content = "[earlier tool output trimmed]";
      }
    }
  }
  async complete() {
    const key = await this.transport.authorization();
    const doFetch = this.transport.fetch ?? globalThis.fetch;
    const res = await doFetch(`${this.transport.gateway}/v1/chat/completions`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${key}`,
        ...this.transport.betaPassword !== void 0 ? { "x-beta-password": this.transport.betaPassword } : {}
      },
      body: JSON.stringify({
        model: this.model,
        messages: this.messages,
        tools: this.tools.map((t) => ({
          type: "function",
          function: { name: t.name, description: t.description, parameters: t.parameters }
        })),
        tool_choice: "auto",
        temperature: 0.2
      })
    });
    if (!res.ok) {
      let detail = `HTTP ${res.status}`;
      try {
        const body = await res.json();
        const e = body.error;
        detail = typeof e === "string" ? e : e?.message ?? detail;
      } catch {
      }
      throw new AgentError(detail, res.status);
    }
    const json = await res.json();
    const message = json.choices?.[0]?.message;
    if (message === void 0) throw new AgentError("empty completion", 502);
    const h = (name) => res.headers.get(name) ?? void 0;
    const latency = Number(h("x-bitos-latency-ms"));
    const usd = Number(h("x-bitos-usd"));
    if (Number.isFinite(usd)) this.totalUsd += usd;
    this.events.onRoute({
      ...h("x-bitos-lane") !== void 0 ? { lane: h("x-bitos-lane") } : {},
      ...h("x-bitos-provider") !== void 0 ? { provider: h("x-bitos-provider") } : {},
      ...h("x-bitos-model") !== void 0 ? { model: h("x-bitos-model") } : {},
      ...Number.isFinite(latency) ? { latencyMs: latency } : {},
      ...Number.isFinite(usd) ? { usd } : {}
    });
    return message;
  }
};

// ../../packages/agent/src/browser.ts
import path6 from "node:path";
var MAX_PAGE_TEXT = 12e3;
var NAV_TIMEOUT_MS = 3e4;
var ACTION_TIMEOUT_MS = 15e3;
var Session = class {
  constructor(opts) {
    this.opts = opts;
  }
  browser = null;
  page = null;
  args() {
    const args = [
      // /dev/shm is 64 MB in the image; Chromium would rather have the disk.
      "--disable-dev-shm-usage",
      "--hide-scrollbars",
      "--mute-audio",
      `--user-data-dir=${this.opts.profileDir ?? "/work/browser"}`
    ];
    if (this.opts.sandbox === false) args.push("--no-sandbox");
    return args;
  }
  async open() {
    if (this.page !== null) return this.page;
    const specifier = "playwright-core";
    let pw;
    try {
      pw = await import(specifier);
    } catch {
      throw new Error(
        "no browser on this machine: playwright-core is not installed (BitOS OS carries it; a laptop does not)"
      );
    }
    this.browser = await pw.chromium.launch({
      executablePath: this.opts.executablePath,
      args: this.args(),
      timeout: NAV_TIMEOUT_MS
    });
    const page = await this.browser.newPage();
    page.setDefaultTimeout(ACTION_TIMEOUT_MS);
    this.page = page;
    return page;
  }
  /** The page, or an error telling the model to open one first. */
  current() {
    if (this.page === null) throw new Error('no page open \u2014 call browse with action "open" first');
    return this.page;
  }
  async close() {
    const browser = this.browser;
    this.page = null;
    this.browser = null;
    if (browser !== null) await browser.close();
  }
};
function http(url) {
  if (!/^https?:\/\//i.test(url)) throw new Error("url must start with http:// or https://");
  return url;
}
async function where(page, body) {
  const title = await page.title().catch(() => "");
  const head = `${page.url()}${title === "" ? "" : `
${title}`}`;
  return body === void 0 ? head : clip(`${head}

${body}`, MAX_PAGE_TEXT);
}
function browserTool(opts) {
  const session = new Session(opts);
  const read = {
    name: "browse",
    description: 'Read the web with a real browser, so pages that need JavaScript work. Actions: "open" loads a url and returns its text; "read" returns the current page again, or the text of one CSS selector; "close" ends the session. Text is truncated \u2014 ask for a selector when a page is large.',
    parameters: {
      type: "object",
      properties: {
        action: { type: "string", enum: ["open", "read", "close"] },
        url: { type: "string", description: 'For "open": an http(s) url.' },
        selector: { type: "string", description: 'For "read": a CSS selector, default "body".' }
      },
      required: ["action"]
    },
    mutating: false,
    run: async (args, ctx) => {
      const action = str(args["action"], "action");
      if (action === "close") {
        await session.close();
        return "browser closed";
      }
      if (action === "open") {
        const url = http(str(args["url"], "url"));
        const page = await session.open();
        ctx.note(`\u2192 ${url}`);
        await page.goto(url, { timeout: NAV_TIMEOUT_MS, waitUntil: "load" });
        return where(page, await page.innerText("body").catch(() => "(no text on this page)"));
      }
      if (action === "read") {
        const page = session.current();
        const selector = typeof args["selector"] === "string" && args["selector"] !== "" ? args["selector"] : "body";
        return where(page, await page.innerText(selector));
      }
      return `ERROR: unknown action ${action}`;
    }
  };
  const act = {
    name: "browse_act",
    description: 'Act on the page that browse opened: "click" a CSS selector, "type" text into one (optionally pressing Enter), or "screenshot" the page into a file. Acting on a website is an outward action and is confirmed.',
    parameters: {
      type: "object",
      properties: {
        action: { type: "string", enum: ["click", "type", "screenshot"] },
        selector: { type: "string", description: 'CSS selector for "click" and "type".' },
        text: { type: "string", description: 'For "type": the text to enter.' },
        enter: { type: "boolean", description: 'For "type": press Enter afterwards.' },
        path: { type: "string", description: 'For "screenshot": where to write the PNG.' }
      },
      required: ["action"]
    },
    mutating: true,
    run: async (args, ctx) => {
      const action = str(args["action"], "action");
      const page = session.current();
      if (action === "screenshot") {
        const file = resolvePath(ctx.cwd, str(args["path"], "path"));
        if (!await ctx.confirm("browse_act", `screenshot ${page.url()} to ${shown(ctx.cwd, file)}`)) return DENIED;
        await page.screenshot({ path: file, fullPage: true });
        return `wrote a screenshot of ${page.url()} to ${shown(ctx.cwd, file)}`;
      }
      const selector = str(args["selector"], "selector");
      if (action === "click") {
        if (!await ctx.confirm("browse_act", `click ${selector} on ${page.url()}`)) return DENIED;
        await page.click(selector, { timeout: ACTION_TIMEOUT_MS });
        return where(page, await page.innerText("body").catch(() => ""));
      }
      if (action === "type") {
        const text = str(args["text"], "text");
        const enter = args["enter"] === true;
        const summary = `type ${JSON.stringify(text.length > 40 ? `${text.slice(0, 37)}\u2026` : text)} into ${selector} on ${page.url()}${enter ? " and press Enter" : ""}`;
        if (!await ctx.confirm("browse_act", summary)) return DENIED;
        await page.fill(selector, text, { timeout: ACTION_TIMEOUT_MS });
        if (enter) await page.press(selector, "Enter", { timeout: ACTION_TIMEOUT_MS });
        return where(page, await page.innerText("body").catch(() => ""));
      }
      return `ERROR: unknown action ${action}`;
    }
  };
  return [read, act];
}
function browserToolFromEnv(env = process.env) {
  const executablePath = env["BITOS_BROWSER"];
  if (executablePath === void 0 || executablePath === "") return [];
  return browserTool({
    executablePath,
    sandbox: env["BITOS_BROWSER_NO_SANDBOX"] !== "1",
    ...env["BITOS_BROWSER_PROFILE"] !== void 0 ? { profileDir: env["BITOS_BROWSER_PROFILE"] } : { profileDir: path6.join("/work", "browser") }
  });
}

// ../../packages/agent/src/index.ts
function defaultTools(env = process.env) {
  return [...LOCAL_TOOLS, ...browserToolFromEnv(env)];
}

// src/agent.ts
async function authorization(config) {
  if (config.apiKey !== void 0) return config.apiKey;
  if (config.token === void 0) {
    throw new GatewayError("No API key for the agent \u2014 run: bitos login", 401);
  }
  const minted = await postJson(config, "/api/keys", {
    name: `bitos cli on ${os6.hostname()}`
  });
  if (typeof minted.key !== "string") throw new GatewayError("could not mint an API key", 502);
  config.apiKey = minted.key;
  saveConfig(config);
  return minted.key;
}
function transportFor(config) {
  return {
    gateway: config.gateway,
    authorization: () => authorization(config),
    ...config.betaPassword !== void 0 ? { betaPassword: config.betaPassword } : {}
  };
}
var Agent2 = class extends Agent {
  constructor(config, ctx, events, model = "bitos/code") {
    super(transportFor(config), ctx, events, { model, tools: defaultTools() });
  }
  async turn(input) {
    try {
      return await super.turn(input);
    } catch (err) {
      if (err instanceof AgentError) throw new GatewayError(err.message, err.status);
      throw err;
    }
  }
};

// src/ui.ts
var isTTY = process.stdout.isTTY === true;
var esc = (code, s) => isTTY ? `\x1B[${code}m${s}\x1B[0m` : s;
var c = {
  dim: (s) => esc("2", s),
  bold: (s) => esc("1", s),
  amber: (s) => esc("38;5;214", s),
  red: (s) => esc("31", s),
  ink: (s) => esc("38;5;240", s)
};
var MARK = [
  "8.8.777.6.",
  "88877776.5",
  "888.777.6.",
  "8887777666",
  "8.877.7.6.",
  "88777766.5",
  "88777.6.5.",
  "777765.6.5",
  "7.6.5.5.4.",
  "6666.5.4.4"
];
var SHADE = {
  "8": "\u2588\u2588",
  "7": "\u2593\u2593",
  "6": "\u2592\u2592",
  "5": "\u2591\u2591",
  "4": "\u2219 ",
  ".": "  "
};
function renderMark() {
  return MARK.map(
    (row) => [...row].map((ch) => {
      const cell = SHADE[ch] ?? "  ";
      return ch === "5" || ch === "4" ? c.dim(cell) : cell;
    }).join("")
  );
}
function banner(lines) {
  const mark = renderMark();
  const out2 = [];
  const height = Math.max(mark.length, lines.length);
  for (let i = 0; i < height; i++) {
    const left = mark[i] ?? " ".repeat(20);
    const right = lines[i] ?? "";
    out2.push(`  ${left}   ${right}`);
  }
  return out2.join("\n");
}
var WAVE_CELLS = 10;
var WAVE_SHADES = [" ", "\u2591", "\u2592", "\u2593", "\u2588"];
function waveFrame(t) {
  const head = t % (WAVE_CELLS + 4);
  let s = "";
  for (let i = 0; i < WAVE_CELLS; i++) {
    const d = head - i;
    const shade = d < 0 || d > 4 ? 0 : 4 - d;
    const cell = WAVE_SHADES[shade];
    s += shade === 4 ? c.amber(cell) : shade === 0 ? c.dim("\xB7") : c.ink(cell);
  }
  return s;
}
var Live = class {
  route = {};
  label = null;
  timer = null;
  t = 0;
  running = false;
  start() {
    this.route = {};
    this.label = null;
    this.running = true;
    if (!isTTY) return;
    this.stopTimer();
    this.timer = setInterval(() => {
      this.t++;
      this.draw();
    }, 80);
    this.draw();
  }
  /** The model is working: run the wave with this label. */
  think(label) {
    this.label = label;
    if (isTTY) this.draw();
  }
  /** Waiting is over for now (a tool is running, say). */
  rest() {
    this.label = null;
    if (isTTY) this.draw();
  }
  learn(route) {
    this.route = { ...this.route, ...route };
    if (isTTY) {
      this.draw();
      return;
    }
    if (route.lane !== void 0 || route.provider !== void 0) {
      process.stdout.write(`${this.strip(false)}
`);
    }
  }
  /** Print a line above the live line. */
  print(text) {
    if (isTTY && this.running) {
      process.stdout.write(`\r\x1B[2K${text}
`);
      this.draw();
    } else {
      process.stdout.write(`${text}
`);
    }
  }
  /** Stop animating; return the receipt line for the caller to print. */
  finish(extra = "") {
    this.running = false;
    this.label = null;
    this.stopTimer();
    if (isTTY) process.stdout.write("\r\x1B[2K");
    return this.strip(false, extra);
  }
  draw() {
    const wave = this.label === null ? "" : `   ${waveFrame(this.t)} ${c.dim(this.label)}`;
    process.stdout.write(`\r\x1B[2K${this.strip(true)}${wave}`);
  }
  link(index, animate) {
    const pos = this.t % 16;
    const onThis = Math.floor(pos / 4) === index;
    const cell = pos % 4;
    let s = "";
    for (let i = 0; i < 4; i++) {
      s += animate && this.running && onThis && i === cell ? c.amber("\u25AA") : c.dim("\xB7");
    }
    return s;
  }
  strip(animate, extra = "") {
    const r = this.route;
    const stage = (label, placeholder) => label === void 0 ? c.dim(placeholder) : c.bold(label);
    const network = r.provider === void 0 ? void 0 : r.model !== void 0 ? `${r.provider}\xB7${r.model.split("/").pop()}` : r.provider;
    const tail = !this.running && r.latencyMs !== void 0 ? c.dim(`  ${(r.latencyMs / 1e3).toFixed(1)}s${r.usd !== void 0 ? ` \xB7 $${r.usd.toFixed(4)}` : ""}${extra}`) : c.dim(extra);
    return `  ${stage("input", "input")}${this.link(0, animate)}${stage("brain", "brain")}${this.link(1, animate)}${stage(r.lane, "lane")}${this.link(2, animate)}${stage(network, "network")}${this.link(3, animate)}${stage(this.running ? void 0 : "answer", "answer")}${tail}`;
  }
  stopTimer() {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
};
function renderMarkdown(md) {
  const lines = md.replace(/\r\n/g, "\n").split("\n");
  const out2 = [];
  let inFence = false;
  for (const raw of lines) {
    if (raw.trimStart().startsWith("```")) {
      inFence = !inFence;
      out2.push(c.dim(inFence ? "\u250C\u2500\u2500" : "\u2514\u2500\u2500"));
      continue;
    }
    if (inFence) {
      out2.push(`${c.dim("\u2502")} ${raw}`);
      continue;
    }
    let line = raw;
    const heading = /^(#{1,6})\s+(.*)$/.exec(line);
    if (heading !== null) {
      out2.push(c.bold(heading[2]));
      continue;
    }
    line = line.replace(/^(\s*)[-*]\s+/, "$1\u25AA ");
    line = line.replace(/\*\*(.+?)\*\*/g, (_, t) => c.bold(t));
    line = line.replace(/`([^`]+)`/g, (_, t) => c.amber(t));
    out2.push(line);
  }
  return out2.join("\n");
}

// src/repl.ts
var LANES = [
  "auto",
  "chat",
  "code",
  "search",
  "research",
  "data",
  "forecast",
  "weather",
  "image",
  "translate",
  "summarize",
  "extract",
  "verify",
  "files",
  "swe",
  "shopping"
];
var REPL_HELP = `${c.bold("bitos console")} \u2014 an agent in this directory.

  In ${c.bold("agent")} mode (default) the model can read, search, edit and write files
  here and run commands. Reads just happen; writes and commands ask first \u2014
  answer y / n, or a (always, for this session).

  /agent           agent mode (tools, this directory)
  /chat            plain mode: one task per line on the brain, no tools
  /lane <name>     in chat mode, pin the lane (${LANES.join(", ")})
  /yes             stop asking for permission this session
  /new             forget the conversation so far
  /whoami          the signed-in address
  /help            this list
  /exit            leave (Ctrl-D works too)`;
function fmtStep(step) {
  const who = step.model !== void 0 ? `${step.provider} \xB7 ${step.model}` : step.provider;
  const secs = `${(step.latencyMs / 1e3).toFixed(1)}s`;
  if (step.error !== void 0) {
    return c.dim(`  \u2717 ${who} ${secs} ${c.red(step.error.slice(0, 60))}`);
  }
  const cost = step.usdCost > 0 ? ` $${step.usdCost.toFixed(4)}` : "";
  return c.dim(`  \u2713 ${who} ${secs}${cost}`);
}
async function newThread(config) {
  try {
    const res = await postJson(config, "/api/conversations", {});
    return typeof res.id === "string" ? res.id : void 0;
  } catch {
    return void 0;
  }
}
async function runRepl() {
  const config = loadConfig();
  let address = config.address ?? null;
  try {
    const me = await getJson(config, "/api/auth/me");
    address = me.address;
  } catch (err) {
    if (err instanceof GatewayError && err.status === 401) {
      const caps = await getJson(config, "/api/capabilities").catch(
        () => ({ authRequired: false })
      );
      if (config.token !== void 0 || caps.authRequired === true) {
        process.stderr.write(`bitos: ${err.message}
`);
        process.exit(1);
      }
    }
  }
  let lane;
  let mode = "agent";
  let autoYes = process.argv.includes("--yes") || process.env["BITOS_YES"] === "1";
  let threadId = await newThread(config);
  let exiting = false;
  let inputDone = false;
  process.stdout.write(
    `${banner([
      "",
      "",
      `${c.bold("BitOS")}  ${c.dim("one balance \xB7 every web3 intelligence")}`,
      "",
      c.dim(config.gateway),
      c.dim(address !== null ? `${address.slice(0, 8)}\u2026${address.slice(-6)}` : "anonymous \xB7 dev gateway"),
      "",
      c.dim(`agent in ${process.cwd()}`),
      c.dim("/help for commands \xB7 /exit to leave"),
      ""
    ])}

`
  );
  const live = new Live();
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: isTTY });
  const prompt = () => {
    const tag = mode === "chat" ? c.amber(`[chat${lane !== void 0 ? `:${lane}` : ""}] `) : "";
    rl.setPrompt(`${tag}${c.bold("\u203A")} `);
    rl.prompt();
  };
  let pendingAnswer = null;
  const ask = (question) => new Promise((resolve) => {
    pendingAnswer = resolve;
    rl.setPrompt(question);
    rl.prompt();
  });
  const agentCtx = {
    cwd: process.cwd(),
    confirm: async (tool, summary) => {
      if (autoYes) return true;
      live.rest();
      live.print(`
${c.amber("?")} ${c.bold(tool)}  ${summary}`);
      const answer = (await ask(`${c.dim("allow? [y/n/a]")} `)).trim().toLowerCase();
      if (answer === "a" || answer === "always") {
        autoYes = true;
        return true;
      }
      return answer === "y" || answer === "yes";
    },
    note: (line) => live.print(c.dim(`  ${line}`))
  };
  const agent = new Agent2(config, agentCtx, {
    onThinking: () => live.think("thinking"),
    onRoute: (route) => live.learn(route),
    onToolStart: (name, args) => {
      live.rest();
      const brief = Object.entries(args).map(([k, v]) => `${k}=${typeof v === "string" ? JSON.stringify(v.length > 60 ? `${v.slice(0, 57)}\u2026` : v) : String(v)}`).join(" ");
      live.print(`  ${c.amber("\u25AA")} ${c.bold(name)} ${c.dim(brief)}`);
    },
    onToolEnd: (_name, result, ms) => {
      const first = result.split("\n")[0].slice(0, 90);
      live.print(c.dim(`    \u21B3 ${first}${result.includes("\n") ? " \u2026" : ""} (${ms}ms)`));
    }
  });
  const handle = async (line) => {
    const text = line.trim();
    if (text === "") return;
    if (text.startsWith("/")) {
      const [cmd, ...rest] = text.slice(1).split(/\s+/);
      switch (cmd) {
        case "exit":
        case "quit":
        case "q":
          exiting = true;
          return;
        case "help":
          process.stdout.write(`${REPL_HELP}
`);
          return;
        case "lane": {
          const want = rest[0];
          if (want === void 0 || !LANES.includes(want)) {
            process.stdout.write(`${c.red("usage:")} /lane <${LANES.join("|")}>
`);
            return;
          }
          lane = want === "auto" ? void 0 : want;
          process.stdout.write(c.dim(`lane: ${want}
`));
          return;
        }
        case "new":
          threadId = await newThread(config);
          agent.reset();
          process.stdout.write(c.dim("new thread\n"));
          return;
        case "agent":
          mode = "agent";
          process.stdout.write(c.dim(`agent mode in ${process.cwd()}
`));
          return;
        case "chat":
          mode = "chat";
          process.stdout.write(c.dim("chat mode: tasks on the brain, no tools\n"));
          return;
        case "yes":
          autoYes = true;
          process.stdout.write(c.dim("permission prompts off for this session\n"));
          return;
        case "whoami":
          process.stdout.write(`${address ?? "(anonymous \u2014 dev gateway)"}
`);
          return;
        default:
          process.stdout.write(`${c.red("unknown command")} /${cmd} \u2014 /help
`);
          return;
      }
    }
    const started = Date.now();
    let steps = 0;
    if (mode === "agent") {
      live.start();
      try {
        const done = await agent.turn(text);
        const receipt = live.finish(
          ` \xB7 ${done.rounds} round${done.rounds > 1 ? "s" : ""}${done.toolCalls > 0 ? ` \xB7 ${done.toolCalls} tool call${done.toolCalls > 1 ? "s" : ""}` : ""} \xB7 ${((Date.now() - started) / 1e3).toFixed(1)}s total`
        );
        process.stdout.write(`
${renderMarkdown(done.text)}

${receipt}

`);
      } catch (err) {
        live.finish();
        process.stdout.write(`${c.red("\u2717")} ${err instanceof Error ? err.message : String(err)}

`);
      }
      return;
    }
    live.start();
    live.think("routing");
    try {
      const outcome = await streamTask(
        config,
        {
          input: text,
          ...lane !== void 0 ? { category: lane } : {},
          ...threadId !== void 0 ? { conversationId: threadId } : {}
        },
        {
          onAccepted: (info) => {
            if (info.category !== void 0) {
              live.learn({ lane: info.category });
              live.think(`${info.category} lane`);
            }
          },
          onStep: (step) => {
            steps++;
            live.learn({ provider: step.provider, ...step.model !== void 0 ? { model: step.model } : {} });
            live.print(fmtStep(step));
            live.think("thinking");
          }
        }
      );
      if (outcome.status === "error") {
        live.finish();
        process.stdout.write(`${c.red("\u2717")} ${outcome.error ?? "task failed"}

`);
        return;
      }
      live.learn({
        ...outcome.category !== void 0 ? { lane: outcome.category } : {},
        ...typeof outcome.latencyMs === "number" ? { latencyMs: outcome.latencyMs } : {},
        ...typeof outcome.usdCost === "number" ? { usd: outcome.usdCost } : {}
      });
      const receipt = live.finish(
        ` \xB7 ${outcome.agentId ?? "?"}${steps > 0 ? ` \xB7 ${steps} step${steps > 1 ? "s" : ""}` : ""}`
      );
      process.stdout.write(`
${renderMarkdown(outcome.output ?? "")}

${receipt}

`);
    } catch (err) {
      live.finish();
      process.stdout.write(`${c.red("\u2717")} ${err instanceof Error ? err.message : String(err)}

`);
    }
  };
  const queue = [];
  const leave = () => {
    process.stdout.write(`${c.dim("\nbye")}
`);
    process.exit(0);
  };
  let pumping = false;
  const pump = async () => {
    if (pumping) return;
    pumping = true;
    try {
      while (queue.length > 0 && !exiting) {
        await handle(queue.shift());
        if (!exiting && !inputDone) prompt();
      }
    } finally {
      pumping = false;
    }
    if (exiting || inputDone) leave();
  };
  rl.on("line", (line) => {
    if (pendingAnswer !== null) {
      const resolve = pendingAnswer;
      pendingAnswer = null;
      resolve(line);
      return;
    }
    queue.push(line);
    void pump();
  });
  rl.on("close", () => {
    inputDone = true;
    if (!pumping && queue.length === 0) leave();
  });
  prompt();
}

// src/main.ts
var HELP = `bitos \u2014 every web3 intelligence, one command away.

USAGE
  bitos                     Open the console: a conversation in your terminal
  bitos <command> [args]

COMMANDS
  login [--gateway <url>]   Sign in with your wallet, via your browser
  logout                    Drop the saved session
  whoami                    The signed-in address and balance
  ask <question\u2026>           Run a task; --lane <category> pins the lane
  files ls                  Your folder: uploads and memory notes
  files put <path\u2026>         Upload files or whole folders
  files get <name|id> [-o <path>]   Download one file
  files cat <name|id>       Print a text file (a memory note, say)
  files rm <name|id>        Delete one file
  status                    Gateway health at a glance
  config set <k> <v>        Set gateway | beta-password
  install                   Put bitos on your PATH (~/.local/bin) so it runs anywhere
  update                    Replace this binary with the gateway's latest (npm installs: npm i -g bitos@latest)
  version                   Print the version

Files live in your account's folder on the gateway, mirrored to the
storage network (Hippius, SN75). Memory notes \u2014 one Markdown file per
conversation \u2014 are written there as you talk.`;
function out(line) {
  process.stdout.write(`${line}
`);
}
function fatal(message) {
  process.stderr.write(`bitos: ${message}
`);
  process.exit(1);
}
function openBrowser(url) {
  const cmd = process.platform === "darwin" ? "open" : process.platform === "win32" ? "start" : "xdg-open";
  try {
    spawn(cmd, [url], { stdio: "ignore", detached: true }).unref();
  } catch {
  }
}
var sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function cmdLogin(args) {
  const config = loadConfig();
  const gwFlag = args.indexOf("--gateway");
  if (gwFlag !== -1) {
    const url2 = args[gwFlag + 1] ?? fatal("--gateway needs a URL");
    config.gateway = url2.replace(/\/$/, "");
  }
  const { code, expiresInMs } = await postJson(
    config,
    "/api/auth/handoff/start",
    {}
  );
  const url = `${config.gateway}/connect?code=${code}`;
  out("Opening your browser to sign in with your wallet\u2026");
  out(`If nothing opens, visit:
  ${url}`);
  openBrowser(url);
  const deadline = Date.now() + expiresInMs;
  while (Date.now() < deadline) {
    await sleep(1500);
    const poll = await getJson(
      config,
      `/api/auth/handoff/${code}`
    );
    if (!poll.pending && poll.token !== void 0 && poll.address !== void 0) {
      config.token = poll.token;
      config.address = poll.address;
      try {
        const minted = await postJson(config, "/api/keys", {
          name: `bitos cli on ${os7.hostname()}`
        });
        if (typeof minted.key === "string") config.apiKey = minted.key;
      } catch {
      }
      saveConfig(config);
      out(`Signed in as ${poll.address}`);
      return;
    }
  }
  fatal("sign-in timed out \u2014 run bitos login again");
}
async function cmdWhoami() {
  const config = loadConfig();
  const me = await getJson(config, "/api/auth/me");
  out(`address  ${me.address}`);
  try {
    const credits = await getJson(config, "/api/credits");
    const usd = typeof credits.usd === "number" ? credits.usd : void 0;
    if (usd !== void 0) out(`credits  $${usd.toFixed(2)}`);
  } catch {
  }
  out(`gateway  ${config.gateway}`);
}
async function cmdAsk(args) {
  const config = loadConfig();
  let category;
  const laneFlag = args.indexOf("--lane");
  if (laneFlag !== -1) {
    category = args[laneFlag + 1] ?? fatal("--lane needs a category");
    args.splice(laneFlag, 2);
  }
  const input = args.join(" ").trim();
  if (input === "") fatal('nothing to ask \u2014 usage: bitos ask "your question"');
  const task = await streamTask(
    config,
    { input, ...category !== void 0 ? { category } : {} },
    {
      onStep: (step) => {
        const who = step.model !== void 0 ? `${step.provider} \xB7 ${step.model}` : step.provider;
        process.stderr.write(
          step.error !== void 0 ? `  \u2717 ${who} ${(step.latencyMs / 1e3).toFixed(1)}s ${step.error.slice(0, 60)}
` : `  \u2713 ${who} ${(step.latencyMs / 1e3).toFixed(1)}s
`
        );
      }
    }
  );
  if (task.status === "error") fatal(task.error ?? "task failed");
  out(process.stdout.isTTY ? renderMarkdown(task.output ?? "") : task.output ?? "");
  const cost = typeof task.usdCost === "number" ? `$${task.usdCost.toFixed(4)}` : "?";
  const ms = typeof task.latencyMs === "number" ? `${(task.latencyMs / 1e3).toFixed(1)}s` : "?";
  process.stderr.write(`
[${task.category ?? "?"} \xB7 ${task.agentId ?? "?"} \xB7 ${cost} \xB7 ${ms}]
`);
}
var fullPath = (r) => r.dir === "" ? r.name : `${r.dir}/${r.name}`;
async function listRows(config) {
  const { files } = await getJson(config, "/api/files");
  return files;
}
function pickRow(rows, ref) {
  const hit = rows.find((r) => r.id === ref) ?? rows.find((r) => fullPath(r) === ref) ?? rows.find((r) => r.name === ref);
  if (hit === void 0) fatal(`no file named '${ref}' \u2014 see: bitos files ls`);
  return hit;
}
function fmtBytes(n) {
  if (n < 1024) return `${n}B`;
  return n < 1024 * 1024 ? `${(n / 1024).toFixed(1)}K` : `${(n / (1024 * 1024)).toFixed(1)}M`;
}
function walk(root, base) {
  const stat = fs6.statSync(root);
  if (stat.isFile()) return [{ abs: root, dir: base, name: path7.basename(root) }];
  if (!stat.isDirectory()) return [];
  const folder = base === "" ? path7.basename(root) : `${base}/${path7.basename(root)}`;
  const found = [];
  for (const entry of fs6.readdirSync(root)) {
    if (entry.startsWith(".")) continue;
    found.push(...walk(path7.join(root, entry), folder));
  }
  return found;
}
async function cmdFiles(args) {
  const config = loadConfig();
  const sub = args[0] ?? "ls";
  if (sub === "ls") {
    const rows = await listRows(config);
    if (rows.length === 0) {
      out("(empty \u2014 add something, or talk: memory notes land here)");
      return;
    }
    for (const r of rows) {
      const tag = r.kind === "anchor" ? "note " : "file ";
      out(`${fmtBytes(r.sizeBytes).padStart(7)}  ${r.mirrored ? "sn75 " : "local"}  ${tag} ${fullPath(r)}`);
    }
    return;
  }
  if (sub === "put") {
    const targets = args.slice(1);
    if (targets.length === 0) fatal("usage: bitos files put <file-or-folder\u2026>");
    for (const target of targets) {
      if (!fs6.existsSync(target)) fatal(`no such path: ${target}`);
      for (const f of walk(path7.resolve(target), "")) {
        const bytes = fs6.readFileSync(f.abs);
        await postBytes(config, "/api/files", new Uint8Array(bytes), {
          "x-file-name": encodeURIComponent(f.name),
          ...f.dir !== "" ? { "x-file-dir": encodeURIComponent(f.dir) } : {}
        });
        out(`\u2191 ${f.dir === "" ? f.name : `${f.dir}/${f.name}`} (${fmtBytes(bytes.length)})`);
      }
    }
    return;
  }
  if (sub === "get" || sub === "cat") {
    const ref = args[1] ?? fatal(`usage: bitos files ${sub} <name|id>${sub === "get" ? " [-o <path>]" : ""}`);
    const row = pickRow(await listRows(config), ref);
    const bytes = await getBytes(config, `/api/files/${row.id}`);
    if (sub === "cat") {
      process.stdout.write(Buffer.from(bytes).toString("utf8"));
      return;
    }
    const oFlag = args.indexOf("-o");
    const dest = oFlag !== -1 ? args[oFlag + 1] ?? fatal("-o needs a path") : row.name;
    fs6.writeFileSync(dest, bytes);
    out(`\u2193 ${dest} (${fmtBytes(bytes.length)})`);
    return;
  }
  if (sub === "rm") {
    const ref = args[1] ?? fatal("usage: bitos files rm <name|id>");
    const row = pickRow(await listRows(config), ref);
    await del(config, `/api/files/${row.id}`);
    out(`deleted ${fullPath(row)}`);
    return;
  }
  fatal(`unknown files command '${sub}' \u2014 ls | put | get | cat | rm`);
}
async function cmdStatus() {
  const config = loadConfig();
  const health = await getJson(
    config,
    "/api/health"
  );
  const up = Object.entries(health.providers).filter(([, h]) => h.ok);
  out(`gateway    ${config.gateway} (${health.ok ? "ok" : "DOWN"})`);
  out(`providers  ${up.length}/${Object.keys(health.providers).length} healthy`);
  out(`           ${up.map(([id]) => id).join(", ")}`);
}
function cmdConfig(args) {
  const [action, k, v] = args;
  if (action !== "set" || k === void 0 || v === void 0) {
    fatal("usage: bitos config set gateway|beta-password <value>");
  }
  const config = loadConfig();
  if (k === "gateway") config.gateway = v.replace(/\/$/, "");
  else if (k === "beta-password") config.betaPassword = v;
  else fatal(`unknown config key '${k}'`);
  saveConfig(config);
  out("saved");
}
function installedWithNpm(self) {
  let real = self;
  try {
    real = fs6.realpathSync(self);
  } catch {
  }
  return real.split(path7.sep).includes("node_modules");
}
function cmdInstall(args) {
  const self = process.argv[1] ?? fatal("cannot locate this binary");
  if (installedWithNpm(self)) {
    out("bitos is installed through npm and already on your PATH; nothing to do.");
    return;
  }
  const code = fs6.readFileSync(self, "utf8");
  if (!code.startsWith("#!/usr/bin/env node")) fatal("run install from the downloaded bitos script");
  const explicit = args.indexOf("--dir");
  const candidates = explicit !== -1 ? [args[explicit + 1] ?? fatal("--dir needs a path")] : ["/usr/local/bin", path7.join(os7.homedir(), ".local", "bin")];
  for (const dir of candidates) {
    try {
      fs6.mkdirSync(dir, { recursive: true });
      fs6.accessSync(dir, fs6.constants.W_OK);
    } catch {
      continue;
    }
    const dest = path7.join(dir, "bitos");
    fs6.writeFileSync(dest, code, { mode: 493 });
    out(`installed ${dest}`);
    const onPath = (process.env["PATH"] ?? "").split(path7.delimiter).includes(dir);
    if (!onPath) {
      const rc = (process.env["SHELL"] ?? "").endsWith("zsh") ? "~/.zshrc" : "~/.bashrc";
      out(`${dir} is not on your PATH yet \u2014 add this line to ${rc}, then open a new terminal:`);
      out(`  export PATH="${dir}:$PATH"`);
    } else {
      out("open a new terminal and run: bitos");
    }
    return;
  }
  fatal("no writable install folder found \u2014 try: bitos install --dir <folder-on-your-PATH>");
}
async function cmdUpdate() {
  const self = process.argv[1] ?? fatal("cannot locate this binary");
  if (installedWithNpm(self)) {
    out("bitos is installed through npm \u2014 update it the npm way:");
    out("  npm i -g bitos@latest");
    return;
  }
  const config = loadConfig();
  const res = await fetch(`${config.gateway}/download/bitos.mjs`, {
    headers: config.betaPassword !== void 0 ? { "x-beta-password": config.betaPassword } : {}
  });
  if (!res.ok) fatal(`the gateway has no CLI build to offer (HTTP ${res.status})`);
  const code = await res.text();
  if (!code.startsWith("#!/usr/bin/env node")) fatal("downloaded file does not look like bitos");
  fs6.writeFileSync(self, code, { mode: 493 });
  out(`updated ${self}`);
}
async function main() {
  const major = Number(process.versions.node.split(".")[0]);
  if (major < 20) fatal(`Node ${process.versions.node} is too old \u2014 bitos needs Node 20 or newer`);
  const argv = process.argv.slice(2).filter((a) => a !== "--yes");
  const [cmd, ...args] = argv;
  try {
    switch (cmd) {
      case void 0:
      case "chat":
      case "console":
        return await runRepl();
      case "version":
      case "--version":
      case "-v":
        return out(`bitos ${"0.1.0"}`);
      case "install":
        return cmdInstall(args);
      case "update":
        return await cmdUpdate();
      case "login":
        return await cmdLogin(args);
      case "logout": {
        const config = loadConfig();
        delete config.token;
        delete config.address;
        delete config.apiKey;
        saveConfig(config);
        return out("signed out");
      }
      case "whoami":
        return await cmdWhoami();
      case "ask":
        return await cmdAsk(args);
      case "files":
        return await cmdFiles(args);
      case "status":
        return await cmdStatus();
      case "config":
        return cmdConfig(args);
      case "help":
      case "--help":
      case "-h":
        return out(HELP);
      default:
        fatal(`unknown command '${cmd}' \u2014 run: bitos help`);
    }
  } catch (err) {
    if (err instanceof GatewayError) fatal(err.message);
    throw err;
  }
}
void main();
