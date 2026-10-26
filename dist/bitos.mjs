#!/usr/bin/env node

// src/main.ts
import fs11 from "node:fs";
import os9 from "node:os";
import path13 from "node:path";
import { spawn } from "node:child_process";

// src/config.ts
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
var AUTO_MODEL = "bitos/code";
function isModelId(value) {
  return /^bitos\/(auto|chat|code)$/.test(value) || /^[a-z0-9][a-z0-9-]*::.+$/.test(value);
}
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
      ...typeof raw.betaPassword === "string" ? { betaPassword: raw.betaPassword } : {},
      ...typeof raw.model === "string" && isModelId(raw.model) ? { model: raw.model } : {}
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
async function getJson(config, path14) {
  const res = await fetch(`${config.gateway}${path14}`, { headers: headers(config) });
  if (!res.ok) await fail(res);
  return await res.json();
}
async function postJson(config, path14, body) {
  const res = await fetch(`${config.gateway}${path14}`, {
    method: "POST",
    headers: headers(config, { "content-type": "application/json" }),
    body: JSON.stringify(body)
  });
  if (!res.ok) await fail(res);
  return await res.json();
}
async function postBytes(config, path14, bytes, extra) {
  const res = await fetch(`${config.gateway}${path14}`, {
    method: "POST",
    headers: headers(config, { "content-type": "application/octet-stream", ...extra }),
    body: bytes
  });
  if (!res.ok) await fail(res);
  return await res.json();
}
async function getBytes(config, path14) {
  const res = await fetch(`${config.gateway}${path14}`, { headers: headers(config) });
  if (!res.ok) await fail(res);
  return new Uint8Array(await res.arrayBuffer());
}
async function del(config, path14) {
  const res = await fetch(`${config.gateway}${path14}`, {
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
import { execFile as execFile2 } from "node:child_process";
import fs10 from "node:fs";
import path12 from "node:path";
import readline from "node:readline";

// ../../packages/agent/src/tools.ts
import fs2 from "node:fs";
import os2 from "node:os";
import path2 from "node:path";
import { execFile } from "node:child_process";
var MAX_TOOL_OUTPUT = 12e3;
var MAX_FILE_READ = 6e4;
var COMMAND_TIMEOUT_MS = 12e4;
var DENIED = "DENIED by the user. Do not retry; ask what they want instead.";
function denied(verdict) {
  if (typeof verdict === "object" && verdict.reason.trim() !== "") {
    return `DENIED by the user, who said: "${verdict.reason.trim()}". Do not retry the same thing; follow what they said, or ask.`;
  }
  return DENIED;
}
function clip(text2, max = MAX_TOOL_OUTPUT) {
  if (text2.length <= max) return text2;
  const head = text2.slice(0, max);
  return `${head}
\u2026[${text2.length - max} more characters truncated]`;
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
var DIFF_PREVIEW_LINES = 12;
function diffPreview(before, after, maxLines = DIFF_PREVIEW_LINES) {
  const side = (text2, sign) => {
    const lines = text2.split("\n");
    const kept = lines.slice(0, maxLines).map((l) => `${sign} ${l}`);
    if (lines.length > maxLines) kept.push(`${sign} \u2026 ${lines.length - maxLines} more line${lines.length - maxLines === 1 ? "" : "s"}`);
    return kept;
  };
  return [...side(before, "-"), ...side(after, "+")].join("\n");
}
var SECRET_NAME = /KEY|SECRET|TOKEN|PASSWORD|PASSWD|PRIVATE|CREDENTIAL/i;
var SECRET_VALUE_CHARS = 16;
function scrubSecrets(text2) {
  return text2.split("\n").map((line) => {
    const assignment = /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["']?(\S+?)["']?\s*$/.exec(line);
    if (assignment !== null && SECRET_NAME.test(assignment[1]) && assignment[2].length >= SECRET_VALUE_CHARS) {
      return `${assignment[1]}=<redacted: looks like a secret>`;
    }
    const hit = /^([^:\s]+):\d*:?/.exec(line);
    if (hit !== null && isSecretFile(hit[1])) return "<redacted: a line from a secrets file>";
    return line;
  }).join("\n");
}
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
      const text2 = fs2.readFileSync(file, "utf8");
      const numbered = text2.split("\n").map((l, i) => `${String(i + 1).padStart(4)}  ${l}`).join("\n");
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
          let text2;
          try {
            text2 = fs2.readFileSync(full, "utf8");
          } catch {
            continue;
          }
          const lines = text2.split("\n");
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
      const verdict = await ctx.confirm(
        "write_file",
        `${existed ? "overwrite" : "create"} ${shown(ctx.cwd, file)} (${content.length} chars)`,
        { kind: "file", path: file, action: existed ? "overwrite" : "create", preview: content.split("\n").slice(0, DIFF_PREVIEW_LINES).join("\n") }
      );
      if (verdict !== true) return denied(verdict);
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
      const text2 = fs2.readFileSync(file, "utf8");
      const count = text2.split(before).length - 1;
      if (count === 0) return "ERROR: `old` not found in the file \u2014 re-read it and copy the text exactly.";
      if (count > 1) return `ERROR: \`old\` occurs ${count} times \u2014 include more surrounding lines to make it unique.`;
      const preview = `- ${before.split("\n")[0].slice(0, 70)}
+ ${after.split("\n")[0].slice(0, 70)}`;
      const verdict = await ctx.confirm("edit_file", `${shown(ctx.cwd, file)}
${preview}`, {
        kind: "file",
        path: file,
        action: "edit",
        diff: diffPreview(before, after)
      });
      if (verdict !== true) return denied(verdict);
      fs2.writeFileSync(file, text2.replace(before, after));
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
      const verdict = await ctx.confirm("run_command", label, { kind: "command", command, cwd: where2 });
      if (verdict !== true) return denied(verdict);
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
            resolve(clip(scrubSecrets(`${body.trim()}
[exit ${code}]`)));
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
  let text2;
  try {
    text2 = fs3.readFileSync(file, "utf8");
  } catch {
    return null;
  }
  if (text2.trim() === "") return null;
  const truncated = text2.length > MAX_RULES_CHARS;
  return { path: file, text: truncated ? `${text2.slice(0, MAX_RULES_CHARS)}
[cut here \u2014 the file continues]` : text2, truncated };
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
var LISTED_DESCRIPTION = 120;
var LISTED_PERSONAL = 40;
var SKILL_LIST = "list";
function personalSkillDirs(home = os4.homedir(), env = process.env) {
  const config = env["XDG_CONFIG_HOME"] ?? path4.join(home, ".config");
  return [path4.join(config, "bitos", "skills"), path4.join(home, ".claude", "skills"), path4.join(home, ".agents", "skills")];
}
function parseSkillHeader(text2) {
  const m = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/.exec(text2);
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
function readDir(dir, scope) {
  let names;
  try {
    names = fs4.readdirSync(dir);
  } catch {
    return [];
  }
  const out2 = [];
  for (const name of names.sort()) {
    const file = path4.join(dir, name, SKILL_FILE);
    let text2;
    try {
      text2 = fs4.readFileSync(file, "utf8");
    } catch {
      continue;
    }
    const header = parseSkillHeader(text2);
    if (header !== null && header.name === name) out2.push({ name, description: header.description.replace(/\s+/g, " "), file, scope });
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
    for (const sub of PROJECT_SKILL_DIRS) take(readDir(path4.join(dir, sub), "project"));
    const atRepoRoot = fs4.existsSync(path4.join(dir, ".git"));
    const parent = path4.dirname(dir);
    if (atRepoRoot || parent === dir) break;
    dir = parent;
  }
  for (const d of personalSkillDirs(home, env)) take(readDir(d, "personal"));
  return [...seen.values()].sort((a, b) => a.name.localeCompare(b.name));
}
var clipDescription = (s) => s.length > LISTED_DESCRIPTION ? `${s.slice(0, LISTED_DESCRIPTION - 1)}\u2026` : s;
function skillListing(skills, full = false) {
  const project = skills.filter((s) => s.scope === "project");
  const personal = skills.filter((s) => s.scope === "personal");
  const lines = project.map((s) => `- ${s.name}: ${clipDescription(s.description)}`);
  if (personal.length > 0) {
    if (full) lines.push(...personal.map((s) => `- ${s.name}: ${clipDescription(s.description)}`));
    else {
      const named = personal.slice(0, LISTED_PERSONAL).map((s) => s.name).join(", ");
      const more = personal.length > LISTED_PERSONAL ? ` \u2026 and ${personal.length - LISTED_PERSONAL} more` : "";
      lines.push(`Personal skills, by name (load "${SKILL_LIST}" for their descriptions): ${named}${more}`);
    }
  }
  return lines.join("\n");
}
function skillTool(skills) {
  if (skills.length === 0) return null;
  return {
    name: "skill",
    description: `Load one of the skills available here \u2014 a method or checklist written for this kind of task. Call it before starting work the skill covers; its text comes back for you to follow.

Available skills:
${skillListing(skills)}`,
    parameters: {
      type: "object",
      properties: { name: { type: "string", description: `The skill to load, from the list; "${SKILL_LIST}" shows every skill with its description.` } },
      required: ["name"]
    },
    mutating: false,
    run: async (args) => {
      const name = typeof args["name"] === "string" ? args["name"].trim() : "";
      if (name === SKILL_LIST) return clip(`Skills available here:
${skillListing(skills, true)}`, MAX_SKILL_CHARS);
      const found = skills.find((s) => s.name === name);
      if (found === void 0) return `No skill named '${name}'. Available: ${skills.map((s) => s.name).join(", ")}`;
      const text2 = fs4.readFileSync(found.file, "utf8");
      const body = text2.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n?/, "").trim();
      return clip(`Skill '${found.name}' (${found.file}):

${body}`, MAX_SKILL_CHARS);
    }
  };
}

// ../../packages/agent/src/loop.ts
var MAX_ROUNDS = 25;
var CONTEXT_LIMIT = 16e4;
var AUTO_COMPACT_CHARS = 15e4;
var COMPACT_INPUT_CHARS = 12e4;
var PLAN_MODE_PROMPT = "PLAN MODE: you may only read, search and list; the tools that change files or run commands are withheld. Investigate, then answer with a plan: the files to touch, the changes in order, the checks to run, and the open questions if any. Do not pretend to have made a change.";
var COMPACT_PROMPT = "You compact a coding session for the agent that will continue it. Write a dense summary, in the language of the conversation, of: the user's goal and constraints; what was done \u2014 files created or changed (paths), commands run and what they showed, decisions taken; what is still open, or was asked and not answered yet; facts learned about the project worth keeping (paths, names, conventions). Keep exact paths, names and values; drop tool output that no longer matters. No preamble.";
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
var SNAPSHOT_ENTRIES = 40;
function gitBranch(cwd) {
  try {
    const head = fs5.readFileSync(path5.join(cwd, ".git", "HEAD"), "utf8").trim();
    const m = /^ref: refs\/heads\/(.+)$/.exec(head);
    return m === null ? head.slice(0, 12) : m[1];
  } catch {
    return null;
  }
}
function projectSnapshot(cwd) {
  let entries = [];
  try {
    entries = fs5.readdirSync(cwd, { withFileTypes: true }).filter((e) => e.name !== "node_modules" && e.name !== ".git").map((e) => e.isDirectory() ? `${e.name}/` : e.name).sort();
  } catch {
    return "";
  }
  const branch = gitBranch(cwd);
  const shown2 = entries.slice(0, SNAPSHOT_ENTRIES).join("  ");
  const more = entries.length > SNAPSHOT_ENTRIES ? ` \u2026 and ${entries.length - SNAPSHOT_ENTRIES} more` : "";
  return `${branch === null ? "Not a git checkout." : `Git branch: ${branch}.`} Top level: ${shown2 === "" ? "(empty)" : shown2}${more}`;
}
function defaultSystemPrompt(cwd) {
  return [
    "You are bitos, a coding agent running in the user's terminal, with inference from decentralized web3 networks.",
    `Working directory: ${cwd}. Home: ${os5.homedir()}. Platform: ${os5.platform()} ${os5.arch()}.`,
    projectSnapshot(cwd),
    "You have tools to read, search, edit and write files and to run shell commands ANYWHERE on this",
    "machine: relative paths hang off the working directory, absolute paths and ~ reach the rest.",
    "Prefer the working directory unless the user points elsewhere or the task clearly lives elsewhere.",
    "Act, do not narrate: when the request is about this project, look first (list_dir, read_file,",
    "search) and then do the work; when asked to build or fix something, state the plan in one line,",
    "make the edits with edit_file or write_file, run the relevant checks with run_command (tests,",
    "typecheck, git diff) and report what you actually did, with file paths. Ask a question only",
    "when the request is genuinely ambiguous, and then ask one precise question, not a menu.",
    "Never invent file contents or command output \u2014 call the tool.",
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
  let waited = 0;
  const timed = {
    ...ctx,
    confirm: async (name, summary, detail) => {
      const asked = Date.now();
      try {
        return await ctx.confirm(name, summary, detail);
      } finally {
        waited += Date.now() - asked;
      }
    }
  };
  const started = Date.now();
  const elapsed = () => Date.now() - started - waited;
  try {
    const result = await tool.run(args, timed);
    events.onToolEnd(tool.name, result, elapsed());
    return result;
  } catch (err) {
    const msg = `ERROR: ${err instanceof Error ? err.message : String(err)}`;
    events.onToolEnd(tool.name, msg, elapsed());
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
    this.lane = options.lane;
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
  lane;
  maxRounds;
  /** Cost across the session, from the gateway's usage (estimates when absent). */
  totalUsd = 0;
  /** Tokens across the session, as the gateway counted them. */
  totalTokens = { input: 0, output: 0 };
  /** The tools the model is offered, the skill loader included when there are skills. */
  get toolNames() {
    return this.tools.map((t) => t.name);
  }
  /** The model on the wire: a routed `bitos/*` id or a pinned `provider::model`. */
  get currentModel() {
    return this.model;
  }
  /** Switch models mid-session; the conversation carries on. */
  setModel(model) {
    this.model = model;
  }
  /** Plan mode: the model is offered the read-only tools only and told to plan. */
  plan = false;
  get planMode() {
    return this.plan;
  }
  setPlanMode(on) {
    this.plan = on;
  }
  /**
   * Put something on the record without a model call — a command the user
   * ran themselves and what it printed, say. Lands as a user turn and a
   * short acknowledgement, so the next completion sees it in order.
   */
  addExchange(user, assistant = "Noted.") {
    this.messages.push({ role: "user", content: user }, { role: "assistant", content: assistant });
  }
  /** Characters of conversation after the system prompt — what compaction goes by. */
  historyChars() {
    let n = 0;
    for (const m of this.messages.slice(1)) {
      n += (m.content ?? "").length;
      for (const call of m.tool_calls ?? []) n += call.function.name.length + call.function.arguments.length;
    }
    return n;
  }
  /**
   * Replace the conversation with a summary of it, written by the model:
   * what was asked, what was done (files, commands, results), what is
   * still open. The next turn starts from the summary and the session
   * file keeps it the same way. Null when there is nothing to compact.
   */
  async compact(signal, focus) {
    if (this.messages.length < 3) return null;
    const before = this.historyChars();
    const transcript = this.messages.slice(1).map((m) => {
      const calls = (m.tool_calls ?? []).map((c2) => `[called ${c2.function.name} ${c2.function.arguments.slice(0, 200)}]`).join(" ");
      const body = m.role === "tool" ? clip(m.content ?? "", 600) : m.content ?? "";
      return `${m.role}: ${body}${calls === "" ? "" : ` ${calls}`}`;
    }).join("\n\n");
    const reply = await this.request(
      [
        { role: "system", content: focus === void 0 || focus.trim() === "" ? COMPACT_PROMPT : `${COMPACT_PROMPT} Give most room to: ${focus.trim()}.` },
        { role: "user", content: clip(transcript, COMPACT_INPUT_CHARS) }
      ],
      [],
      signal
    );
    const summary = (reply.content ?? "").trim();
    if (summary === "") throw new AgentError("the summary came back empty", 502);
    this.messages.length = 1;
    this.messages.push(
      { role: "user", content: `[The conversation so far, compacted]
${summary}` },
      { role: "assistant", content: "Understood \u2014 continuing from this summary." }
    );
    this.recent = [];
    const after = this.historyChars();
    this.events.onCompact?.({ before, after });
    return { before, after };
  }
  reset() {
    this.messages.length = 1;
    this.recent = [];
  }
  /** The conversation so far, without the system prompt: what a session file keeps. */
  history() {
    return structuredClone(this.messages.slice(1));
  }
  /** Continue an earlier conversation: its messages after this session's own system prompt. */
  restore(history) {
    this.messages.length = 1;
    for (const m of history) {
      if (m.role === "system") continue;
      this.messages.push(structuredClone(m));
    }
    this.recent = [];
  }
  /** The turn in flight, so it can be cut short. */
  aborter = null;
  /**
   * Stop the running turn: the completion in flight is abandoned and the
   * turn's messages are dropped, so the conversation stays consistent. A
   * tool already running (a shell command, say) finishes on its own.
   */
  cancel() {
    if (this.aborter === null) return false;
    this.aborter.abort();
    return true;
  }
  /** Signatures of the last calls, for the repeat guard. */
  recent = [];
  /**
   * A model that issues the same call three times running is stuck; the
   * third time the user is asked, and a no goes back as a refusal.
   */
  async guarded(call) {
    const signature = `${call.function.name}(${call.function.arguments})`;
    const repeats = this.recent.filter((s) => s === signature).length;
    this.recent = [...this.recent.slice(-(REPEAT_LIMIT - 2)), signature];
    if (repeats >= REPEAT_LIMIT - 1) {
      const allowed = await this.ctx.confirm("repeat", `${call.function.name} for the ${repeats + 1}rd time with the same input`);
      if (allowed !== true) return REPEAT_REFUSED;
    }
    return runTool(this.tools, call, this.ctx, this.events);
  }
  /** One user turn: loops through tool calls until the model answers in prose. */
  async turn(input) {
    if (this.historyChars() > AUTO_COMPACT_CHARS) {
      try {
        await this.compact();
      } catch {
      }
    }
    const base = this.messages.length;
    this.messages.push({ role: "user", content: input });
    const usdBefore = this.totalUsd;
    const tokensBefore = { ...this.totalTokens };
    const done = (text2, rounds2, toolCalls2) => ({
      text: text2,
      rounds: rounds2,
      toolCalls: toolCalls2,
      usd: this.totalUsd - usdBefore,
      tokens: { input: this.totalTokens.input - tokensBefore.input, output: this.totalTokens.output - tokensBefore.output }
    });
    const aborter = new AbortController();
    this.aborter = aborter;
    const cancelled = (rounds2, toolCalls2) => {
      this.messages.length = base;
      this.recent = [];
      return { ...done("Cancelled.", rounds2, toolCalls2), cancelled: true };
    };
    let rounds = 0;
    let toolCalls = 0;
    try {
      for (; ; ) {
        if (rounds >= this.maxRounds) {
          return done(`Stopped after ${this.maxRounds} rounds without a final answer \u2014 tell me how to continue.`, rounds, toolCalls);
        }
        rounds++;
        this.events.onThinking();
        let reply;
        try {
          reply = await this.complete(aborter.signal);
        } catch (err) {
          if (aborter.signal.aborted) return cancelled(rounds, toolCalls);
          throw err;
        }
        const calls = reply.tool_calls ?? [];
        this.messages.push({ role: "assistant", content: reply.content, ...calls.length > 0 ? { tool_calls: calls } : {} });
        if (calls.length === 0) {
          return done(reply.content ?? "", rounds, toolCalls);
        }
        for (const call of calls) {
          if (aborter.signal.aborted) return cancelled(rounds, toolCalls);
          toolCalls++;
          const result = await this.guarded(call);
          this.messages.push({ role: "tool", content: result, tool_call_id: call.id });
        }
        if (aborter.signal.aborted) return cancelled(rounds, toolCalls);
        this.trim();
      }
    } finally {
      this.aborter = null;
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
  /** The conversation as the model sees it this round: in plan mode, the read-only tools and the plan instruction. */
  async complete(signal) {
    const [system, ...rest] = this.messages;
    const outgoing = this.plan && system !== void 0 ? [{ role: "system", content: `${system.content ?? ""}

${PLAN_MODE_PROMPT}` }, ...rest] : this.messages;
    const offered = this.plan ? this.tools.filter((t) => !t.mutating) : this.tools;
    return this.request(outgoing, offered, signal);
  }
  /** One completion over the wire; its usage goes to the totals, its route to the events. */
  async request(messages, tools, signal) {
    const key = await this.transport.authorization();
    const doFetch = this.transport.fetch ?? globalThis.fetch;
    const res = await doFetch(`${this.transport.gateway}/v1/chat/completions`, {
      method: "POST",
      ...signal !== void 0 ? { signal } : {},
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${key}`,
        ...this.transport.betaPassword !== void 0 ? { "x-beta-password": this.transport.betaPassword } : {},
        ...this.lane !== void 0 ? { "x-bitos-lane": this.lane } : {}
      },
      body: JSON.stringify({
        model: this.model,
        messages,
        ...tools.length > 0 ? {
          tools: tools.map((t) => ({
            type: "function",
            function: { name: t.name, description: t.description, parameters: t.parameters }
          })),
          tool_choice: "auto"
        } : {},
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
    const tokens = json.usage === void 0 ? void 0 : { input: Math.max(0, json.usage.prompt_tokens ?? 0), output: Math.max(0, json.usage.completion_tokens ?? 0) };
    if (tokens !== void 0) {
      this.totalTokens.input += tokens.input;
      this.totalTokens.output += tokens.output;
    }
    this.events.onRoute({
      ...h("x-bitos-lane") !== void 0 ? { lane: h("x-bitos-lane") } : {},
      ...h("x-bitos-provider") !== void 0 ? { provider: h("x-bitos-provider") } : {},
      ...h("x-bitos-model") !== void 0 ? { model: h("x-bitos-model") } : {},
      ...Number.isFinite(latency) ? { latencyMs: latency } : {},
      ...Number.isFinite(usd) ? { usd } : {},
      ...tokens !== void 0 ? { tokens } : {}
    });
    return message;
  }
};

// ../../packages/agent/src/permissions.ts
import os6 from "node:os";
import path6 from "node:path";
var NO_RULES = { allow: [], deny: [] };
function parseRule(text2) {
  const m = /^([a-z][a-z0-9_]*)(?:\((.*)\))?$/s.exec(text2.trim());
  if (m === null) return null;
  const tool = m[1];
  const inner = (m[2] ?? "").trim();
  if (inner === "" || inner === "*") return { tool, match: "any", value: "" };
  const prefix = /^(.*?)(?::\*| \*)$/.exec(inner);
  if (prefix !== null && prefix[1] !== "") return { tool, match: "prefix", value: prefix[1] };
  return { tool, match: "exact", value: inner };
}
function ruleMatches(rule, tool, target) {
  if (rule.tool !== tool) return false;
  if (rule.match === "any") return true;
  if (rule.match === "exact") return target === rule.value;
  return target === rule.value || target.startsWith(rule.value.endsWith("/") ? rule.value : `${rule.value} `);
}
function decide(rules, tool, target) {
  const hit = (list) => list.find((text2) => {
    const rule = parseRule(text2);
    return rule !== null && ruleMatches(rule, tool, target);
  });
  const denied2 = hit(rules.deny);
  if (denied2 !== void 0) return { decision: "deny", rule: denied2 };
  const allowed = hit(rules.allow);
  if (allowed !== void 0) return { decision: "allow", rule: allowed };
  return { decision: "ask" };
}
var SUBCOMMAND_TOOLS = /* @__PURE__ */ new Set(["git", "npm", "pnpm", "yarn", "npx", "bun", "docker", "cargo", "go", "gh", "kubectl", "make", "pip", "brew", "bitos"]);
function commandPrefix(command) {
  const words = command.trim().split(/\s+/);
  const first = words[0] ?? "";
  const second = words[1];
  if (SUBCOMMAND_TOOLS.has(first) && second !== void 0 && !second.startsWith("-")) return `${first} ${second}`;
  return first;
}
function ruleFor(tool, target) {
  return tool === "run_command" ? `run_command(${commandPrefix(target)}:*)` : tool;
}
var any = () => true;
var never = () => false;
var noArgsOr = (flags) => (args) => args.every((a) => flags.includes(a));
var versionOnly = (args) => args.length === 1 && /^(-v|-V|--version|version)$/.test(args[0]);
var firstArg = (pattern) => (args) => pattern.test(args[0] ?? "");
var GIT_READ = /* @__PURE__ */ new Set([
  "status",
  "log",
  "diff",
  "show",
  "blame",
  "ls-files",
  "rev-parse",
  "shortlog",
  "describe",
  "grep",
  "cat-file",
  "rev-list",
  "merge-base",
  "count-objects",
  "ls-remote",
  "ls-tree",
  "version",
  "help",
  "check-ignore",
  "name-rev"
]);
var gitCheck = (args) => {
  const sub = args[0];
  if (sub === void 0) return false;
  const rest = args.slice(1);
  if (GIT_READ.has(sub)) return true;
  if (sub === "branch") return noArgsOr(["-a", "-r", "-v", "-vv", "-l", "--list", "--show-current", "--all"])(rest);
  if (sub === "tag") return noArgsOr(["-l", "--list"])(rest);
  if (sub === "remote") return noArgsOr(["-v", "--verbose"])(rest);
  if (sub === "stash" || sub === "worktree") return rest.length === 1 && rest[0] === "list";
  if (sub === "reflog") return rest.length === 0 || rest.length === 1 && rest[0] === "show";
  if (sub === "config") return rest[0] !== void 0 && /^(--get|--get-all|--list|-l|--get-regexp)$/.test(rest[0]);
  return false;
};
var packageRead = firstArg(/^(ls|list|view|info|outdated|why|root|prefix|-v|--version|audit)$/);
var READ_ONLY = {
  ls: any,
  dir: any,
  cat: any,
  head: any,
  tail: any,
  wc: any,
  grep: any,
  egrep: any,
  fgrep: any,
  rg: any,
  ag: any,
  pwd: any,
  echo: any,
  printf: any,
  which: any,
  whereis: any,
  type: any,
  file: any,
  stat: any,
  du: any,
  df: any,
  tree: any,
  date: any,
  uname: any,
  hostname: any,
  whoami: any,
  id: any,
  basename: any,
  dirname: any,
  realpath: any,
  readlink: any,
  diff: any,
  cmp: any,
  comm: any,
  sort: any,
  uniq: any,
  cut: any,
  tr: any,
  jq: any,
  column: any,
  nl: any,
  strings: any,
  md5: any,
  md5sum: any,
  shasum: any,
  sha1sum: any,
  sha256sum: any,
  true: any,
  false: any,
  test: any,
  "[": any,
  awk: any,
  find: (args) => !args.some((a) => /^-(delete|exec|execdir|ok|okdir|fprint0?|fprintf|fls)$/.test(a)),
  sed: (args) => !args.some((a) => a === "--in-place" || /^-[a-zA-Z]*i/.test(a)),
  xargs: never,
  man: never,
  less: never,
  more: never,
  git: gitCheck,
  npm: packageRead,
  pnpm: packageRead,
  yarn: packageRead,
  bun: packageRead,
  node: versionOnly,
  python: versionOnly,
  python3: versionOnly,
  ruby: versionOnly,
  java: versionOnly,
  rustc: versionOnly,
  tsc: versionOnly,
  cargo: versionOnly,
  go: firstArg(/^(version|env)$/),
  docker: firstArg(/^(ps|images|--version|version)$/),
  brew: firstArg(/^(list|--version)$/),
  pip: firstArg(/^(list|show|--version)$/)
};
var SHELL_SYNTAX = /&&|\|\||[;&<>`\n]|\$\(|\$\{/;
var unquote = (arg) => arg.replace(/^["']|["']$/g, "");
var looksLikePath = (arg) => /^[/~.]/.test(arg) || arg.includes("/");
function argsStayUnder(args, cwd) {
  const root = path6.resolve(cwd);
  return args.every((raw) => {
    const arg = unquote(raw);
    if (arg.startsWith("-") || !looksLikePath(arg)) return true;
    const abs = arg.startsWith("~") ? path6.join(os6.homedir(), arg.slice(1)) : path6.resolve(root, arg);
    const rel = path6.relative(root, abs);
    return rel === "" || !rel.startsWith("..") && !path6.isAbsolute(rel);
  });
}
function isReadOnlyCommand(command, cwd) {
  const text2 = command.trim();
  if (text2 === "" || SHELL_SYNTAX.test(text2)) return false;
  return text2.split("|").every((segment) => {
    const words = segment.trim().split(/\s+/).filter((w) => w !== "");
    const program = words[0];
    if (program === void 0) return false;
    const check = READ_ONLY[path6.basename(program)];
    if (check === void 0) return false;
    const args = words.slice(1);
    if (args.some((a) => isSecretFile(unquote(a)))) return false;
    if (cwd !== void 0 && !argsStayUnder(args, cwd)) return false;
    return check(args);
  });
}
var FILE_OPS = /* @__PURE__ */ new Set(["mkdir", "touch", "mv", "cp", "rmdir"]);
function isFileOpCommand(command, cwd) {
  const text2 = command.trim();
  if (text2 === "" || SHELL_SYNTAX.test(text2) || text2.includes("|")) return false;
  const words = text2.split(/\s+/);
  const program = words[0];
  if (program === void 0 || !FILE_OPS.has(path6.basename(program))) return false;
  const args = words.slice(1);
  if (args.some((a) => isSecretFile(unquote(a)))) return false;
  return argsStayUnder(args, cwd);
}

// ../../packages/agent/src/browser.ts
import path7 from "node:path";
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
        const verdict = await ctx.confirm("browse_act", `screenshot ${page.url()} to ${shown(ctx.cwd, file)}`);
        if (verdict !== true) return denied(verdict);
        await page.screenshot({ path: file, fullPage: true });
        return `wrote a screenshot of ${page.url()} to ${shown(ctx.cwd, file)}`;
      }
      const selector = str(args["selector"], "selector");
      if (action === "click") {
        const verdict = await ctx.confirm("browse_act", `click ${selector} on ${page.url()}`);
        if (verdict !== true) return denied(verdict);
        await page.click(selector, { timeout: ACTION_TIMEOUT_MS });
        return where(page, await page.innerText("body").catch(() => ""));
      }
      if (action === "type") {
        const text2 = str(args["text"], "text");
        const enter = args["enter"] === true;
        const summary = `type ${JSON.stringify(text2.length > 40 ? `${text2.slice(0, 37)}\u2026` : text2)} into ${selector} on ${page.url()}${enter ? " and press Enter" : ""}`;
        const verdict = await ctx.confirm("browse_act", summary);
        if (verdict !== true) return denied(verdict);
        await page.fill(selector, text2, { timeout: ACTION_TIMEOUT_MS });
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
    ...env["BITOS_BROWSER_PROFILE"] !== void 0 ? { profileDir: env["BITOS_BROWSER_PROFILE"] } : { profileDir: path7.join("/work", "browser") }
  });
}

// ../../packages/agent/src/index.ts
function defaultTools(env = process.env) {
  return [...LOCAL_TOOLS, ...browserToolFromEnv(env)];
}

// src/agent.ts
import os7 from "node:os";
async function authorization(config) {
  if (config.apiKey !== void 0) return config.apiKey;
  if (config.token === void 0) {
    throw new GatewayError("No API key for the agent \u2014 run: bitos login", 401);
  }
  const minted = await postJson(config, "/api/keys", {
    name: `bitos cli on ${os7.hostname()}`
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
  constructor(config, ctx, events, model = config.model ?? AUTO_MODEL) {
    super(transportFor(config), ctx, events, { model, tools: defaultTools(), lane: "code" });
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

// src/install.ts
import fs6 from "node:fs";
import os8 from "node:os";
import path8 from "node:path";
var VERSION = true ? "0.2.0" : "dev";
var BUILD = true ? "2026-10-26" : "unbundled";
var FOOTER = /\/\/ bitos (\S+) build (\S+)\s*$/;
var FOOTER_BYTES = 200;
function realpath(file) {
  try {
    return fs6.realpathSync(file);
  } catch {
    return file;
  }
}
function installedWithNpm(self) {
  return realpath(self).split(path8.sep).includes("node_modules");
}
function tilde(file, home = os8.homedir()) {
  return file === home || file.startsWith(home + path8.sep) ? `~${file.slice(home.length)}` : file;
}
function versionOf(file) {
  let fd = null;
  try {
    const size = fs6.statSync(file).size;
    const length = Math.min(FOOTER_BYTES, size);
    const tail = Buffer.alloc(length);
    fd = fs6.openSync(file, "r");
    fs6.readSync(fd, tail, 0, length, size - length);
    const match = FOOTER.exec(tail.toString("utf8"));
    return match === null ? null : { version: match[1], build: match[2] };
  } catch {
    return null;
  } finally {
    if (fd !== null) fs6.closeSync(fd);
  }
}
function pathDirs(env) {
  return (env["PATH"] ?? "").split(path8.delimiter).filter((dir) => dir !== "");
}
function othersOnPath(self, env = process.env) {
  const seen = /* @__PURE__ */ new Set([realpath(self)]);
  const found = [];
  for (const dir of pathDirs(env)) {
    const candidate = path8.join(dir, "bitos");
    let real;
    try {
      if (!fs6.statSync(candidate).isFile()) continue;
      real = fs6.realpathSync(candidate);
    } catch {
      continue;
    }
    if (seen.has(real)) continue;
    seen.add(real);
    found.push(candidate);
  }
  return found;
}
function hiddenBy(self, env = process.env) {
  if (installedWithNpm(self)) return null;
  const dirs = pathDirs(env);
  const mine = realpath(path8.dirname(self));
  const at = dirs.findIndex((dir) => realpath(dir) === mine);
  if (at === -1) return null;
  const pathOf = (list) => ({ PATH: list.join(path8.delimiter) });
  if (othersOnPath(self, pathOf(dirs.slice(0, at))).length > 0) return null;
  const behind = othersOnPath(self, pathOf(dirs.slice(at + 1)));
  const npmAt = behind.findIndex(installedWithNpm);
  const hidden = npmAt === -1 ? behind[0] : behind[npmAt];
  if (hidden === void 0) return null;
  return {
    path: hidden,
    npm: npmAt !== -1,
    version: versionOf(hidden)?.version ?? null,
    remove: npmAt === -1 ? [] : [self, ...behind.slice(0, npmAt)]
  };
}
function shadowNote(self, env = process.env) {
  if (self === void 0) return null;
  const hidden = hiddenBy(self, env);
  if (hidden === null) return null;
  const version = hidden.version === null ? "" : ` (${hidden.version})`;
  return hidden.npm ? `the npm install at ${tilde(hidden.path)}${version} is hidden by this copy \u2014 rm ${hidden.remove.map((file) => tilde(file)).join(" ")} to run it` : `another copy at ${tilde(hidden.path)}${version} is hidden by this one`;
}
function describeInstall(self, gateway) {
  const head = `bitos ${VERSION} \xB7 build ${BUILD}`;
  if (self === void 0) return `${head} \xB7 unknown location \xB7 updates from ${gateway}`;
  return installedWithNpm(self) ? `${head} \xB7 npm` : `${head} \xB7 copy at ${tilde(self)} \xB7 updates from ${gateway}`;
}

// src/mentions.ts
import fs7 from "node:fs";
var MAX_MENTION_CHARS = 6e4;
var TOKEN = /(^|[\s(])@([^\s@()]+)/g;
function expandMentions(text2, cwd) {
  const mentions = [];
  const skipped = [];
  const blocks = [];
  const seen = /* @__PURE__ */ new Set();
  for (const match of text2.matchAll(TOKEN)) {
    const token = match[2].replace(/[.,;:!?]+$/, "");
    if (token === "" || seen.has(token)) continue;
    seen.add(token);
    const file = resolvePath(cwd, token);
    if (isSecretFile(file)) {
      skipped.push(token);
      continue;
    }
    let content;
    try {
      if (!fs7.statSync(file).isFile()) continue;
      content = fs7.readFileSync(file, "utf8");
    } catch {
      continue;
    }
    mentions.push({ token, path: file, lines: content.split("\n").length, chars: content.length });
    blocks.push(`<file path="${shown(cwd, file)}">
${clip(content, MAX_MENTION_CHARS)}
</file>`);
  }
  return { text: blocks.length === 0 ? text2 : `${text2}

${blocks.join("\n\n")}`, mentions, skipped };
}

// src/permissions.ts
import fs8 from "node:fs";
import path9 from "node:path";
var MODES = ["ask", "edits", "all", "plan"];
function isMode(value) {
  return MODES.includes(value);
}
var PERMISSIONS_FILE = path9.join(".bitos", "permissions.json");
var PermissionStore = class {
  constructor(cwd) {
    this.cwd = cwd;
    this.file = path9.join(cwd, PERMISSIONS_FILE);
  }
  file;
  /** The folder's rules; a missing or broken file is no rules. */
  load() {
    try {
      const raw = JSON.parse(fs8.readFileSync(this.file, "utf8"));
      const list = (v) => Array.isArray(v) ? v.filter((x) => typeof x === "string") : [];
      return { allow: list(raw.allow), deny: list(raw.deny) };
    } catch {
      return { ...NO_RULES, allow: [], deny: [] };
    }
  }
  save(rules) {
    fs8.mkdirSync(path9.dirname(this.file), { recursive: true });
    const ignore = path9.join(this.cwd, ".bitos", ".gitignore");
    if (!fs8.existsSync(ignore)) fs8.writeFileSync(ignore, "*\n");
    fs8.writeFileSync(this.file, `${JSON.stringify(rules, null, 2)}
`);
  }
};
function isUnder(root, file) {
  const rel = path9.relative(path9.resolve(root), path9.resolve(file));
  return rel === "" || !rel.startsWith("..") && !path9.isAbsolute(rel);
}
function targetOf(summary, detail, cwd) {
  if (detail?.kind === "command") return detail.command;
  if (detail?.kind === "file") return shown(cwd, detail.path);
  return summary.split("\n")[0] ?? "";
}
function policy(mode, rules, cwd, tool, summary, detail) {
  const target = targetOf(summary, detail, cwd);
  const ruling = decide(rules, tool, target);
  if (ruling.decision === "deny") return { decision: "deny", rule: ruling.rule };
  if (mode === "all") return { decision: "allow", why: "mode" };
  if (ruling.decision === "allow") return { decision: "allow", why: "rule", rule: ruling.rule };
  if (detail?.kind === "file" && isSecretFile(detail.path)) return { decision: "ask" };
  if (mode === "edits" && detail?.kind === "file" && isUnder(cwd, detail.path)) return { decision: "allow", why: "mode" };
  if (detail?.kind === "command" && isUnder(cwd, detail.cwd)) {
    if (isReadOnlyCommand(detail.command, detail.cwd)) return { decision: "allow", why: "read-only" };
    if (mode === "edits" && isFileOpCommand(detail.command, detail.cwd)) return { decision: "allow", why: "mode" };
  }
  return { decision: "ask" };
}
function alwaysOption(tool, summary, detail, cwd) {
  if (detail === void 0) return null;
  const rule = ruleFor(tool, targetOf(summary, detail, cwd));
  if (detail.kind === "command") return { words: `"${rule.slice("run_command(".length, -":*)".length)}"`, rule };
  return { words: tool === "edit_file" ? "edits" : "new files", rule };
}
function parseAnswer(line) {
  const text2 = line.trim();
  const word = text2.toLowerCase();
  if (word === "" || word === "y" || word === "yes") return { kind: "yes" };
  if (word === "a" || word === "always") return { kind: "always" };
  if (word === "s" || word === "session" || word === "all") return { kind: "session" };
  if (word === "n" || word === "no") return { kind: "no" };
  const reasoned = /^(?:n|no)[:,\s]+(.+)$/i.exec(text2);
  return { kind: "no", reason: reasoned === null ? text2 : reasoned[1].trim() };
}

// src/render.ts
import path10 from "node:path";

// src/ui.ts
var isTTY = process.stdout.isTTY === true;
var esc = (code, s) => isTTY ? `\x1B[${code}m${s}\x1B[0m` : s;
var c = {
  dim: (s) => esc("2", s),
  bold: (s) => esc("1", s),
  amber: (s) => esc("38;5;214", s),
  red: (s) => esc("31", s),
  green: (s) => esc("32", s),
  ink: (s) => esc("38;5;240", s)
};
var ANSI = /\x1b\[[0-9;]*m/g;
function visibleWidth(s) {
  return [...s.replace(ANSI, "")].length;
}
function fit(s, width) {
  if (visibleWidth(s) <= width) return s;
  let out2 = "";
  let seen = 0;
  let styled = false;
  for (const part of s.split(/(\x1b\[[0-9;]*m)/)) {
    if (part.startsWith("\x1B[")) {
      out2 += part;
      styled = true;
      continue;
    }
    for (const ch of part) {
      if (seen >= width) break;
      out2 += ch;
      seen++;
    }
    if (seen >= width) break;
  }
  return styled ? `${out2}\x1B[0m` : out2;
}
function lineWidth() {
  return Math.max(20, (process.stdout.columns ?? 80) - 1);
}
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
function fmtTokens(n) {
  return n >= 1e3 ? `${(n / 1e3).toFixed(1)}k` : String(n);
}
var FULL = { model: true, counters: true, hint: true, wave: true };
var DEGRADATIONS = [
  FULL,
  { ...FULL, model: false },
  { ...FULL, model: false, wave: false },
  { ...FULL, model: false, wave: false, hint: false },
  { model: false, wave: false, hint: false, counters: false }
];
var INTERRUPT_HINT = "esc to interrupt";
var Live = class {
  route = {};
  label = null;
  timer = null;
  t = 0;
  running = false;
  paused = false;
  startedAt = 0;
  tokensUsed = 0;
  redraw = () => {
    if (this.running && !this.paused && isTTY) this.draw();
  };
  start() {
    this.route = {};
    this.label = null;
    this.running = true;
    this.paused = false;
    this.startedAt = Date.now();
    this.tokensUsed = 0;
    if (!isTTY) return;
    this.stopTimer();
    process.stdout.on("resize", this.redraw);
    this.tick();
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
  /**
   * Hand the terminal to a question (a permission prompt): the strip stops
   * redrawing and clears its line, so the prompt stays where the user can
   * see it and answer it. `resume` brings the strip back.
   */
  pause() {
    this.paused = true;
    this.label = null;
    this.stopTimer();
    if (isTTY) process.stdout.write("\r\x1B[2K");
  }
  resume() {
    this.paused = false;
    if (!isTTY || !this.running) return;
    this.tick();
    this.draw();
  }
  learn(route) {
    const { tokens, ...rest } = route;
    if (tokens !== void 0) this.tokensUsed += tokens.input + tokens.output;
    this.route = { ...this.route, ...rest };
    if (isTTY) {
      if (this.running && !this.paused) this.draw();
      return;
    }
    if (route.lane !== void 0 || route.provider !== void 0) {
      process.stdout.write(`${this.strip(false, true)}
`);
    }
  }
  /** Print a line above the live line. */
  print(text2) {
    if (isTTY && this.running && !this.paused) {
      process.stdout.write(`\r\x1B[2K${text2}
`);
      this.draw();
    } else {
      process.stdout.write(`${text2}
`);
    }
  }
  /** Stop animating; return the receipt line for the caller to print — with the model's name when it fits, the provider alone when not. */
  finish(extra = "") {
    this.running = false;
    this.label = null;
    this.stopTimer();
    if (isTTY) {
      process.stdout.off("resize", this.redraw);
      process.stdout.write("\r\x1B[2K");
    }
    const full = this.strip(false, true, extra);
    return !isTTY || visibleWidth(full) <= lineWidth() ? full : this.strip(false, false, extra);
  }
  /** The line for a terminal `width` columns wide: the widest version that fits, else a hard cut. */
  line(width) {
    for (const parts of DEGRADATIONS) {
      const candidate = this.compose(parts);
      if (visibleWidth(candidate) <= width) return candidate;
    }
    return fit(this.compose(DEGRADATIONS[DEGRADATIONS.length - 1]), width);
  }
  tick() {
    this.timer = setInterval(() => {
      this.t++;
      this.draw();
    }, 80);
  }
  draw() {
    process.stdout.write(`\r\x1B[2K${this.line(lineWidth())}`);
  }
  compose(parts) {
    const strip = this.strip(true, parts.model);
    if (this.label === null) {
      return parts.counters && this.running ? `${strip}${c.dim(`   ${this.counters()}`)}` : strip;
    }
    const wave = parts.wave ? `   ${waveFrame(this.t)} ` : "   ";
    const tail = [this.label, ...parts.counters ? [this.counters()] : [], ...parts.hint ? [INTERRUPT_HINT] : []].join(" \xB7 ");
    return `${strip}${wave}${c.dim(tail)}`;
  }
  /** How long this turn has run and what it has used so far. */
  counters() {
    const secs = Math.max(0, Math.round((Date.now() - this.startedAt) / 1e3));
    return this.tokensUsed > 0 ? `${secs}s \xB7 ${fmtTokens(this.tokensUsed)} tokens` : `${secs}s`;
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
  /** The network stage: provider and model, or the provider alone when the line is short of room. */
  network(full) {
    const r = this.route;
    if (r.provider === void 0) return void 0;
    if (!full || r.model === void 0) return r.provider;
    return `${r.provider}\xB7${r.model.split("/").pop()}`;
  }
  strip(animate, fullModel, extra = "") {
    const r = this.route;
    const stage = (label, placeholder) => label === void 0 ? c.dim(placeholder) : c.bold(label);
    const tail = !this.running && r.latencyMs !== void 0 ? c.dim(`  ${(r.latencyMs / 1e3).toFixed(1)}s${r.usd !== void 0 ? ` \xB7 $${r.usd.toFixed(4)}` : ""}${extra}`) : c.dim(extra);
    return `  ${stage("input", "input")}${this.link(0, animate)}${stage("brain", "brain")}${this.link(1, animate)}${stage(r.lane, "lane")}${this.link(2, animate)}${stage(this.network(fullModel), "network")}${this.link(3, animate)}${stage(this.running ? void 0 : "answer", "answer")}${tail}`;
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

// src/render.ts
var RESULT_LINES = 6;
var CALL_CHARS = 160;
var LISTING_CHARS = 110;
var cut = (s, max) => s.length > max ? `${s.slice(0, max - 1)}\u2026` : s;
var one = (s) => s.replace(/\s+/g, " ").trim();
var text = (v) => typeof v === "string" ? v : v === void 0 ? "" : String(v);
function argSummary(name, args) {
  switch (name) {
    case "read_file":
    case "edit_file":
      return text(args["path"]);
    case "list_dir":
      return text(args["path"]) === "" ? "." : text(args["path"]);
    case "search":
      return `${JSON.stringify(text(args["pattern"]))}${text(args["path"]) === "" ? "" : ` in ${text(args["path"])}`}`;
    case "write_file":
      return `${text(args["path"])} (${text(args["content"]).length} chars)`;
    case "run_command":
      return `${one(text(args["command"]))}${text(args["cwd"]) === "" ? "" : ` in ${text(args["cwd"])}`}`;
    case "skill":
      return text(args["name"]);
    default:
      return Object.entries(args).map(([k, v]) => `${k}=${typeof v === "string" ? JSON.stringify(cut(one(v), 60)) : String(v)}`).join(" ");
  }
}
function renderCall(name, args) {
  return `${c.amber("\u25CF")} ${c.bold(name)}${c.dim(`(${cut(argSummary(name, args), CALL_CHARS)})`)}`;
}
var ms = (n) => n < 1e3 ? `${n}ms` : `${(n / 1e3).toFixed(1)}s`;
function under(lines, took) {
  const first = lines[0] ?? "";
  const head = `  ${c.dim("\u23BF")}  ${first}${took === null ? "" : c.dim(` (${ms(took)})`)}`;
  return [head, ...lines.slice(1).map((l) => `     ${l}`)];
}
function excerpt(body, max = RESULT_LINES) {
  const lines = body.split("\n").filter((l, i, all) => !(i === all.length - 1 && l === ""));
  const kept = lines.slice(0, max).map((l) => c.dim(fit(l, 120)));
  if (lines.length > max) kept.push(c.dim(`\u2026 +${lines.length - max} line${lines.length - max === 1 ? "" : "s"}`));
  return kept;
}
function renderResult(name, args, result, took) {
  const firstLine = result.split("\n")[0] ?? "";
  if (/^(ERROR|DENIED|REFUSED)/.test(result)) return under([c.red(cut(one(result), 200))], took);
  switch (name) {
    case "read_file": {
      const lines = result.split("\n").length;
      const truncated = /truncated\]$/.test(result.trimEnd());
      return under([c.dim(`read ${lines} line${lines === 1 ? "" : "s"}${truncated ? ", cut" : ""}`)], took);
    }
    case "list_dir": {
      const entries = result === "" ? [] : result.split("\n");
      return under([c.dim(`${entries.length} entr${entries.length === 1 ? "y" : "ies"}: ${cut(entries.join("  "), LISTING_CHARS)}`)], took);
    }
    case "search": {
      if (result.startsWith("(no matches)")) return under([c.dim("no matches")], took);
      const hits = result.split("\n").filter((l) => l !== "");
      return under([c.dim(`${hits.length}${result.includes("truncated]") ? "+" : ""} match${hits.length === 1 ? "" : "es"}`), ...excerpt(hits.slice(0, 3).join("\n"), 3)], took);
    }
    case "run_command": {
      const exit = /\n?\[exit (\d+)\]\s*$/.exec(result);
      const code = exit === null ? 0 : Number(exit[1]);
      const body = exit === null ? result : result.slice(0, exit.index);
      const lines = body.trim() === "" ? [c.dim("(no output)")] : excerpt(body);
      if (code !== 0) lines.push(c.red(`exit ${code}`));
      return under(lines, took);
    }
    default: {
      const lines = excerpt(result, name === "skill" ? 1 : RESULT_LINES);
      return under(lines.length > 0 ? lines : [c.dim(firstLine)], took);
    }
  }
}
function renderDiff(diff) {
  return diff.split("\n").map((line) => {
    if (line.startsWith("- ")) return c.red(line);
    if (line.startsWith("+ ")) return c.green(line);
    return c.dim(line);
  });
}
function renderQuestion(tool, summary, detail, cwd, hints) {
  const lines = [];
  if (detail?.kind === "command") {
    const where2 = path10.resolve(detail.cwd) === path10.resolve(cwd) ? "" : c.dim(` in ${shown(cwd, detail.cwd)}`);
    lines.push(`${c.amber("?")} ${c.bold(tool)}  ${detail.command}${where2}`);
  } else if (detail?.kind === "file") {
    lines.push(`${c.amber("?")} ${c.bold(tool)}  ${detail.action} ${shown(cwd, detail.path)}`);
    if (detail.diff !== void 0) lines.push(...renderDiff(detail.diff).map((l) => `  ${l}`));
    else if (detail.preview !== void 0 && detail.preview !== "") {
      lines.push(...detail.preview.split("\n").map((l) => `  ${c.green(`+ ${l}`)}`));
    }
  } else {
    lines.push(`${c.amber("?")} ${c.bold(tool)}  ${summary.split("\n")[0] ?? ""}`);
  }
  const options = [
    `${c.bold("y")} yes`,
    ...hints.always === null ? [] : [`${c.bold("a")} always allow ${hints.always} here`],
    `${c.bold("s")} yes to all this session`,
    `${c.bold("n")} no ${c.dim("(n <why> tells the agent)")}`
  ];
  lines.push(`  ${c.dim(options.join(" \xB7 "))}`);
  return lines.map((l) => visibleWidth(l) > 200 ? fit(l, 200) : l);
}

// src/sessions.ts
import fs9 from "node:fs";
import path11 from "node:path";
var SESSIONS_DIR = path11.join(".bitos", "sessions");
var MAX_SESSIONS = 30;
var GIST_CHARS = 48;
function stamp(d) {
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}
function isRecord(v) {
  if (typeof v !== "object" || v === null) return false;
  const r = v;
  return typeof r["id"] === "string" && typeof r["updatedAt"] === "string" && Array.isArray(r["messages"]) && typeof r["turns"] === "number";
}
var SessionStore = class {
  constructor(cwd) {
    this.cwd = cwd;
    this.dir = path11.join(cwd, SESSIONS_DIR);
  }
  dir;
  /** A fresh record; nothing touches the disk until the first save. */
  create(model) {
    const now = /* @__PURE__ */ new Date();
    return {
      id: stamp(now),
      createdAt: now.toISOString(),
      updatedAt: now.toISOString(),
      cwd: this.cwd,
      model,
      turns: 0,
      usd: 0,
      tokens: { input: 0, output: 0 },
      messages: []
    };
  }
  /** Write the record; the folder is created on the first save and ignores itself. */
  save(record) {
    const next = { ...record, updatedAt: (/* @__PURE__ */ new Date()).toISOString() };
    fs9.mkdirSync(this.dir, { recursive: true });
    const ignore = path11.join(this.cwd, ".bitos", ".gitignore");
    if (!fs9.existsSync(ignore)) fs9.writeFileSync(ignore, "*\n");
    fs9.writeFileSync(path11.join(this.dir, `${next.id}.json`), `${JSON.stringify(next, null, 1)}
`);
    this.prune();
    return next;
  }
  /** Every session in this folder, newest first; unreadable files are skipped. */
  list() {
    let names;
    try {
      names = fs9.readdirSync(this.dir).filter((n) => n.endsWith(".json"));
    } catch {
      return [];
    }
    const out2 = [];
    for (const name of names) {
      try {
        const parsed = JSON.parse(fs9.readFileSync(path11.join(this.dir, name), "utf8"));
        if (isRecord(parsed)) out2.push(parsed);
      } catch {
      }
    }
    return out2.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt) || b.id.localeCompare(a.id));
  }
  /** The session to continue: the newest one that holds a conversation. */
  latest() {
    return this.list().find((s) => s.turns > 0 && s.messages.length > 0) ?? null;
  }
  /** How a session reads in the banner and in /sessions. */
  label(record) {
    const when = new Date(record.updatedAt);
    const today = /* @__PURE__ */ new Date();
    const sameDay = when.toDateString() === today.toDateString();
    const p = (n) => String(n).padStart(2, "0");
    const time = `${p(when.getHours())}:${p(when.getMinutes())}`;
    const day = sameDay ? "today" : `${when.getFullYear()}-${p(when.getMonth() + 1)}-${p(when.getDate())}`;
    const asked = record.messages.filter((m) => m.role === "user");
    const first = ((asked.find((m) => !(m.content ?? "").startsWith("[")) ?? asked[0])?.content ?? "").replace(/\s+/g, " ").trim();
    const gist = first.slice(0, GIST_CHARS);
    return `${day} ${time} \xB7 ${record.turns} turn${record.turns === 1 ? "" : "s"}${gist === "" ? "" : ` \xB7 "${gist}${first.length > GIST_CHARS ? "\u2026" : ""}"`}`;
  }
  prune() {
    const all = this.list();
    for (const old of all.slice(MAX_SESSIONS)) {
      try {
        fs9.rmSync(path11.join(this.dir, `${old.id}.json`));
      } catch {
      }
    }
  }
};

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
var COMMANDS = [
  "mode",
  "plan",
  "yes",
  "permissions",
  "compact",
  "model",
  "models",
  "usage",
  "cost",
  "status",
  "sessions",
  "resume",
  "new",
  "clear",
  "agent",
  "chat",
  "lane",
  "whoami",
  "help",
  "exit"
];
var REPL_HELP = `${c.bold("bitos console")} \u2014 an agent in this directory.

  Type a task. The agent reads, searches, edits and writes files here and runs
  commands, asking before it changes anything \u2014 unless the mode, a rule of this
  folder or the built-in read-only list (ls, cat, grep, git status\u2026) answers.

  at a question   ${c.bold("Enter")}/${c.bold("y")} yes \xB7 ${c.bold("a")} always allow this here (saved in ${PERMISSIONS_FILE})
                  ${c.bold("s")} yes to everything this session \xB7 ${c.bold("n")} no \u2014 "n <why>" tells the agent why
  while it runs   ${c.bold("Esc")} or Ctrl-C cancels the turn
  in a message    ${c.bold("@path")} attaches the file \xB7 a line starting with ${c.bold("!")} runs a command yourself, on the record

  /mode [ask|edits|all|plan]  ask (default) \xB7 edits: file edits here just happen \xB7 all: nothing asks \xB7 plan: read-only, plan first
  /plan  /yes                 plan mode \xB7 all mode
  /permissions [clear|allow <rule>|deny <rule>]   this folder's rules, e.g. run_command(git:*), edit_file
  /compact [focus]            fold the conversation into a summary (happens on its own past 150k chars)
  /model [id|auto]            pin a model (provider::model, see /models) or let the brain route
  /models                     the models you can pin, from every network
  /usage                      this session's tokens and cost (/cost)
  /status                     gateway, account, model, mode, session
  /sessions  /resume <n>      this folder's sessions \xB7 continue one
  /new                        forget the conversation so far (/clear)
  /chat  /agent  /lane <name> plain mode on the brain, no tools \xB7 back to the agent \xB7 pin the lane
  /whoami  /help  /exit`;
function modelLabel(model) {
  return model === AUTO_MODEL ? "auto \xB7 routed by the brain, code lane" : model;
}
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
function startMode() {
  const flag = process.argv.indexOf("--mode");
  const want = flag !== -1 ? process.argv[flag + 1] : process.env["BITOS_MODE"];
  if (want !== void 0 && isMode(want)) return want;
  return process.argv.includes("--yes") || process.env["BITOS_YES"] === "1" ? "all" : "ask";
}
function completer(cwd) {
  return (line) => {
    if (line.startsWith("/") && !line.includes(" ")) {
      const word = line.slice(1);
      const hits = COMMANDS.filter((cmd) => cmd.startsWith(word)).map((cmd) => `/${cmd}`);
      return [hits, line];
    }
    const at = /(?:^|\s)@([^\s@]*)$/.exec(line);
    if (at === null) return [[], line];
    const partial = at[1];
    const dir = partial.endsWith("/") ? partial : path12.dirname(partial) === "." && !partial.includes("/") ? "" : `${path12.dirname(partial)}/`;
    const base = partial.endsWith("/") ? "" : path12.basename(partial);
    let entries = [];
    try {
      entries = fs10.readdirSync(path12.resolve(cwd, dir === "" ? "." : dir), { withFileTypes: true }).filter((e) => e.name.startsWith(base) && e.name !== "node_modules" && e.name !== ".git").map((e) => `${dir}${e.name}${e.isDirectory() ? "/" : ""}`);
    } catch {
    }
    return [entries.map((e) => `@${e}`), `@${partial}`];
  };
}
function runOwnCommand(command, cwd) {
  return new Promise((resolve) => {
    execFile2(process.env["SHELL"] ?? "/bin/sh", ["-lc", command], { cwd, timeout: 12e4, maxBuffer: 8 * 1024 * 1024 }, (err, stdout, stderr) => {
      const code = err !== null && typeof err.code === "number" ? err.code : err !== null ? 1 : 0;
      resolve(`${stdout}${stderr !== "" ? `
[stderr]
${stderr}` : ""}`.trim() + (code === 0 ? "" : `
[exit ${code}]`));
    });
  });
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
  let chat = false;
  let mode = startMode();
  let threadId = await newThread(config);
  let exiting = false;
  let inputDone = false;
  const cwd = process.cwd();
  const branch = gitBranch(cwd);
  const rules = loadRules(cwd);
  const skills = discoverSkills(cwd);
  const startModel = process.env["BITOS_MODEL"] ?? config.model ?? AUTO_MODEL;
  let turns = 0;
  const sessions = new SessionStore(cwd);
  const resumed = process.argv.includes("--new") ? null : sessions.latest();
  let session = resumed ?? sessions.create(startModel);
  if (resumed !== null) turns = resumed.turns;
  const store = new PermissionStore(cwd);
  let permissions = store.load();
  let running = false;
  const shadow = shadowNote(process.argv[1]);
  const ruleCount = () => permissions.allow.length + permissions.deny.length;
  process.stdout.write(
    `${banner([
      "",
      `${c.bold("BitOS")}  ${c.dim("one balance \xB7 every web3 intelligence")}`,
      c.dim(describeInstall(process.argv[1], config.gateway)),
      ...shadow === null ? [] : [c.amber(shadow)],
      c.dim(config.gateway),
      c.dim(address !== null ? `${address.slice(0, 8)}\u2026${address.slice(-6)}` : "anonymous \xB7 dev gateway"),
      c.dim(resumed !== null ? `session ${sessions.label(resumed)} \xB7 /new for a fresh one` : "new session \xB7 kept in .bitos/ as you go"),
      c.dim(`agent in ${cwd}${branch !== null ? ` \xB7 ${branch}` : ""}`),
      c.dim(`model ${modelLabel(startModel)}`),
      c.dim(`mode ${mode} \xB7 ${ruleCount()} rule${ruleCount() === 1 ? "" : "s"} in ${PERMISSIONS_FILE} \xB7 ${rules.project !== null ? path12.basename(rules.project.path) : "no AGENTS.md"} \xB7 ${skills.length} skill${skills.length === 1 ? "" : "s"}`),
      c.dim("/help for commands \xB7 /exit to leave")
    ])}

`
  );
  const live = new Live();
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: isTTY, completer: completer(cwd) });
  const prompt = () => {
    const tag = chat ? c.amber(`[chat${lane !== void 0 ? `:${lane}` : ""}] `) : mode === "ask" ? "" : c.amber(`[${mode}] `);
    rl.setPrompt(`${tag}${c.bold("\u203A")} `);
    rl.prompt();
  };
  let pendingAnswer = null;
  const ask = (question) => new Promise((resolve) => {
    pendingAnswer = resolve;
    rl.setPrompt(question);
    rl.prompt();
  });
  let waitedMs = 0;
  const agentCtx = {
    cwd,
    confirm: async (tool, summary, detail) => {
      const verdict = policy(mode, permissions, cwd, tool, summary, detail);
      if (verdict.decision === "deny") {
        live.print(`  ${c.dim("\u23BF")}  ${c.red(`blocked by the rule ${verdict.rule}`)}`);
        return { reason: `blocked by the permission rule ${verdict.rule} of this folder` };
      }
      if (verdict.decision === "allow") {
        if (detail?.kind === "file" && detail.diff !== void 0) for (const line of renderDiff(detail.diff)) live.print(`     ${line}`);
        return true;
      }
      const always = alwaysOption(tool, summary, detail, cwd);
      live.pause();
      process.stdout.write(`
${renderQuestion(tool, summary, detail, cwd, { always: always === null ? null : always.words }).join("\n")}
`);
      const asked = Date.now();
      const answer = parseAnswer(await ask(`${c.dim("allow?")} ${c.bold("\u203A")} `));
      waitedMs += Date.now() - asked;
      live.resume();
      switch (answer.kind) {
        case "yes":
          return true;
        case "always": {
          if (always === null) return true;
          permissions = { ...permissions, allow: [...permissions.allow, always.rule] };
          store.save(permissions);
          live.print(`  ${c.dim("\u23BF")}  ${c.dim(`allowed from now on in this folder: ${always.rule} (${PERMISSIONS_FILE})`)}`);
          return true;
        }
        case "session":
          mode = "all";
          live.print(`  ${c.dim("\u23BF")}  ${c.dim("nothing asks for the rest of this session \xB7 /mode ask to go back")}`);
          return true;
        case "no":
          return answer.reason === void 0 ? false : { reason: answer.reason };
      }
    },
    note: (line) => live.print(c.dim(`  ${line}`))
  };
  let callArgs = {};
  const agent = new Agent2(config, agentCtx, {
    onThinking: () => live.think("thinking"),
    onRoute: (route) => live.learn(route),
    onToolStart: (name, args) => {
      live.rest();
      callArgs = args;
      live.print(renderCall(name, args));
    },
    onToolEnd: (name, result, took) => {
      for (const line of renderResult(name, callArgs, result, took)) live.print(line);
    },
    onCompact: ({ before, after }) => live.print(c.dim(`  compacted the conversation: ${fmtTokens(before)} \u2192 ${fmtTokens(after)} chars`))
  }, startModel);
  agent.setPlanMode(mode === "plan");
  const load = (record) => {
    agent.restore(record.messages);
    agent.totalUsd = record.usd;
    agent.totalTokens.input = record.tokens.input;
    agent.totalTokens.output = record.tokens.output;
    turns = record.turns;
    if (record.threadId !== void 0) threadId = record.threadId;
  };
  if (resumed !== null) load(resumed);
  const persist = () => {
    session = sessions.save({
      ...session,
      turns,
      usd: agent.totalUsd,
      tokens: { ...agent.totalTokens },
      model: agent.currentModel,
      messages: agent.history(),
      ...threadId !== void 0 ? { threadId } : {}
    });
  };
  const setMode = (next) => {
    mode = next;
    agent.setPlanMode(next === "plan");
    const what = {
      ask: "writes and commands ask, unless a rule or the read-only list answers",
      edits: "file edits in this folder just happen; commands still ask",
      all: "nothing asks for the rest of this session",
      plan: "read-only tools only; the agent plans, you decide \u2014 /mode ask to build"
    };
    process.stdout.write(c.dim(`mode ${next}: ${what[next]}
`));
  };
  const command = async (cmd, rest) => {
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
      case "mode": {
        const want = rest[0];
        if (want === void 0) {
          process.stdout.write(c.dim(`mode ${mode} \u2014 /mode ${MODES.filter((m) => m !== mode).join("|")}
`));
          return;
        }
        if (!isMode(want)) {
          process.stdout.write(`${c.red("usage:")} /mode ${MODES.join("|")}
`);
          return;
        }
        setMode(want);
        return;
      }
      case "plan":
        setMode("plan");
        return;
      case "yes":
        setMode("all");
        return;
      case "permissions": {
        const [action, ...ruleWords] = rest;
        const rule = ruleWords.join(" ");
        if (action === "clear") {
          permissions = { allow: [], deny: [] };
          store.save(permissions);
          process.stdout.write(c.dim(`no rules in ${PERMISSIONS_FILE}
`));
          return;
        }
        if ((action === "allow" || action === "deny") && rule !== "") {
          permissions = { ...permissions, [action]: [...permissions[action], rule] };
          store.save(permissions);
          process.stdout.write(c.dim(`${action}: ${rule} (${PERMISSIONS_FILE})
`));
          return;
        }
        if (action !== void 0) {
          process.stdout.write(`${c.red("usage:")} /permissions [clear | allow <rule> | deny <rule>]
`);
          return;
        }
        const lines = [
          ...permissions.allow.map((r) => `  ${c.green("allow")}  ${r}`),
          ...permissions.deny.map((r) => `  ${c.red("deny ")}  ${r}`)
        ];
        process.stdout.write(
          `${lines.length === 0 ? c.dim(`  no rules yet \u2014 answer a question with "a", or /permissions allow run_command(git:*)`) : lines.join("\n")}
${c.dim(`  mode ${mode} \xB7 built-in: read-only commands in this folder never ask \xB7 ${PERMISSIONS_FILE}`)}
`
        );
        return;
      }
      case "compact": {
        live.start();
        live.think("compacting");
        running = true;
        try {
          const result = await agent.compact(void 0, rest.join(" "));
          live.finish();
          process.stdout.write(c.dim(result === null ? "nothing to compact yet\n" : `compacted: ${fmtTokens(result.before)} \u2192 ${fmtTokens(result.after)} chars
`));
          if (result !== null) persist();
        } catch (err) {
          live.finish();
          process.stdout.write(`${c.red("\u2717")} ${err instanceof Error ? err.message : String(err)}
`);
        } finally {
          running = false;
        }
        return;
      }
      case "model": {
        const want = rest[0];
        if (want === void 0) {
          process.stdout.write(c.dim(`model: ${modelLabel(agent.currentModel)}
`));
          return;
        }
        const next = want === "auto" ? AUTO_MODEL : want;
        if (!isModelId(next)) {
          process.stdout.write(`${c.red("usage:")} /model auto | provider::model \u2014 see /models
`);
          return;
        }
        agent.setModel(next);
        if (next === AUTO_MODEL) delete config.model;
        else config.model = next;
        saveConfig(config);
        process.stdout.write(c.dim(`model: ${modelLabel(next)} \u2014 saved for next time too
`));
        return;
      }
      case "models": {
        try {
          const catalog = await getJson(config, "/api/models");
          const current = agent.currentModel;
          const mark = (id) => id === current ? c.amber("\u25CF") : c.dim("\u25CB");
          const lines = [`  ${mark(AUTO_MODEL)} auto  ${c.dim("routed by the brain, code lane")}`];
          for (const p of catalog.providers) {
            for (const m of p.models) lines.push(`  ${mark(`${p.id}::${m}`)} ${p.id}::${m}`);
          }
          process.stdout.write(`${lines.join("\n")}
${c.dim("  /model <id> to pin one \xB7 /model auto to let the brain route")}
`);
        } catch (err) {
          process.stdout.write(`${c.red("\u2717")} ${err instanceof Error ? err.message : String(err)}
`);
        }
        return;
      }
      case "usage":
      case "cost":
      case "stats":
        process.stdout.write(
          `${c.dim(`session: $${agent.totalUsd.toFixed(4)} \xB7 ${fmtTokens(agent.totalTokens.input)} in \xB7 ${fmtTokens(agent.totalTokens.output)} out \xB7 ${turns} turn${turns === 1 ? "" : "s"} \xB7 ${fmtTokens(agent.historyChars())} chars of context \xB7 model ${modelLabel(agent.currentModel)}`)}
`
        );
        return;
      case "status":
        process.stdout.write(
          [
            `  gateway   ${config.gateway}`,
            `  account   ${address ?? "anonymous"}`,
            `  model     ${modelLabel(agent.currentModel)}`,
            `  mode      ${mode} \xB7 ${ruleCount()} rule${ruleCount() === 1 ? "" : "s"}`,
            `  folder    ${cwd}${branch !== null ? ` \xB7 ${branch}` : ""}`,
            `  session   ${sessions.label(session)} \xB7 ${fmtTokens(agent.historyChars())} chars \xB7 $${agent.totalUsd.toFixed(4)}`,
            `  rules     ${rules.project !== null ? rules.project.path : "no AGENTS.md"} \xB7 ${skills.length} skill${skills.length === 1 ? "" : "s"}`
          ].join("\n") + "\n"
        );
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
      case "clear":
        threadId = await newThread(config);
        agent.reset();
        agent.totalUsd = 0;
        agent.totalTokens.input = 0;
        agent.totalTokens.output = 0;
        turns = 0;
        session = sessions.create(agent.currentModel);
        process.stdout.write(c.dim("new session\n"));
        return;
      case "sessions": {
        const all = sessions.list();
        if (all.length === 0) {
          process.stdout.write(c.dim("no sessions in this folder yet\n"));
          return;
        }
        all.forEach((s, i) => process.stdout.write(`  ${s.id === session.id ? c.amber("\u25CF") : c.dim("\u25CB")} ${i + 1}. ${sessions.label(s)}
`));
        process.stdout.write(c.dim("  /resume <n> to continue one\n"));
        return;
      }
      case "resume": {
        const n = Number(rest[0]);
        const pick = sessions.list()[n - 1];
        if (rest[0] === void 0 || !Number.isInteger(n) || pick === void 0) {
          process.stdout.write(`${c.red("usage:")} /resume <n> \u2014 see /sessions
`);
          return;
        }
        session = pick;
        load(pick);
        process.stdout.write(c.dim(`continuing ${sessions.label(pick)}
`));
        return;
      }
      case "agent":
        chat = false;
        process.stdout.write(c.dim(`agent mode in ${cwd}
`));
        return;
      case "chat":
        chat = true;
        process.stdout.write(c.dim("chat mode: tasks on the brain, no tools\n"));
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
  };
  const agentTurn = async (text2) => {
    const expanded = expandMentions(text2, cwd);
    for (const m of expanded.mentions) process.stdout.write(`  ${c.dim("\u23BF")}  ${c.dim(`attached ${m.token} (${m.lines} line${m.lines === 1 ? "" : "s"})`)}
`);
    for (const s of expanded.skipped) process.stdout.write(`  ${c.dim("\u23BF")}  ${c.red(`not attached: ${s} holds secrets`)}
`);
    const started = Date.now();
    waitedMs = 0;
    live.start();
    running = true;
    try {
      const done = await agent.turn(expanded.text);
      if (done.cancelled === true) {
        live.finish();
        process.stdout.write(`${c.dim("cancelled \u2014 that turn was dropped")}

`);
        return;
      }
      turns++;
      live.learn({ usd: done.usd });
      const total = ((Date.now() - started - waitedMs) / 1e3).toFixed(1);
      const waited = waitedMs > 1e3 ? ` \xB7 ${Math.round(waitedMs / 1e3)}s at questions` : "";
      const receipt = live.finish(
        ` \xB7 ${fmtTokens(done.tokens.input)} in \xB7 ${fmtTokens(done.tokens.output)} out \xB7 ${done.rounds} round${done.rounds > 1 ? "s" : ""}${done.toolCalls > 0 ? ` \xB7 ${done.toolCalls} tool call${done.toolCalls > 1 ? "s" : ""}` : ""} \xB7 ${total}s${waited}`
      );
      process.stdout.write(`
${renderMarkdown(done.text)}

${receipt}

`);
      persist();
    } catch (err) {
      live.finish();
      process.stdout.write(`${c.red("\u2717")} ${err instanceof Error ? err.message : String(err)}

`);
    } finally {
      running = false;
    }
  };
  const chatTurn = async (text2) => {
    let steps = 0;
    live.start();
    live.think("routing");
    try {
      const outcome = await streamTask(
        config,
        {
          input: text2,
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
      persist();
    } catch (err) {
      live.finish();
      process.stdout.write(`${c.red("\u2717")} ${err instanceof Error ? err.message : String(err)}

`);
    }
  };
  const handle = async (line) => {
    const text2 = line.trim();
    if (text2 === "") return;
    if (text2 === "help" || text2 === "?") {
      process.stdout.write(`${REPL_HELP}
`);
      return;
    }
    if (text2.startsWith("/")) {
      const [cmd, ...rest] = text2.slice(1).split(/\s+/);
      await command(cmd ?? "", rest);
      return;
    }
    if (text2.startsWith("!")) {
      const own = text2.slice(1).trim();
      if (own === "") return;
      process.stdout.write(`${c.dim(`$ ${own}`)}
`);
      const output = scrubSecrets(await runOwnCommand(own, cwd));
      const lines = output.split("\n");
      process.stdout.write(`${lines.slice(0, 40).map((l) => c.dim(l)).join("\n")}${lines.length > 40 ? c.dim(`
\u2026 +${lines.length - 40} lines`) : ""}
`);
      agent.addExchange(`[The user ran this themselves in ${cwd}]
$ ${own}
${clip(output, 12e3)}`, "Noted.");
      persist();
      return;
    }
    if (chat) await chatTurn(text2);
    else await agentTurn(text2);
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
  const interrupt = () => {
    if (running && agent.cancel()) {
      live.print(c.dim("cancelling\u2026"));
      return true;
    }
    if (pendingAnswer !== null) {
      const resolve = pendingAnswer;
      pendingAnswer = null;
      process.stdout.write("\n");
      resolve("n");
      return true;
    }
    return false;
  };
  rl.on("SIGINT", () => {
    if (!interrupt()) leave();
  });
  if (isTTY) {
    process.stdin.on("keypress", (_char, key) => {
      if (key?.name === "escape") interrupt();
    });
  }
  prompt();
}

// src/main.ts
var NPM_PACKAGE = "bitos-cli";
var HELP = `bitos \u2014 every web3 intelligence, one command away.

USAGE
  bitos                     Open the console: an agent in this folder
  bitos --model <id>        The console on a pinned model (provider::model, or auto)
  bitos --mode <m>          The console in a mode: ask (default) \xB7 edits \xB7 all \xB7 plan
  bitos --new               The console on a fresh session (it continues the folder's last one otherwise)
  bitos run "task\u2026"         One task in this folder, no console: the answer on stdout, tool activity on stderr;
                            --json for a JSON result \xB7 --yes to allow edits and commands \xB7 --mode edits|plan
  bitos <command> [args]

COMMANDS
  models                    The models you can pin, from every network
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
  config set <k> <v>        Set gateway | beta-password | model (provider::model, or auto)
  install                   Put bitos on your PATH (~/.local/bin) so it runs anywhere
  update                    Replace this binary with the gateway's latest (npm installs: npm i -g bitos-cli@latest)
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
var sleep = (ms2) => new Promise((r) => setTimeout(r, ms2));
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
          name: `bitos cli on ${os9.hostname()}`
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
  const ms2 = typeof task.latencyMs === "number" ? `${(task.latencyMs / 1e3).toFixed(1)}s` : "?";
  process.stderr.write(`
[${task.category ?? "?"} \xB7 ${task.agentId ?? "?"} \xB7 ${cost} \xB7 ${ms2}]
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
  const stat = fs11.statSync(root);
  if (stat.isFile()) return [{ abs: root, dir: base, name: path13.basename(root) }];
  if (!stat.isDirectory()) return [];
  const folder = base === "" ? path13.basename(root) : `${base}/${path13.basename(root)}`;
  const found = [];
  for (const entry of fs11.readdirSync(root)) {
    if (entry.startsWith(".")) continue;
    found.push(...walk(path13.join(root, entry), folder));
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
      if (!fs11.existsSync(target)) fatal(`no such path: ${target}`);
      for (const f of walk(path13.resolve(target), "")) {
        const bytes = fs11.readFileSync(f.abs);
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
    fs11.writeFileSync(dest, bytes);
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
  else if (k === "model") {
    if (v === "auto") delete config.model;
    else if (isModelId(v)) config.model = v;
    else fatal(`not a model id: '${v}' \u2014 provider::model (see: bitos models) or auto`);
  } else fatal(`unknown config key '${k}'`);
  saveConfig(config);
  out("saved");
}
async function cmdModels() {
  const config = loadConfig();
  const catalog = await getJson(config, "/api/models");
  const current = config.model ?? AUTO_MODEL;
  out(`${current === AUTO_MODEL ? "\u25CF" : "\u25CB"} auto  (routed by the brain, code lane)`);
  for (const p of catalog.providers) {
    for (const m of p.models) {
      const id = `${p.id}::${m}`;
      out(`${id === current ? "\u25CF" : "\u25CB"} ${id}`);
    }
  }
  out("\npin one: bitos config set model <id>   \xB7   one run: bitos --model <id>   \xB7   back: bitos config set model auto");
}
function cmdInstall(args) {
  const self = process.argv[1] ?? fatal("cannot locate this binary");
  if (installedWithNpm(self)) {
    out("bitos is installed through npm and already on your PATH; nothing to do.");
    return;
  }
  const code = fs11.readFileSync(self, "utf8");
  if (!code.startsWith("#!/usr/bin/env node")) fatal("run install from the downloaded bitos script");
  const explicit = args.indexOf("--dir");
  const candidates = explicit !== -1 ? [args[explicit + 1] ?? fatal("--dir needs a path")] : ["/usr/local/bin", path13.join(os9.homedir(), ".local", "bin")];
  for (const dir of candidates) {
    try {
      fs11.mkdirSync(dir, { recursive: true });
      fs11.accessSync(dir, fs11.constants.W_OK);
    } catch {
      continue;
    }
    const dest = path13.join(dir, "bitos");
    fs11.writeFileSync(dest, code, { mode: 493 });
    out(`installed ${dest}`);
    const onPath = (process.env["PATH"] ?? "").split(path13.delimiter).includes(dir);
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
function cmdVersion() {
  out(describeInstall(process.argv[1], loadConfig().gateway));
  const shadow = shadowNote(process.argv[1]);
  if (shadow !== null) out(shadow);
}
async function cmdUpdate() {
  const self = process.argv[1] ?? fatal("cannot locate this binary");
  if (installedWithNpm(self)) {
    out("bitos is installed through npm \u2014 update it the npm way:");
    out(`  npm i -g ${NPM_PACKAGE}@latest`);
    return;
  }
  if (hiddenBy(self)?.npm === true) fatal(`${shadowNote(self)}
nothing updated: npm keeps that one current, and this copy would still run first`);
  const config = loadConfig();
  const res = await fetch(`${config.gateway}/download/bitos.mjs`, {
    headers: config.betaPassword !== void 0 ? { "x-beta-password": config.betaPassword } : {}
  });
  if (!res.ok) fatal(`the gateway has no CLI build to offer (HTTP ${res.status})`);
  const code = await res.text();
  if (!code.startsWith("#!/usr/bin/env node")) fatal("downloaded file does not look like bitos");
  fs11.writeFileSync(self, code, { mode: 493 });
  out(`updated ${self}`);
}
async function cmdRun(args) {
  const json = args.includes("--json");
  const yes = args.includes("--yes");
  const task = args.filter((a) => a !== "--json" && a !== "--yes").join(" ").trim();
  if (task === "") fatal('usage: bitos run "task" [--json] [--yes] [--mode edits|plan|all]');
  const config = loadConfig();
  const cwd = process.cwd();
  const fromEnv = process.env["BITOS_MODE"];
  const mode = yes ? "all" : fromEnv !== void 0 && isMode(fromEnv) ? fromEnv : "ask";
  const rules = new PermissionStore(cwd).load();
  const log = (line) => {
    process.stderr.write(`${line}
`);
  };
  const ctx = {
    cwd,
    confirm: async (tool, summary, detail) => {
      const verdict = policy(mode, rules, cwd, tool, summary, detail);
      if (verdict.decision === "allow") return true;
      return {
        reason: verdict.decision === "deny" ? `blocked by the permission rule ${verdict.rule} of this folder` : `nobody can approve this in a non-interactive run; start bitos run with --yes or --mode edits, or add a rule to ${PERMISSIONS_FILE}`
      };
    },
    note: (line) => log(`  ${line}`)
  };
  let callArgs = {};
  const agent = new Agent2(
    config,
    ctx,
    {
      onThinking: () => void 0,
      onRoute: () => void 0,
      onToolStart: (name, toolArgs) => {
        callArgs = toolArgs;
        log(renderCall(name, toolArgs));
      },
      onToolEnd: (name, result, took) => {
        for (const line of renderResult(name, callArgs, result, took)) log(line);
      }
    },
    process.env["BITOS_MODEL"] ?? config.model ?? AUTO_MODEL
  );
  agent.setPlanMode(mode === "plan");
  const done = await agent.turn(expandMentions(task, cwd).text);
  if (json) {
    out(JSON.stringify({ text: done.text, model: agent.currentModel, mode, usd: done.usd, tokens: done.tokens, rounds: done.rounds, toolCalls: done.toolCalls }));
    return;
  }
  out(process.stdout.isTTY ? renderMarkdown(done.text) : done.text);
  log(
    `
[$${done.usd.toFixed(4)} \xB7 ${fmtTokens(done.tokens.input)} in \xB7 ${fmtTokens(done.tokens.output)} out \xB7 ${done.rounds} round${done.rounds === 1 ? "" : "s"} \xB7 ${done.toolCalls} tool call${done.toolCalls === 1 ? "" : "s"}]`
  );
}
async function main() {
  const major = Number(process.versions.node.split(".")[0]);
  if (major < 20) fatal(`Node ${process.versions.node} is too old \u2014 bitos needs Node 20 or newer`);
  const argv = process.argv.slice(2).filter((a) => a !== "--yes" && a !== "--new");
  const modeFlag = argv.indexOf("--mode");
  if (modeFlag !== -1) {
    const want = argv[modeFlag + 1] ?? fatal("--mode needs one of: ask, edits, all, plan");
    if (!isMode(want)) fatal(`not a mode: '${want}' \u2014 ask, edits, all or plan`);
    process.env["BITOS_MODE"] = want;
    argv.splice(modeFlag, 2);
  }
  const modelFlag = argv.indexOf("--model");
  if (modelFlag !== -1) {
    const want = argv[modelFlag + 1] ?? fatal("--model needs an id \u2014 see: bitos models");
    const model = want === "auto" ? AUTO_MODEL : want;
    if (!isModelId(model)) fatal(`not a model id: '${want}' \u2014 provider::model (see: bitos models) or auto`);
    process.env["BITOS_MODEL"] = model;
    argv.splice(modelFlag, 2);
  }
  if (argv[0] === "models") {
    await cmdModels();
    return;
  }
  const [cmd, ...args] = argv;
  try {
    switch (cmd) {
      case void 0:
      case "chat":
      case "console":
        return await runRepl();
      case "run":
        return await cmdRun(args);
      case "version":
      case "--version":
      case "-v":
        return cmdVersion();
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
// bitos 0.2.0 build 2026-10-26
