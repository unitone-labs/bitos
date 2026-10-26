# bitos — the BitOS console in your terminal

An agent on your machine, with inference from decentralized web3 networks.
It reads, searches, edits and writes files where you run it and runs shell
commands, looping through tool calls until it can answer — in the spirit
of Claude Code, with the model coming from Bittensor subnets and the other
networks BitOS routes to, and a receipt for every step.

## Install

```
npm i -g bitos-cli      # Node 20 or newer; the command is `bitos`
bitos login             # your browser opens; your wallet signs there
bitos                   # the console
```

`bitos update` tells you how to update the way you installed. On a
private-beta gateway, set the password first: `bitos config set beta-password …`.

If the banner says `copy at …` although you installed with npm, a
downloaded copy earlier on your PATH is running instead of the npm one.
The banner (and `bitos version`, `bitos update`) names the copy that is
hidden and the `rm` that puts npm's back in charge.

Without npm, from a running gateway:

```
curl -fsSL https://bitos.dev/download/bitos.mjs -o bitos && chmod +x bitos
./bitos install         # copies it to ~/.local/bin (or /usr/local/bin)
```

From the repo: `pnpm --filter bitos build && node apps/cli/dist/bitos.mjs help`.

## The console

`bitos` with no arguments is the console. In **agent** mode (the default)
the model can read, search, edit and write files here and run commands.
Reads just happen. Writes and commands ask first — unless the mode, a rule
of this folder or the built-in read-only list already answers (see
Permissions). Each tool call prints as it runs, the way Claude Code taught
everyone to read it: one line for the call, a few for what came back, a
diff before every edit; the answer gets a light Markdown pass; the receipt
closes the turn.

While the model works, one line shows the route (lane, network, model),
how long the turn has run and what it has used so far; it fits the
terminal's width. `Esc` or Ctrl-C cancels the turn. `@path` in a message
attaches that file; a line starting with `!` runs a command yourself and
puts its output on the record for the agent. Tab completes `/commands`
and `@paths`.

What the agent knows about your project:

- **Rules.** The nearest `AGENTS.md` walking up from the folder you are in
  to the git root (`CLAUDE.md` as the fallback) rides in the system prompt,
  and so do your own rules from `~/.config/bitos/AGENTS.md` (or
  `~/.claude/CLAUDE.md`).
- **Skills.** `SKILL.md` folders in `.bitos/skills`, `.claude/skills` and
  `.agents/skills` (project, walking up) and in `~/.config/bitos/skills`,
  `~/.claude/skills`, `~/.agents/skills` appear as a `skill` tool the model
  loads on demand — the same folders OpenCode and Claude Code read.
- **Secrets stay out.** `.env` and `.env.*` files (templates excepted) are
  never read or searched; the model is told to ask you for the one value.
- **No loops.** The same call three times running is stopped and you are
  asked.

## Permissions

At a question, `Enter` or `y` says yes once; `a` allows that kind of call
from now on in this folder (the rule is written to
`.bitos/permissions.json`, personal, never committed); `s` stops the
questions for the session; `n` says no — and `n <why>` tells the agent
why, which is what it hears instead of trying another way.

Rules are spelled the way Claude Code spells them: `run_command(git:*)`
(any command starting with `git`), `run_command(npm test)` (exactly that),
`edit_file`, `write_file(src/:*)`. `/permissions` lists them,
`/permissions allow <rule>` and `deny <rule>` add one, `clear` empties the
file. A deny rule wins in every mode.

Modes, with `/mode` in the console or `--mode` when starting it:

- `ask` (default): writes and commands ask, unless a rule answers.
- `edits`: file edits inside this folder just happen, and so do `mkdir`,
  `touch`, `mv` and `cp` inside it; commands still ask.
- `all`: nothing asks (`/yes`, `--yes`).
- `plan`: the model gets the read-only tools only and is told to plan;
  `/mode ask` to build.

Built in, in every mode: a command that only reads — `ls`, `cat`, `grep`,
`find`, `git status`, `git diff`, `npm ls`, a pipeline of those — runs
without a question as long as it stays inside this folder and names no
secrets file. Command output that looks like a secret (`API_KEY=…`, a
line from `.env`) is redacted before the model sees it.

## Sessions

The conversation is kept in `.bitos/sessions/` in the folder you run
`bitos` in, written after every turn, and the next `bitos` there continues
it — say "continue" and the agent knows what you meant. `bitos --new`
starts fresh, `/sessions` lists the folder's sessions, `/resume <n>`
switches to one. The folder ignores itself in git. Ctrl-C cancels the
turn in progress (the messages of that turn are dropped) and leaves only
when nothing is running.

A long conversation folds: past 150k characters the next turn starts by
compacting the history into a summary written by the model (what was
asked, what was done, what is open), and `/compact [focus]` does it on
demand. `/usage` shows how much context the session carries.

## Models and cost

By default the brain routes every call (`auto`: the code lane, the best
network for it). Pin a model instead — any `provider::model` the gateway
runs, listed by `/models` in the console or `bitos models` outside it:

```
/models                                   what you can pin, ● marks the current one
/model chutes::deepseek-ai/DeepSeek-V3.2  pin it, for this session and the next
/model auto                               back to the brain
bitos --model gm::gpt-5.4 …               one run on a pinned model
bitos config set model <id|auto>          the saved default
```

Every turn's receipt shows what it cost and how many tokens went in and
out; `/usage` totals the session. The banner says which folder, branch,
model, rules file (`AGENTS.md`) and skills the agent starts with.

The loop lives in the terminal, not on the gateway: it speaks OpenAI-style
tool calling to `/v1/chat/completions` (model `bitos/code`, or a pinned
`provider::model`), so the tools run against YOUR files and shell while
inference comes from web3 networks. Login mints the personal API key that
transport needs.

`/chat` switches to the plain mode (one task per line on the brain, no
tools, `/lane` pins a lane); `/agent` switches back. `/new` forgets the
conversation, `/exit` leaves. Piping a script in works too — lines run in
order and the process exits after the last answer.

## Commands

```
bitos login                        # browser sign-in
bitos run "add tests for utils"    # one agent task here, no console: --json, --yes, --mode edits|plan
bitos ask "price of sn64"          # one task on the brain, receipt on stderr
bitos ask --lane code "…"          # pin the lane
bitos files put ./docs             # upload a whole folder
bitos files ls                     # uploads and memory notes, by name
bitos files get docs/plan.txt
bitos files cat memory/weather-in-lisbon.md   # print a memory note
bitos files rm docs/plan.txt
bitos status
bitos config set beta-password …   # private-beta gateways
```

Files are your account's folder on the gateway, mirrored to the storage
network (Hippius, SN75) and removed from it when you delete. Memory notes —
one Markdown file per conversation, naming the facts the conversation
taught the platform — are written into `memory/` as you talk; the same list
shows under Files in the web app.

Sign-in never touches your keys: `bitos login` opens bitos.dev/connect in
your real browser, your wallet extension signs there, and the session token
comes back through a one-time code (the same handoff the desktop app uses).
