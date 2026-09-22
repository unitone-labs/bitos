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

Without npm, from a running gateway:

```
curl -fsSL https://bitos.dev/download/bitos.mjs -o bitos && chmod +x bitos
./bitos install         # copies it to ~/.local/bin (or /usr/local/bin)
```

From the repo: `pnpm --filter bitos build && node apps/cli/dist/bitos.mjs help`.

## The console

`bitos` with no arguments is the console. In **agent** mode (the default)
the model can read, search, edit and write files here and run commands.
Reads just happen; writes and commands ask first (`y` / `n` / `a` for always
this session, or start with `--yes`). Each tool call prints as it runs; the
answer gets a light Markdown pass.

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
bitos ask "price of sn64"          # one task, receipt on stderr
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
