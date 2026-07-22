# pi-herdr-subagents

Async subagents for [pi](https://github.com/badlogic/pi-mono) running exclusively in [herdr](https://herdr.dev). Spawn, orchestrate, and manage sub-agent sessions in dedicated herdr tabs or panes. **Fully non-blocking** — the main agent keeps working while subagents run in the background.

## How It Works

Call `subagent()` and it **returns immediately**. The sub-agent runs in its own terminal pane. A live widget above the input shows all tracked agents with their projected state — for example `starting`, `active`, `waiting`, `interrupted`, `stalled`, `running`, or `finalizing`. The header summarizes **active** (processing) vs **open** (not processing). When every tracked subagent is open, the border switches to amber. When a sub-agent finishes, its result is **steered back** into the main session as an async notification — triggering a new turn so the agent can process it.

```
╭─ Subagents ──────────────────── 1 active · 1 open ─╮
│ 00:23  Scout: Auth (scout)        active · bash 7m │
│ 00:45  Scout: DB (scout)                waiting 2m │
╰────────────────────────────────────────────────────╯
```

For parallel execution, just call `subagent` multiple times — they all run concurrently:

```typescript
subagent({ name: "Scout: Auth", agent: "scout", task: "Analyze auth module" });
subagent({ name: "Scout: DB", agent: "scout", task: "Map database schema" });
// Both return immediately, results steer back independently
```

## Development

Run the normal test suite and lint locally:

```bash
npm test
npm run lint
```

Run the deterministic process smoke test from any shell:

```bash
npm run test:smoke
```

The smoke test starts an isolated named headless herdr server, then launches one real Owner Pi session and one real child Pi session inside it. Both use a scripted test-only Faux Provider, so the test requires no model credentials, network access, paid inference, or existing herdr TUI session. It requires the `herdr` executable, cleans up the temporary named session, cannot change the user's active workspace or pane focus, and has a 30-second hard timeout.

## Install

Install the package from npm:

```bash
pi install npm:pi-herdr-subagents
```

This project does not install or load `HazAT/pi-interactive-subagents` automatically.

Changing the `package.json` version on `main` automatically creates a matching Git tag and GitHub Release, generates release notes, and publishes the package to npm. For authentication, versioning, verification, and troubleshooting, see [RELEASING.md](RELEASING.md).

Start herdr, then run pi inside it:

```bash
herdr
pi
```

herdr is the only supported terminal environment. The extension requires `HERDR_ENV=1` and the `herdr` CLI to be available.

If your shell startup is slow and subagent commands sometimes get dropped before the prompt is ready, set `PI_SUBAGENT_SHELL_READY_DELAY_MS` to a higher value (defaults to `500`):

```bash
export PI_SUBAGENT_SHELL_READY_DELAY_MS=2500
```

Subagent tabs and panes are created without stealing keyboard focus. Launch commands target child panes by explicit ID, so focus and command delivery are independent. Note: the `interactive` option controls parent status notifications, not terminal focus.

## What's Included

### Extensions

**Subagents** — 4 primary orchestration tools, 3 durable collaboration tools, 3 commands, plus subagent-only lifecycle tools:

| Tool                 | Description                                                                                 |
| -------------------- | ------------------------------------------------------------------------------------------- |
| `subagent`           | Spawn a sub-agent in a dedicated herdr pane (async — returns immediately)             |
| `subagent_interrupt` | Interrupt a running Pi-backed subagent's current turn                                       |
| `subagents_list`     | List available agent definitions                                                            |
| `subagent_resume`    | Resume a previous sub-agent session (async)                                                 |

| Collaboration tool | Description |
| ------------------ | ----------- |
| `agent_send`       | Send Signals and Requests, or Answer a Request |
| `agent_cancel`     | Cancel an unresolved Request created by the current Agent |
| `agent_inspect`    | Inspect capability-filtered durable Agent and Request state |

| Command                    | Description                          |
| -------------------------- | ------------------------------------ |
| `/plan`                    | Start a full planning workflow       |
| `/iterate`                 | Fork into a subagent for quick fixes |
| `/subagent <agent> <task>` | Spawn a named agent directly         |

### Bundled Agents

| Agent             | Default runtime       | Role                                                                                     |
| ----------------- | --------------------- | ---------------------------------------------------------------------------------------- |
| **planner**       | Config, then parent   | Brainstorming — clarifies requirements, explores approaches, writes plans, creates todos |
| **scout**         | Config, then parent   | Fast codebase reconnaissance — maps files, patterns, conventions                         |
| **worker**        | Config, then parent   | Implements tasks from todos — writes code, runs tests, makes polished commits            |
| **reviewer**      | Config, then parent   | Reviews code for bugs, security issues, correctness                                      |
| **visual-tester** | Config, then parent   | Visual QA via Chrome CDP — screenshots, responsive testing, interaction testing          |

Bundled agents use model defaults from `config.json` when configured; otherwise they inherit the parent model. Thinking defaults still come from agent frontmatter or the parent level. The orchestrating agent can override either field for a specific task using an exact authenticated model ID and a supported Pi thinking level. Prefer changing thinking before changing models.

Agent discovery follows priority: **project-local** (`.pi/agents/`) > **global** (`~/.pi/agent/agents/`) > **package-bundled**. Override any bundled agent by placing your own version in the higher-priority location.

---

## Async Subagent Flow

```
1. Agent calls subagent()          → returns immediately ("started")
2. Sub-agent runs in herdr pane    → widget shows live status
3. User keeps chatting             → main session fully interactive
4. Sub-agent explicitly exits      → result steered back; ordinary settlement stays open
5. Main agent processes result     → continues with new context
```

Multiple subagents run concurrently — each steers its result back independently as it finishes. The live widget above the input tracks every agent still in flight:

```
╭─ Subagents ──────────────────── 1 active · 2 open ─╮
│ 01:23  Scout: Auth (scout)            active · write 7m │
│ 00:45  Researcher (researcher)               stalled 4m │
│ 00:12  Scout: DB (scout)                      starting… │
╰─────────────────────────────────────────────────────────╯
```

Completion messages render with a colored background and are expandable with `Ctrl+O` to show the full summary and session file path. Completed rows are removed from the widget as soon as their result is delivered or suppressed.

### In-progress status updates

The widget projects each sub-agent from a **process + turn lifecycle**:

- **Herdr pane inspection** is the coarse authority for whether the child process is present and whether Herdr reports it as idle, working, blocked, or done.
- **Child activity snapshots** enrich the label with Pi-only detail (tool name, streaming, etc.) when available.
- Session JSONL is still used for transcript, resume, lineage, and result extraction — not for liveness.

Projected labels include:

- `starting` — launched; pane/activity confirmation is still settling
- `active` — processing work (agent turn, provider request, streaming, or tool execution)
- `blocked` — Herdr reports the child as blocked
- `waiting` — turn finished; the process is intentionally open for more input or another stage
- `interrupted` — the current turn was cancelled (Escape / `subagent_interrupt`); the process stays open and is **not** treated as active processing
- `stalled` — pane inspection is unhealthy long enough that the parent can no longer trust the run
- `running` — fallback when only coarse process presence is known (e.g. non-Pi backends)
- `finalizing` — completion was observed and delivery is in progress; the process elapsed timer freezes here

The widget header counts **active** vs **open**:

- **active** — `active`, `starting`, `running`, or `blocked`
- **open** — everything else still tracked (`waiting`, `interrupted`, `stalled`, `finalizing`, …)

When `activeCount === 0` (every tracked row is open), the border uses an amber accent. Process elapsed time (`MM:SS` on the left) freezes when the process reaches finalizing/completed/failed. Interrupt does **not** freeze that process clock; the interrupted state shows its own duration on the right while the process remains open.

A fixed internal watchdog marks a run as `stalled` when pane inspection fails or the pane disappears without a completion sidecar; valid long-running `active` or `waiting` states do not become `stalled` just because time passes. When a run enters `stalled` or recovers from it, the parent agent receives a steer message so it can react. All other status transitions stay in the widget only.

**Interactive subagents stay silent.** Long-running user-driven subagents (e.g. `planner`, or any `/iterate` fork) do not wake the parent session on `stalled`/`recovered` transitions — the user is working directly in the subagent's pane, and a steer message there would just burn an orchestrator turn on a no-op "still waiting" ping. The widget still updates normally, and activity snapshots are still recorded/classified regardless of the `interactive` setting. By default, agents with `auto-exit: true` are treated as autonomous and get stall pings; agents without it are treated as interactive and stay quiet. Override per-agent with `interactive: true|false` in frontmatter, or per-spawn with `interactive: true|false` on the tool call.

#### Configuration

Status display is controlled by `config.json` in the extension directory. Copy `config.json.example` to get started:

```bash
cp config.json.example config.json
```

```json
{
  "status": {
    "enabled": true
  },
  "models": {
    "agents": {}
  }
}
```

The copyable example is model-neutral, so it works without requiring credentials for a specific provider. To configure models, replace the empty section with exact IDs from your authenticated model catalog:

```json
{
  "models": {
    "default": "your-provider/your-default-model",
    "agents": {
      "scout": "your-provider/your-fast-model",
      "reviewer": "your-provider/your-review-model"
    }
  }
}
```

`models.default` sets the model for subagents that do not specify a model. `models.agents` sets per-agent defaults, keyed by the agent name passed to `subagent({ agent: ... })`. Explicit `model` tool arguments take precedence, followed by agent frontmatter, per-agent config, the global default, and finally the parent model. Model values must be exact authenticated `provider/model-id` references.

`config.json` is gitignored so local overrides don't get committed.

---

## Spawning Subagents

```typescript
// Named agent with defaults from agent definition or config.json
subagent({ name: "Scout", agent: "scout", task: "Analyze the codebase..." });

// Force a full-context fork for this spawn
subagent({ name: "Iterate", fork: true, task: "Fix the bug where..." });

// Agent defaults can choose a different session-mode via frontmatter
subagent({ name: "Planner", agent: "planner", task: "Work through the design with me" });

// Custom working directory
subagent({ name: "Designer", agent: "game-designer", cwd: "agents/game-designer", task: "..." });
```

### Parameters

| Parameter              | Type    | Default        | Description                                                                                       |
| ---------------------- | ------- | -------------- | ------------------------------------------------------------------------------------------------- |
| `name`                 | string  | required       | Display name (shown in widget and pane title)                                                     |
| `task`                 | string  | required       | Task prompt for the sub-agent                                                                     |
| `agent`                | string  | —              | Load defaults from agent definition                                                               |
| `fork`                 | boolean | `false`        | Force the full-context fork mode for this spawn, overriding any agent `session-mode` frontmatter  |
| `interactive`          | boolean | derived        | Mark this spawn as interactive (don't wake the parent on stall/recovery). Defaults to the agent's `interactive` frontmatter, otherwise the inverse of `auto-exit`. |
| `model`                | string  | configured or parent | Exact authenticated `provider/model-id`; resolution is tool argument → agent frontmatter → per-agent config → global config → parent |
| `thinking`             | string  | parent level   | Pi thinking level (`off` through `max`); omit to inherit the parent                                |
| `systemPrompt`         | string  | —              | Append to system prompt                                                                           |
| `skills`               | string  | —              | Comma-separated skill names                                                                       |
| `tools`                | string  | —              | Comma-separated tool names                                                                        |
| `cwd`                  | string  | —              | Working directory for the sub-agent (see [Role Folders](#role-folders))                           |

---

## Interrupting a running subagent

Use `subagent_interrupt` to cancel the active turn of a running Pi-backed subagent:

```typescript
subagent_interrupt({ id: "abcd1234" });
// or
subagent_interrupt({ name: "Scout" });
```

This sends Escape to the child pane, cancelling the in-progress model turn. The subagent session stays alive — the pane, session file, and background polling all remain intact. The request itself does not claim success: canonical state becomes `interrupted` only after Pi confirms the active turn aborted and fully settled. Until then, status remains active or waiting according to confirmed evidence. If the child starts work later, the durable activation returns to `active`; explicit exit and `caller_ping` still flow through the legacy result path.

This is a turn-level interrupt, not a method for forcibly terminating a subagent session.

> **Note:** Only Pi-backed subagents are supported. Claude-backed runs will return an error.

---

## agent_send — Signals, Requests, and Answers

`agent_send` delivers actionable work to a known Agent or answers an existing Request:

```typescript
// A Signal: no reply obligation.
agent_send({ target: { agent: agentId }, message: "Status update", onAccepted: "continue" });

// A Request: creates one durable Answer obligation using this message ID.
agent_send({ target: { agent: agentId }, message: "Review this", responseRequired: true, onAccepted: "continue" });

// An Answer: destination and delivery timing come from the Request.
agent_send({ target: { request: requestId }, message: "Review complete", onAccepted: "continue" });
```

Subagents may finish through the shared mechanical Completion Gate either with
`agent_complete()` after prior messages are accepted, or by adding
`onAccepted: "complete"` to a final Signal or Answer. Terminal sends cannot
create a new Response Requirement. Completion is atomic with final-message
acceptance and is rejected with structured blockers while durable obligations
remain. The Workflow Owner cannot complete.

`subagent_done` temporarily remains available for the legacy parent-result
relay until its dedicated migration. Protocol-completed activations shut down
gracefully without selecting or relaying a second legacy result.

Only the Agent addressed by a Request may answer it. The first queued Answer closes the Request; retrying that same Answer is idempotent. An Answer can also set `responseRequired: true` to create its own Request atomically. A Request becomes resolved only when its Answer is committed to the requester’s inbox; unresolved Requests remain durable Agent dependencies.

## agent_cancel — Request Cancellation

`agent_cancel` cancels one unresolved Request created by the current Agent:

```typescript
agent_cancel({ request: requestId });
```

Only the original requester has cancellation authority. Cancellation and Answer acceptance arbitrate through one durable commit: whichever commits first wins, and the losing operation has no Request-state effects. Retrying a successful cancellation is idempotent and cannot create another notice.

If the responder has not durably received the Request, cancellation suppresses its inbox pointer without waking the responder. If the Request was delivered, the runtime queues one correlated Steer Protocol Notice, using ordinary inbox ordering and delivery confirmation, so a waiting responder wakes with actionable cancellation context. The notice is runtime-authored; its canonical payload and delivery state live on the Request rather than in an Agent transcript.

Cancellation removes the requester's dependency and permits completion when no other blockers remain. It does not fabricate an Answer, undo completed work, or claim to roll back external side effects. This tool cancels Requests only; it does not cancel Agent activations.

## agent_inspect — Read-only Workflow State

`agent_inspect` reads capability-filtered durable state without waking Agents, creating activations, reserving ownership, changing protocol state, or writing transcript content:

```typescript
agent_inspect({ target: { agent: agentId } });
agent_inspect({ target: { request: requestId } });
agent_inspect({ target: { directChildren: true } });
agent_inspect({ target: { workflow: true } }); // Workflow Owner only
```

A caller may inspect a known Agent ID, but that does not grant control, ownership, or enumeration. Non-owner Agents can enumerate only their direct children; the Workflow Owner can enumerate the complete Workflow. Human Interrupt and undeclared-settlement state is redacted so canonical question, response, notice payloads, and model-facing identities are not duplicated.

Inspection reports Agent activation/waiting state, Human Interrupt state, undeclared-settlement correction state, and Request `open`, `answered`, `resolved`, or `cancelled` state. Cancelled Request projections show the satisfied requester dependency and cancellation-notice delivery metadata without duplicating the canonical notice payload. Orphan state, Operation Review details, and Incident Visibility remain deferred to their producer work and are not represented by placeholders.

An explicit child `tools` allowlist remains exact: include `agent_inspect` or `agent_cancel` when that restricted child needs the corresponding operation. Without an explicit allowlist both are available by default; `deny-tools` removes either tool by name.

## caller_ping — Child-to-Parent Help Request

The `caller_ping` tool lets a subagent request help from its parent agent. When called, the child session **exits** and the parent receives a notification with the help message. The parent can then **resume** the child session with a response using `subagent_resume`.

**`caller_ping` parameters:**
- `message` (required): What you need help with

**`subagent_resume` parameters:**
- `sessionPath` (required): Path to the child session `.jsonl` file
- `name` (optional): Display name for the resumed pane (defaults to `Resume`)
- `message` (optional): Follow-up prompt to send after resuming
- `autoExit` (optional): Whether the resumed session should auto-exit after its next response. Defaults to `true` for autonomous follow-up work; set `false` when resuming for an interactive handoff.

**Interaction flow:**
1. Child calls `caller_ping({ message: "Not sure which schema to use" })`
2. Child session exits (like `subagent_done`)
3. Parent receives a steer notification: *"Sub-agent Worker needs help: Not sure which schema to use"*
4. Parent resumes the child session via `subagent_resume` with the response
5. Child picks up where it left off with the parent's guidance

**Example:**
```typescript
// Inside a worker subagent
await caller_ping({
  message: "Found two conflicting migration files — should I use v1 or v2?"
});
// Session exits here. Parent receives the ping, then resumes this session
// with guidance like "Use v2, v1 is deprecated"
```

> **Note:** `caller_ping` is only available inside subagent contexts. Calling it from a standalone pi session returns an error.

---

## The `/plan` Workflow

The `/plan` command orchestrates a full planning-to-implementation pipeline.

```
/plan Add a dark mode toggle to the settings page
```

```
Phase 1: Investigation    → Quick codebase scan
Phase 2: Planning         → Interactive planner subagent (user collaborates)
Phase 3: Review Plan      → Confirm todos, adjust if needed
Phase 4: Execute          → Scout + sequential workers implement todos
Phase 5: Review           → Reviewer subagent checks all changes
```

The parent workspace and tab names stay unchanged. Subagents are created in newly named tabs or panes for each phase.

---

## The `/iterate` Workflow

For quick, focused work without polluting the main session's context.

```
/iterate Fix the off-by-one error in the pagination logic
```

This always forks the current session into a subagent with full conversation context. It does not inherit an agent default `session-mode`. Make the fix, verify it, and exit to return. The main session gets a summary of what was done.

---

## Custom Agents

Place a `.md` file in `.pi/agents/` (project) or `~/.pi/agent/agents/` (global):

```markdown
---
name: my-agent
description: Does something specific
model: anthropic/claude-sonnet-4-6
thinking: minimal
tools: read, bash, edit, write
session-mode: lineage-only
spawning: false
---

# My Agent

You are a specialized agent that does X...
```

### Frontmatter Reference

| Field         | Type    | Description                                                                                                                                                                                                                                                                 |
| ------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`        | string  | Agent name (used in `agent: "my-agent"`)                                                                                                                                                                                                                                    |
| `description` | string  | Shown in `subagents_list` output                                                                                                                                                                                                                                            |
| `model`       | string  | Optional exact authenticated model default; omit to inherit the parent                                                                                                                                                                                                      |
| `thinking`    | string  | Optional Pi thinking default (`off` through `max`); omit to inherit the parent                                                                                                                                                                                                                                 |
| `tools`       | string  | Comma-separated **native pi tools only**: `read`, `bash`, `edit`, `write`, `grep`, `find`, `ls`                                                                                                                                                                             |
| `skills`      | string  | Comma-separated skill names to auto-load                                                                                                                                                                                                                                    |
| `session-mode` | string | Default child-session mode: `standalone`, `lineage-only`, or `fork` |
| `spawning`    | boolean | Set `false` to deny all subagent-spawning tools                                                                                                                                                                                                                             |
| `deny-tools`  | string  | Comma-separated extension tool names to deny                                                                                                                                                                                                                                |
| `auto-exit`   | boolean | Auto-shutdown when the agent finishes its turn — no `subagent_done` call needed. If the user sends any input, auto-exit is permanently disabled and the user takes over the session. Recommended for autonomous agents (scout, worker); not for interactive ones (planner). Also determines the default value of `interactive` (see below). |
| `interactive` | boolean | derived        | Override whether stall/recovery transitions wake the parent session. Defaults to the inverse of `auto-exit`: autonomous agents (`auto-exit: true`) are non-interactive and get stall pings; agents without `auto-exit` are interactive and stay quiet. Explicit values take precedence. |
| `cwd`         | string  | Default working directory (absolute or relative to project root)                                                                                                                                                                                                            |
| `disable-model-invocation` | boolean | Hide this agent from discovery surfaces like `subagents_list`. The agent still remains directly invokable by explicit name via `subagent({ agent: "name", ... })`. |

---

Discovery still resolves precedence before visibility filtering. If a project-local hidden agent has the same name as a visible global or bundled agent, the hidden project agent wins and the lower-precedence agent does not appear in `subagents_list`.

### `session-mode`

Choose how a subagent session starts:

- `standalone` — default fresh session with no lineage link to the caller
- `lineage-only` — fresh blank child session with `parentSession` linkage, but no copied turns from the caller
- `fork` — linked child session seeded with the caller's prior conversation context

`lineage-only` is useful when you want session discovery and fork lineage UX to show the relationship later, but you do **not** want the child to inherit the parent's turns.

`fork: true` on the tool call always forces the `fork` mode for that specific spawn. `/iterate` uses this explicit override on purpose.

```yaml
---
name: planner
session-mode: lineage-only
---
```

### `auto-exit`

When set to `true`, the agent session shuts down automatically as soon as the agent finishes its turn — no explicit `subagent_done` call is needed.

**Behavior:**

- The session closes after the agent's final message (on the `agent_end` event)
- If the user sends **any input** before the agent finishes, auto-exit is permanently disabled for that session — the user takes over interactively
- The modeHint injected into the agent's task is adjusted accordingly: autonomous agents see "Complete your task autonomously." rather than instructions to call `subagent_done`

**When to use:**

- ✅ Autonomous agents (scout, worker, reviewer) that run to completion
- ❌ Interactive agents (planner, iterate) where the user drives the session

```yaml
---
name: scout
auto-exit: true
---
```

### `interactive`

Controls whether status transitions (`stalled`, `recovered`) wake the parent session with a steer message.

**Default:** the inverse of `auto-exit`. Autonomous agents (`auto-exit: true`) are non-interactive and ping the parent on stall/recovery; agents without `auto-exit` are interactive and stay quiet. Bare spawns with no agent defs (e.g. `/iterate` with `fork: true`) are treated as interactive.

**Why it exists:** Interactive agents can run for minutes or hours while the user thinks, types, and reads in the subagent's pane. Child snapshots still update the widget, but stalled/recovered supervision messages rarely need to wake the parent for user-driven sessions. Skipping the steer keeps the parent quiet until the child actually finishes.

**When to override:**

- Set `interactive: false` on an agent that doesn't auto-exit but you still want stall pings for
- Set `interactive: true` on an autonomous agent you'd rather check on yourself

```yaml
---
name: planner
# interactive defaults to true because auto-exit is not set
---
```

Or per spawn:

```typescript
subagent({ name: "Scout", agent: "scout", interactive: true, task: "..." });
```

---

## Tool Access Control

By default, every sub-agent can spawn further sub-agents. Control this with frontmatter:

### `spawning: false`

Denies all subagent lifecycle tools (`subagent`, `subagent_interrupt`, `subagents_list`, `subagent_resume`):

```yaml
---
name: worker
spawning: false
---
```

### `deny-tools`

Fine-grained control over individual extension tools:

```yaml
---
name: focused-agent
deny-tools: subagent
---
```

### Recommended Configuration

| Agent      | `spawning`  | Rationale                                    |
| ---------- | ----------- | -------------------------------------------- |
| planner    | _(default)_ | Legitimately spawns scouts for investigation |
| worker     | `false`     | Should implement tasks, not delegate         |
| researcher | `false`     | Should research, not spawn                   |
| reviewer   | `false`     | Should review, not spawn                     |
| scout      | `false`     | Should gather context, not spawn             |

---

## Role Folders

The `cwd` parameter lets sub-agents start in a specific directory with its own configuration:

```
project/
├── agents/
│   ├── game-designer/
│   │   └── CLAUDE.md          ← "You are a game designer..."
│   ├── sre/
│   │   ├── CLAUDE.md          ← "You are an SRE specialist..."
│   │   └── .pi/skills/        ← SRE-specific skills
│   └── narrative/
│       └── CLAUDE.md          ← "You are a narrative designer..."
```

```typescript
subagent({ name: "Game Designer", cwd: "agents/game-designer", task: "Design the combat system" });
subagent({ name: "SRE", cwd: "agents/sre", task: "Review deployment pipeline" });
```

Set a default `cwd` in agent frontmatter:

```yaml
---
name: game-designer
cwd: ./agents/game-designer
spawning: false
---
```

---

## Tools Widget

Every sub-agent session displays a compact tools widget showing available and denied tools. Toggle with `Ctrl+J`:

```
[scout] — 12 tools · 4 denied  (Ctrl+J)              ← collapsed
[scout] — 12 available  (Ctrl+J to collapse)          ← expanded
  read, bash, edit, write, todo, ...
  denied: subagent, subagents_list, ...
```

---

## Requirements

- [pi](https://github.com/badlogic/pi-mono) — the coding agent
- [herdr](https://herdr.dev) — the required terminal workspace

```bash
herdr
pi
```

Other multiplexers and terminal backends are not supported.

---

## Acknowledgements

The sub-agent status supervision and turn-only interruption features were inspired by [RepoPrompt](https://repoprompt.com/)'s sub-agent snapshot polling and run cancellation features.

---

## License

MIT
