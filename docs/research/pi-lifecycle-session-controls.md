# Pi lifecycle and session-control capabilities

Research for [issue #2](https://github.com/ewgdg/pi-herdr-subagents/issues/2), verified against the repository and the locally installed Pi 0.80.10 packages on July 18, 2026.

## Conclusion

Pi exposes the boundary needed to know that an agent is truly idle: `agent_settled`. It does **not** expose an intentional, reason-bearing state for “waiting for a response.” `agent_settled` carries no reason or dependency data, so a completed response, a wait for a human, and a wait for another agent are indistinguishable unless an extension records the distinction. Explicit completion must likewise be an extension-owned action rather than an inference from Pi lifecycle events.

This can be implemented entirely in the Pi extension. Pi already supplies same-session wake and queue controls, durable custom entries, graceful shutdown, session resume, and extension UI hooks. Pi does not supply a cross-process API that sends to a live runtime by session ID/path; workflow addressing and transport therefore also belong to the extension.

## Source roots

Pointers below are relative to these roots:

- **Repository:** this branch.
- **Pi:** installed `@earendil-works/pi-coding-agent` 0.80.10 (`package.json:1-5`).
- **Agent core:** Pi's installed `@earendil-works/pi-agent-core` 0.80.10 (`node_modules/@earendil-works/pi-agent-core/package.json:1-5`).

## Lifecycle boundary and missing intent

Pi documents `agent_end` as the end of one low-level run; automatic retry, compaction/retry, or queued continuation may still follow. `agent_settled` is emitted only when no automatic continuation remains and is the correct status/idle boundary (`docs/extensions.md:551-565`; `docs/rpc.md:801-853`). The session runtime marks the run inactive before emitting `agent_settled` (`dist/core/agent-session.js:298-319`), while the core loop drains steering and follow-up queues before its final `agent_end` (`node_modules/@earendil-works/pi-agent-core/dist/agent-loop.js:80-171`).

Neither event expresses intent. `AgentEndEvent` contains only `type` and `messages`; `AgentSettledEvent` contains only `type` (`dist/core/extensions/types.d.ts:526-537`). There is no lifecycle field for a waiting category, reason, dependency, expected responder, or completion declaration. Therefore:

- Use `agent_settled` to observe that Pi will not continue automatically.
- Treat “no explicit terminal action + settlement” as a candidate implicit wait rule only if the protocol chooses that rule.
- Persist the reason for human, agent, or operational waiting in extension-owned state.
- Represent explicit completion with an extension tool/command/marker; do not infer it from `agent_end` or `agent_settled`.

The current repository listens to `agent_end`, not `agent_settled`. In non-auto-exit mode it records every `agent_end` as `waiting`; in auto-exit mode, except for an aborted latest assistant turn, it writes completion evidence, marks done, and requests shutdown (`pi-extension/subagents/subagent-done.ts:16-38,181-210`; `pi-extension/subagents/activity.ts:412-421`). That classification predates the stronger settlement event and can report waiting before Pi has exhausted automatic continuation.

## Current repository flow

### Child lifecycle and completion

The child extension maintains an extension-owned activity snapshot with phases and detailed provider/tool/turn events. `agentEndWaiting()` clears active state and records phase `waiting`; `agentEndDone()`, `callerPing()`, and `subagentDone()` mark the snapshot done (`pi-extension/subagents/activity.ts:390-421,501-509`).

Completion paths are:

- Auto-exit: normal/error `agent_end` writes `<session>.exit`, records done, and calls `ctx.shutdown()`; an aborted turn remains open (`pi-extension/subagents/subagent-done.ts:16-38,181-210`).
- `caller_ping`: writes a reason-bearing `ping` sidecar and calls `ctx.shutdown()` (`pi-extension/subagents/subagent-done.ts:271-302`).
- `subagent_done`: writes a `done` sidecar and calls `ctx.shutdown()` (`pi-extension/subagents/subagent-done.ts:306-323`).

These sidecars are already extension-owned explicit terminal evidence. They are a stronger basis for completion than lifecycle inference.

### Parent observation and delivery

The parent watcher polls three evidence sources: the exit sidecar, a terminal sentinel/tail, and Herdr pane inspection. A missing pane without completion evidence becomes an error after a bounded artifact race window (`pi-extension/subagents/completion.ts:128-177`). After completion, the parent reads the child session for the last assistant message, closes the pane, records completed/failed state, and returns a structured result (`pi-extension/subagents/index.ts:1443-1557`).

Herdr's coarse status is projected into lifecycle state: `working` becomes active, `blocked` becomes blocked, and `idle` or `done` becomes waiting after work has occurred (`pi-extension/subagents/lifecycle.ts:118-174`). Pi activity enriches that projection, but Herdr remains the process/pane observation source.

Completion and ping delivery into the parent use the parent's current `ExtensionAPI.sendMessage()` with `{ triggerTurn: true, deliverAs: "steer" }`, which queues during an active turn or wakes an idle parent (`pi-extension/subagents/index.ts:1729-1797`). Runtime state keeps a map of running subagents and preserves it only across extension reload; other parent shutdown reasons abort watchers and clear the map (`pi-extension/subagents/index.ts:560-606`).

### Interrupt and resume

`subagent_interrupt` sends Escape to the child's pane. Its contract intentionally keeps the pane, session, watcher, and running entry alive (`pi-extension/subagents/index.ts:1873-1930`; `pi-extension/subagents/herdr.ts:246-254`).

`subagent_resume` starts a new pane/process using `pi --session <path>`, reloads the child extension, optionally supplies an initial message, registers a new watcher, and later delivers the result back to the parent (`pi-extension/subagents/index.ts:1991-2165`). Pi itself supports `/resume`, `/new`, `/fork`, `/clone`, and `pi --session <path|id>` (`README.md:176-196,235-249`).

## Pi mechanisms available to the extension

### Completion, loop termination, abort, and process exit

A custom tool can record completion and call `ctx.shutdown()`. Pi documents shutdown as graceful and available from event handlers, tools, commands, and shortcuts. Interactive mode defers exit until the agent is idle after queued steering/follow-up work; RPC mode defers it to the next idle command boundary; print mode ignores it because print mode exits after prompts (`docs/extensions.md:1007-1026`). Interactive mode checks a pending shutdown request at `agent_settled` (`dist/modes/interactive/interactive-mode.js:1248-1257,2438-2441`); RPC mode similarly tracks a shutdown request and exits at its idle boundary (`dist/modes/rpc/rpc-mode.js:251-268,555-580`). `session_shutdown` is emitted before exit (`docs/extensions.md:1011-1026`).

A tool result may return `terminate: true` to suppress the automatic follow-up model call when every finalized result in that tool batch terminates. This ends the agent loop; it is not process shutdown (`docs/extensions.md:1880-1918`; `examples/extensions/structured-output.ts:1-43`; agent core `dist/agent-loop.js:108-145`). A terminal completion/wait tool may need both an extension marker and `terminate: true`; completion additionally needs `ctx.shutdown()` when the process should exit.

`ctx.abort()`/Escape aborts the current operation but leaves the session available for further input (`docs/extensions.md:1007-1010`; `README.md:203-223`). This matches the repository's interrupt behavior.

### Same-session delivery and wake-up

Pi's extension API is sufficient for delivery to the runtime in which the extension is loaded:

- `pi.sendMessage()` injects a custom context message. `steer` delivers at the next turn boundary; `followUp` waits until current work ends; `nextTurn` waits for a future user prompt. `triggerTurn: true` wakes an idle runtime for `steer`/`followUp` (`docs/extensions.md:1379-1400`).
- `pi.sendUserMessage()` creates a real user message, starts a turn when idle, and requires `steer` or `followUp` while streaming (`docs/extensions.md:1402-1426`).
- SDK equivalents are `prompt`, `steer`, and `followUp` (`docs/sdk.md:180-234`). RPC exposes the same prompt/steer/follow-up queue model and emits `queue_update`, `agent_end`, and `agent_settled` (`docs/rpc.md:42-124,801-853`).
- `ctx.isIdle()`, `ctx.hasPendingMessages()`, and `agent_settled` expose local delivery/settlement state (`docs/extensions.md:1007-1019`).

The runtime implementation confirms that these operations target the current session object: custom messages go to its local pending/steering/follow-up queues or immediately invoke its agent (`dist/core/agent-session.js:1060-1138`).

### Session replacement and durable extension state

Extensions can persist protocol markers with `pi.appendEntry()` and reconstruct them from session entries after reload/resume (`docs/extensions.md:1430-1444`). Commands additionally receive `waitForIdle`, `newSession`, `fork`, `navigateTree`, `switchSession`, and `reload`; these controls are command-only because replacement from arbitrary event/tool contexts can deadlock or leave stale bindings (`dist/core/extensions/types.d.ts:241-301`; `docs/extensions.md:1072-1318`). SDK users can own replacement through `AgentSessionRuntime` (`docs/sdk.md:139-177`).

## No cross-process session-addressed send API

Pi's send signatures accept message content and delivery options, but no session ID, session path, process handle, or recipient (`dist/core/extensions/types.d.ts:888-907`). They are methods on the current `ExtensionAPI`/session runtime, so they do not address another live Pi process.

Other first-party mechanisms do not fill that gap:

- `pi.events` is an event bus shared by extensions supplied to one resource loader/process; the SDK example requires passing the same event-bus object (`docs/sdk.md:623-637`; `examples/extensions/event-bus.ts:1-43`).
- Persisted session discovery/resume yields files and replacement sessions, not a handle to an already-live runtime.
- `SessionManager.setSessionFile()` loads entries into an in-memory array once. Later writes append from that manager's cached entries/leaf (`dist/core/session-manager.js:533-598,615-697`). Appending JSONL externally therefore does not enqueue a message in a live runtime, and concurrent managers writing one file have no routing/coordination contract.
- Herdr `pane run` can inject terminal text into a known pane (`pi-extension/subagents/herdr.ts:246-250`), but that is terminal injection, not a Pi session-addressed API.

Pi's first-party `file-trigger` example demonstrates the viable pattern: an extension watches an external file, then calls its own runtime-bound `pi.sendMessage(..., { triggerTurn: true })` (`examples/extensions/file-trigger.ts:1-38`). A workflow mailbox, socket, or RPC supervisor can generalize this pattern without changing Herdr.

## Extension-only implementation options

All required mechanics can remain in this repository's Pi extension:

1. **Protocol actions:** tools/commands for explicit completion and targeted waiting; absence of a terminal action plus `agent_settled` can represent implicit human waiting if adopted as a protocol rule.
2. **Reason-bearing state:** typed waiting kind, reason, dependency/recipient, and timestamps persisted with `pi.appendEntry()` and rendered with extension UI.
3. **Same-process delivery:** use `sendMessage`/`sendUserMessage` with `steer`, `followUp`, `nextTurn`, and `triggerTurn` according to recipient activity.
4. **Workflow addressing:** extend the existing running-subagent registry, scoped to visible workflow capabilities, with recipient-to-pane/session/runtime metadata.
5. **Cross-process transport:** choose extension-owned mailbox/file watching, a local socket, targeted Herdr terminal injection, or children launched and retained under Pi RPC control. The receiving extension converts transport input into Pi's same-session send APIs.
6. **Completion and exit:** persist explicit terminal evidence, optionally return `terminate: true` to avoid an extra model call, then use `ctx.shutdown()` when the process should exit.
7. **Resume safety:** launch `pi --session` only after establishing that no live process owns that session file.

No Pi core or Herdr change is required for these mechanics. Durability, acknowledgement, ownership, and failure semantics remain architecture decisions rather than missing primitives.

## Decision questions surfaced

1. Is “no explicit terminal action + `agent_settled`” the sole rule for implicit human waiting?
2. Should explicit wait be represented by a tool, or only by persisted state set through another protocol action?
3. Should terminal tools return `terminate: true` to prevent an extra provider call?
4. Should completion drain queued work through graceful shutdown, or reject/discard messages queued after completion?
5. Must waiting dependencies and undelivered messages survive process restart?
6. What delivery contract is required: at-most-once, at-least-once, or effectively exactly-once with IDs and acknowledgements?
7. Which cross-process transport should be authoritative: extension mailbox/socket, Herdr terminal injection, or RPC-owned child runtimes?
8. How is single ownership enforced so two processes cannot resume/write the same session concurrently?
9. What happens when a recipient disappears between address resolution and delivery?
10. Should current status derivation migrate from `agent_end` to `agent_settled`, with explicit markers overriding settlement classification?
