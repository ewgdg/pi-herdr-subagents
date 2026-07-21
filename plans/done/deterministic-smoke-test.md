# Deterministic smoke test

## Goal

Replace the slow, model-dependent integration suite with one fast deterministic herdr smoke test that exercises the real Owner Pi → subagent extension → child Pi → completion path without network access, credentials, or paid model calls.

## Intention

Keep exactly one process-boundary check. Move no deleted scenario forward unless existing unit/protocol coverage exposes a real gap. Use Pi AI's Faux Provider through a test-only extension and an explicit Owner/child response script.

## Scope & Constraints

- Delete `test/integration/` and the `test:integration` command.
- Delete `.pi/skills/run-integration-tests/` and remove its settings entry; add no replacement skill.
- Add `npm run test:smoke`; keep `npm test` environment-independent.
- The smoke command must run from any shell by starting an isolated named headless herdr server.
- Load the Faux Provider into both Owner and child processes without production test hooks.
- Use an isolated `PI_CODING_AGENT_DIR` and a temporary role-based JSON response script.
- Expected runtime is under 10 seconds; hard timeout is 30 seconds.
- On failure, include useful pane/session diagnostics.
- Update `README.md`.

## Confirmed Test Seam

The public seam is `npm run test:smoke`. Observable success requires a real Owner Pi process to load the working-tree extension, call `subagent`, launch a real child Pi process, have the child write a marker file, and receive child completion back in the Owner session.

## Work Plan

1. Add the smoke test and test-only Faux Provider fixture, then run it red.
2. Add the minimal harness/configuration needed to load the fixture in Owner and child processes.
3. Make the smoke test green and enforce the runtime/timeout contract.
4. Remove the old integration suite, command, fixtures, and skill.
5. Update README development instructions.
6. Run unit tests, lint, and the smoke test; review the final diff.

## Validation

- `npm test`
- `npm run lint`
- `npm run test:smoke`
- Confirm `npm run test:smoke` passes with all inherited `HERDR_*` variables removed.
- Confirm no `PI_TEST_MODEL`, `PI_TEST_TIMEOUT`, `test:integration`, or `run-integration-tests` references remain.

## Progress

- [x] Design and public seam confirmed with the user.
- [x] Failing smoke test added.
- [x] Faux Provider fixture and isolated process configuration implemented.
- [x] Smoke test passing within the runtime contract.
- [x] Legacy integration suite and skill removed.
- [x] Documentation updated.
- [x] Full task-scoped validation complete; unrelated repository failures are recorded below.

## Surprises & Discoveries

- Herdr child panes do not inherit arbitrary inline environment assignments from the Owner Pi process. The response script therefore lives at a fixed path under the isolated `PI_CODING_AGENT_DIR`, which the launcher already propagates.
- Durable Workflow children do not receive `PI_SUBAGENT_AUTO_EXIT`; the scripted child must call `subagent_done` explicitly.
- Pi performs the normal post-tool continuation after `subagent_done` before shutdown settles, so the child script includes one trailing summary response to avoid Faux queue exhaustion and keep result extraction stable.
- Herdr workspace `--env` values do not propagate to tabs created later. To keep the smoke truly offline, the Owner starts with `--offline`; both fresh and resumed production child launch paths now share preservation of the general Pi startup constraints `PI_OFFLINE`, `PI_SKIP_VERSION_CHECK`, and `PI_TELEMETRY`. The Faux Provider rejects either smoke role unless offline mode is present.
- The 30-second cap requires bounding the harness's own herdr subprocesses, not only its polling loop. The harness reserves its final four seconds for diagnostics and cleanup and gives every herdr operation an explicit timeout.
- Running the smoke workspace in the user's live herdr session is unsafe because closing a non-focused temporary workspace can still change global focus. A uniquely named headless session isolates workspace, pane, socket, and focus state while preserving the real process boundary.
- The repository's existing `npm test` command has one unrelated failure: `test/test.ts` expects the bundled `planner` Agent Definition to be interactive, while `agents/planner.md` has no `interactive` or `auto-exit` frontmatter. Neither file is changed by this work.

## Outcomes & Retrospective

- `npm run test:smoke` passed repeated runs at roughly 2.5 seconds each, including after offline and timeout enforcement.
- Running the smoke command with all inherited `HERDR_*` variables removed passes in roughly 2.7 seconds.
- Two concurrent outside-herdr smoke runs passed with distinct named sessions, and both sessions were deleted afterward.
- With `herdr` absent from `PATH`, the smoke fails immediately at preflight.
- A fake headless server stuck in startup failed at the 26-second completion budget, preserved server diagnostics, executed both named-session stop and delete within the reserved cleanup budget, and left no orphan.
- Process-group watchdog probes confirmed that SIGTERM-ignoring descendants are killed even when their command or server leader exits first; both preflight and long-lived server reproductions left no orphan.
- `npm run lint`, `git diff --check`, and `npm run test:protocol` passed.
- A deliberately hanging fake `herdr` executable that ignored `SIGTERM` was killed as a process group with no orphan in about 0.9 seconds.
- The shared child-environment unit test passes, including `PI_OFFLINE=1` and `PI_TELEMETRY=0` preservation.
- `npm test` passed 232 of 233 tests and reproduced the unrelated bundled-planner expectation failure noted above.

## Decisions

- No live-model test exists.
- The fake provider follows explicit scripts; it does not interpret prompts.
- No production extension code receives test-only hooks.
- Existing scenarios are not recreated one-for-one.
- No ADR or domain glossary entry is needed because this is test infrastructure and easy to reverse.
