# Subagent Model Routing Refactor

**Status:** Proposed
**Date:** 2026-07-13

## Goal

Every subagent inherits the parent agent's model and thinking level by default. The orchestrating agent may override either value when the task warrants it, choosing from Pi's authenticated models and supported thinking levels.

The extension does not silently choose a model from a tier. The orchestrating agent infers task requirements; the extension supplies the available choices, validates the request, resolves inheritance, and launches the child.

## Reference Design

`pi-tidy-subagents` provides the right foundation:

1. Omitted `model` and `thinking` inherit the parent runtime.
2. Thinking is the primary task-specific control.
3. Explicit models use exact authenticated `provider/model-id` values.
4. Explicit unsupported thinking fails before launch.
5. Inherited thinking clamps to the selected model's capabilities.
6. Requested, resolved, and observed runtime values remain distinct.
7. The child runtime is checked after startup.

We should adopt these rules while keeping our Herdr-based asynchronous lifecycle.

## Routing Responsibility

### Orchestrating agent

The parent agent decides whether to inherit or override based on the task:

- Inherit both for ordinary work that suits the current session runtime.
- Change only thinking when the same model needs more or less reasoning.
- Change model when cost, speed, modality, context size, or capability warrants it.
- Change both for tasks whose model and reasoning requirements differ materially from the parent.

### Extension

The extension:

- exposes a compact authenticated-model catalog to the parent agent;
- resolves omitted fields from the parent runtime;
- validates exact model identity and authentication;
- validates explicit thinking against the selected model;
- clamps inherited thinking when the selected model cannot support it;
- records selection provenance and effective runtime;
- never mutates the parent model or thinking level.

## Tool Interface

Keep the existing optional fields. Do not add semantic tiers.

```ts
subagent({
  name: "Scout",
  agent: "scout",
  task: "Map the authentication entry points",
  // model omitted: inherit parent
  // thinking omitted: inherit parent
});
```

Thinking-only override:

```ts
subagent({
  name: "Architecture review",
  agent: "reviewer",
  thinking: "high",
  task: "Review the module boundaries and concurrency assumptions",
});
```

Model and thinking override:

```ts
subagent({
  name: "Cheap lookup",
  agent: "scout",
  model: "deepseek/deepseek-v4-flash",
  thinking: "minimal",
  task: "Find the declaration and its callers",
});
```

### Schema guidance

`model`:

> Exact authenticated `provider/model-id`. Omit to inherit the parent model. Select another model only when task capability, speed, cost, modality, or context requirements warrant it.

`thinking`:

> Pi thinking level: `off|minimal|low|medium|high|xhigh|max`. Omit to inherit the parent level. Prefer changing thinking before changing models: minimal/low for bounded mechanical work, medium for ordinary implementation or review, high+ for architecture, concurrency, security, or hard diagnosis.

## Precedence

Resolve each field independently:

1. Explicit tool-call `model` or `thinking`
2. Agent frontmatter `model` or `thinking`
3. Parent model or thinking

Bundled agent definitions should omit `model` and `thinking`, so they inherit by default. Project and global agent definitions may still pin exact defaults when users need them.

User instructions and project instructions guide what the orchestrating agent places in the explicit tool-call fields; the extension does not parse those instructions into hidden overrides.

## Available Model Catalog

The parent agent cannot choose a valid exact model unless it knows which models are available.

Use `ctx.modelRegistry.getAvailable()` as the authority. Fall back to `getAll()` filtered with `hasConfiguredAuth()` for compatibility. Never shell out to `pi --list-models` in production.

Expose a compact catalog containing:

- exact `provider/model-id`;
- reasoning support;
- supported thinking levels;
- text/image input support;
- context window;
- maximum output;
- declared input/output cost when available.

Example guidance:

```text
Authenticated subagent models:
- deepseek/deepseek-v4-flash — reasoning, text, 128k context, low declared cost
- anthropic/claude-sonnet-4-6 — reasoning, text+image, 200k context
- anthropic/claude-opus-4-6 — reasoning, text+image, 200k context, higher declared cost

Default: inherit parent model and thinking. Override thinking first. Override model only when the task warrants it. Use exact IDs.
```

### Delivery seam

Create the `subagent` tool during `session_start`, when `ctx.modelRegistry` and `ctx.model` are available. Build its `promptGuidelines` from the live authenticated catalog.

If registering the tool during `session_start` conflicts with reload behavior, retain one tool registration and inject the catalog through `before_agent_start`. Keep catalog construction in a pure helper either way.

Limit the catalog to concise facts. Do not embed model-quality claims that Pi's registry cannot prove.

## Runtime Planning Module

Create `pi-extension/subagents/runtime-routing.ts` as the seam between launch orchestration and runtime selection.

### Interface

```ts
export interface RuntimeRequest {
  model?: string;
  thinking?: ThinkingLevel;
}

export interface ParentRuntime {
  provider: string;
  modelId: string;
  thinking: ThinkingLevel;
}

export interface ResolvedRuntimePlan {
  provider: string;
  modelId: string;
  model: string;
  thinking: ThinkingLevel;
  modelSource: "request" | "agent" | "parent";
  thinkingSource: "request" | "agent" | "parent";
  requestedModel?: string;
  requestedThinking?: ThinkingLevel;
  thinkingAdjustment?: {
    from: ThinkingLevel;
    to: ThinkingLevel;
    reason: "non-reasoning" | "inherited-clamp";
  };
}

export function resolveRuntimePlan(
  request: RuntimeRequest,
  agentDefaults: RuntimeRequest,
  parent: ParentRuntime,
  registry: ModelRegistryAdapter,
): ResolvedRuntimePlan;
```

`launchSubagent()` consumes `ResolvedRuntimePlan`. It should not parse model IDs, inspect authentication, or decide thinking support.

### Registry adapter

```ts
export interface ModelRegistryAdapter {
  find(provider: string, modelId: string): RoutingModel | undefined;
  available(): RoutingModel[];
}
```

`RoutingModel` retains only the fields routing needs:

- provider and ID;
- reasoning and `thinkingLevelMap`;
- input modalities;
- context window and maximum output;
- declared costs.

This gives tests a small in-memory adapter instead of coupling them to Pi's full model registry.

## Model Validation

Parse explicit model references at the first `/`, allowing model IDs that contain additional slashes.

Before creating a Herdr pane:

1. Require exact `provider/model-id` syntax.
2. Find the exact identity in Pi's registry.
3. Require configured authentication.
4. Reject aliases and fuzzy patterns.
5. Return a diagnostic naming the invalid value and available exact alternatives.

Omitted model resolves to the parent model without requiring the orchestrating agent to restate it.

## Thinking Resolution

Use `getSupportedThinkingLevels` and `clampThinkingLevel` from `@earendil-works/pi-ai`.

- Explicit thinking is intent: reject it if unsupported by the selected model.
- Omitted thinking inherits the parent or agent default as a preference, then clamps.
- A non-reasoning selected model resolves inherited thinking to `off`.
- Diagnostics list supported alternatives.
- Do not rely only on Pi CLI clamping; the parent needs the resolved value before launch.

## Requested, Resolved, and Observed Runtime

Track three states:

| State | Meaning |
| --- | --- |
| Requested | Explicit tool-call and agent-default values |
| Resolved | Parent-side inheritance, validation, and thinking clamp |
| Observed | Child runtime found in its session after startup |

Add the plan to `RunningSubagent` and completion details:

```ts
runtimePlan: ResolvedRuntimePlan
```

Initially, launch and display the resolved plan. Then add child-session observation by reading startup `model_change` and thinking entries from the JSONL session. Warn or fail if the observed model differs from the resolved model. If observed thinking differs because Pi clamps it further, use observed thinking as the effective display value and record the adjustment.

## Bundled Agent Migration

Remove `model` and `thinking` from all bundled agent frontmatter:

- planner
- scout
- worker
- reviewer
- visual-tester

Roles remain behavioral definitions: tools, skills, prompts, session mode, interaction mode, and auto-exit. Runtime capability comes from parent inheritance unless the orchestrating agent explicitly overrides it for the task.

Update README language from fixed model assignments to suggested routing behavior:

| Agent | Default runtime | Typical override guidance |
| --- | --- | --- |
| scout | inherit parent | minimal thinking or cheaper/faster model for bounded lookup |
| worker | inherit parent | low/medium normally; high for difficult diagnosis |
| reviewer | inherit parent | medium normally; high for architecture/security/concurrency |
| planner | inherit parent | medium/high depending on design ambiguity |
| visual-tester | inherit parent | minimal/low unless image capability requires another model |

## Selection Guidance

Give the orchestrating agent short task-shape guidance:

| Task shape | Model action | Thinking action |
| --- | --- | --- |
| Exact lookup, file mapping | inherit; cheaper model optional | minimal |
| Mechanical edit | inherit | low |
| Ordinary implementation | inherit | parent or medium |
| Ordinary review | inherit | medium |
| Architecture or ambiguous planning | stronger model only if available and warranted | high |
| Concurrency, security, hard diagnosis | stronger reasoning model may be warranted | high or xhigh |
| Image-dependent work | select an image-capable model if parent lacks images | task-dependent |
| Cost-sensitive classification | cheaper model may be warranted | minimal |

These are instructions for the orchestrating agent, not hidden extension heuristics.

## Implementation Phases

### Phase 1: Extract and Validate Runtime Planning

1. Add `runtime-routing.ts` and its registry adapter.
2. Capture parent runtime from `ctx.model` and `pi.getThinkingLevel()`.
3. Replace direct `params.model ?? agent.model` and `params.thinking ?? agent.thinking` logic with `resolveRuntimePlan`.
4. Validate exact explicit models and authentication before pane creation.
5. Validate explicit thinking and clamp inherited thinking.
6. Add runtime provenance to `RunningSubagent`, launch details, and results.

Keep bundled defaults unchanged during this phase to isolate the architecture change.

### Phase 2: Expose Authenticated Models to the Agent

1. Add a pure catalog builder over `modelRegistry.getAvailable()`.
2. Include supported thinking levels and capability metadata.
3. Deliver the compact catalog through dynamic `promptGuidelines` or `before_agent_start`.
4. Update model/thinking schema descriptions with thinking-first guidance.
5. Ensure reload refreshes the catalog.

### Phase 3: Migrate Bundled Agents to Parent Inheritance

1. Remove hardcoded model and thinking defaults from bundled agents.
2. Update README and frontmatter documentation.
3. Preserve exact project/global agent defaults.
4. Add tests proving bundled agents inherit parent runtime.
5. Add routing-guidance evaluation fixtures for representative task shapes.

### Phase 4: Observe Child Runtime

1. Parse startup model/thinking state from child session JSONL.
2. Record requested, resolved, and observed values.
3. Detect model mismatches and observed thinking adjustments.
4. Show effective model/thinking in the widget and completion result.
5. Add heterogeneous integration coverage when multiple authenticated models exist.

## Test Strategy

### Unit tests

Create `test/runtime-routing.test.ts` covering:

- parent model and thinking inheritance;
- model-only, thinking-only, and combined overrides;
- project/global agent defaults;
- exact IDs whose model portion contains `/`;
- unknown and unauthenticated explicit models;
- unsupported explicit thinking with alternatives;
- inherited thinking clamps;
- non-reasoning model resolves inherited thinking to `off`;
- requested/resolved provenance;
- compact authenticated-model catalog formatting.

Use an in-memory registry adapter. No network calls.

### Routing evaluations

Use observational task fixtures:

- bounded lookup;
- mechanical implementation;
- ordinary implementation;
- ordinary review;
- architectural judgment;
- concurrency analysis;
- image-dependent task;
- cost-sensitive classification.

Record whether the agent inherited or selected each field and whether that matches the guidance. Keep live evaluations opt-in and non-release-blocking.

### Integration tests

- omission produces the parent model and thinking;
- explicit exact model and thinking reach the child;
- thinking-only override preserves the parent model;
- unsupported thinking fails before pane creation;
- unknown or unauthenticated model fails before pane creation;
- child observed runtime matches resolved runtime;
- heterogeneous siblings can use different authenticated models;
- tests skip with actionable diagnostics when fewer than two models are available;
- teardown leaves no `/tmp/pi-integ-*` panes.

Allow `PI_TEST_MODELS=provider/a,provider/b` to make heterogeneous tests deterministic. Otherwise discover authenticated models from Pi's registry.

## Risks and Mitigations

- **The agent chooses a poor model.** Keep thinking-first guidance, exact choices, visible capabilities, and observational routing evaluations.
- **The model catalog bloats the prompt.** Keep one compact line per authenticated model and cap optional metadata.
- **Many authenticated models make the list too large.** Prefer configured/recent providers or provide a bounded catalog plus a `subagents_list` detail surface.
- **Registry metadata does not measure intelligence.** Describe capabilities and cost only; do not label models as objectively powerful.
- **Explicit thinking is unsupported.** Fail before pane creation with alternatives.
- **Launch and actual runtime differ.** Add observed-runtime verification in Phase 4.
- **Behavior changes during extraction.** Keep current bundled defaults until Phase 3.

## Non-Goals

- Automatic extension-side model ranking
- Semantic `fast|balanced|powerful` tiers
- Fuzzy or alias model matching at launch
- Mutating the parent runtime
- Automatically parsing user or project prose into hidden overrides
- Benchmarking model intelligence
- Replacing Herdr/TUI child execution with RPC
