#!/usr/bin/env node

/** PROTOTYPE — interactive walkthrough of the candidate agent-facing control interface. */

import { createInterface } from "node:readline";

type InterfaceVariant = "semantic-tools" | "command-envelope";
type CompletionComposition = "separate" | "fused";
type ScenarioName = "autonomous" | "hitl" | "review";
type WorkState = "active" | "waiting(human)" | "waiting(agent)" | "ended(completed)";

type AgentState = {
  id: string;
  work: WorkState;
  unresolved: string[];
};

type RequestState = {
  id: string;
  from: string;
  to: string;
  status: "queued" | "delivered" | "answered" | "resolved";
};

type PrototypeState = {
  interfaceVariant: InterfaceVariant;
  completionComposition: CompletionComposition;
  scenario: ScenarioName;
  step: number;
  agents: Record<string, AgentState>;
  requests: RequestState[];
  lastCall: string;
  events: string[];
};

type ScenarioStep = {
  label: string;
  call: (state: PrototypeState) => string;
  apply: (state: PrototypeState) => void;
};

const bold = "\x1b[1m";
const dim = "\x1b[2m";
const green = "\x1b[32m";
const yellow = "\x1b[33m";
const red = "\x1b[31m";
const cyan = "\x1b[36m";
const reset = "\x1b[0m";

function agent(id: string, work: WorkState = "active"): AgentState {
  return { id, work, unresolved: [] };
}

function initialState(
  interfaceVariant: InterfaceVariant,
  completionComposition: CompletionComposition,
  scenario: ScenarioName,
): PrototypeState {
  const agents =
    scenario === "autonomous"
      ? { worker: agent("worker-session") }
      : scenario === "hitl"
        ? { worker: agent("worker-session"), spawner: agent("spawner-session") }
        : { implementer: agent("implementer-session"), reviewer: agent("reviewer-session") };

  return {
    interfaceVariant,
    completionComposition,
    scenario,
    step: 0,
    agents,
    requests: [],
    lastCall: "(none yet)",
    events: ["Scenario initialized."],
  };
}

function sendCall(
  state: PrototypeState,
  input: {
    kind: "signal" | "request";
    to: string;
    message: string;
    delivery?: "steer" | "deferred";
    answerDelivery?: "steer" | "deferred";
    after: "continue" | "settle";
  },
): string {
  const messageFields = [
    `  to: "${input.to}",`,
    `  message: "${input.message}",`,
    input.delivery ? `  delivery: "${input.delivery}",` : undefined,
    input.answerDelivery ? `  answerDelivery: "${input.answerDelivery}",` : undefined,
  ].filter((field): field is string => field !== undefined);

  if (state.interfaceVariant === "semantic-tools") {
    return `agent_send({\n  kind: "${input.kind}",\n${messageFields.join("\n")}\n  after: "${input.after}"\n})`;
  }

  return `agent_control({\n  command: {\n    type: "message.${input.kind}",\n${messageFields.map((field) => `  ${field}`).join("\n")}\n  },\n  after: "${input.after}"\n})`;
}

function answerCall(
  state: PrototypeState,
  input: {
    request: string;
    outcome: "fulfilled" | "unable";
    message: string;
    after: "continue" | "settle" | { complete: string };
  },
): string {
  const after =
    typeof input.after === "string"
      ? `"${input.after}"`
      : `{ complete: "${input.after.complete}" }`;

  if (state.interfaceVariant === "semantic-tools") {
    return `agent_answer({\n  request: "${input.request}",\n  outcome: "${input.outcome}",\n  message: "${input.message}",\n  after: ${after}\n})`;
  }

  return `agent_control({\n  command: {\n    type: "message.answer",\n    request: "${input.request}",\n    outcome: "${input.outcome}",\n    message: "${input.message}"\n  },\n  after: ${after}\n})`;
}

function completeCall(state: PrototypeState, result: string): string {
  if (state.interfaceVariant === "semantic-tools") {
    return `agent_complete({ result: "${result}" })`;
  }
  return `agent_control({\n  command: { type: "agent.complete", result: "${result}" }\n})`;
}

function setWork(state: PrototypeState, name: string, work: WorkState): void {
  state.agents[name]!.work = work;
}

function addRequest(state: PrototypeState, id: string, from: string, to: string): void {
  state.requests.push({ id, from, to, status: "queued" });
  state.agents[from]!.unresolved.push(id);
}

function resolveRequest(state: PrototypeState, id: string): void {
  const request = state.requests.find((candidate) => candidate.id === id)!;
  request.status = "resolved";
  const sender = state.agents[request.from]!;
  sender.unresolved = sender.unresolved.filter((requestId) => requestId !== id);
  sender.work = "active";
}

const scenarios: Record<ScenarioName, { title: string; purpose: string; steps: ScenarioStep[] }> = {
  autonomous: {
    title: "Autonomous worker",
    purpose: "Completion is the only collaboration action required for ordinary autonomous work.",
    steps: [
      {
        label: "Worker completes explicitly",
        call: (state) => completeCall(state, "Implemented parser validation; tests pass."),
        apply(state) {
          setWork(state, "worker", "ended(completed)");
          state.events.push("Completion result committed before the activation ended.");
          state.events.push("No redundant Signal to the Spawner was needed.");
        },
      },
    ],
  },
  hitl: {
    title: "Human-in-the-loop escalation",
    purpose: "Escalation is an ordinary Request to the known Spawner; asking a human uses no control call.",
    steps: [
      {
        label: "Worker requests a decision and settles",
        call: (state) =>
          sendCall(state, {
            kind: "request",
            to: "spawner-session",
            message: "Schema v1 and v2 conflict. Which should I use?",
            answerDelivery: "steer",
            after: "settle",
          }),
        apply(state) {
          addRequest(state, "req-schema", "worker", "spawner");
          setWork(state, "worker", "waiting(agent)");
          state.events.push("Request acceptance and dependency creation committed atomically.");
          state.events.push("after=settle suppressed the otherwise automatic follow-up model turn.");
        },
      },
      {
        label: "Spawner receives the Request",
        call: () => `(runtime commits Inbox Batch containing req-schema)`,
        apply(state) {
          state.requests[0]!.status = "delivered";
          setWork(state, "spawner", "active");
          state.events.push("Actionable delivery started the waiting recipient.");
        },
      },
      {
        label: "Spawner asks the human",
        call: () => `assistant: "Should the worker use schema v1 or v2?"\n(no control call)`,
        apply(state) {
          setWork(state, "spawner", "waiting(human)");
          state.events.push("Natural settlement with no dependency became waiting(human).");
        },
      },
      {
        label: "Human answers",
        call: () => `user: "Use v2; v1 is obsolete."`,
        apply(state) {
          setWork(state, "spawner", "active");
          state.events.push("Human input started a new Spawner run.");
        },
      },
      {
        label: "Spawner answers the Worker",
        call: (state) =>
          answerCall(state, {
            request: "req-schema",
            outcome: "fulfilled",
            message: "Use v2; v1 is obsolete.",
            after: "settle",
          }),
        apply(state) {
          state.requests[0]!.status = "answered";
          setWork(state, "spawner", "waiting(human)");
          state.events.push("Request ID determined Answer authority, destination, and delivery timing.");
          state.events.push("The Spawner settled without inventing an escalation or resume operation.");
        },
      },
      {
        label: "Answer reaches the Worker",
        call: () => `(runtime commits Answer for req-schema to the Worker transcript)`,
        apply(state) {
          resolveRequest(state, "req-schema");
          state.events.push("Answer delivery—not acceptance—resolved the dependency and woke the Worker.");
        },
      },
      {
        label: "Worker completes",
        call: (state) => completeCall(state, "Implemented schema v2; tests pass."),
        apply(state) {
          setWork(state, "worker", "ended(completed)");
          state.events.push("The durable Agent remained the same across both runs.");
        },
      },
    ],
  },
  review: {
    title: "Reviewer–implementer loop",
    purpose: "Requests preserve peer collaboration; disposition controls whether each peer continues or becomes message-wakeable.",
    steps: [
      {
        label: "Implementer requests review",
        call: (state) =>
          sendCall(state, {
            kind: "request",
            to: "reviewer-session",
            message: "Review revision abc123.",
            delivery: "deferred",
            after: "settle",
          }),
        apply(state) {
          addRequest(state, "req-review-1", "implementer", "reviewer");
          setWork(state, "implementer", "waiting(agent)");
          state.events.push("The Workflow Owner did not relay or receive the peer Request.");
        },
      },
      {
        label: "Reviewer receives the first review Request",
        call: () => `(runtime delivers req-review-1 to reviewer-session)`,
        apply(state) {
          state.requests[0]!.status = "delivered";
          setWork(state, "reviewer", "active");
          state.events.push("Deferred delivery began after the recipient's prior work settled.");
        },
      },
      {
        label: "Reviewer requests changes and remains reusable",
        call: (state) =>
          answerCall(state, {
            request: "req-review-1",
            outcome: "fulfilled",
            message: "Changes required: serialize the retry path.",
            after: "settle",
          }),
        apply(state) {
          state.requests[0]!.status = "answered";
          setWork(state, "reviewer", "waiting(human)");
          resolveRequest(state, "req-review-1");
          state.events.push("The reviewer settled instead of completing, so a later Request can wake it.");
        },
      },
      {
        label: "Implementer fixes and requests another review",
        call: (state) =>
          sendCall(state, {
            kind: "request",
            to: "reviewer-session",
            message: "Retry path fixed in def456; review again.",
            after: "settle",
          }),
        apply(state) {
          addRequest(state, "req-review-2", "implementer", "reviewer");
          setWork(state, "implementer", "waiting(agent)");
          setWork(state, "reviewer", "active");
          state.events.push("Messaging a waiting reviewer scheduled a new run without lifecycle authority.");
        },
      },
      {
        label: "Reviewer approves",
        call: (state) =>
          answerCall(state, {
            request: "req-review-2",
            outcome: "fulfilled",
            message: "Approved.",
            after:
              state.completionComposition === "separate"
                ? "continue"
                : { complete: "Review finished." },
          }),
        apply(state) {
          state.requests[1]!.status = "answered";
          if (state.completionComposition === "separate") {
            setWork(state, "reviewer", "active");
            state.events.push("Separate completion preserves one terminal lifecycle operation but needs another model turn.");
          } else {
            setWork(state, "reviewer", "ended(completed)");
            state.events.push("Fused disposition saves a model turn but makes messaging another completion entry point.");
          }
        },
      },
      {
        label: "Reviewer completes under the separate-completion variant",
        call: (state) =>
          state.completionComposition === "separate"
            ? completeCall(state, "Review finished; def456 approved.")
            : `(no call — completion was fused into the Answer command)`,
        apply(state) {
          if (state.completionComposition === "separate") {
            setWork(state, "reviewer", "ended(completed)");
            state.events.push("The extra turn ends with the sole explicit completion operation.");
          } else {
            state.events.push("Fused completion already ended the reviewer activation.");
          }
        },
      },
      {
        label: "Approval reaches the Implementer",
        call: () => `(runtime commits Answer for req-review-2 to implementer-session)`,
        apply(state) {
          resolveRequest(state, "req-review-2");
          state.events.push("Accepted outbound Answers remain deliverable after the responder completes.");
        },
      },
      {
        label: "Implementer completes",
        call: (state) => completeCall(state, "Implemented and approved revision def456."),
        apply(state) {
          setWork(state, "implementer", "ended(completed)");
          state.events.push("Both peers ended explicitly; the Workflow Owner stayed supervisory.");
        },
      },
    ],
  },
};

let state = initialState("semantic-tools", "separate", "autonomous");

function colorWork(work: WorkState): string {
  if (work === "active") return `${green}${work}${reset}`;
  if (work.startsWith("waiting")) return `${yellow}${work}${reset}`;
  return `${red}${work}${reset}`;
}

function renderInterface(): void {
  console.log(`${bold}Candidate interface${reset}`);
  if (state.interfaceVariant === "semantic-tools") {
    console.log(`${cyan}agent_send${reset}({ kind, to, message, delivery?, answerDelivery?, after })`);
    console.log(`${cyan}agent_answer${reset}({ request, outcome, message, after })`);
    console.log(`${cyan}agent_complete${reset}({ result })`);
  } else {
    console.log(`${cyan}agent_control${reset}({ command: { type: "message.signal" | "message.request" | "message.answer" | "agent.complete", ... }, after? })`);
    console.log(`${dim}One tool; command type selects the protocol operation.${reset}`);
  }
  console.log(`${dim}Runtime allocates Message IDs. Answer destination and timing derive from the Request.${reset}`);
  console.log(`${dim}No inspect, wait, escalate, cancel, resume, or Workflow Owner alias is included.${reset}`);
  if (state.completionComposition === "separate") {
    console.log(`${dim}after = continue | settle; completion remains a separate operation.${reset}`);
  } else {
    console.log(`${dim}after also accepts { complete: result } for a final outbound message.${reset}`);
  }
}

function render(): void {
  console.clear();
  const scenario = scenarios[state.scenario];
  console.log(`${bold}PROTOTYPE — Minimal agent-facing control interface${reset}`);
  console.log(`${dim}Protocol: ${state.interfaceVariant === "semantic-tools" ? "three semantic tools" : "single command envelope"}${reset}`);
  console.log(`${dim}Completion: ${state.completionComposition === "separate" ? "separate operation" : "fused after final message"}${reset}\n`);
  renderInterface();

  console.log(`\n${bold}${scenario.title}${reset}`);
  console.log(`${dim}${scenario.purpose}${reset}`);
  console.log(`${bold}progress${reset} ${state.step}/${scenario.steps.length}`);

  console.log(`\n${bold}Last call / runtime action${reset}`);
  console.log(state.lastCall);

  console.log(`\n${bold}Agents${reset}`);
  for (const [name, current] of Object.entries(state.agents)) {
    const dependencies = current.unresolved.length > 0 ? `  requests=[${current.unresolved.join(", ")}]` : "";
    console.log(`${bold}${name.padEnd(12)}${reset} ${colorWork(current.work)}${dependencies}`);
  }

  console.log(`\n${bold}Requests${reset}`);
  if (state.requests.length === 0) {
    console.log(`${dim}(none)${reset}`);
  } else {
    for (const request of state.requests) {
      console.log(
        `${bold}${request.id}${reset}  ${request.from} → ${request.to}  ${request.status}`,
      );
    }
  }

  console.log(`\n${bold}Recent events${reset}`);
  for (const event of state.events.slice(-5)) console.log(`${dim}•${reset} ${event}`);

  console.log(`\n${bold}Actions${reset}`);
  console.log(`${bold}1${reset} autonomous   ${bold}2${reset} human-in-loop   ${bold}3${reset} reviewer–implementer`);
  console.log(`${bold}n${reset} next step    ${bold}v${reset} switch protocol    ${bold}c${reset} switch completion composition`);
  console.log(`${bold}z${reset} reset        ${bold}q${reset} quit`);
  process.stdout.write(`\n${bold}>${reset} `);
}

function chooseScenario(scenario: ScenarioName): void {
  state = initialState(state.interfaceVariant, state.completionComposition, scenario);
}

function nextStep(): void {
  const steps = scenarios[state.scenario].steps;
  const step = steps[state.step];
  if (!step) {
    state.events.push("Scenario already complete.");
    return;
  }
  state.lastCall = step.call(state);
  step.apply(state);
  state.events.push(step.label);
  state.step += 1;
}

const readline = createInterface({ input: process.stdin, output: process.stdout });
readline.on("line", (line) => {
  switch (line.trim().toLowerCase()) {
    case "1":
      chooseScenario("autonomous");
      break;
    case "2":
      chooseScenario("hitl");
      break;
    case "3":
      chooseScenario("review");
      break;
    case "n":
      nextStep();
      break;
    case "v":
      state = initialState(
        state.interfaceVariant === "semantic-tools" ? "command-envelope" : "semantic-tools",
        state.completionComposition,
        state.scenario,
      );
      break;
    case "c":
      state = initialState(
        state.interfaceVariant,
        state.completionComposition === "separate" ? "fused" : "separate",
        state.scenario,
      );
      break;
    case "z":
      state = initialState(state.interfaceVariant, state.completionComposition, state.scenario);
      break;
    case "q":
      readline.close();
      return;
  }
  render();
});
readline.on("close", () => process.exit(0));

render();
