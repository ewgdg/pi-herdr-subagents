#!/usr/bin/env node

/** PROTOTYPE — interactive walkthrough of the candidate agent-facing messaging interface. */

import { createInterface } from "node:readline";

type ScenarioName = "autonomous" | "hitl" | "review";
type WaitingReason = "human" | "agent" | "operation";
type WorkPhase = "active" | "waiting" | "ended(completed)";

type AgentState = {
  id: string;
  activation: number;
  phase: WorkPhase;
  waitingOn: WaitingReason[];
  unresolvedRequests: string[];
};

type RequestState = {
  id: string;
  from: string;
  to: string;
  status: "queued" | "delivered" | "answered" | "resolved";
};

type PrototypeState = {
  scenario: ScenarioName;
  step: number;
  agents: Record<string, AgentState>;
  requests: RequestState[];
  lastCall: string;
  events: string[];
};

type ScenarioStep = {
  label: string;
  call: string;
  apply: (state: PrototypeState) => void;
};

const bold = "\x1b[1m";
const dim = "\x1b[2m";
const green = "\x1b[32m";
const yellow = "\x1b[33m";
const red = "\x1b[31m";
const cyan = "\x1b[36m";
const reset = "\x1b[0m";

function agent(id: string): AgentState {
  return {
    id,
    activation: 1,
    phase: "active",
    waitingOn: [],
    unresolvedRequests: [],
  };
}

function initialState(scenario: ScenarioName): PrototypeState {
  const rootName = scenario === "review" ? "owner" : "spawner";
  return {
    scenario,
    step: 0,
    agents: { [rootName]: agent(`${rootName}-session`) },
    requests: [],
    lastCall: "(none yet)",
    events: ["Scenario initialized."],
  };
}

function spawnAgent(state: PrototypeState, name: string, id: string): void {
  state.agents[name] = agent(id);
}

function setActive(state: PrototypeState, name: string): void {
  state.agents[name]!.phase = "active";
  state.agents[name]!.waitingOn = [];
}

function settle(state: PrototypeState, name: string, extraReasons: WaitingReason[] = []): void {
  const current = state.agents[name]!;
  const reasons = new Set<WaitingReason>(extraReasons);
  if (current.unresolvedRequests.length > 0) reasons.add("agent");
  current.phase = "waiting";
  current.waitingOn = [...reasons];
  if (current.waitingOn.length === 0) current.waitingOn = ["human"];
}

function complete(state: PrototypeState, name: string): void {
  const current = state.agents[name]!;
  current.phase = "ended(completed)";
  current.waitingOn = [];
}

function addRequest(
  state: PrototypeState,
  id: string,
  from: string,
  to: string,
  status: RequestState["status"],
): void {
  state.requests.push({ id, from, to, status });
  state.agents[from]!.unresolvedRequests.push(id);
}

function acceptAnswer(state: PrototypeState, requestId: string): void {
  state.requests.find((request) => request.id === requestId)!.status = "answered";
}

function deliverAnswer(state: PrototypeState, requestId: string): void {
  const request = state.requests.find((candidate) => candidate.id === requestId)!;
  request.status = "resolved";
  const requester = state.agents[request.from]!;
  requester.unresolvedRequests = requester.unresolvedRequests.filter((id) => id !== requestId);
  setActive(state, request.from);
}

const scenarios: Record<ScenarioName, { title: string; purpose: string; steps: ScenarioStep[] }> = {
  autonomous: {
    title: "Autonomous worker",
    purpose: "Spawn carries the initial Request; the terminal Answer is also completion.",
    steps: [
      {
        label: "Spawner creates the Worker with its initial Request",
        call: `agent_send({
  target: {
    spawn: { agent: "worker", name: "Pagination worker" }
  },
  message: "Implement pagination and run the tests.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          spawnAgent(state, "worker", "worker-session");
          addRequest(state, "req-task", "spawner", "worker", "delivered");
          settle(state, "spawner");
          state.events.push("Spawn, initial Request, dependency creation, and first activation committed together.");
          state.events.push("The Worker began with the Request already in its initial context.");
        },
      },
      {
        label: "Worker sends its terminal Answer and completes",
        call: `agent_send({
  target: { request: "req-task" },
  message: "Implemented pagination; all tests pass.",
  onAccepted: "complete"
})`,
        apply(state) {
          acceptAnswer(state, "req-task");
          complete(state, "worker");
          state.events.push("Answer acceptance and Worker completion committed as one operation.");
          state.events.push("No agent_complete call or duplicate completion result exists.");
        },
      },
      {
        label: "Terminal Answer reaches the Spawner",
        call: `(runtime commits the Answer for req-task to spawner-session)`,
        apply(state) {
          deliverAnswer(state, "req-task");
          state.events.push("Answer delivery resolved the dependency and woke the Spawner.");
        },
      },
    ],
  },

  hitl: {
    title: "Human-in-the-loop escalation",
    purpose: "A nested Request wakes the Spawner; its plain-text Answer returns the decision.",
    steps: [
      {
        label: "Spawner creates the Worker with its initial Request",
        call: `agent_send({
  target: {
    spawn: { agent: "worker", name: "Migration worker" }
  },
  message: "Prepare the migration.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          spawnAgent(state, "worker", "worker-session");
          addRequest(state, "req-task", "spawner", "worker", "delivered");
          settle(state, "spawner");
          state.events.push("The initial task was carried by spawn instead of a second message call.");
        },
      },
      {
        label: "Worker requests a human decision through its Spawner",
        call: `agent_send({
  target: { agent: "spawner-session" },
  message: "Schema v1 and v2 conflict. Which should I use?",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          addRequest(state, "req-schema", "worker", "spawner", "delivered");
          settle(state, "worker");
          setActive(state, "spawner");
          state.events.push("The Request woke the waiting Spawner without a lifecycle-control call.");
        },
      },
      {
        label: "Spawner asks the human",
        call: `assistant: "Should the Worker use schema v1 or v2?"
(no control call)`,
        apply(state) {
          settle(state, "spawner", ["human"]);
          state.events.push("The Spawner now waits on both its child result and human input.");
        },
      },
      {
        label: "Human provides the decision",
        call: `user: "Use v2; v1 is obsolete."`,
        apply(state) {
          setActive(state, "spawner");
          state.events.push("Human input started a new Spawner run.");
        },
      },
      {
        label: "Spawner answers the Worker's Request",
        call: `agent_send({
  target: { request: "req-schema" },
  message: "Use v2; v1 is obsolete.",
  onAccepted: "settle"
})`,
        apply(state) {
          acceptAnswer(state, "req-schema");
          settle(state, "spawner");
          state.events.push("The Request target supplied Answer authority, routing, and delivery timing.");
        },
      },
      {
        label: "Decision reaches the Worker",
        call: `(runtime commits the Answer for req-schema to worker-session)`,
        apply(state) {
          deliverAnswer(state, "req-schema");
          state.events.push("The Worker woke and continued the original task.");
        },
      },
      {
        label: "Worker sends its final Answer and completes",
        call: `agent_send({
  target: { request: "req-task" },
  message: "Migration prepared with schema v2; tests pass.",
  onAccepted: "complete"
})`,
        apply(state) {
          acceptAnswer(state, "req-task");
          complete(state, "worker");
          state.events.push("The final work message itself completed the Worker.");
        },
      },
      {
        label: "Final result reaches the Spawner",
        call: `(runtime commits the Answer for req-task to spawner-session)`,
        apply(state) {
          deliverAnswer(state, "req-task");
          state.events.push("The Spawner's original task dependency resolved.");
        },
      },
    ],
  },

  review: {
    title: "Reviewer–implementer loop",
    purpose: "A Request can spawn a reviewer and later auto-resume its ended Agent when the requester has Child Control.",
    steps: [
      {
        label: "Owner spawns the Implementer with its initial Request",
        call: `agent_send({
  target: {
    spawn: { agent: "worker", name: "Implementer" }
  },
  message: "Implement the routing change and obtain review approval.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          spawnAgent(state, "implementer", "implementer-session");
          addRequest(state, "req-implementation", "owner", "implementer", "delivered");
          settle(state, "owner");
          state.events.push("The Owner waits on the Implementer's terminal Answer.");
        },
      },
      {
        label: "Implementer spawns a Reviewer with the review Request",
        call: `agent_send({
  target: {
    spawn: { agent: "reviewer", name: "Routing reviewer" }
  },
  message: "Review revision abc123.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          spawnAgent(state, "reviewer", "reviewer-session");
          addRequest(state, "req-review-1", "implementer", "reviewer", "delivered");
          settle(state, "implementer");
          state.events.push("The review Request was present in the Reviewer's first model context.");
        },
      },
      {
        label: "Reviewer answers and requests the revision in one message",
        call: `agent_send({
  target: { request: "req-review-1" },
  message: "Changes required: serialize the retry path, then send the revision.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          acceptAnswer(state, "req-review-1");
          addRequest(state, "req-revision", "reviewer", "implementer", "queued");
          settle(state, "reviewer");
          state.events.push("One accepted message closed req-review-1 and opened req-revision atomically.");
          state.events.push("The message ID of the Answer is also the new Request ID.");
        },
      },
      {
        label: "Answer-and-Request reaches the Implementer",
        call: `(runtime commits one message that answers req-review-1 and delivers req-revision)`,
        apply(state) {
          deliverAnswer(state, "req-review-1");
          state.requests.find((request) => request.id === "req-revision")!.status = "delivered";
          state.events.push("The Implementer woke with the review result and its next reply obligation.");
        },
      },
      {
        label: "Implementer answers the revision Request and requests re-review",
        call: `agent_send({
  target: { request: "req-revision" },
  message: "Retry path fixed in def456; please review again.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          acceptAnswer(state, "req-revision");
          addRequest(state, "req-review-2", "implementer", "reviewer", "queued");
          settle(state, "implementer");
          state.events.push("The reverse message closed req-revision and opened req-review-2 atomically.");
        },
      },
      {
        label: "Revision Answer and re-review Request reach the Reviewer",
        call: `(runtime commits one message that answers req-revision and delivers req-review-2)`,
        apply(state) {
          deliverAnswer(state, "req-revision");
          state.requests.find((request) => request.id === "req-review-2")!.status = "delivered";
          state.events.push("The Reviewer woke with the revision and a new Request to answer.");
        },
      },
      {
        label: "Reviewer approves and completes",
        call: `agent_send({
  target: { request: "req-review-2" },
  message: "Approved.",
  response: "none",
  onAccepted: "complete"
})`,
        apply(state) {
          acceptAnswer(state, "req-review-2");
          complete(state, "reviewer");
          state.events.push("No new response obligation allowed the final Answer to complete the Reviewer.");
        },
      },
      {
        label: "Approval reaches the Implementer",
        call: `(runtime commits the Answer for req-review-2 to implementer-session)`,
        apply(state) {
          deliverAnswer(state, "req-review-2");
          state.events.push("The Implementer woke with approval.");
        },
      },
      {
        label: "Authorized Request automatically resumes the ended Reviewer",
        call: `agent_send({
  target: { agent: "reviewer-session" },
  message: "Verify the final release note.",
  response: { required: true, delivery: "steer" },
  onAccepted: "settle"
})`,
        apply(state) {
          state.agents.reviewer!.activation += 1;
          setActive(state, "reviewer");
          addRequest(state, "req-release-note", "implementer", "reviewer", "delivered");
          settle(state, "implementer");
          state.events.push("Child Control authorized a new Reviewer activation.");
          state.events.push("Activation creation and Request acceptance committed atomically.");
        },
      },
      {
        label: "Reviewer answers the follow-up and completes again",
        call: `agent_send({
  target: { request: "req-release-note" },
  message: "Release note verified.",
  response: "none",
  onAccepted: "complete"
})`,
        apply(state) {
          acceptAnswer(state, "req-release-note");
          complete(state, "reviewer");
          state.events.push("The resumed activation also ended through its useful final Answer.");
        },
      },
      {
        label: "Release-note Answer reaches the Implementer",
        call: `(runtime commits the Answer for req-release-note to implementer-session)`,
        apply(state) {
          deliverAnswer(state, "req-release-note");
          state.events.push("The follow-up dependency resolved.");
        },
      },
      {
        label: "Implementer answers the Owner and completes",
        call: `agent_send({
  target: { request: "req-implementation" },
  message: "Revision def456 implemented, approved, and release-note verified.",
  response: "none",
  onAccepted: "complete"
})`,
        apply(state) {
          acceptAnswer(state, "req-implementation");
          complete(state, "implementer");
          state.events.push("The final Answer is the implementation result and terminal action.");
        },
      },
      {
        label: "Implementation result reaches the Owner",
        call: `(runtime commits the Answer for req-implementation to owner-session)`,
        apply(state) {
          deliverAnswer(state, "req-implementation");
          state.events.push("Every spawned Agent completed through a useful outbound Answer.");
        },
      },
    ],
  },
};

let state = initialState("autonomous");

function formatWork(current: AgentState): string {
  if (current.phase === "active") return `${green}active${reset}`;
  if (current.phase === "ended(completed)") return `${red}ended(completed)${reset}`;
  return `${yellow}waiting(${current.waitingOn.join("+")})${reset}`;
}

function renderInterface(): void {
  console.log(`${bold}Candidate interface${reset}`);
  console.log(`${cyan}agent_send${reset}({ target, message, response?, delivery?, onAccepted })`);
  console.log(`${dim}target = { agent: AgentId } | { spawn: SpawnSpec } | { request: RequestId }${reset}`);
  console.log(`${dim}response = "none" | { required: true, delivery: "steer" | "deferred" }${reset}`);
  console.log(`${dim}onAccepted = "continue" | "settle" | "complete"${reset}`);
  console.log(`${dim}Target and response are orthogonal: a Request target plus required response is Answer + new Request.${reset}`);
  console.log(`${dim}The same shape also derives Signal, Request, spawn, and authorized auto-resume behavior.${reset}`);
  console.log(`${dim}All content uses one plain message string. There is no agent_answer or agent_complete tool.${reset}`);
}

function render(): void {
  console.clear();
  const scenario = scenarios[state.scenario];
  console.log(`${bold}PROTOTYPE — Minimal agent-facing messaging interface${reset}\n`);
  renderInterface();

  console.log(`\n${bold}${scenario.title}${reset}`);
  console.log(`${dim}${scenario.purpose}${reset}`);
  console.log(`${bold}progress${reset} ${state.step}/${scenario.steps.length}`);

  console.log(`\n${bold}Last call / runtime action${reset}`);
  console.log(state.lastCall);

  console.log(`\n${bold}Agents${reset}`);
  for (const [name, current] of Object.entries(state.agents)) {
    const dependencies =
      current.unresolvedRequests.length > 0
        ? `  requests=[${current.unresolvedRequests.join(", ")}]`
        : "";
    console.log(
      `${bold}${name.padEnd(12)}${reset} ${formatWork(current)}  activation=${current.activation}${dependencies}`,
    );
  }

  console.log(`\n${bold}Requests${reset}`);
  if (state.requests.length === 0) {
    console.log(`${dim}(none)${reset}`);
  } else {
    for (const request of state.requests) {
      console.log(`${bold}${request.id}${reset}  ${request.from} → ${request.to}  ${request.status}`);
    }
  }

  console.log(`\n${bold}Recent events${reset}`);
  for (const event of state.events.slice(-6)) console.log(`${dim}•${reset} ${event}`);

  console.log(`\n${bold}Actions${reset}`);
  console.log(`${bold}1${reset} autonomous   ${bold}2${reset} human-in-loop   ${bold}3${reset} reviewer–implementer`);
  console.log(`${bold}n${reset} next step    ${bold}z${reset} reset    ${bold}q${reset} quit`);
  process.stdout.write(`\n${bold}>${reset} `);
}

function chooseScenario(scenario: ScenarioName): void {
  state = initialState(scenario);
}

function nextStep(): void {
  const step = scenarios[state.scenario].steps[state.step];
  if (!step) {
    state.events.push("Scenario already complete.");
    return;
  }
  state.lastCall = step.call;
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
    case "z":
      state = initialState(state.scenario);
      break;
    case "q":
      readline.close();
      return;
  }
  render();
});
readline.on("close", () => process.exit(0));

render();
