#!/usr/bin/env node

/** PROTOTYPE — terminal shell around the pure routing state model. */

import { createInterface } from "node:readline";
import {
  initialState,
  reduce,
  type PrototypeAction,
  type PrototypeState,
} from "./model.ts";

const bold = "\x1b[1m";
const dim = "\x1b[2m";
const reset = "\x1b[0m";
const green = "\x1b[32m";
const yellow = "\x1b[33m";
const red = "\x1b[31m";

let state = initialState();

function valueColor(value: string): string {
  if (["running", "open", "bound", "held", "queued", "committed"].includes(value)) {
    return green;
  }
  if (["waiting", "scheduled", "pending", "acceptance_unknown", "recipient_unreachable"].includes(value)) {
    return yellow;
  }
  if (["failed", "completed", "absent", "rejected_permanent"].includes(value)) {
    return red;
  }
  return "";
}

function field(name: string, value: string): string {
  return `${bold}${name.padEnd(20)}${reset} ${valueColor(value)}${value}${reset}`;
}

function renderMessages(current: PrototypeState): string[] {
  if (current.messages.length === 0) return [`${dim}(none)${reset}`];
  return current.messages.map((message) => {
    const sequence = message.acceptanceSequence == null ? "-" : String(message.acceptanceSequence);
    const batch = message.batchId ?? "-";
    return [
      `${bold}${message.id}${reset}`,
      `timing=${message.delivery}`,
      `seq=${sequence}`,
      `pointer=${valueColor(message.pointer)}${message.pointer}${reset}`,
      `transcript=${valueColor(message.recipientTranscript)}${message.recipientTranscript}${reset}`,
      `sender=${valueColor(message.senderOutcome)}${message.senderOutcome}${reset}`,
      `replay=${message.replayedAcknowledgement ? "yes" : "no"}`,
      `batch=${batch}`,
    ].join("  ");
  });
}

function render(): void {
  console.clear();
  console.log(`${bold}PROTOTYPE — Cross-process routing and delivery${reset}`);
  console.log(
    `${dim}Candidate: recipient-finalized acceptance + local IPC + SQLite + fenced ownership${reset}\n`,
  );

  console.log(`${bold}Recipient Agent${reset}`);
  console.log(field("activation", state.activation));
  console.log(field("work", state.work));
  console.log(field("run", `${state.run}${state.runId ? ` (${state.runId})` : ""}`));
  console.log(field("router endpoint", state.endpoint));
  console.log(field("ownership lock", state.ownershipLock));
  console.log(field("ownership epoch", String(state.ownershipEpoch)));
  console.log(field("wake", state.wake));
  console.log(field("next sequence", String(state.nextSequence)));
  console.log(
    field(
      "armed faults",
      [
        state.loseNextAcknowledgement ? "lose-ack" : undefined,
        state.crashAfterNextTranscriptCommit ? "commit-crash" : undefined,
      ]
        .filter(Boolean)
        .join(", ") || "none",
    ),
  );

  console.log(`\n${bold}Messages${reset}`);
  for (const line of renderMessages(state)) console.log(line);

  console.log(`\n${bold}Recent events${reset}`);
  for (const event of state.events) console.log(`${dim}•${reset} ${event}`);

  console.log(`\n${bold}Actions${reset}`);
  console.log(
    `${bold}s${reset} ${dim}send Steer${reset}       ` +
      `${bold}d${reset} ${dim}send Deferred${reset}    ` +
      `${bold}r${reset} ${dim}retry ambiguous/rejected${reset}`,
  );
  console.log(
    `${bold}b${reset} ${dim}turn boundary${reset}    ` +
      `${bold}t${reset} ${dim}agent settled${reset}    ` +
      `${bold}w${reset} ${dim}start scheduled wake${reset}`,
  );
  console.log(
    `${bold}c${reset} ${dim}crash process${reset}    ` +
      `${bold}o${reset} ${dim}authorized resume${reset} ` +
      `${bold}u${reset} ${dim}duplicate resume${reset}`,
  );
  console.log(`${bold}f${reset} ${dim}stale owner commit attempt${reset}`);
  console.log(
    `${bold}i${reset} ${dim}toggle interrupted${reset} ` +
      `${bold}e${reset} ${dim}complete activation${reset}`,
  );
  console.log(
    `${bold}l${reset} ${dim}lose next ack${reset}    ` +
      `${bold}k${reset} ${dim}crash after transcript commit${reset}`,
  );
  console.log(`${bold}z${reset} ${dim}reset${reset}            ${bold}q${reset} ${dim}quit${reset}`);
  process.stdout.write(`\n${bold}>${reset} `);
}

function actionFor(input: string): PrototypeAction | undefined {
  switch (input.trim().toLowerCase()) {
    case "s":
      return { type: "send", delivery: "steer" };
    case "d":
      return { type: "send", delivery: "deferred" };
    case "r":
      return { type: "retry" };
    case "b":
      return { type: "turnBoundary" };
    case "t":
      return { type: "settle" };
    case "w":
      return { type: "startWake" };
    case "c":
      return { type: "crash" };
    case "o":
      return { type: "resume" };
    case "u":
      return { type: "duplicateResume" };
    case "f":
      return { type: "staleOwnerCommit" };
    case "i":
      return { type: "interrupt" };
    case "e":
      return { type: "complete" };
    case "l":
      return { type: "loseNextAcknowledgement" };
    case "k":
      return { type: "crashAfterNextTranscriptCommit" };
    case "z":
      return { type: "reset" };
    default:
      return undefined;
  }
}

const readline = createInterface({ input: process.stdin, output: process.stdout });
readline.on("line", (line) => {
  if (line.trim().toLowerCase() === "q") {
    readline.close();
    return;
  }
  const action = actionFor(line);
  if (action) state = reduce(state, action);
  render();
});
readline.on("close", () => process.exit(0));

render();
