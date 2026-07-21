import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxText,
  fauxToolCall,
  type Context,
  type FauxResponseStep,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import {
  SMOKE_SCRIPT_FILENAME,
  type SmokeScript,
  type SmokeScriptExpectation,
  type SmokeScriptStep,
} from "./script.ts";

const PROVIDER_ID = "smoke-faux";
const MODEL_ID = "scripted";
const API_ID = "smoke-faux-api";

function readScript(): SmokeScript {
  const agentDirectory = process.env.PI_CODING_AGENT_DIR;
  if (!agentDirectory) throw new Error("PI_CODING_AGENT_DIR is required by the smoke Faux Provider.");
  const scriptPath = join(agentDirectory, SMOKE_SCRIPT_FILENAME);
  const script = JSON.parse(readFileSync(scriptPath, "utf8")) as Partial<SmokeScript>;
  if (!Array.isArray(script.owner) || !Array.isArray(script.child)) {
    throw new Error("Smoke Faux Provider script requires owner and child response arrays.");
  }
  return script as SmokeScript;
}

function lastMessage(context: Context): Context["messages"][number] | undefined {
  return context.messages.at(-1);
}

function assertExpectedInput(
  role: "owner" | "child",
  stepIndex: number,
  expectation: SmokeScriptExpectation,
  context: Context,
): void {
  const serializedContext = JSON.stringify(context);
  if (expectation.contextIncludes && !serializedContext.includes(expectation.contextIncludes)) {
    throw new Error(
      `Faux script mismatch for ${role} step ${stepIndex + 1}: context does not include ${JSON.stringify(expectation.contextIncludes)}.`,
    );
  }

  if (expectation.toolResult) {
    const message = lastMessage(context);
    if (message?.role !== "toolResult" || message.toolName !== expectation.toolResult) {
      throw new Error(
        `Faux script mismatch for ${role} step ${stepIndex + 1}: expected latest tool result ${JSON.stringify(expectation.toolResult)}, got ${JSON.stringify(message)}.`,
      );
    }
  }
}

function scriptedResponses(role: "owner" | "child", steps: SmokeScriptStep[]): FauxResponseStep[] {
  return steps.map((step, stepIndex) => (context) => {
    assertExpectedInput(role, stepIndex, step.expect, context);
    if (step.respond.tool) {
      const content = [
        ...(typeof step.respond.text === "string" ? [fauxText(step.respond.text)] : []),
        fauxToolCall(step.respond.tool, step.respond.arguments ?? {}, {
          id: `${role}-tool-${stepIndex + 1}`,
        }),
      ];
      return fauxAssistantMessage(
        content,
        { stopReason: "toolUse" },
      );
    }
    if (typeof step.respond.text === "string") return fauxAssistantMessage(step.respond.text);
    throw new Error(`Faux script ${role} step ${stepIndex + 1} has no response.`);
  });
}

export default function smokeFauxProvider(pi: ExtensionAPI): void {
  if (process.env.PI_OFFLINE !== "1") {
    throw new Error("Smoke Faux Provider requires PI_OFFLINE=1 in both Owner and child processes.");
  }
  const role = process.env.PI_SUBAGENT_ID ? "child" : "owner";
  const script = readScript();
  const faux = fauxProvider({
    api: API_ID,
    provider: PROVIDER_ID,
    models: [{ id: MODEL_ID, name: "Scripted Smoke Model", reasoning: false, input: ["text"] }],
    tokenSize: { min: 1_000_000, max: 1_000_000 },
  });
  faux.setResponses(scriptedResponses(role, script[role]));

  pi.registerProvider(PROVIDER_ID, {
    name: "Smoke Faux Provider",
    baseUrl: "http://localhost:0",
    apiKey: "smoke-test",
    api: API_ID,
    models: [
      {
        id: MODEL_ID,
        name: "Scripted Smoke Model",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 32_000,
        maxTokens: 4_096,
      },
    ],
    streamSimple: faux.provider.streamSimple,
  });
}
