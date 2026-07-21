export const SMOKE_SCRIPT_FILENAME = "smoke-faux-script.json";

export interface SmokeScriptExpectation {
  contextIncludes?: string;
  toolResult?: string;
}

export interface SmokeScriptResponse {
  text?: string;
  tool?: string;
  arguments?: Record<string, unknown>;
}

export interface SmokeScriptStep {
  expect: SmokeScriptExpectation;
  respond: SmokeScriptResponse;
}

export interface SmokeScript {
  owner: SmokeScriptStep[];
  child: SmokeScriptStep[];
}
