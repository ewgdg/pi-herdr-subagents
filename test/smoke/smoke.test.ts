import { test } from "node:test";
import { runDeterministicSmoke, SMOKE_HARD_TIMEOUT_MS } from "./harness.ts";

test(
  "headless herdr carries Owner Pi through child completion",
  { timeout: SMOKE_HARD_TIMEOUT_MS + 1_000 },
  runDeterministicSmoke,
);
