import { createHash, randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CURRENT_IPC_VERSION,
  listenForFramedIpc,
  connectFramedIpc,
  type FramedIpcConnection,
  type FramedIpcServer,
} from "../coordination/framed-ipc.ts";

export const PROVISIONAL_SPAWN_ENDPOINT_ENV = "PI_WORKFLOW_PROVISIONAL_SPAWN_ENDPOINT";
export const PROVISIONAL_AGENT_RUN_KIND_ENV = "PI_WORKFLOW_PROVISIONAL_RUN_KIND";
const READY = "provisional-spawn.ready";
const PROJECT = "provisional-spawn.project";
const PROJECTED = "provisional-spawn.projected";
const RELEASE = "provisional-spawn.release";
const RELEASED = "provisional-spawn.released";
const ABORT = "provisional-spawn.abort";
const DEFAULT_PHASE_TIMEOUT_MS = 15_000;

export interface ProvisionalSpawnCommit { runId: string; fencingEpoch: number; }
export interface ProvisionalSpawnReady { routerEndpoint: string; }

/** Metadata-only plan; the recipient resolves payload from the sender JSONL. */
export interface ProvisionalSpawnProjection {
  senderSessionPath: string;
  messageId: string;
  sourceEntryId: string;
  senderAgentId: string;
  recipientAgentId: string;
  payloadDigest: string;
  activationIntent: string;
  agentDefinition: string;
  agentName: string;
}

/** Spawner-side startup fence; it creates no durable protocol rows. */
export class ProvisionalSpawnGate {
  readonly endpoint: string;
  readonly #phaseTimeoutMs: number;
  #server: FramedIpcServer | undefined;
  #child: FramedIpcConnection | undefined;
  #readyResolve!: (ready: ProvisionalSpawnReady) => void;
  #readyReject!: (error: Error) => void;
  readonly #ready: Promise<ProvisionalSpawnReady>;
  #projectedResolve!: () => void;
  #projectedReject!: (error: Error) => void;
  #projected: Promise<void> | undefined;
  #projectionPlan: ProvisionalSpawnProjection | undefined;
  #releasedResolve!: () => void;
  #releasedReject!: (error: Error) => void;
  #released: Promise<void> | undefined;
  #closed = false;

  private constructor(endpoint: string, phaseTimeoutMs: number) {
    this.endpoint = endpoint;
    this.#phaseTimeoutMs = phaseTimeoutMs;
    this.#ready = new Promise<ProvisionalSpawnReady>((resolve, reject) => {
      this.#readyResolve = resolve;
      this.#readyReject = reject;
    });
  }

  static async create(options: { phaseTimeoutMs?: number } = {}): Promise<ProvisionalSpawnGate> {
    const endpoint = process.platform === "win32"
      ? `\\\\.\\pipe\\pi-herdr-provisional-${randomUUID()}`
      : join(tmpdir(), `pi-herdr-provisional-${createHash("sha256").update(randomUUID()).digest("hex").slice(0, 24)}.sock`);
    const timeout = options.phaseTimeoutMs ?? DEFAULT_PHASE_TIMEOUT_MS;
    if (!Number.isSafeInteger(timeout) || timeout <= 0) throw new RangeError("Provisional phase timeout must be a positive integer");
    const gate = new ProvisionalSpawnGate(endpoint, timeout);
    gate.#server = await listenForFramedIpc(endpoint, (connection) => gate.#accept(connection));
    return gate;
  }

  waitUntilReady(): Promise<ProvisionalSpawnReady> { return this.#bounded("READY", this.#ready); }

  async project(plan: ProvisionalSpawnProjection): Promise<void> {
    if (this.#projectionPlan) {
      if (!sameProjection(this.#projectionPlan, plan)) throw new Error("Conflicting provisional Spawn projection plan");
      return this.#projected!;
    }
    this.#projectionPlan = plan;
    this.#projected = new Promise<void>((resolve, reject) => { this.#projectedResolve = resolve; this.#projectedReject = reject; });
    await this.#send(PROJECT, plan);
    return this.#bounded("PROJECT", this.#projected);
  }

  /** Publish committed ownership only after its durable transaction commits. */
  async release(commit: ProvisionalSpawnCommit): Promise<void> {
    this.#released ??= new Promise<void>((resolve, reject) => { this.#releasedResolve = resolve; this.#releasedReject = reject; });
    await this.#send(RELEASE, commit);
    return this.#bounded("RELEASE", this.#released);
  }

  async abort(): Promise<void> { await this.#send(ABORT, undefined).catch(() => undefined); }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#rejectAll(new Error("Provisional Spawner closed before completing its phase"));
    await this.#server?.close();
    this.#server = undefined;
  }

  #accept(connection: FramedIpcConnection): void {
    connection.onError((error) => this.#rejectAll(error));
    void connection.closed.then((result) => {
      if (result.kind === "failed") this.#rejectAll(result.error);
      else if (!this.#closed) this.#rejectAll(new Error("Provisional child disconnected before completing its phase"));
    });
    connection.onMessage((frame) => {
      if (frame.type === READY && !this.#child) {
        if (!isReady(frame.payload)) { this.#rejectAll(new Error("Invalid provisional Router readiness handshake")); connection.end(); return; }
        this.#child = connection;
        this.#readyResolve(frame.payload);
        return;
      }
      if (frame.type === PROJECTED && this.#child === connection) { this.#projectedResolve?.(); return; }
      if (frame.type === RELEASED && this.#child === connection) { this.#releasedResolve?.(); return; }
      this.#rejectAll(new Error("Invalid provisional spawn handshake"));
      connection.end();
    });
  }

  #rejectAll(error: Error): void {
    this.#readyReject(error);
    this.#projectedReject?.(error);
    this.#releasedReject?.(error);
  }

  async #send(type: string, payload: unknown): Promise<void> {
    if (!this.#child) throw new Error("Provisional child never reported readiness");
    await this.#child.send({ version: CURRENT_IPC_VERSION, type, payload });
    if (type === ABORT) this.#child.end();
  }

  async #bounded<T>(phase: "READY" | "PROJECT" | "RELEASE", pending: Promise<T>): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        pending,
        new Promise<T>((_resolve, reject) => {
          timer = setTimeout(() => reject(new Error(`Provisional ${phase} phase timed out after ${this.#phaseTimeoutMs}ms`)), this.#phaseTimeoutMs);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
}

export async function awaitProvisionalSpawnCommit(
  endpoint: string,
  ready: ProvisionalSpawnReady,
  options: {
    project?(plan: ProvisionalSpawnProjection): Promise<void> | void;
    release?(commit: ProvisionalSpawnCommit): Promise<void> | void;
    phaseTimeoutMs?: number;
  } = {},
): Promise<ProvisionalSpawnCommit> {
  const connection = await connectFramedIpc(endpoint);
  const timeout = options.phaseTimeoutMs ?? DEFAULT_PHASE_TIMEOUT_MS;
  let settled = false;
  let rejectPeerClosed!: (error: Error) => void;
  const peerClosed = new Promise<never>((_resolve, reject) => { rejectPeerClosed = reject; });
  // The race below owns this rejection after success as well, so normal local
  // connection teardown cannot become an unhandled rejection.
  const racePeerClosed = <T>(pending: Promise<T>): Promise<T> => Promise.race([pending, peerClosed]);
  try {
    const outcome = new Promise<ProvisionalSpawnCommit>((resolve, reject) => {
      const fail = (error: Error) => {
        if (settled) return;
        settled = true;
        reject(error);
      };
      const succeed = (commit: ProvisionalSpawnCommit) => {
        if (settled) return;
        settled = true;
        resolve(commit);
      };
      connection.onError(fail);
      void connection.closed.then((result) => {
        if (settled) return;
        const error = result.kind === "failed"
          ? result.error
          : new Error("Provisional Spawner disconnected before completing its phase");
        rejectPeerClosed(error);
        fail(error);
      });
      connection.onMessage((frame) => {
        if (frame.type === PROJECT && isProjection(frame.payload)) {
          void boundedChildPhase("PROJECT", timeout, racePeerClosed(Promise.resolve().then(() => options.project?.(frame.payload))))
            .then(async () => {
              if (settled) return;
              await racePeerClosed(connection.send({ version: CURRENT_IPC_VERSION, type: PROJECTED }));
            }).catch(fail);
          return;
        }
        if (frame.type === RELEASE && isCommit(frame.payload)) {
          void boundedChildPhase("RELEASE", timeout, racePeerClosed(Promise.resolve().then(() => options.release?.(frame.payload))))
            .then(async () => {
              if (settled) return;
              await racePeerClosed(connection.send({ version: CURRENT_IPC_VERSION, type: RELEASED }));
              succeed(frame.payload);
            }).catch(fail);
          return;
        }
        fail(frame.type === ABORT ? new Error("Spawn preparation was aborted") : new Error("Invalid provisional spawn commit handshake"));
      });
    });
    await connection.send({ version: CURRENT_IPC_VERSION, type: READY, payload: ready });
    return await boundedChildPhase("READY", timeout, racePeerClosed(outcome));
  } finally {
    settled = true;
    connection.end();
  }
}

async function boundedChildPhase<T>(phase: string, timeout: number, pending: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([pending, new Promise<T>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Provisional ${phase} phase timed out after ${timeout}ms`)), timeout);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}

function isReady(value: unknown): value is ProvisionalSpawnReady { return !!value && typeof value === "object" && typeof (value as { routerEndpoint?: unknown }).routerEndpoint === "string"; }
function isProjection(value: unknown): value is ProvisionalSpawnProjection { if (!value || typeof value !== "object") return false; const candidate = value as Record<string, unknown>; return ["senderSessionPath", "messageId", "sourceEntryId", "senderAgentId", "recipientAgentId", "payloadDigest", "activationIntent", "agentDefinition", "agentName"].every((field) => typeof candidate[field] === "string" && candidate[field]); }
function isCommit(value: unknown): value is ProvisionalSpawnCommit { return !!value && typeof value === "object" && typeof (value as { runId?: unknown }).runId === "string" && typeof (value as { fencingEpoch?: unknown }).fencingEpoch === "number"; }
function sameProjection(left: ProvisionalSpawnProjection, right: ProvisionalSpawnProjection): boolean { return left.senderSessionPath === right.senderSessionPath && left.messageId === right.messageId && left.sourceEntryId === right.sourceEntryId && left.senderAgentId === right.senderAgentId && left.recipientAgentId === right.recipientAgentId && left.payloadDigest === right.payloadDigest && left.activationIntent === right.activationIntent && left.agentDefinition === right.agentDefinition && left.agentName === right.agentName; }
