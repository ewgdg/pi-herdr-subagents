import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import {
  createServer,
  type Server,
  Socket,
} from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { afterEach, describe, it } from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import {
  SQLiteCoordinationStore,
  type OwnershipAcquisition,
  type OwnershipToken,
} from "../pi-extension/subagents/coordination/sqlite-coordination.ts";
import {
  CURRENT_IPC_VERSION,
  FramedIpcConnection,
  FramedMessageDecoder,
  connectFramedIpc,
  encodeFramedMessage,
} from "../pi-extension/subagents/coordination/framed-ipc.ts";
import { SQLiteWorkflowStore } from "../pi-extension/subagents/protocol/sqlite-workflow-store.ts";

const workerPath = fileURLToPath(new URL("./fixtures/coordination-worker.ts", import.meta.url));
const WORKER_TIMEOUT_MS = 30_000;
const temporaryDirectories: string[] = [];
const activeWorkers = new Set<WorkerHandle>();

interface WorkerHandle {
  child: ChildProcessWithoutNullStreams;
  nextResult<T>(): Promise<T>;
  completed: Promise<void>;
  expectSignalExit(signal?: NodeJS.Signals): void;
  terminateForCleanup(): void;
}

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "pi-herdr-coordination-"));
  temporaryDirectories.push(directory);
  return directory;
}

function spawnWorker(...args: string[]): WorkerHandle {
  const child = spawn(process.execPath, [workerPath, ...args], {
    stdio: ["pipe", "pipe", "pipe"],
  });
  const lines = createInterface({ input: child.stdout });
  const queued: unknown[] = [];
  const waiters: Array<{
    resolve(value: unknown): void;
    reject(error: Error): void;
  }> = [];
  let stderr = "";
  let exitError: Error | undefined;
  let expectedSignal: NodeJS.Signals | undefined;
  let forcedFailure: Error | undefined;

  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  lines.on("line", (line) => {
    const value = JSON.parse(line) as unknown;
    const waiter = waiters.shift();
    if (waiter) waiter.resolve(value);
    else queued.push(value);
  });

  const completed = new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => {
      clearTimeout(workerTimeout);
      if (!forcedFailure && (code === 0 || signal === expectedSignal)) {
        resolve();
      } else {
        exitError = forcedFailure ?? new Error(
          `worker exited with code ${code} signal ${signal ?? "none"}: ${stderr}`,
        );
        reject(exitError);
      }
      const outputError = exitError ?? new Error("worker exited before expected output");
      for (const waiter of waiters.splice(0)) waiter.reject(outputError);
    });
  });

  const workerTimeout = setTimeout(() => {
    forcedFailure = new Error(`worker exceeded ${WORKER_TIMEOUT_MS}ms: ${stderr}`);
    child.kill();
  }, WORKER_TIMEOUT_MS);

  const handle: WorkerHandle = {
    child,
    completed,
    expectSignalExit(signal = "SIGTERM") {
      expectedSignal = signal;
    },
    terminateForCleanup() {
      if (child.exitCode !== null || child.signalCode !== null) return;
      expectedSignal = "SIGTERM";
      child.stdin.end();
      child.kill();
    },
    async nextResult<T>() {
      if (queued.length > 0) return queued.shift() as T;
      if (exitError) throw exitError;
      return await new Promise<T>((resolve, reject) => {
        const waiter = {
          resolve(value: unknown) {
            clearTimeout(timeout);
            resolve(value as T);
          },
          reject(error: Error) {
            clearTimeout(timeout);
            reject(error);
          },
        };
        const timeout = setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index >= 0) waiters.splice(index, 1);
          reject(new Error(`timed out waiting for worker output: ${stderr}`));
        }, 10_000);
        waiters.push(waiter);
      });
    },
  };
  activeWorkers.add(handle);
  void completed.then(
    () => activeWorkers.delete(handle),
    () => activeWorkers.delete(handle),
  );
  return handle;
}

async function runWorker<T>(...args: string[]): Promise<T> {
  const worker = spawnWorker(...args);
  const result = await worker.nextResult<T>();
  await worker.completed;
  return result;
}

function acquired(result: OwnershipAcquisition): OwnershipToken {
  assert.equal(result.acquired, true, "expected ownership acquisition to succeed");
  return result.token;
}

async function openSocketPair(
  options: ConstructorParameters<typeof FramedIpcConnection>[1] = {},
  writableHighWaterMark?: number,
): Promise<{
  connection: FramedIpcConnection;
  transport: Socket;
  peer: Socket;
  server: Server;
}> {
  const server = createServer();
  const endpoint = process.platform === "win32"
    ? `\\\\.\\pipe\\pi-herdr-${process.pid}-${randomUUID()}`
    : join(await temporaryDirectory(), "framed-ipc.sock");
  server.listen(endpoint);
  await once(server, "listening");
  const accepted = once(server, "connection") as Promise<[Socket]>;
  const transport = new Socket({ writableHighWaterMark });
  transport.connect(endpoint);
  await once(transport, "connect");
  const [peer] = await accepted;
  return {
    connection: new FramedIpcConnection(transport, options),
    transport,
    peer,
    server,
  };
}

async function closeServer(server: Server): Promise<void> {
  if (!server.listening) return;
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

afterEach(async () => {
  const workers = [...activeWorkers];
  for (const worker of workers) worker.terminateForCleanup();
  await Promise.allSettled(workers.map((worker) => worker.completed));
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true })),
  );
});

describe("versioned length-prefixed IPC framing", () => {
  it("decodes fragmented and coalesced frames without losing message boundaries", () => {
    const first = encodeFramedMessage({
      version: CURRENT_IPC_VERSION,
      type: "first",
      payload: { value: 1 },
    });
    const second = encodeFramedMessage({
      version: CURRENT_IPC_VERSION,
      type: "second",
      payload: { value: 2 },
    });
    const decoder = new FramedMessageDecoder();

    assert.deepEqual(decoder.push(first.subarray(0, 2)), []);
    assert.deepEqual(decoder.push(Buffer.concat([first.subarray(2), second])), [
      { version: CURRENT_IPC_VERSION, type: "first", payload: { value: 1 } },
      { version: CURRENT_IPC_VERSION, type: "second", payload: { value: 2 } },
    ]);
  });

  it("rejects partial frame headers and payloads when input ends", () => {
    const partialHeader = new FramedMessageDecoder();
    partialHeader.push(Buffer.from([0, 0]));
    assert.throws(() => partialHeader.finish(), /truncated IPC frame header/);

    const partialPayload = new FramedMessageDecoder();
    const header = Buffer.alloc(4);
    header.writeUInt32BE(10, 0);
    partialPayload.push(Buffer.concat([header, Buffer.from("short")]));
    assert.throws(() => partialPayload.finish(), /expected 10 payload bytes, received 5/);
  });

  it("exchanges framed messages between independent local processes", async () => {
    const directory = await temporaryDirectory();
    const endpoint = process.platform === "win32"
      ? `\\\\.\\pipe\\pi-herdr-${process.pid}-${Date.now()}`
      : join(directory, "coordination.sock");
    const server = spawnWorker("ipc-server", endpoint);
    assert.deepEqual(await server.nextResult(), { ready: true });

    const client = spawnWorker("ipc-client", endpoint);
    assert.deepEqual(await client.nextResult(), {
      received: ["ack:first", "ack:second", "ack:finish"],
    });
    assert.deepEqual(await server.nextResult(), {
      received: ["first", "second", "finish"],
    });
    await Promise.all([client.completed, server.completed]);
  });

  it("closes an IPC server with an idle client without leaving the worker alive", async () => {
    const directory = await temporaryDirectory();
    const endpoint = process.platform === "win32"
      ? `\\\\.\\pipe\\pi-herdr-idle-close-${process.pid}-${Date.now()}`
      : join(directory, "idle-close.sock");
    const worker = spawnWorker("ipc-idle-close", endpoint);
    assert.deepEqual(await worker.nextResult(), { ready: true });
    const client = await connectFramedIpc(endpoint);
    worker.child.stdin.write("close\n");
    assert.deepEqual(await worker.nextResult(), { closed: true });
    await worker.completed;
    assert.deepEqual(await client.closed, { kind: "closed" });
  });

  it("waits for a backpressured transport to flush before resolving", async () => {
    const maximumFrameBytes = 64 * 1024;
    const { connection, transport, peer, server } = await openSocketPair(
      { maximumFrameBytes },
      1,
    );
    peer.pause();
    transport.cork();
    let sendCompleted = false;

    try {
      const sending = connection.send({
        version: CURRENT_IPC_VERSION,
        type: "large",
        payload: "x".repeat(16 * 1024),
      }).then(() => {
        sendCompleted = true;
      });
      await nextTurn();
      assert.equal(transport.writableNeedDrain, true, "test must establish backpressure");
      assert.equal(sendCompleted, false);

      peer.on("data", () => {});
      peer.resume();
      transport.uncork();
      await sending;
      connection.end();
      assert.deepEqual(await connection.closed, { kind: "closed" });
      await assert.rejects(
        connection.send({ version: CURRENT_IPC_VERSION, type: "too-late" }),
        /not writable/,
      );
    } finally {
      transport.uncork();
      peer.destroy();
      await closeServer(server);
    }
  });

  it("reports malformed input exactly once and records failed closure", async () => {
    const { connection, peer, server } = await openSocketPair();
    const errors: Error[] = [];
    connection.onError((error) => errors.push(error));
    const payload = Buffer.from(JSON.stringify({ version: 99, type: "unsupported" }));
    const frame = Buffer.alloc(4 + payload.byteLength);
    frame.writeUInt32BE(payload.byteLength, 0);
    payload.copy(frame, 4);

    try {
      peer.write(frame);
      const closeResult = await connection.closed;
      assert.equal(closeResult.kind, "failed");
      assert.equal(errors.length, 1);
      assert.match(errors[0].message, /unsupported IPC message version/);
    } finally {
      peer.destroy();
      await closeServer(server);
    }
  });

  it("records a truncated frame at peer EOF as a failed closure", async () => {
    const { connection, peer, server } = await openSocketPair();
    const errors: Error[] = [];
    connection.onError((error) => errors.push(error));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(100, 0);

    try {
      peer.end(Buffer.concat([header, Buffer.from("abc")]));
      const closeResult = await connection.closed;
      assert.equal(closeResult.kind, "failed");
      assert.equal(errors.length, 1);
      assert.match(errors[0].message, /expected 100 payload bytes, received 3/);
    } finally {
      peer.destroy();
      await closeServer(server);
    }
  });

  it("fails when undelivered inbound messages exceed the configured bound", async () => {
    const { connection, peer, server } = await openSocketPair({
      maximumPendingMessages: 1,
    });
    const errors: Error[] = [];
    connection.onError((error) => errors.push(error));
    const frames = Buffer.concat([
      encodeFramedMessage({ version: CURRENT_IPC_VERSION, type: "first" }),
      encodeFramedMessage({ version: CURRENT_IPC_VERSION, type: "second" }),
    ]);

    try {
      peer.write(frames);
      const closeResult = await connection.closed;
      assert.equal(closeResult.kind, "failed");
      assert.equal(errors.length, 1);
      assert.match(errors[0].message, /exceeded 1 pending messages/);
    } finally {
      peer.destroy();
      await closeServer(server);
    }
  });
});

describe("SQLite cross-process coordination", () => {
  it("retries fresh non-WAL direct Signal initialization under concurrent startup", async () => {
    const directory = await temporaryDirectory();
    const rounds = 25;
    const workersPerRound = 4;
    for (let round = 0; round < rounds; round += 1) {
      const databasePath = join(directory, `signals-${round}.sqlite`);
      new SQLiteWorkflowStore(databasePath).close();
      const database = new DatabaseSync(databasePath);
      database.exec(`
        PRAGMA journal_mode = DELETE;
      `);
      database.close();

      const workers = Array.from(
        { length: workersPerRound },
        () => spawnWorker("signal-initialize", databasePath),
      );
      assert.deepEqual(
        await Promise.all(workers.map((worker) => worker.nextResult())),
        Array.from({ length: workersPerRound }, () => ({ initialized: true })),
      );
      await Promise.all(workers.map((worker) => worker.completed));
      const upgraded = new DatabaseSync(databasePath);
      for (const table of ["direct_signal_messages", "pending_message_pointers"]) {
        const columns = upgraded.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
        assert.equal(columns.some((column) => column.name === "delivery_timing"), true);
      }
      upgraded.close();
    }
  });

  it("upgrades launch policy schema once under concurrent pre-upgrade Workflow startup", async () => {
    const directory = await temporaryDirectory();
    const databasePath = join(directory, "workflow-upgrade.sqlite");
    const legacy = new DatabaseSync(databasePath);
    legacy.exec(`
      PRAGMA journal_mode = DELETE;
      CREATE TABLE workflow_metadata (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1), owner_agent_id TEXT NOT NULL,
        owner_session_path TEXT NOT NULL, created_at_ms INTEGER NOT NULL
      ) STRICT;
      CREATE TABLE workflow_agents (
        agent_id TEXT PRIMARY KEY, session_path TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
        agent_definition TEXT, spawner_agent_id TEXT REFERENCES workflow_agents(agent_id),
        delegation_policy TEXT CHECK (delegation_policy IN ('disabled', 'approval-required', 'autonomous')),
        created_at_ms INTEGER NOT NULL
      ) STRICT;
    `);
    legacy.close();

    const workers = Array.from({ length: 4 }, () => spawnWorker("workflow-initialize", databasePath));
    assert.deepEqual(
      await Promise.all(workers.map((worker) => worker.nextResult())),
      Array.from({ length: 4 }, () => ({ initialized: true })),
    );
    await Promise.all(workers.map((worker) => worker.completed));
    const upgraded = new DatabaseSync(databasePath);
    const columns = upgraded.prepare("PRAGMA table_info(workflow_agents)").all() as Array<{ name: string }>;
    assert.equal(columns.filter((column) => column.name === "launch_policy_json").length, 1);
    upgraded.close();
  });

  it("allows independent processes to atomically update shared state", async () => {
    const directory = await temporaryDirectory();
    const databasePath = join(directory, "coordination.sqlite");
    const store = new SQLiteCoordinationStore(databasePath);
    assert.equal(store.compareAndSetState("shared-counter", null, "0"), true);
    store.close();

    await Promise.all(
      Array.from({ length: 4 }, () => runWorker("increment", databasePath, "50")),
    );

    const reader = new SQLiteCoordinationStore(databasePath);
    assert.deepEqual(reader.readState("shared-counter"), { value: "200", version: 201 });
    assert.equal(reader.integrityCheck(), "ok");
    reader.close();
  });

  it("grants exactly one owner when independent processes race", async () => {
    const directory = await temporaryDirectory();
    const databasePath = join(directory, "coordination.sqlite");
    new SQLiteCoordinationStore(databasePath).close();

    const attempts = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        runWorker<OwnershipAcquisition>(
          "acquire",
          databasePath,
          "pi-session",
          `run-${index}`,
        )),
    );

    assert.equal(attempts.filter((attempt) => attempt.acquired).length, 1);
    const winner = attempts.find((attempt) => attempt.acquired);
    assert.ok(winner?.acquired);
    assert.equal(winner.token.epoch, 1);
  });

  it("increases fencing epochs and rejects stale protected mutations", async () => {
    const directory = await temporaryDirectory();
    const databasePath = join(directory, "coordination.sqlite");
    const first = acquired(await runWorker(
      "acquire",
      databasePath,
      "pi-session",
      "run-one",
    ));
    assert.deepEqual(
      await runWorker("fenced-write", databasePath, JSON.stringify(first), "status", "first"),
      { written: true },
    );
    assert.deepEqual(
      await runWorker("release", databasePath, JSON.stringify(first)),
      { released: true },
    );

    const second = acquired(await runWorker(
      "acquire",
      databasePath,
      "pi-session",
      "run-two",
    ));
    assert.ok(second.epoch > first.epoch);
    assert.deepEqual(
      await runWorker("fenced-write", databasePath, JSON.stringify(first), "status", "stale"),
      { written: false },
    );
    assert.deepEqual(
      await runWorker("fenced-write", databasePath, JSON.stringify(second), "status", "current"),
      { written: true },
    );

    const reader = new SQLiteCoordinationStore(databasePath);
    assert.deepEqual(reader.readFencedState("pi-session", "status"), {
      value: "current",
      fencingEpoch: second.epoch,
    });
    reader.close();
  });

  it("releases only the exact owner after confirmed process exit", async () => {
    const directory = await temporaryDirectory();
    const databasePath = join(directory, "coordination.sqlite");
    const owner = spawnWorker("hold", databasePath, "pi-session", "departing-run");
    const first = acquired(await owner.nextResult<OwnershipAcquisition>());

    const blocked = await runWorker<OwnershipAcquisition>(
      "acquire",
      databasePath,
      "pi-session",
      "early-replacement",
    );
    assert.equal(blocked.acquired, false);

    owner.expectSignalExit();
    owner.child.kill();
    await owner.completed;

    const coordinator = new SQLiteCoordinationStore(databasePath);
    assert.equal(
      coordinator.releaseOwnership({ ...first, epoch: first.epoch + 1 }),
      false,
    );
    assert.equal(coordinator.releaseOwnership(first), true);
    coordinator.close();

    const replacement = acquired(await runWorker(
      "acquire",
      databasePath,
      "pi-session",
      "replacement-run",
    ));
    assert.ok(replacement.epoch > first.epoch);

    const delayedCoordinator = new SQLiteCoordinationStore(databasePath);
    assert.equal(delayedCoordinator.releaseOwnership(first), false);
    delayedCoordinator.close();

    const overlapping = await runWorker<OwnershipAcquisition>(
      "acquire",
      databasePath,
      "pi-session",
      "overlapping-run",
    );
    assert.equal(overlapping.acquired, false);
  });

  it("allows replacement only after a live owner voluntarily releases", async () => {
    const directory = await temporaryDirectory();
    const databasePath = join(directory, "coordination.sqlite");
    const owner = spawnWorker("hold", databasePath, "pi-session", "graceful-run");
    const first = acquired(await owner.nextResult<OwnershipAcquisition>());

    owner.child.stdin.write("release\n");
    assert.deepEqual(await owner.nextResult(), { released: true });
    await owner.completed;

    const replacement = acquired(await runWorker(
      "acquire",
      databasePath,
      "pi-session",
      "next-run",
    ));
    assert.ok(replacement.epoch > first.epoch);
  });
});
