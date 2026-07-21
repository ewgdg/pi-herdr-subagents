import process from "node:process";
import {
  SQLiteCoordinationStore,
  type OwnershipToken,
} from "../../pi-extension/subagents/coordination/sqlite-coordination.ts";
import {
  CURRENT_IPC_VERSION,
  connectFramedIpc,
  listenForFramedIpc,
} from "../../pi-extension/subagents/coordination/framed-ipc.ts";
import { DirectSignalStore } from "../../pi-extension/subagents/protocol/sqlite-message-store.ts";

function writeResult(result: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(`${JSON.stringify(result)}\n`, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

function parseToken(serialized: string): OwnershipToken {
  return JSON.parse(serialized) as OwnershipToken;
}

async function incrementSharedState(databasePath: string, iterationsText: string): Promise<void> {
  const store = new SQLiteCoordinationStore(databasePath);
  const iterations = Number(iterationsText);
  for (let completed = 0; completed < iterations;) {
    const current = store.readState("shared-counter");
    const currentValue = current ? Number(current.value) : 0;
    const updated = store.compareAndSetState(
      "shared-counter",
      current?.version ?? null,
      String(currentValue + 1),
    );
    if (updated) completed += 1;
  }
  await writeResult(store.readState("shared-counter"));
  store.close();
}

async function acquire(databasePath: string, resourceId: string, ownerId: string): Promise<void> {
  const store = new SQLiteCoordinationStore(databasePath);
  await writeResult(store.acquireOwnership(resourceId, ownerId));
  store.close();
}

async function release(databasePath: string, serializedToken: string): Promise<void> {
  const store = new SQLiteCoordinationStore(databasePath);
  await writeResult({ released: store.releaseOwnership(parseToken(serializedToken)) });
  store.close();
}

async function fencedWrite(
  databasePath: string,
  serializedToken: string,
  stateKey: string,
  value: string,
): Promise<void> {
  const store = new SQLiteCoordinationStore(databasePath);
  await writeResult({ written: store.writeFencedState(parseToken(serializedToken), stateKey, value) });
  store.close();
}

async function holdOwnership(databasePath: string, resourceId: string, ownerId: string): Promise<void> {
  const store = new SQLiteCoordinationStore(databasePath);
  const acquisition = store.acquireOwnership(resourceId, ownerId);
  await writeResult(acquisition);
  if (!acquisition.acquired) {
    store.close();
    process.exitCode = 2;
    return;
  }

  process.stdin.setEncoding("utf8");
  process.stdin.resume();
  await new Promise<void>((resolve, reject) => process.stdin.once("data", (command) => {
    void (async () => {
      if (command.trim() === "release") {
        await writeResult({ released: store.releaseOwnership(acquisition.token) });
      }
      store.close();
      process.stdin.pause();
      resolve();
    })().catch(reject);
  }));
}

async function runIpcServer(endpoint: string): Promise<void> {
  const received: string[] = [];
  let finish: () => void;
  let fail: (error: Error) => void;
  const finished = new Promise<void>((resolve, reject) => {
    finish = resolve;
    fail = reject;
  });
  const server = await listenForFramedIpc(endpoint, (connection) => {
    connection.onError(fail);
    connection.onMessage((message) => {
      void (async () => {
        received.push(message.type);
        await connection.send({
          version: CURRENT_IPC_VERSION,
          type: `ack:${message.type}`,
          payload: message.payload,
        });
        if (message.type === "finish") {
          connection.end();
          const closeResult = await connection.closed;
          if (closeResult.kind === "failed") throw closeResult.error;
          await server.close();
          await writeResult({ received });
          finish();
        }
      })().catch(fail);
    });
  });
  await writeResult({ ready: true });
  await finished;
}

async function runIpcClient(endpoint: string): Promise<void> {
  const connection = await connectFramedIpc(endpoint);
  const received: string[] = [];
  let finish: () => void;
  let fail: (error: Error) => void;
  const finished = new Promise<void>((resolve, reject) => {
    finish = resolve;
    fail = reject;
  });
  connection.onError(fail);
  connection.onMessage((message) => {
    received.push(message.type);
    if (message.type === "ack:finish") {
      void writeResult({ received }).then(finish, fail);
    }
  });
  await connection.send({ version: CURRENT_IPC_VERSION, type: "first", payload: { sequence: 1 } });
  await connection.send({ version: CURRENT_IPC_VERSION, type: "second", payload: { sequence: 2 } });
  await connection.send({ version: CURRENT_IPC_VERSION, type: "finish", payload: { sequence: 3 } });
  await finished;
  const closeResult = await connection.closed;
  if (closeResult.kind === "failed") throw closeResult.error;
}

async function runIdleIpcClose(endpoint: string): Promise<void> {
  const server = await listenForFramedIpc(endpoint, () => {});
  await writeResult({ ready: true });
  process.stdin.setEncoding("utf8");
  process.stdin.resume();
  await new Promise<void>((resolve, reject) => process.stdin.once("data", (command) => {
    void (async () => {
      if (command.trim() !== "close") throw new Error(`Unknown idle IPC command: ${command}`);
      await server.close();
      await writeResult({ closed: true });
      process.stdin.pause();
      resolve();
    })().catch(reject);
  }));
}

async function upgradeSignalStore(databasePath: string): Promise<void> {
  const store = new DirectSignalStore(databasePath);
  store.close();
  await writeResult({ upgraded: true });
}

const [command, ...args] = process.argv.slice(2);

switch (command) {
  case "increment":
    await incrementSharedState(args[0], args[1]);
    break;
  case "acquire":
    await acquire(args[0], args[1], args[2]);
    break;
  case "release":
    await release(args[0], args[1]);
    break;
  case "fenced-write":
    await fencedWrite(args[0], args[1], args[2], args[3]);
    break;
  case "hold":
    await holdOwnership(args[0], args[1], args[2]);
    break;
  case "ipc-server":
    await runIpcServer(args[0]);
    break;
  case "ipc-client":
    await runIpcClient(args[0]);
    break;
  case "ipc-idle-close":
    await runIdleIpcClose(args[0]);
    break;
  case "signal-upgrade":
    await upgradeSignalStore(args[0]);
    break;
  default:
    throw new Error(`Unknown coordination worker command: ${command}`);
}
