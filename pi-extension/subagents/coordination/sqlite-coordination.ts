import { DatabaseSync } from "node:sqlite";

const DEFAULT_BUSY_TIMEOUT_MS = 5_000;

export interface CoordinationState {
  value: string;
  version: number;
}

export interface OwnershipToken {
  resourceId: string;
  ownerId: string;
  epoch: number;
}

export type OwnershipAcquisition =
  | { acquired: true; token: OwnershipToken }
  | { acquired: false; currentOwner: OwnershipToken };

export interface FencedState {
  value: string;
  fencingEpoch: number;
}

interface StateRow {
  value: string;
  version: number;
}

interface OwnerRow {
  owner_id: string;
  fencing_epoch: number;
}

interface EpochRow {
  last_epoch: number;
}

interface FencedStateRow {
  value: string;
  fencing_epoch: number;
}

export class SQLiteCoordinationStore {
  readonly #database: DatabaseSync;
  #closed = false;

  constructor(databasePath: string, busyTimeoutMs = DEFAULT_BUSY_TIMEOUT_MS) {
    this.#database = new DatabaseSync(databasePath, { timeout: busyTimeoutMs });
    this.#database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;

      CREATE TABLE IF NOT EXISTS coordination_state (
        state_key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        version INTEGER NOT NULL CHECK (version > 0)
      ) STRICT;

      CREATE TABLE IF NOT EXISTS ownership_epochs (
        resource_id TEXT PRIMARY KEY,
        last_epoch INTEGER NOT NULL CHECK (last_epoch > 0)
      ) STRICT;

      CREATE TABLE IF NOT EXISTS ownership (
        resource_id TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        fencing_epoch INTEGER NOT NULL CHECK (fencing_epoch > 0)
      ) STRICT;

      CREATE TABLE IF NOT EXISTS fenced_state (
        resource_id TEXT NOT NULL,
        state_key TEXT NOT NULL,
        value TEXT NOT NULL,
        fencing_epoch INTEGER NOT NULL CHECK (fencing_epoch > 0),
        PRIMARY KEY (resource_id, state_key)
      ) STRICT;
    `);
  }

  close(): void {
    if (this.#closed) return;
    this.#database.close();
    this.#closed = true;
  }

  readState(stateKey: string): CoordinationState | undefined {
    const row = this.#database
      .prepare("SELECT value, version FROM coordination_state WHERE state_key = ?")
      .get(stateKey) as StateRow | undefined;
    return row ? { value: row.value, version: Number(row.version) } : undefined;
  }

  compareAndSetState(
    stateKey: string,
    expectedVersion: number | null,
    value: string,
  ): boolean {
    if (expectedVersion === null) {
      const result = this.#database
        .prepare(`
          INSERT OR IGNORE INTO coordination_state (state_key, value, version)
          VALUES (?, ?, 1)
        `)
        .run(stateKey, value);
      return Number(result.changes) === 1;
    }

    const result = this.#database
      .prepare(`
        UPDATE coordination_state
        SET value = ?, version = version + 1
        WHERE state_key = ? AND version = ?
      `)
      .run(value, stateKey, expectedVersion);
    return Number(result.changes) === 1;
  }

  acquireOwnership(resourceId: string, ownerId: string): OwnershipAcquisition {
    return this.#withImmediateTransaction(() => {
      const currentOwner = this.#readOwner(resourceId);
      if (currentOwner) {
        return { acquired: false, currentOwner };
      }

      const epochRow = this.#database
        .prepare("SELECT last_epoch FROM ownership_epochs WHERE resource_id = ?")
        .get(resourceId) as EpochRow | undefined;
      const epoch = Number(epochRow?.last_epoch ?? 0) + 1;
      this.#database
        .prepare(`
          INSERT INTO ownership_epochs (resource_id, last_epoch)
          VALUES (?, ?)
          ON CONFLICT (resource_id) DO UPDATE SET last_epoch = excluded.last_epoch
        `)
        .run(resourceId, epoch);
      this.#database
        .prepare(`
          INSERT INTO ownership (resource_id, owner_id, fencing_epoch)
          VALUES (?, ?, ?)
        `)
        .run(resourceId, ownerId, epoch);

      return {
        acquired: true,
        token: { resourceId, ownerId, epoch },
      };
    });
  }

  releaseOwnership(token: OwnershipToken): boolean {
    return this.#releaseExactOwner(token);
  }

  writeFencedState(token: OwnershipToken, stateKey: string, value: string): boolean {
    return this.#withImmediateTransaction(() => {
      const currentOwner = this.#readOwner(token.resourceId);
      if (!currentOwner || !sameOwnership(currentOwner, token)) return false;

      this.#database
        .prepare(`
          INSERT INTO fenced_state (resource_id, state_key, value, fencing_epoch)
          VALUES (?, ?, ?, ?)
          ON CONFLICT (resource_id, state_key) DO UPDATE SET
            value = excluded.value,
            fencing_epoch = excluded.fencing_epoch
        `)
        .run(token.resourceId, stateKey, value, token.epoch);
      return true;
    });
  }

  readFencedState(resourceId: string, stateKey: string): FencedState | undefined {
    const row = this.#database
      .prepare(`
        SELECT value, fencing_epoch
        FROM fenced_state
        WHERE resource_id = ? AND state_key = ?
      `)
      .get(resourceId, stateKey) as FencedStateRow | undefined;
    return row
      ? { value: row.value, fencingEpoch: Number(row.fencing_epoch) }
      : undefined;
  }

  integrityCheck(): string {
    const row = this.#database.prepare("PRAGMA integrity_check").get() as
      | { integrity_check: string }
      | undefined;
    return row?.integrity_check ?? "unknown";
  }

  #readOwner(resourceId: string): OwnershipToken | undefined {
    const row = this.#database
      .prepare(`
        SELECT owner_id, fencing_epoch
        FROM ownership
        WHERE resource_id = ?
      `)
      .get(resourceId) as OwnerRow | undefined;
    return row
      ? {
          resourceId,
          ownerId: row.owner_id,
          epoch: Number(row.fencing_epoch),
        }
      : undefined;
  }

  #releaseExactOwner(token: OwnershipToken): boolean {
    const result = this.#database
      .prepare(`
        DELETE FROM ownership
        WHERE resource_id = ? AND owner_id = ? AND fencing_epoch = ?
      `)
      .run(token.resourceId, token.ownerId, token.epoch);
    return Number(result.changes) === 1;
  }

  #withImmediateTransaction<T>(operation: () => T): T {
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.isTransaction) this.#database.exec("ROLLBACK");
      throw error;
    }
  }
}

function sameOwnership(left: OwnershipToken, right: OwnershipToken): boolean {
  return left.resourceId === right.resourceId
    && left.ownerId === right.ownerId
    && left.epoch === right.epoch;
}
