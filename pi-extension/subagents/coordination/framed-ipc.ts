import { createConnection, createServer, type Server, type Socket } from "node:net";

export const CURRENT_IPC_VERSION = 1;
export const DEFAULT_MAXIMUM_FRAME_BYTES = 1024 * 1024;
export const DEFAULT_MAXIMUM_PENDING_MESSAGES = 1024;
const FRAME_LENGTH_BYTES = 4;

export interface FramedIpcMessage {
  version: number;
  type: string;
  payload?: unknown;
}

export type MessageListener = (message: FramedIpcMessage) => void;
export type ErrorListener = (error: Error) => void;

export interface FramedIpcConnectionOptions {
  maximumFrameBytes?: number;
  maximumPendingMessages?: number;
}

export type FramedIpcCloseResult =
  | { kind: "closed" }
  | { kind: "failed"; error: Error };

export function encodeFramedMessage(
  message: FramedIpcMessage,
  maximumFrameBytes = DEFAULT_MAXIMUM_FRAME_BYTES,
): Buffer {
  validateMessage(message);
  const payload = Buffer.from(JSON.stringify(message), "utf8");
  if (payload.byteLength > maximumFrameBytes) {
    throw new RangeError(
      `IPC frame is ${payload.byteLength} bytes; maximum is ${maximumFrameBytes}`,
    );
  }
  const frame = Buffer.allocUnsafe(FRAME_LENGTH_BYTES + payload.byteLength);
  frame.writeUInt32BE(payload.byteLength, 0);
  payload.copy(frame, FRAME_LENGTH_BYTES);
  return frame;
}

export class FramedMessageDecoder {
  readonly #maximumFrameBytes: number;
  #buffer = Buffer.alloc(0);

  constructor(maximumFrameBytes = DEFAULT_MAXIMUM_FRAME_BYTES) {
    this.#maximumFrameBytes = maximumFrameBytes;
  }

  push(chunk: Uint8Array): FramedIpcMessage[] {
    this.#buffer = this.#buffer.byteLength === 0
      ? Buffer.from(chunk)
      : Buffer.concat([this.#buffer, chunk]);
    const messages: FramedIpcMessage[] = [];

    while (this.#buffer.byteLength >= FRAME_LENGTH_BYTES) {
      const payloadLength = this.#buffer.readUInt32BE(0);
      if (payloadLength > this.#maximumFrameBytes) {
        throw new RangeError(
          `IPC frame declares ${payloadLength} bytes; maximum is ${this.#maximumFrameBytes}`,
        );
      }
      const frameLength = FRAME_LENGTH_BYTES + payloadLength;
      if (this.#buffer.byteLength < frameLength) break;

      const serialized = this.#buffer.subarray(FRAME_LENGTH_BYTES, frameLength).toString("utf8");
      const message = JSON.parse(serialized) as unknown;
      validateMessage(message);
      messages.push(message);
      this.#buffer = this.#buffer.subarray(frameLength);
    }

    return messages;
  }

  finish(): void {
    if (this.#buffer.byteLength === 0) return;
    if (this.#buffer.byteLength < FRAME_LENGTH_BYTES) {
      throw new Error(
        `truncated IPC frame header: received ${this.#buffer.byteLength} of ${FRAME_LENGTH_BYTES} bytes`,
      );
    }
    const payloadLength = this.#buffer.readUInt32BE(0);
    const receivedPayloadBytes = this.#buffer.byteLength - FRAME_LENGTH_BYTES;
    throw new Error(
      `truncated IPC frame: expected ${payloadLength} payload bytes, received ${receivedPayloadBytes}`,
    );
  }
}

export class FramedIpcConnection {
  readonly #socket: Socket;
  readonly #decoder: FramedMessageDecoder;
  readonly #maximumFrameBytes: number;
  readonly #maximumPendingMessages: number;
  readonly #messageListeners = new Set<MessageListener>();
  readonly #errorListeners = new Set<ErrorListener>();
  readonly #pendingMessages: FramedIpcMessage[] = [];
  #terminalError: Error | undefined;
  #closed = false;
  #inputFinalized = false;
  readonly closed: Promise<FramedIpcCloseResult>;

  constructor(socket: Socket, options: FramedIpcConnectionOptions = {}) {
    this.#socket = socket;
    this.#maximumFrameBytes = options.maximumFrameBytes ?? DEFAULT_MAXIMUM_FRAME_BYTES;
    this.#maximumPendingMessages = options.maximumPendingMessages
      ?? DEFAULT_MAXIMUM_PENDING_MESSAGES;
    this.#decoder = new FramedMessageDecoder(this.#maximumFrameBytes);
    this.closed = new Promise((resolve) => {
      socket.once("close", () => {
        this.#finalizeInput();
        this.#closed = true;
        resolve(this.#terminalError
          ? { kind: "failed", error: this.#terminalError }
          : { kind: "closed" });
      });
    });

    socket.on("end", () => this.#finalizeInput());

    socket.on("data", (chunk) => {
      let messages: FramedIpcMessage[];
      try {
        messages = this.#decoder.push(chunk);
      } catch (error) {
        const normalized = normalizeError(error);
        this.#notifyError(normalized);
        socket.destroy(normalized);
        return;
      }
      for (const message of messages) {
        if (this.#terminalError) break;
        this.#deliver(message);
      }
    });
    socket.on("error", (error) => this.#notifyError(error));
  }

  send(message: FramedIpcMessage): Promise<void> {
    if (this.#closed || this.#socket.destroyed || !this.#socket.writable) {
      return Promise.reject(new Error("IPC connection is not writable"));
    }
    const frame = encodeFramedMessage(message, this.#maximumFrameBytes);
    return new Promise((resolve, reject) => {
      this.#socket.write(frame, (error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  onMessage(listener: MessageListener): () => void {
    this.#messageListeners.add(listener);
    for (const message of this.#pendingMessages.splice(0)) listener(message);
    return () => this.#messageListeners.delete(listener);
  }

  onError(listener: ErrorListener): () => void {
    this.#errorListeners.add(listener);
    if (this.#terminalError) listener(this.#terminalError);
    return () => this.#errorListeners.delete(listener);
  }

  end(): void {
    this.#socket.end();
  }

  #deliver(message: FramedIpcMessage): void {
    if (this.#messageListeners.size === 0) {
      if (this.#pendingMessages.length >= this.#maximumPendingMessages) {
        const error = new RangeError(
          `IPC connection exceeded ${this.#maximumPendingMessages} pending messages`,
        );
        this.#notifyError(error);
        this.#socket.destroy(error);
        return;
      }
      this.#pendingMessages.push(message);
      return;
    }
    for (const listener of this.#messageListeners) listener(message);
  }

  #notifyError(error: Error): void {
    if (this.#terminalError) return;
    this.#terminalError = error;
    for (const listener of this.#errorListeners) listener(error);
  }

  #finalizeInput(): void {
    if (this.#inputFinalized) return;
    this.#inputFinalized = true;
    try {
      this.#decoder.finish();
    } catch (error) {
      const normalized = normalizeError(error);
      this.#notifyError(normalized);
      if (!this.#socket.destroyed) this.#socket.destroy(normalized);
    }
  }
}

export class FramedIpcServer {
  readonly #server: Server;

  constructor(server: Server) {
    this.#server = server;
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.#server.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }
}

export async function listenForFramedIpc(
  endpoint: string,
  onConnection: (connection: FramedIpcConnection) => void,
  options: FramedIpcConnectionOptions = {},
): Promise<FramedIpcServer> {
  const server = createServer((socket) =>
    onConnection(new FramedIpcConnection(socket, options)));
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => reject(error);
    server.once("error", onError);
    server.listen(endpoint, () => {
      server.off("error", onError);
      resolve();
    });
  });
  return new FramedIpcServer(server);
}

export async function connectFramedIpc(
  endpoint: string,
  options: FramedIpcConnectionOptions = {},
): Promise<FramedIpcConnection> {
  const socket = createConnection(endpoint);
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => reject(error);
    socket.once("error", onError);
    socket.once("connect", () => {
      socket.off("error", onError);
      resolve();
    });
  });
  return new FramedIpcConnection(socket, options);
}

function validateMessage(value: unknown): asserts value is FramedIpcMessage {
  if (!value || typeof value !== "object") {
    throw new TypeError("IPC message must be an object");
  }
  const candidate = value as Record<string, unknown>;
  if (candidate.version !== CURRENT_IPC_VERSION) {
    throw new TypeError(
      `unsupported IPC message version ${String(candidate.version)}; expected ${CURRENT_IPC_VERSION}`,
    );
  }
  if (typeof candidate.type !== "string" || candidate.type.length === 0) {
    throw new TypeError("IPC message type must be a non-empty string");
  }
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
