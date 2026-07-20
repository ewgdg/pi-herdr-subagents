export function latestAssistantTurnWasAborted(messages: unknown[] | undefined): boolean {
  if (!messages) return false;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index] as { role?: unknown; stopReason?: unknown } | undefined;
    if (message?.role === "assistant") return message.stopReason === "aborted";
  }
  return false;
}
