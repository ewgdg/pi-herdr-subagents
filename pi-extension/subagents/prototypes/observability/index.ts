/**
 * PROTOTYPE — three disposable TUI directions for subagent observability.
 *
 * Question: how should Pi show waiting reasons, queued requests, delegated
 * workflows, and suspicious states without creating unnecessary model turns?
 */

import type {
  ExtensionAPI,
  ExtensionCommandContext,
  Theme,
} from "@earendil-works/pi-coding-agent";
import {
  Key,
  matchesKey,
  truncateToWidth,
  visibleWidth,
  type Component,
  type TUI,
} from "@earendil-works/pi-tui";

type VariantKey = "A" | "B" | "C";
type AgentState = "active" | "waiting-human" | "waiting-agent" | "waiting-operation" | "stalled";

interface PrototypeAgent {
  name: string;
  role: string;
  model: string;
  effort: string;
  state: AgentState;
  duration: string;
  reason: string;
  queue: string;
  detail: string;
}

interface VariantDefinition {
  key: VariantKey;
  name: string;
  thesis: string;
}

const VARIANTS: VariantDefinition[] = [
  {
    key: "A",
    name: "Roster first",
    thesis: "Keep the whole workflow visible; expand one Agent only when debugging.",
  },
  {
    key: "B",
    name: "Attention inbox",
    thesis: "Collapse healthy work and spend persistent space only on human decisions or risk.",
  },
  {
    key: "C",
    name: "Workflow map",
    thesis: "Make delegation and Request dependencies primary; derive attention from the graph.",
  },
];

const AGENTS: PrototypeAgent[] = [
  {
    name: "Main",
    role: "Workflow Owner",
    model: "openai-codex/gpt-5.6-sol",
    effort: "high",
    state: "active",
    duration: "00:42",
    reason: "integrating independent results",
    queue: "1 Answer queued",
    detail: "Owns the Workflow and can inspect every participant without relaying peer messages.",
  },
  {
    name: "Auth scout",
    role: "scout",
    model: "google/gemini-3-flash",
    effort: "low",
    state: "waiting-agent",
    duration: "04:18",
    reason: "Request req-31 → Security review",
    queue: "Answer accepted · Steer",
    detail: "The dependency resolves when the Answer is committed to this Agent's session.",
  },
  {
    name: "Security review",
    role: "reviewer",
    model: "anthropic/claude-opus-4.6",
    effort: "high",
    state: "active",
    duration: "02:07",
    reason: "reviewing token rotation",
    queue: "1 Request delivered",
    detail: "The reviewer is the only Agent allowed to answer req-31.",
  },
  {
    name: "Database research",
    role: "researcher",
    model: "openai-codex/gpt-5.4-mini",
    effort: "medium",
    state: "waiting-operation",
    duration: "11:03",
    reason: "CI index job op-8",
    queue: "0 messages",
    detail: "Operational waiting is diagnostic context. It does not imply human input is required.",
  },
  {
    name: "Observability prototype",
    role: "designer",
    model: "openai-codex/gpt-5.6-sol",
    effort: "high",
    state: "waiting-human",
    duration: "01:26",
    reason: "choose a rendering direction",
    queue: "0 messages",
    detail: "Human waiting is quiet but clearly actionable. It must not manufacture a model turn.",
  },
  {
    name: "Migration worker",
    role: "worker",
    model: "google/gemini-3-pro",
    effort: "medium",
    state: "stalled",
    duration: "03:12",
    reason: "Recipient Inbox Router unreachable",
    queue: "2 accepted · 1 in doubt",
    detail: "Accepted pointers are durable. Resume requires lifecycle authority; messaging alone cannot restart it.",
  },
];

const ATTENTION_ITEMS = [
  {
    level: "act" as const,
    title: "Migration worker needs recovery",
    detail: "Endpoint lost for 3m 12s · 2 accepted messages remain durable",
    action: "Workflow Owner or direct Spawner may resume",
  },
  {
    level: "decide" as const,
    title: "Observability prototype waits for you",
    detail: "Choose a rendering direction; no model turn is running",
    action: "Open Agent or send input",
  },
  {
    level: "watch" as const,
    title: "Message req-47 acceptance is in doubt",
    detail: "Retry with the same Message Identity; never create a replacement",
    action: "Automatic probe may clear this without human action",
  },
];

export default function observabilityPrototype(pi: ExtensionAPI): void {
  pi.registerCommand("observability-prototype", {
    description: "Compare three throwaway subagent observability directions",
    handler: async (_args: string, ctx: ExtensionCommandContext) => {
      if (ctx.mode !== "tui") {
        ctx.ui.notify("The observability prototype requires Pi's interactive TUI.", "warning");
        return;
      }

      const choice = await ctx.ui.custom<VariantDefinition | undefined>(
        (tui, theme, _keybindings, done) =>
          new ObservabilityPrototype(tui, theme, done),
      );

      if (!choice) return;
      ctx.ui.setEditorText(
        `For “Define observability and human-attention behavior”, choose Variant ${choice.key} — ${choice.name}. ` +
          "Keep: <what works>. Change: <what does not>. Steal from another variant: <optional>.",
      );
      ctx.ui.notify(`Variant ${choice.key} selected; feedback prompt placed in the editor.`, "info");
    },
  });
}

class ObservabilityPrototype implements Component {
  private variantIndex = 0;
  private selectedAgentIndex = 0;
  private selectedAttentionIndex = 0;
  private showDetail = true;
  private showRoster = false;

  constructor(
    private readonly tui: TUI,
    private readonly theme: Theme,
    private readonly done: (choice: VariantDefinition | undefined) => void,
  ) {}

  handleInput(data: string): void {
    if (matchesKey(data, Key.escape)) {
      this.done(undefined);
      return;
    }
    if (matchesKey(data, Key.enter)) {
      this.done(VARIANTS[this.variantIndex]);
      return;
    }
    if (matchesKey(data, Key.left)) {
      this.variantIndex = (this.variantIndex + VARIANTS.length - 1) % VARIANTS.length;
    } else if (matchesKey(data, Key.right)) {
      this.variantIndex = (this.variantIndex + 1) % VARIANTS.length;
    } else if (matchesKey(data, Key.up)) {
      if (this.currentVariantKey() === "B" && !this.showRoster) {
        this.selectedAttentionIndex = Math.max(0, this.selectedAttentionIndex - 1);
      } else {
        this.selectedAgentIndex = Math.max(0, this.selectedAgentIndex - 1);
      }
    } else if (matchesKey(data, Key.down)) {
      if (this.currentVariantKey() === "B" && !this.showRoster) {
        this.selectedAttentionIndex = Math.min(ATTENTION_ITEMS.length - 1, this.selectedAttentionIndex + 1);
      } else {
        this.selectedAgentIndex = Math.min(AGENTS.length - 1, this.selectedAgentIndex + 1);
      }
    } else if (matchesKey(data, Key.space)) {
      this.showDetail = !this.showDetail;
    } else if (data.toLowerCase() === "r" && this.currentVariantKey() === "B") {
      this.showRoster = !this.showRoster;
    } else {
      return;
    }
    this.tui.requestRender();
  }

  render(width: number): string[] {
    const frameWidth = Math.max(1, Math.min(width, 108));
    const variant = VARIANTS[this.variantIndex];
    const content = [
      ...this.renderVariantHeader(variant),
      "",
      ...this.renderVariant(variant.key, frameWidth - 4),
      "",
      this.theme.fg(
        "dim",
        "Local runtime projection only — status changes never enter model context or trigger a turn.",
      ),
      this.theme.fg(
        "dim",
        "←/→ variant  ↑/↓ select  r roster  space details  enter choose  esc close",
      ),
    ];

    return this.frame(content, frameWidth, `Observability prototype · ${variant.key}/3`);
  }

  invalidate(): void {}

  private renderVariantHeader(variant: VariantDefinition): string[] {
    return [
      `${this.theme.fg("accent", this.theme.bold(`${variant.key} — ${variant.name}`))}`,
      this.theme.fg("muted", variant.thesis),
    ];
  }

  private renderVariant(key: VariantKey, width: number): string[] {
    if (key === "A") return this.renderRoster(width);
    if (key === "B") return this.renderAttentionInbox(width);
    return this.renderWorkflowMap(width);
  }

  private renderRoster(width: number): string[] {
    const lines = [
      this.section("Live participants", "5 healthy · 1 suspicious"),
    ];

    for (const [index, agent] of AGENTS.entries()) {
      const selected = index === this.selectedAgentIndex;
      const prefix = selected ? this.theme.fg("accent", "▶") : " ";
      const name = selected ? this.theme.bold(agent.name) : agent.name;
      const left = `${prefix} ${agent.duration}  ${name} (${agent.role})`;
      const right = `${this.stateLabel(agent.state)}  ${agent.queue}`;
      lines.push(this.splitLine(left, right, width));
      lines.push(`    ${this.theme.fg("dim", `${agent.model} · effort ${agent.effort}`)}`);
      lines.push(`    ${this.theme.fg("dim", "↳")} ${this.reasonText(agent)}`);
    }

    if (this.showDetail) {
      const selected = AGENTS[this.selectedAgentIndex];
      lines.push("");
      lines.push(this.section(`Inspector · ${selected.name}`, "read-only"));
      lines.push(`  ${selected.detail}`);
      lines.push(`  ${this.theme.fg("muted", "Waiting reason:")} ${selected.reason}`);
    }

    return lines;
  }

  private renderAttentionInbox(width: number): string[] {
    const lines = [
      this.section("Needs attention", "2 actions · 1 watch"),
    ];

    for (const [index, item] of ATTENTION_ITEMS.entries()) {
      const selected = !this.showRoster && index === this.selectedAttentionIndex;
      const prefix = selected ? this.theme.fg("accent", "▶") : " ";
      const badge = item.level === "act"
        ? this.theme.fg("error", this.theme.bold("ACT"))
        : item.level === "decide"
          ? this.theme.fg("warning", this.theme.bold("DECIDE"))
          : this.theme.fg("muted", "WATCH");
      lines.push(`${prefix} ${badge}  ${this.theme.bold(item.title)}`);
      lines.push(`     ${this.theme.fg("muted", item.detail)}`);
      if (this.showDetail) lines.push(`     ${this.theme.fg("accent", "↳")} ${item.action}`);
      lines.push("");
    }

    lines.push(this.section("Everything else", "quiet by default"));
    lines.push(this.splitLine("3 Agents active", "1 Request · 1 Answer queued", width));
    lines.push(this.splitLine("2 Agents waiting normally", "human 1 · agent 1 · operation 1", width));
    lines.push(this.theme.fg("dim", "Press r to open the roster. Healthy transitions remain passive runtime events."));

    if (this.showRoster) {
      lines.push("");
      lines.push(this.section("Roster", "↑/↓ inspect · r close"));
      for (const [index, agent] of AGENTS.entries()) {
        const selected = index === this.selectedAgentIndex;
        const prefix = selected ? this.theme.fg("accent", "▶") : " ";
        lines.push(this.splitLine(`${prefix} ${agent.name} (${agent.role})`, this.stateLabel(agent.state), width));
        lines.push(`    ${this.theme.fg("dim", `${agent.model} · effort ${agent.effort}`)}`);
        if (selected && this.showDetail) {
          lines.push(`    ${this.theme.fg("muted", "↳")} ${agent.reason} · ${agent.queue}`);
        }
      }
    }
    return lines;
  }

  private renderWorkflowMap(width: number): string[] {
    const selected = AGENTS[this.selectedAgentIndex];
    const lines = [
      this.section("Workflow · Main", "capability-scoped visibility"),
      `${this.theme.fg("accent", "●")} Main  ${this.theme.fg("muted", "active · integrating results")}`,
      `├─ ${this.stateGlyph("waiting-agent")} Auth scout  ${this.theme.fg("muted", "waiting(agent)")}`,
      `│  └─ req-31 ${this.theme.fg("accent", "────▶")} Security review  ${this.theme.fg("muted", "active")}`,
      `│     └─ Answer ${this.theme.fg("warning", "queued · Steer")}`,
      `├─ ${this.stateGlyph("waiting-operation")} Database research  ${this.theme.fg("muted", "waiting(operation: op-8)")}`,
      `├─ ${this.stateGlyph("waiting-human")} Observability prototype  ${this.theme.fg("warning", "waiting(human)")}`,
      `└─ ${this.stateGlyph("stalled")} Migration worker  ${this.theme.fg("error", "stalled · recovery needed")}`,
      `   ├─ 2 Pending Message Pointers ${this.theme.fg("success", "durable")}`,
      `   └─ req-47 ${this.theme.fg("warning", "AcceptanceInDoubt")}`,
      "",
      this.section("Path inspector", selected.name),
      `  State: ${this.stateLabel(selected.state)} · ${selected.duration}`,
      `  Runtime: ${this.theme.fg("muted", `${selected.model} · effort ${selected.effort}`)}`,
      `  Reason: ${this.reasonText(selected)}`,
    ];

    if (this.showDetail) {
      lines.push(`  Queue: ${selected.queue}`);
      lines.push(`  ${this.theme.fg("dim", selected.detail)}`);
    }

    lines.push("");
    lines.push(
      this.splitLine(
        this.theme.fg("error", "1 recovery action"),
        this.theme.fg("warning", "1 human decision"),
        width,
      ),
    );
    return lines;
  }

  private stateLabel(state: AgentState): string {
    if (state === "active") return this.theme.fg("accent", "active");
    if (state === "waiting-human") return this.theme.fg("warning", "waiting · human");
    if (state === "waiting-agent") return this.theme.fg("muted", "waiting · agent");
    if (state === "waiting-operation") return this.theme.fg("muted", "waiting · operation");
    return this.theme.fg("error", this.theme.bold("stalled"));
  }

  private currentVariantKey(): VariantKey {
    return VARIANTS[this.variantIndex].key;
  }

  private stateGlyph(state: AgentState): string {
    if (state === "active") return this.theme.fg("accent", "●");
    if (state === "stalled") return this.theme.fg("error", "◆");
    if (state === "waiting-human") return this.theme.fg("warning", "◉");
    return this.theme.fg("muted", "○");
  }

  private reasonText(agent: PrototypeAgent): string {
    const color = agent.state === "stalled"
      ? "error"
      : agent.state === "waiting-human"
        ? "warning"
        : "muted";
    return this.theme.fg(color, agent.reason);
  }

  private section(left: string, right: string): string {
    return `${this.theme.fg("borderMuted", "──")} ${this.theme.bold(left)} ${this.theme.fg("dim", `· ${right}`)}`;
  }

  private splitLine(left: string, right: string, width: number): string {
    const available = Math.max(1, width);
    const rightWidth = visibleWidth(right);
    if (rightWidth + 3 >= available) {
      return truncateToWidth(`${left} · ${right}`, available, "");
    }
    const leftPart = truncateToWidth(left, available - rightWidth - 1, "");
    const padding = " ".repeat(Math.max(1, available - visibleWidth(leftPart) - rightWidth));
    return `${leftPart}${padding}${right}`;
  }

  private frame(content: string[], width: number, title: string): string[] {
    if (width === 1) return content.map(() => this.theme.fg("border", "│"));
    const innerWidth = Math.max(0, width - 2);
    const titleText = `─ ${title} `;
    const topInner = truncateToWidth(titleText, innerWidth, "").padEnd(innerWidth, "─");
    const fit = (line: string): string => {
      const clipped = truncateToWidth(line, innerWidth, "");
      return clipped + " ".repeat(Math.max(0, innerWidth - visibleWidth(clipped)));
    };

    return [
      this.theme.fg("border", `╭${topInner}╮`),
      ...content.map((line) =>
        `${this.theme.fg("border", "│")}${fit(line)}${this.theme.fg("border", "│")}`,
      ),
      this.theme.fg("border", `╰${"─".repeat(innerWidth)}╯`),
    ];
  }
}
