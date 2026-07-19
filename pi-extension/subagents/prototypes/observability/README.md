# Observability and human-attention prototype

**Throwaway prototype for “Define observability and human-attention behavior.”**

Question: how should Pi show structured waiting reasons, queued Requests and Answers, delegated Workflows, and suspicious runtime states without creating unnecessary model turns?

## Run

```bash
npm run prototype:observability
```

Controls:

- `←` / `→`: switch among the three directions
- `↑` / `↓`: select an attention item or inspect another Agent
- `R`: open or close the full roster from **Attention inbox**
- `Space`: toggle detail
- `Enter`: choose the current direction and place a feedback prompt in Pi's editor
- `Esc`: close without choosing

## Directions

- **A — Roster first:** the whole Workflow stays visible; one Agent expands for debugging.
- **B — Attention inbox:** healthy work collapses; persistent space is reserved for decisions and risk, with an on-demand roster showing every Agent's model and effort level.
- **C — Workflow map:** delegation and Request dependencies are primary; attention is derived from the graph.

The prototype uses synthetic state and performs no mutations. Status rendering is explicitly TUI-only: it does not append model context or trigger model turns.

## Validated outcome

The winning production direction is a hybrid built from **B — Attention inbox**:

- An automatic read-only widget appears above Pi's normal editor only while attention items exist. It never takes focus or handles keys.
- A manually opened interactive inbox menu provides arrow-key navigation and quick access to the selected Agent, Request, inspector, or explicit action menu. While open, it temporarily owns TUI focus; `Esc` restores the editor.
- The full flat roster opens from that menu. Every Agent shows its effective model ID and thinking/effort level on a dedicated second line.
- A nested Workflow map is not a default surface because arbitrary delegation depth degrades the layout.
