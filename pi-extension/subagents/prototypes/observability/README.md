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
