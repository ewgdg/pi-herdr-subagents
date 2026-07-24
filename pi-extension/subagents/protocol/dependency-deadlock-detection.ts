import type { DatabaseSync } from "node:sqlite";

export interface DetectedDependencyDeadlock {
  seedAgentIds: string[];
  requestIds: string[];
  witnessKey: string;
}

interface CurrentActivationRow {
  activation_id: string;
  agent_id: string;
  phase: "open" | "ended";
  open_state: "active" | "waiting" | "interrupted" | null;
  revision: number;
}

interface DependencyRow {
  activation_id: string;
}

interface RequestRow {
  request_id: string;
  requester_agent_id: string;
  responder_agent_id: string;
  requester_activation_id: string | null;
  status: "open" | "answered" | "orphaned";
}

/** Confirm closed Request components from one transaction-consistent snapshot. */
export function detectDependencyDeadlocks(database: DatabaseSync): DetectedDependencyDeadlock[] {
  const activations = database.prepare(`
    SELECT activation_id, agent_id, phase, open_state, revision
    FROM agent_activations current
    WHERE NOT EXISTS (
      SELECT 1 FROM agent_activations newer
      WHERE newer.agent_id = current.agent_id
        AND newer.activation_sequence > current.activation_sequence
    )
  `).all() as unknown as CurrentActivationRow[];
  const currentByAgent = new Map(activations.map((row) => [row.agent_id, row]));
  const dependencies = database.prepare(`
    SELECT activation_id FROM activation_dependencies
    ORDER BY activation_id, dependency_kind, dependency_id
  `).all() as unknown as DependencyRow[];
  const dependenciesByActivation = groupBy(dependencies, (row) => row.activation_id);
  const requests = database.prepare(`
    SELECT request_id, requester_agent_id, responder_agent_id,
           requester_activation_id, status
    FROM workflow_requests
    WHERE status IN ('open', 'answered')
      OR (status = 'orphaned' AND orphan_notice_delivery_status = 'accepted')
    ORDER BY request_id
  `).all() as unknown as RequestRow[];
  const requestsByActivation = groupBy(
    requests.filter((request) => request.requester_activation_id !== null),
    (request) => request.requester_activation_id!,
  );
  const pendingRecipients = new Set((database.prepare(`
    SELECT DISTINCT recipient_agent_id FROM pending_message_pointers
  `).all() as Array<{ recipient_agent_id: string }>).map((row) => row.recipient_agent_id));
  const humanWaitingAgents = new Set((database.prepare(`
    SELECT DISTINCT agent_id FROM human_interrupts
    WHERE status IN ('pending', 'response-bound', 'result-pending')
  `).all() as Array<{ agent_id: string }>).map((row) => row.agent_id));

  const candidateEdges = new Map<string, Array<{ agentId: string; requestId: string }>>();
  for (const activation of activations) {
    if (activation.phase !== "open" || activation.open_state !== "waiting") continue;
    if (pendingRecipients.has(activation.agent_id) || humanWaitingAgents.has(activation.agent_id)) continue;
    // Request dependencies live in workflow_requests. Any separately declared
    // lifecycle dependency means the Agent is not waiting solely on Requests.
    if ((dependenciesByActivation.get(activation.activation_id) ?? []).length > 0) continue;
    const activationRequests = requestsByActivation.get(activation.activation_id) ?? [];
    // An accepted Answer or orphan notice is itself a progress source. Only
    // still-open Request obligations can witness a closed deadlock component.
    if (activationRequests.length === 0
      || activationRequests.some((request) => request.status !== "open")) continue;
    candidateEdges.set(activation.agent_id, activationRequests.map((request) => ({
      agentId: request.responder_agent_id,
      requestId: request.request_id,
    })));
  }

  const components = stronglyConnectedComponents(
    [...candidateEdges.keys()],
    (agentId) => (candidateEdges.get(agentId) ?? []).map((edge) => edge.agentId)
      .filter((target) => candidateEdges.has(target)),
  );
  const deadlocks: DetectedDependencyDeadlock[] = [];
  for (const component of components) {
    const members = new Set(component);
    const hasCycle = component.length > 1
      || (candidateEdges.get(component[0]) ?? []).some((edge) => edge.agentId === component[0]);
    if (!hasCycle) continue;
    const closed = component.every((agentId) =>
      (candidateEdges.get(agentId) ?? []).every((edge) => members.has(edge.agentId))
      && currentByAgent.get(agentId)?.open_state === "waiting");
    if (!closed) continue;
    const seedAgentIds = [...component].sort();
    const requestIds = component.flatMap((agentId) => candidateEdges.get(agentId) ?? [])
      .map((edge) => edge.requestId)
      .sort();
    deadlocks.push({
      seedAgentIds,
      requestIds,
      witnessKey: JSON.stringify({
        activations: seedAgentIds.map((agentId) => {
          const activation = currentByAgent.get(agentId)!;
          return [agentId, activation.activation_id, Number(activation.revision)];
        }),
        requestIds,
      }),
    });
  }
  return deadlocks.sort((left, right) =>
    left.seedAgentIds.join("\0").localeCompare(right.seedAgentIds.join("\0")));
}

function groupBy<T, K>(items: T[], key: (item: T) => K): Map<K, T[]> {
  const groups = new Map<K, T[]>();
  for (const item of items) {
    const itemKey = key(item);
    const group = groups.get(itemKey);
    if (group) group.push(item);
    else groups.set(itemKey, [item]);
  }
  return groups;
}

function stronglyConnectedComponents(
  nodes: string[],
  neighbors: (node: string) => string[],
): string[][] {
  let nextIndex = 0;
  const indices = new Map<string, number>();
  const lowLinks = new Map<string, number>();
  const stack: string[] = [];
  const onStack = new Set<string>();
  const components: string[][] = [];

  const visit = (node: string): void => {
    indices.set(node, nextIndex);
    lowLinks.set(node, nextIndex);
    nextIndex += 1;
    stack.push(node);
    onStack.add(node);

    for (const neighbor of neighbors(node)) {
      if (!indices.has(neighbor)) {
        visit(neighbor);
        lowLinks.set(node, Math.min(lowLinks.get(node)!, lowLinks.get(neighbor)!));
      } else if (onStack.has(neighbor)) {
        lowLinks.set(node, Math.min(lowLinks.get(node)!, indices.get(neighbor)!));
      }
    }

    if (lowLinks.get(node) !== indices.get(node)) return;
    const component: string[] = [];
    while (stack.length > 0) {
      const member = stack.pop()!;
      onStack.delete(member);
      component.push(member);
      if (member === node) break;
    }
    components.push(component);
  };

  for (const node of nodes) if (!indices.has(node)) visit(node);
  return components;
}
