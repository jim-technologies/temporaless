import { clone, toBinary } from "@bufbuild/protobuf";

import type { ConnectStore } from "./connectstore.js";
import {
  ClaimResourceType,
  TimerKind,
  WorkflowPlanEdgeKind,
  WorkflowPlanNodeKind,
  WorkflowPlanSchema,
  type ActivityRecord,
  type ClaimRecord,
  type EventRecord,
  type TimerRecord,
  type WorkflowKey,
  type WorkflowPlan,
  type WorkflowPlanNode,
  type WorkflowRecord,
} from "./gen/temporaless/v1/temporaless_pb.js";

const PATH_SEGMENT = /^[A-Za-z0-9._:=-]+$/;
const PROTOBUF_FULL_NAME =
  /^[A-Za-z_][A-Za-z_0-9]*(\.[A-Za-z_][A-Za-z_0-9]*)*$/;
const CALLABLE_NODE_KINDS = new Set<WorkflowPlanNodeKind>([
  WorkflowPlanNodeKind.ACTIVITY,
  WorkflowPlanNodeKind.BRANCH,
]);
const NODE_KINDS = new Set<WorkflowPlanNodeKind>([
  WorkflowPlanNodeKind.ACTIVITY,
  WorkflowPlanNodeKind.BRANCH,
  WorkflowPlanNodeKind.FAN_OUT,
  WorkflowPlanNodeKind.LOOP,
  WorkflowPlanNodeKind.SLEEP,
  WorkflowPlanNodeKind.WAIT_EVENT,
  WorkflowPlanNodeKind.WAIT_WORKFLOW,
]);
const EDGE_KINDS = new Set<WorkflowPlanEdgeKind>([
  WorkflowPlanEdgeKind.CONTROL,
  WorkflowPlanEdgeKind.DATA,
  WorkflowPlanEdgeKind.CONDITIONAL,
  WorkflowPlanEdgeKind.LOOP_BACK,
]);

/**
 * The read-only ConnectStore surface required to inspect one workflow run.
 * Keeping this structural makes projections easy to test while every normal
 * ConnectStore remains assignable to it.
 */
export type RunInspectionStore = Pick<
  ConnectStore,
  "getWorkflow" | "listActivities" | "listTimers" | "listEvents" | "listClaims"
>;

/** Raw durable evidence for one workflow run. */
export interface RunSnapshot {
  /** The single run all records in this snapshot were validated against. */
  key: WorkflowKey;
  workflow: WorkflowRecord | undefined;
  activities: ActivityRecord[];
  timers: TimerRecord[];
  events: EventRecord[];
  claims: ClaimRecord[];
}

/**
 * Durable records whose exact boundary/resource identifier equals node_id.
 * There is deliberately no synthesized status: consumers render the statuses
 * carried by these protobuf records.
 */
export interface ProjectedNode {
  node: WorkflowPlanNode;
  activities: ActivityRecord[];
  timers: TimerRecord[];
  events: EventRecord[];
  claims: ClaimRecord[];
}

/** Records observed during execution that have no matching planned node. */
export interface UnplannedEvidence {
  activities: ActivityRecord[];
  timers: TimerRecord[];
  events: EventRecord[];
  claims: ClaimRecord[];
}

export interface RunProjection {
  plan: WorkflowPlan;
  snapshot: RunSnapshot;
  /** Workflow-execution claims describe the run, not an individual node. */
  runClaims: ClaimRecord[];
  nodes: ProjectedNode[];
  unplanned: UnplannedEvidence;
}

/**
 * Validate the cross-node invariants that protobuf field validation cannot
 * express. It also enforces the protobuf-declared scalar constraints because
 * the TypeScript SDK intentionally has no separate Protovalidate runtime.
 */
export function validateWorkflowPlan(plan: WorkflowPlan): void {
  validateIdentifier("plan_id", plan.planId);
  if (plan.revision <= 0n) {
    throw new Error("workflow plan revision must be greater than zero");
  }
  if (plan.nodes.length === 0) {
    throw new Error("workflow plan must contain at least one node");
  }

  const nodes = new Map<string, WorkflowPlanNode>();
  for (const node of plan.nodes) {
    validateIdentifier("node_id", node.nodeId);
    if (nodes.has(node.nodeId)) {
      throw new Error(`workflow plan has duplicate node_id ${quote(node.nodeId)}`);
    }
    if (node.displayName.length === 0) {
      throw new Error(
        `workflow plan node ${quote(node.nodeId)} display_name is required`,
      );
    }
    if (!NODE_KINDS.has(node.kind)) {
      throw new Error(
        `workflow plan node ${quote(node.nodeId)} has invalid kind ${node.kind}`,
      );
    }
    for (const [field, value] of [
      ["request_type", node.requestType],
      ["response_type", node.responseType],
    ] as const) {
      if (value.length > 0 && !PROTOBUF_FULL_NAME.test(value)) {
        throw new Error(
          `workflow plan node ${quote(node.nodeId)} ${field} must be a protobuf full name`,
        );
      }
    }
    if (CALLABLE_NODE_KINDS.has(node.kind)) {
      for (const [field, value] of [
        ["operation", node.operation],
        ["request_type", node.requestType],
        ["response_type", node.responseType],
      ] as const) {
        if (value.length === 0) {
          throw new Error(
            `workflow plan callable node ${quote(node.nodeId)} ${field} is required`,
          );
        }
      }
    }
    nodes.set(node.nodeId, node);
  }

  const edgeIdentities = new Set<string>();
  const conditionalLabels = new Map<string, Set<string>>();
  const dataTargets = new Set<string>();
  const forwardTargets = new Map<string, string[]>(
    plan.nodes.map((node) => [node.nodeId, []]),
  );
  for (const edge of plan.edges) {
    validateIdentifier("source_node_id", edge.sourceNodeId);
    validateIdentifier("target_node_id", edge.targetNodeId);
    if (!EDGE_KINDS.has(edge.kind)) {
      throw new Error(
        `workflow plan edge ${edgeName(edge)} has invalid kind ${edge.kind}`,
      );
    }

    const identity = [
      edge.sourceNodeId,
      edge.targetNodeId,
      String(edge.kind),
      edge.label,
    ].join("\u0000");
    if (edgeIdentities.has(identity)) {
      throw new Error(`workflow plan has duplicate edge ${edgeName(edge)}`);
    }
    edgeIdentities.add(identity);

    const source = nodes.get(edge.sourceNodeId);
    const target = nodes.get(edge.targetNodeId);
    if (source === undefined) {
      throw new Error(
        `workflow plan edge ${edgeName(edge)} references missing source node`,
      );
    }
    if (target === undefined) {
      throw new Error(
        `workflow plan edge ${edgeName(edge)} references missing target node`,
      );
    }

    if (
      edge.kind === WorkflowPlanEdgeKind.CONDITIONAL &&
      edge.label.length === 0
    ) {
      throw new Error(
        `workflow plan conditional edge ${edgeName(edge)} requires a label`,
      );
    }
    if (edge.kind === WorkflowPlanEdgeKind.CONDITIONAL) {
      if (source.kind !== WorkflowPlanNodeKind.BRANCH) {
        throw new Error(
          `workflow plan conditional edge ${edgeName(edge)} must originate from a BRANCH node`,
        );
      }
      const labels = conditionalLabels.get(edge.sourceNodeId) ?? new Set<string>();
      if (labels.has(edge.label)) {
        throw new Error(
          `workflow plan branch ${quote(edge.sourceNodeId)} has duplicate conditional label ${quote(edge.label)}`,
        );
      }
      labels.add(edge.label);
      conditionalLabels.set(edge.sourceNodeId, labels);
    }
    if (edge.kind === WorkflowPlanEdgeKind.DATA) {
      if (
        !CALLABLE_NODE_KINDS.has(source.kind) ||
        !CALLABLE_NODE_KINDS.has(target.kind)
      ) {
        throw new Error(
          `workflow plan DATA edge ${edgeName(edge)} requires callable endpoints`,
        );
      }
      if (
        source.responseType.length === 0 ||
        target.requestType.length === 0 ||
        source.responseType !== target.requestType
      ) {
        throw new Error(
          `workflow plan DATA edge ${edgeName(edge)} requires matching non-empty source response_type and target request_type`,
        );
      }
      if (dataTargets.has(edge.targetNodeId)) {
        throw new Error(
          `workflow plan node ${quote(edge.targetNodeId)} has more than one incoming DATA edge`,
        );
      }
      dataTargets.add(edge.targetNodeId);
    }
    if (
      edge.kind === WorkflowPlanEdgeKind.LOOP_BACK &&
      source.kind !== WorkflowPlanNodeKind.LOOP &&
      target.kind !== WorkflowPlanNodeKind.LOOP
    ) {
      throw new Error(
        `workflow plan LOOP_BACK edge ${edgeName(edge)} must touch a LOOP node`,
      );
    }
    if (edge.kind !== WorkflowPlanEdgeKind.LOOP_BACK) {
      forwardTargets.get(edge.sourceNodeId)?.push(edge.targetNodeId);
    }
  }

  assertAcyclicForwardGraph(forwardTargets);
}

/**
 * Return the lowercase SHA-256 digest of canonical protobuf binary.
 *
 * Repeated node/edge order remains significant. Map entries are sorted before
 * serialization so equal plans hash identically across object insertion
 * orders and SDKs. Unknown fields remain part of the approved protobuf value.
 */
export async function workflowPlanDigest(plan: WorkflowPlan): Promise<string> {
  validateWorkflowPlan(plan);
  const canonical = clone(WorkflowPlanSchema, plan);
  canonical.annotations = orderedAnnotations(plan.annotations);
  for (const [index, source] of plan.nodes.entries()) {
    canonical.nodes[index]!.annotations = orderedAnnotations(source.annotations);
  }
  const binary = toBinary(WorkflowPlanSchema, canonical);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", binary);
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

/** Read every run-scoped record kind concurrently from a ConnectStore. */
export async function inspectRun(
  store: RunInspectionStore,
  key: WorkflowKey,
): Promise<RunSnapshot> {
  validateWorkflowKey(key);
  const [workflow, activities, timers, events, claims] = await Promise.all([
    store.getWorkflow(key),
    store.listActivities(key),
    store.listTimers(key),
    store.listEvents(key),
    store.listClaims(key),
  ]);
  const snapshot: RunSnapshot = {
    key,
    workflow,
    activities: sortedBy(activities, (record) => record.key?.activityId ?? ""),
    timers: sortedBy(timers, (record) => record.key?.timerId ?? ""),
    events: sortedBy(events, (record) => record.key?.eventId ?? ""),
    claims: sortedBy(claims, (record) => record.key?.claimId ?? ""),
  };
  validateSnapshotKeys(snapshot);
  return snapshot;
}

/**
 * Join an intended plan to observed durable records by exact caller-owned ID.
 *
 * Unmatched records are retained rather than hidden, and absence of evidence
 * is left as absence. In particular this function never invents RUNNING or
 * SKIPPED states for code execution that Temporaless did not record.
 */
export function projectWorkflowRun(
  plan: WorkflowPlan,
  snapshot: RunSnapshot,
): RunProjection {
  validateWorkflowPlan(plan);
  validateSnapshotKeys(snapshot);
  const activityNodeIds = new Set(
    plan.nodes
      .filter(
        (node) =>
          node.kind === WorkflowPlanNodeKind.ACTIVITY ||
          node.kind === WorkflowPlanNodeKind.BRANCH,
      )
      .map((node) => node.nodeId),
  );
  const sleepNodeIds = new Set(
    plan.nodes
      .filter((node) => node.kind === WorkflowPlanNodeKind.SLEEP)
      .map((node) => node.nodeId),
  );
  const pollNodeIds = new Set(
    plan.nodes
      .filter(
        (node) =>
          node.kind === WorkflowPlanNodeKind.WAIT_EVENT ||
          node.kind === WorkflowPlanNodeKind.WAIT_WORKFLOW,
      )
      .map((node) => node.nodeId),
  );
  const eventNodeIds = new Set(
    plan.nodes
      .filter((node) => node.kind === WorkflowPlanNodeKind.WAIT_EVENT)
      .map((node) => node.nodeId),
  );

  const nodes = sortedBy(plan.nodes, (node) => node.nodeId).map<ProjectedNode>(
    (node) => {
      const isActivity =
        node.kind === WorkflowPlanNodeKind.ACTIVITY ||
        node.kind === WorkflowPlanNodeKind.BRANCH;
      const hasBoundaryTimer =
        node.kind === WorkflowPlanNodeKind.SLEEP ||
        node.kind === WorkflowPlanNodeKind.WAIT_EVENT ||
        node.kind === WorkflowPlanNodeKind.WAIT_WORKFLOW;
      return {
        node,
        activities: isActivity
          ? sortedBy(
              snapshot.activities.filter(
                (record) => record.key?.activityId === node.nodeId,
              ),
              (record) => record.key?.activityId ?? "",
            )
          : [],
        timers: sortedBy(
          snapshot.timers.filter(
            (record) =>
              (isActivity &&
                record.timerKind === TimerKind.ACTIVITY_RETRY &&
                record.retryActivityId === node.nodeId) ||
              (node.kind === WorkflowPlanNodeKind.SLEEP &&
                record.timerKind === TimerKind.SLEEP &&
                record.key?.timerId === node.nodeId) ||
              ((node.kind === WorkflowPlanNodeKind.WAIT_EVENT ||
                node.kind === WorkflowPlanNodeKind.WAIT_WORKFLOW) &&
                record.timerKind === TimerKind.POLL &&
                record.key?.timerId === node.nodeId),
          ),
          (record) => record.key?.timerId ?? "",
        ),
        events:
          node.kind === WorkflowPlanNodeKind.WAIT_EVENT
            ? sortedBy(
                snapshot.events.filter(
                  (record) => record.key?.eventId === node.nodeId,
                ),
                (record) => record.key?.eventId ?? "",
              )
            : [],
        claims: isActivity || hasBoundaryTimer
          ? sortedBy(
              snapshot.claims.filter(
                (record) =>
                  ((isActivity &&
                    record.resourceType === ClaimResourceType.ACTIVITY) ||
                    (hasBoundaryTimer &&
                      record.resourceType === ClaimResourceType.TIMER)) &&
                  record.resourceId === node.nodeId,
              ),
              (record) => record.key?.claimId ?? "",
            )
          : [],
      };
    },
  );

  return {
    plan,
    snapshot,
    runClaims: sortedBy(
      snapshot.claims.filter(
        (record) => record.resourceType === ClaimResourceType.WORKFLOW,
      ),
      (record) => record.key?.claimId ?? "",
    ),
    nodes,
    unplanned: {
      activities: sortedBy(
        snapshot.activities.filter(
          (record) =>
            record.key === undefined ||
            !activityNodeIds.has(record.key.activityId),
        ),
        (record) => record.key?.activityId ?? "",
      ),
      timers: sortedBy(
        snapshot.timers.filter(
          (record) =>
            !(
              (record.timerKind === TimerKind.SLEEP &&
                record.key !== undefined &&
                sleepNodeIds.has(record.key.timerId)) ||
              (record.timerKind === TimerKind.POLL &&
                record.key !== undefined &&
                pollNodeIds.has(record.key.timerId)) ||
              (record.timerKind === TimerKind.ACTIVITY_RETRY &&
                activityNodeIds.has(record.retryActivityId))
            ),
        ),
        (record) => record.key?.timerId ?? "",
      ),
      events: sortedBy(
        snapshot.events.filter(
          (record) =>
            record.key === undefined || !eventNodeIds.has(record.key.eventId),
        ),
        (record) => record.key?.eventId ?? "",
      ),
      claims: sortedBy(
        snapshot.claims.filter(
          (record) =>
            record.resourceType !== ClaimResourceType.WORKFLOW &&
            (record.resourceType !== ClaimResourceType.ACTIVITY ||
              !activityNodeIds.has(record.resourceId)) &&
            (record.resourceType !== ClaimResourceType.TIMER ||
              (!sleepNodeIds.has(record.resourceId) &&
                !pollNodeIds.has(record.resourceId))),
        ),
        (record) => record.key?.claimId ?? "",
      ),
    },
  };
}

function validateIdentifier(field: string, value: string): void {
  if (
    value.length === 0 ||
    !PATH_SEGMENT.test(value) ||
    value === "." ||
    value === ".."
  ) {
    throw new Error(
      `workflow plan ${field} must be a non-empty storage-safe identifier`,
    );
  }
}

function assertAcyclicForwardGraph(targets: Map<string, string[]>): void {
  const indegree = new Map<string, number>(
    [...targets.keys()].map((nodeId) => [nodeId, 0]),
  );
  for (const outgoing of targets.values()) {
    for (const target of outgoing) {
      indegree.set(target, (indegree.get(target) ?? 0) + 1);
    }
  }
  const ready = [...indegree.entries()]
    .filter(([, degree]) => degree === 0)
    .map(([nodeId]) => nodeId);
  let visited = 0;
  while (ready.length > 0) {
    const nodeId = ready.pop();
    if (nodeId === undefined) {
      break;
    }
    visited += 1;
    for (const target of targets.get(nodeId) ?? []) {
      const degree = (indegree.get(target) ?? 0) - 1;
      indegree.set(target, degree);
      if (degree === 0) {
        ready.push(target);
      }
    }
  }
  if (visited !== targets.size) {
    throw new Error(
      "workflow plan forward edges must be acyclic; use LOOP_BACK for loops",
    );
  }
}

function orderedAnnotations(
  annotations: Readonly<Record<string, string>>,
): Record<string, string> {
  const values = { ...annotations };
  const keys = Object.keys(values).sort(compareUtf8);
  // Protobuf-ES represents maps as objects. A proxy is needed here because
  // ECMAScript otherwise reorders integer-looking property names regardless
  // of insertion order, which would defeat protobuf's lexical map ordering.
  return new Proxy(values, {
    ownKeys: () => keys,
  });
}

function edgeName(edge: {
  sourceNodeId: string;
  targetNodeId: string;
  kind: WorkflowPlanEdgeKind;
  label: string;
}): string {
  const label = edge.label.length === 0 ? "" : ` label=${quote(edge.label)}`;
  return `${quote(edge.sourceNodeId)} -> ${quote(edge.targetNodeId)} kind=${edge.kind}${label}`;
}

function quote(value: string): string {
  return JSON.stringify(value);
}

function sortedBy<T>(values: readonly T[], identity: (value: T) => string): T[] {
  return [...values].sort((left, right) =>
    compareUtf8(identity(left), identity(right)),
  );
}

function compareUtf8(left: string, right: string): number {
  const encoder = new TextEncoder();
  const leftBytes = encoder.encode(left);
  const rightBytes = encoder.encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    const difference = leftBytes[index]! - rightBytes[index]!;
    if (difference !== 0) {
      return difference;
    }
  }
  return leftBytes.length - rightBytes.length;
}

function validateSnapshotKeys(snapshot: RunSnapshot): void {
  validateWorkflowKey(snapshot.key);
  if (snapshot.workflow !== undefined) {
    assertRunKey("workflow", snapshot.workflow.key, snapshot.key);
  }
  for (const record of snapshot.activities) {
    assertRunKey("activity", record.key, snapshot.key);
  }
  for (const record of snapshot.timers) {
    assertRunKey("timer", record.key, snapshot.key);
  }
  for (const record of snapshot.events) {
    assertRunKey("event", record.key, snapshot.key);
  }
  for (const record of snapshot.claims) {
    assertRunKey("claim", record.key, snapshot.key);
  }
}

function validateWorkflowKey(key: WorkflowKey): void {
  for (const [field, value] of [
    ["namespace", key.namespace],
    ["workflow_id", key.workflowId],
    ["run_id", key.runId],
  ] as const) {
    if (
      value.length === 0 ||
      !PATH_SEGMENT.test(value) ||
      value === "." ||
      value === ".."
    ) {
      throw new Error(
        `workflow key ${field} must be a non-empty storage-safe identifier`,
      );
    }
  }
  if (key.namespace.startsWith("_")) {
    throw new Error("workflow key namespace must not start with _");
  }
  if (key.workflowId.startsWith("_")) {
    throw new Error("workflow key workflow_id must not start with _");
  }
}

function assertRunKey(
  recordKind: string,
  actual:
    | {
        namespace: string;
        workflowId: string;
        runId: string;
      }
    | undefined,
  expected: WorkflowKey,
): void {
  if (actual === undefined) {
    throw new Error(`${recordKind} record key is required`);
  }
  if (
    actual.namespace !== expected.namespace ||
    actual.workflowId !== expected.workflowId ||
    actual.runId !== expected.runId
  ) {
    throw new Error(
      `${recordKind} record key does not match snapshot run ` +
        `${quote(expected.namespace)}/${quote(expected.workflowId)}/${quote(expected.runId)}`,
    );
  }
}
