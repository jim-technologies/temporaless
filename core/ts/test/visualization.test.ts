import {
  create,
  createFileRegistry,
  createRegistry,
} from "@bufbuild/protobuf";
import {
  DescriptorProtoSchema,
  FileDescriptorProtoSchema,
  FileDescriptorSetSchema,
  MethodDescriptorProtoSchema,
  ServiceDescriptorProtoSchema,
} from "@bufbuild/protobuf/wkt";
import { WireType } from "@bufbuild/protobuf/wire";
import { describe, expect, it, vi } from "vitest";

import {
  ActivityKeySchema,
  ActivityRecordSchema,
  ActivityStatus,
  ClaimKeySchema,
  ClaimResourceType,
  ClaimRecordSchema,
  EventKeySchema,
  EventRecordSchema,
  TimerKeySchema,
  TimerKind,
  TimerRecordSchema,
  TimerStatus,
  WorkflowKeySchema,
  WorkflowPlanEdgeKind,
  WorkflowPlanEdgeSchema,
  WorkflowPlanNodeKind,
  WorkflowPlanNodeSchema,
  WorkflowPlanSchema,
  WorkflowRecordSchema,
  WorkflowStatus,
  decodeWorkflowPlan,
  file_temporaless_v1_temporaless,
  inspectRun,
  projectWorkflowRun,
  validateWorkflowPlan,
  validateWorkflowPlanWithDescriptors,
  verifyApprovedWorkflowPlan,
  workflowPlanDigest,
  type DescriptorValidationOptions,
  type RunInspectionStore,
  type RunSnapshot,
  type WorkflowPlan,
} from "../src/index.js";

function node(
  nodeId: string,
  kind: WorkflowPlanNodeKind,
  overrides: Partial<{
    displayName: string;
    operation: string;
    requestType: string;
    responseType: string;
  }> = {},
) {
  const callable =
    kind === WorkflowPlanNodeKind.ACTIVITY ||
    kind === WorkflowPlanNodeKind.BRANCH;
  return create(WorkflowPlanNodeSchema, {
    nodeId,
    displayName: overrides.displayName ?? nodeId,
    kind,
    operation:
      overrides.operation ?? (callable ? `example.v1.Service.${nodeId}` : ""),
    requestType:
      overrides.requestType ?? (callable ? "example.v1.Request" : ""),
    responseType:
      overrides.responseType ?? (callable ? "example.v1.Response" : ""),
  });
}

function edge(
  sourceNodeId: string,
  targetNodeId: string,
  kind = WorkflowPlanEdgeKind.CONTROL,
  label = "",
) {
  return create(WorkflowPlanEdgeSchema, {
    sourceNodeId,
    targetNodeId,
    kind,
    label,
  });
}

function plan(
  nodes = [
    node("validate", WorkflowPlanNodeKind.ACTIVITY),
    node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
  ],
  edges = [edge("validate", "approve")],
): WorkflowPlan {
  return create(WorkflowPlanSchema, {
    planId: "approval:export",
    revision: 1n,
    nodes,
    edges,
  });
}

const GET_WORKFLOW_OPERATION =
  "temporaless.v1.RecordStoreService.GetWorkflow";

function descriptorPlan(
  overrides: Partial<{
    kind: WorkflowPlanNodeKind;
    operation: string;
    requestType: string;
    responseType: string;
  }> = {},
): WorkflowPlan {
  return plan(
    [
      node(
        "get-workflow",
        overrides.kind ?? WorkflowPlanNodeKind.ACTIVITY,
        {
          operation: overrides.operation ?? GET_WORKFLOW_OPERATION,
          requestType:
            overrides.requestType ?? "temporaless.v1.GetWorkflowRequest",
          responseType:
            overrides.responseType ?? "temporaless.v1.GetWorkflowResponse",
        },
      ),
    ],
    [],
  );
}

function descriptorOptions(
  ...allowedOperations: string[]
): DescriptorValidationOptions {
  return {
    registry: createRegistry(file_temporaless_v1_temporaless),
    allowedOperations: new Set(allowedOperations),
  };
}

const STREAMING_OPERATION = "streaming.v1.StreamService.Watch";

function streamingDescriptorOptions(): DescriptorValidationOptions {
  const descriptorSet = create(FileDescriptorSetSchema, {
    file: [
      create(FileDescriptorProtoSchema, {
        name: "streaming/v1/streaming.proto",
        package: "streaming.v1",
        syntax: "proto3",
        messageType: [
          create(DescriptorProtoSchema, { name: "WatchRequest" }),
          create(DescriptorProtoSchema, { name: "WatchResponse" }),
        ],
        service: [
          create(ServiceDescriptorProtoSchema, {
            name: "StreamService",
            method: [
              create(MethodDescriptorProtoSchema, {
                name: "Watch",
                inputType: ".streaming.v1.WatchRequest",
                outputType: ".streaming.v1.WatchResponse",
                serverStreaming: true,
              }),
            ],
          }),
        ],
      }),
    ],
  });
  return {
    registry: createFileRegistry(descriptorSet),
    allowedOperations: new Set([STREAMING_OPERATION]),
  };
}

describe("validateWorkflowPlan", () => {
  it.each([
    {
      location: "plan annotations",
      pythonFixture:
        "Cgp1bnNhZmUtbWFwEAEaDgoEd2FpdBIEV2FpdCAGKhMKCV9fcHJvdG9fXxIGaGlkZGVuKhAKC2NvbnN0cnVjdG9yEgFjKgsKBm5vcm1hbBIBbg==",
    },
    {
      location: "node annotations",
      pythonFixture:
        "Cg91bnNhZmUtbm9kZS1tYXAQARowCgR3YWl0EgRXYWl0IAZCEwoJX19wcm90b19fEgZoaWRkZW5CCwoGbm9ybWFsEgFu",
    },
  ])(
    "rejects a non-portable key in Python-produced $location binary before decoding",
    ({ pythonFixture }) => {
      const binary = Uint8Array.from(
        Buffer.from(pythonFixture, "base64"),
      );

      expect(() => decodeWorkflowPlan(binary)).toThrow(
        "not portable across protobuf SDKs",
      );
    },
  );

  it("accepts an explicit loop while keeping forward edges acyclic", () => {
    const workflowPlan = plan(
      [
        node("start", WorkflowPlanNodeKind.ACTIVITY),
        node("repeat", WorkflowPlanNodeKind.LOOP),
        node("finish", WorkflowPlanNodeKind.ACTIVITY),
      ],
      [
        edge("start", "repeat"),
        edge("repeat", "finish"),
        edge("finish", "repeat", WorkflowPlanEdgeKind.LOOP_BACK),
      ],
    );

    expect(() => validateWorkflowPlan(workflowPlan)).not.toThrow();
  });

  it("accepts a typed DATA edge between compatible callable nodes", () => {
    const workflowPlan = plan(
      [
        node("fetch", WorkflowPlanNodeKind.ACTIVITY, {
          responseType: "example.v1.Price",
        }),
        node("persist", WorkflowPlanNodeKind.ACTIVITY, {
          requestType: "example.v1.Price",
        }),
      ],
      [edge("fetch", "persist", WorkflowPlanEdgeKind.DATA)],
    );

    expect(() => validateWorkflowPlan(workflowPlan)).not.toThrow();
  });

  it.each([
    {
      name: "revision exceeds uint64",
      value: (() => {
        const value = plan();
        value.revision = 1n << 64n;
        return value;
      })(),
      message: "must be a valid uint64",
    },
    {
      name: "duplicate node IDs",
      value: plan(
        [
          node("same", WorkflowPlanNodeKind.ACTIVITY),
          node("same", WorkflowPlanNodeKind.SLEEP),
        ],
        [],
      ),
      message: "duplicate node_id",
    },
    {
      name: "duplicate edges",
      value: plan(undefined, [
        edge("validate", "approve"),
        edge("validate", "approve"),
      ]),
      message: "duplicate edge",
    },
    {
      name: "missing callable operation",
      value: plan(
        [
          node("validate", WorkflowPlanNodeKind.ACTIVITY, { operation: "" }),
          node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
        ],
        [edge("validate", "approve")],
      ),
      message: "operation is required",
    },
    {
      name: "missing callable request type",
      value: plan(
        [
          node("validate", WorkflowPlanNodeKind.BRANCH, { requestType: "" }),
          node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
        ],
        [edge("validate", "approve", WorkflowPlanEdgeKind.CONDITIONAL, "yes")],
      ),
      message: "request_type is required",
    },
    {
      name: "missing callable response type",
      value: plan(
        [
          node("validate", WorkflowPlanNodeKind.ACTIVITY, { responseType: "" }),
          node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
        ],
        [edge("validate", "approve")],
      ),
      message: "response_type is required",
    },
    {
      name: "malformed protobuf request type",
      value: plan(
        [
          node("validate", WorkflowPlanNodeKind.ACTIVITY, {
            requestType: ".example.v1.Request",
          }),
          node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
        ],
        [edge("validate", "approve")],
      ),
      message: "request_type must be a protobuf full name",
    },
    {
      name: "unlabelled conditional edge",
      value: plan(
        [
          node("validate", WorkflowPlanNodeKind.BRANCH),
          node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
        ],
        [edge("validate", "approve", WorkflowPlanEdgeKind.CONDITIONAL)],
      ),
      message: "requires a label",
    },
    {
      name: "conditional edge from a non-branch node",
      value: plan(undefined, [
        edge(
          "validate",
          "approve",
          WorkflowPlanEdgeKind.CONDITIONAL,
          "yes",
        ),
      ]),
      message: "must originate from a BRANCH node",
    },
    {
      name: "duplicate labels from one branch",
      value: plan(
        [
          node("decide", WorkflowPlanNodeKind.BRANCH),
          node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
          node("reject", WorkflowPlanNodeKind.ACTIVITY),
        ],
        [
          edge("decide", "approve", WorkflowPlanEdgeKind.CONDITIONAL, "done"),
          edge("decide", "reject", WorkflowPlanEdgeKind.CONDITIONAL, "done"),
        ],
      ),
      message: "duplicate conditional label",
    },
    {
      name: "incompatible DATA edge types",
      value: plan(
        [
          node("fetch", WorkflowPlanNodeKind.ACTIVITY, {
            responseType: "example.v1.Price",
          }),
          node("persist", WorkflowPlanNodeKind.ACTIVITY, {
            requestType: "example.v1.Order",
          }),
        ],
        [edge("fetch", "persist", WorkflowPlanEdgeKind.DATA)],
      ),
      message: "requires matching non-empty",
    },
    {
      name: "structural DATA edge endpoint",
      value: plan(
        [
          node("group", WorkflowPlanNodeKind.FAN_OUT),
          node("persist", WorkflowPlanNodeKind.ACTIVITY),
        ],
        [edge("group", "persist", WorkflowPlanEdgeKind.DATA)],
      ),
      message: "requires callable endpoints",
    },
    {
      name: "multiple DATA inputs to one node",
      value: plan(
        [
          node("left", WorkflowPlanNodeKind.ACTIVITY, {
            responseType: "example.v1.Value",
          }),
          node("right", WorkflowPlanNodeKind.ACTIVITY, {
            responseType: "example.v1.Value",
          }),
          node("join", WorkflowPlanNodeKind.ACTIVITY, {
            requestType: "example.v1.Value",
          }),
        ],
        [
          edge("left", "join", WorkflowPlanEdgeKind.DATA),
          edge("right", "join", WorkflowPlanEdgeKind.DATA),
        ],
      ),
      message: "more than one incoming DATA edge",
    },
    {
      name: "missing source endpoint",
      value: plan(undefined, [edge("missing", "approve")]),
      message: "missing source node",
    },
    {
      name: "missing target endpoint",
      value: plan(undefined, [edge("validate", "missing")]),
      message: "missing target node",
    },
    {
      name: "loop-back edge without a loop node",
      value: plan(undefined, [
        edge(
          "approve",
          "validate",
          WorkflowPlanEdgeKind.LOOP_BACK,
        ),
      ]),
      message: "must touch a LOOP node",
    },
    {
      name: "cycle in ordinary edges",
      value: plan(undefined, [
        edge("validate", "approve"),
        edge("approve", "validate"),
      ]),
      message: "forward edges must be acyclic",
    },
  ])("rejects $name", ({ value, message }) => {
    expect(() => validateWorkflowPlan(value)).toThrow(message);
  });

  it.each([
    {
      name: "node count",
      value: plan(
        Array.from({ length: 65 }, (_, index) =>
          node(`wait:${index}`, WorkflowPlanNodeKind.WAIT_EVENT),
        ),
        [],
      ),
      message: "at most 64 nodes",
    },
    {
      name: "edge count",
      value: plan(
        undefined,
        Array.from({ length: 129 }, () => edge("validate", "approve")),
      ),
      message: "at most 128 edges",
    },
    {
      name: "plan annotation count",
      value: (() => {
        const value = plan();
        for (let index = 0; index < 65; index += 1) {
          value.annotations[`key:${index}`] = "value";
        }
        return value;
      })(),
      message: "at most 64 entries",
    },
    {
      name: "node annotation count",
      value: (() => {
        const value = plan();
        for (let index = 0; index < 33; index += 1) {
          value.nodes[0]!.annotations[`key:${index}`] = "value";
        }
        return value;
      })(),
      message: "at most 32 entries",
    },
    {
      name: "plan identifier length",
      value: (() => {
        const value = plan();
        value.planId = "a".repeat(257);
        return value;
      })(),
      message: "storage-safe identifier",
    },
    {
      name: "node identifier length",
      value: (() => {
        const value = plan();
        value.nodes[0]!.nodeId = "a".repeat(257);
        return value;
      })(),
      message: "storage-safe identifier",
    },
    {
      name: "UTF-8 display name bytes",
      value: (() => {
        const value = plan();
        value.nodes[0]!.displayName = "😀".repeat(65);
        return value;
      })(),
      message: "display_name exceeds 256 bytes",
    },
    {
      name: "non-portable annotation key",
      value: (() => {
        const value = plan();
        Object.defineProperty(value.annotations, "__proto__", {
          configurable: true,
          enumerable: true,
          value: "hidden",
          writable: true,
        });
        return value;
      })(),
      message: "not portable across protobuf SDKs",
    },
    {
      name: "annotation key bytes",
      value: (() => {
        const value = plan();
        value.annotations["a".repeat(129)] = "value";
        return value;
      })(),
      message: "annotation key exceeds 128 bytes",
    },
    {
      name: "annotation value bytes",
      value: (() => {
        const value = plan();
        value.annotations.key = "a".repeat(1025);
        return value;
      })(),
      message: "annotation value exceeds 1024 bytes",
    },
    {
      name: "non-portable node annotation key",
      value: (() => {
        const value = plan();
        Object.defineProperty(value.nodes[0]!.annotations, "__proto__", {
          configurable: true,
          enumerable: true,
          value: "hidden",
          writable: true,
        });
        return value;
      })(),
      message: "not portable across protobuf SDKs",
    },
    {
      name: "operation bytes",
      value: (() => {
        const value = plan();
        value.nodes[0]!.operation = "a".repeat(513);
        return value;
      })(),
      message: "operation exceeds 512 bytes",
    },
    {
      name: "request type bytes",
      value: (() => {
        const value = plan();
        value.nodes[0]!.requestType = "a".repeat(513);
        return value;
      })(),
      message: "request_type exceeds 512 bytes",
    },
    {
      name: "response type bytes",
      value: (() => {
        const value = plan();
        value.nodes[0]!.responseType = "a".repeat(513);
        return value;
      })(),
      message: "response_type exceeds 512 bytes",
    },
    {
      name: "UTF-8 description bytes",
      value: (() => {
        const value = plan();
        value.nodes[0]!.description = "😀".repeat(1025);
        return value;
      })(),
      message: "description exceeds 4096 bytes",
    },
    {
      name: "edge label bytes",
      value: (() => {
        const value = plan();
        value.edges[0]!.label = "a".repeat(257);
        return value;
      })(),
      message: "label exceeds 256 bytes",
    },
  ])("rejects resource overflow: $name", ({ value, message }) => {
    expect(() => validateWorkflowPlan(value)).toThrow(message);
  });

  it("accepts the exact collection limits", () => {
    const nodes = [
      node("loop", WorkflowPlanNodeKind.LOOP),
      ...Array.from({ length: 63 }, (_, index) =>
        node(`wait:${index + 1}`, WorkflowPlanNodeKind.WAIT_EVENT),
      ),
    ];
    const edges = nodes.slice(1).flatMap((value) => [
      edge("loop", value.nodeId, WorkflowPlanEdgeKind.LOOP_BACK),
      edge(value.nodeId, "loop", WorkflowPlanEdgeKind.LOOP_BACK),
    ]);
    edges.push(
      edge("loop", "loop", WorkflowPlanEdgeKind.LOOP_BACK, "first"),
      edge("loop", "loop", WorkflowPlanEdgeKind.LOOP_BACK, "second"),
    );
    const value = plan(nodes, edges);
    for (let index = 0; index < 64; index += 1) {
      value.annotations[`plan:${index}`] = "value";
    }
    for (let index = 0; index < 32; index += 1) {
      value.nodes[0]!.annotations[`node:${index}`] = "value";
    }

    expect(() => validateWorkflowPlan(value)).not.toThrow();
  });

  it("accepts the maximum uint64 revision", () => {
    const value = plan();
    value.revision = 0xffff_ffff_ffff_ffffn;
    expect(() => validateWorkflowPlan(value)).not.toThrow();
  });
});

describe("validateWorkflowPlanWithDescriptors", () => {
  it("accepts an exactly allowlisted unary RPC with matching message types", () => {
    expect(() =>
      validateWorkflowPlanWithDescriptors(
        descriptorPlan(),
        descriptorOptions(GET_WORKFLOW_OPERATION),
      ),
    ).not.toThrow();
  });

  it("accepts a structural-only plan with an explicit empty allowlist", () => {
    expect(() =>
      validateWorkflowPlanWithDescriptors(
        plan(
          [node("approval", WorkflowPlanNodeKind.WAIT_EVENT)],
          [],
        ),
        descriptorOptions(),
      ),
    ).not.toThrow();
  });

  it.each([
    {
      name: "non-RPC operation alias",
      value: descriptorPlan({ operation: "get-workflow" }),
      options: descriptorOptions("get-workflow"),
      message: "exact package.Service.Method",
    },
    {
      name: "RPC operation without a protobuf package",
      value: descriptorPlan({
        operation: "RecordStoreService.GetWorkflow",
      }),
      options: descriptorOptions("RecordStoreService.GetWorkflow"),
      message: "exact package.Service.Method",
    },
    {
      name: "operation absent from the exact allowlist",
      value: descriptorPlan(),
      options: descriptorOptions(),
      message: "is not allowed",
    },
    {
      name: "service absent from the descriptor",
      value: descriptorPlan({
        operation: "temporaless.v1.MissingService.GetWorkflow",
      }),
      options: descriptorOptions(
        "temporaless.v1.MissingService.GetWorkflow",
      ),
      message: "missing protobuf service",
    },
    {
      name: "method absent from the descriptor",
      value: descriptorPlan({
        operation: "temporaless.v1.RecordStoreService.Missing",
      }),
      options: descriptorOptions(
        "temporaless.v1.RecordStoreService.Missing",
      ),
      message: "missing protobuf RPC",
    },
    {
      name: "local JavaScript method name instead of the declared RPC name",
      value: descriptorPlan({
        operation: "temporaless.v1.RecordStoreService.getWorkflow",
      }),
      options: descriptorOptions(
        "temporaless.v1.RecordStoreService.getWorkflow",
      ),
      message: "missing protobuf RPC",
    },
    {
      name: "request type different from the RPC input",
      value: descriptorPlan({
        requestType: "temporaless.v1.PutWorkflowRequest",
      }),
      options: descriptorOptions(GET_WORKFLOW_OPERATION),
      message: "does not match RPC input",
    },
    {
      name: "response type different from the RPC output",
      value: descriptorPlan({
        responseType: "temporaless.v1.PutWorkflowResponse",
      }),
      options: descriptorOptions(GET_WORKFLOW_OPERATION),
      message: "does not match RPC output",
    },
    {
      name: "RPC metadata on a non-callable node",
      value: descriptorPlan({ kind: WorkflowPlanNodeKind.SLEEP }),
      options: descriptorOptions(GET_WORKFLOW_OPERATION),
      message: "non-callable node",
    },
    {
      name: "server-streaming RPC descriptor",
      value: descriptorPlan({
        operation: STREAMING_OPERATION,
        requestType: "streaming.v1.WatchRequest",
        responseType: "streaming.v1.WatchResponse",
      }),
      options: streamingDescriptorOptions(),
      message: "must be unary",
    },
  ])("rejects $name", ({ value, options, message }) => {
    expect(() =>
      validateWorkflowPlanWithDescriptors(value, options),
    ).toThrow(message);
  });

  it.each([
    {
      name: "options",
      options: undefined as unknown as DescriptorValidationOptions,
      message: "descriptor registry is required",
    },
    {
      name: "registry",
      options: {
        allowedOperations: new Set([GET_WORKFLOW_OPERATION]),
      } as unknown as DescriptorValidationOptions,
      message: "descriptor registry is required",
    },
    {
      name: "allowlist",
      options: {
        registry: createRegistry(file_temporaless_v1_temporaless),
      } as unknown as DescriptorValidationOptions,
      message: "operation allowlist is required",
    },
  ])("rejects missing $name after type erasure", ({ options, message }) => {
    expect(() =>
      validateWorkflowPlanWithDescriptors(descriptorPlan(), options),
    ).toThrow(message);
  });

  it.each(["plan", "node", "edge"] as const)(
    "rejects unknown protobuf fields on the $location",
    (location) => {
      const value = descriptorPlan();
      if (location === "edge") {
        value.nodes.push(
          node("approval", WorkflowPlanNodeKind.WAIT_EVENT),
        );
        value.edges.push(edge("get-workflow", "approval"));
      }
      const target =
        location === "plan"
          ? value
          : location === "node"
            ? value.nodes[0]!
            : value.edges[0]!;
      target.$unknown = [
        {
          no: 100,
          wireType: WireType.Varint,
          data: new Uint8Array([1]),
        },
      ];
      expect(() =>
        validateWorkflowPlanWithDescriptors(
          value,
          descriptorOptions(GET_WORKFLOW_OPERATION),
        ),
      ).toThrow("unknown protobuf fields");
    },
  );
});

describe("workflowPlanDigest", () => {
  it("matches the cross-SDK approval-plan fixture", async () => {
    const workflowPlan = create(WorkflowPlanSchema, {
      planId: "approval:export",
      revision: 1n,
      nodes: [
        node("validate", WorkflowPlanNodeKind.ACTIVITY, {
          displayName: "Validate",
          operation: "exports.v1.ExportService.Validate",
          requestType: "exports.v1.ValidateRequest",
          responseType: "exports.v1.ValidateResponse",
        }),
        node("approve", WorkflowPlanNodeKind.WAIT_EVENT, {
          displayName: "Approve",
        }),
      ],
      edges: [edge("validate", "approve")],
    });

    await expect(workflowPlanDigest(workflowPlan)).resolves.toBe(
      "f3ed8cdf8a4aa2fe3d323661dfff0a50c7097aeac1d307784ed2a726810797f0",
    );
  });

  it("matches the Unicode and numeric-map-key cross-SDK fixture", async () => {
    const annotations = {
      "2": "two",
      "10": "ten",
      é: "café",
      "😀": "rocket",
    };
    const workflowPlan = create(WorkflowPlanSchema, {
      planId: "approval:unicode",
      revision: 2n,
      annotations,
      nodes: [
        node("validate", WorkflowPlanNodeKind.ACTIVITY, {
          displayName: "Vérifier 😀",
          operation: "exports.v1.ExportService.Validate",
          requestType: "exports.v1.ValidateRequest",
          responseType: "exports.v1.ValidateResponse",
        }),
      ],
    });
    workflowPlan.nodes[0]!.annotations = { ...annotations };

    await expect(workflowPlanDigest(workflowPlan)).resolves.toBe(
      "c6f9214a9f270eabf45fc518eeb48faa3ca08628fc105264acdd825ca56f9662",
    );
  });

  it("sorts annotation maps before hashing", async () => {
    const left = plan();
    left.annotations.z = "last";
    left.annotations.a = "first";
    left.annotations["2"] = "two";
    left.annotations["10"] = "ten";
    left.nodes[0]!.annotations.z = "last";
    left.nodes[0]!.annotations.a = "first";
    left.nodes[0]!.annotations["2"] = "two";
    left.nodes[0]!.annotations["10"] = "ten";

    const right = plan();
    right.annotations["10"] = "ten";
    right.annotations["2"] = "two";
    right.annotations.a = "first";
    right.annotations.z = "last";
    right.nodes[0]!.annotations["10"] = "ten";
    right.nodes[0]!.annotations["2"] = "two";
    right.nodes[0]!.annotations.a = "first";
    right.nodes[0]!.annotations.z = "last";

    await expect(workflowPlanDigest(left)).resolves.toBe(
      await workflowPlanDigest(right),
    );
  });
});

describe("verifyApprovedWorkflowPlan", () => {
  it("returns the strictly matching digest for an authorized plan", async () => {
    const workflowPlan = descriptorPlan();
    const approved = await workflowPlanDigest(workflowPlan);

    await expect(
      verifyApprovedWorkflowPlan(
        workflowPlan,
        approved,
        descriptorOptions(GET_WORKFLOW_OPERATION),
      ),
    ).resolves.toBe(approved);
  });

  it.each([
    {
      name: "uppercase digest",
      approved: "A".repeat(64),
      message: "64 lowercase hexadecimal",
    },
    {
      name: "short digest",
      approved: "a".repeat(63),
      message: "64 lowercase hexadecimal",
    },
    {
      name: "different lowercase digest",
      approved: "0".repeat(64),
      message: "does not match",
    },
  ])("rejects a $name", async ({ approved, message }) => {
    await expect(
      verifyApprovedWorkflowPlan(
        descriptorPlan(),
        approved,
        descriptorOptions(GET_WORKFLOW_OPERATION),
      ),
    ).rejects.toThrow(message);
  });
});

describe("inspectRun", () => {
  it("reads every run-scoped record kind concurrently", async () => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "approval:export",
      runId: "run-1",
    });
    const workflow = create(WorkflowRecordSchema, {
      key,
      status: WorkflowStatus.IN_PROGRESS,
    });
    const calls: string[] = [];
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const method = <T>(name: string, result: T) =>
      vi.fn(async () => {
        calls.push(name);
        await gate;
        return result;
      });
    const store: RunInspectionStore = {
      getWorkflow: method("workflow", workflow),
      listActivities: method("activities", []),
      listTimers: method("timers", []),
      listEvents: method("events", []),
      listClaims: method("claims", []),
    };

    const inspected = inspectRun(store, key);
    await vi.waitFor(() => expect(calls).toHaveLength(5));
    release?.();

    await expect(inspected).resolves.toEqual({
      key,
      workflow,
      activities: [],
      timers: [],
      events: [],
      claims: [],
    });
    for (const call of Object.values(store)) {
      expect(call).toHaveBeenCalledWith(key);
    }
  });

  it.each([
    {
      name: "empty namespace",
      key: { namespace: "", workflowId: "approval:export", runId: "run-1" },
      message: "namespace must be a non-empty storage-safe identifier",
    },
    {
      name: "reserved namespace",
      key: {
        namespace: "_system",
        workflowId: "approval:export",
        runId: "run-1",
      },
      message: "namespace must not start with _",
    },
    {
      name: "empty workflow ID",
      key: { namespace: "default", workflowId: "", runId: "run-1" },
      message: "workflow_id must be a non-empty storage-safe identifier",
    },
    {
      name: "reserved workflow ID",
      key: {
        namespace: "default",
        workflowId: "_system",
        runId: "run-1",
      },
      message: "workflow_id must not start with _",
    },
    {
      name: "empty run ID",
      key: {
        namespace: "default",
        workflowId: "approval:export",
        runId: "",
      },
      message: "run_id must be a non-empty storage-safe identifier",
    },
    {
      name: "dot run ID",
      key: {
        namespace: "default",
        workflowId: "approval:export",
        runId: "..",
      },
      message: "run_id must be a non-empty storage-safe identifier",
    },
    {
      name: "path-like workflow ID",
      key: {
        namespace: "default",
        workflowId: "approval/export",
        runId: "run-1",
      },
      message: "workflow_id must be a non-empty storage-safe identifier",
    },
  ])("rejects an invalid key with $name before reading", async ({
    key: keyFields,
    message,
  }) => {
    const key = create(WorkflowKeySchema, keyFields);
    const store: RunInspectionStore = {
      getWorkflow: vi.fn(async () => undefined),
      listActivities: vi.fn(async () => []),
      listTimers: vi.fn(async () => []),
      listEvents: vi.fn(async () => []),
      listClaims: vi.fn(async () => []),
    };

    await expect(inspectRun(store, key)).rejects.toThrow(message);
    for (const call of Object.values(store)) {
      expect(call).not.toHaveBeenCalled();
    }
  });

  it.each([
    "workflow",
    "activity",
    "timer",
    "event",
    "claim",
  ] as const)("rejects a mismatched %s record key", async (recordKind) => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "approval:export",
      runId: "run-1",
    });
    const other = {
      namespace: key.namespace,
      workflowId: key.workflowId,
      runId: "other-run",
    };
    const workflow =
      recordKind === "workflow"
        ? create(WorkflowRecordSchema, {
            key: create(WorkflowKeySchema, other),
          })
        : undefined;
    const activities =
      recordKind === "activity"
        ? [
            create(ActivityRecordSchema, {
              key: create(ActivityKeySchema, {
                ...other,
                activityId: "validate",
              }),
            }),
          ]
        : [];
    const timers =
      recordKind === "timer"
        ? [
            create(TimerRecordSchema, {
              key: create(TimerKeySchema, {
                ...other,
                timerId: "delay",
              }),
            }),
          ]
        : [];
    const events =
      recordKind === "event"
        ? [
            create(EventRecordSchema, {
              key: create(EventKeySchema, {
                ...other,
                eventId: "approve",
              }),
            }),
          ]
        : [];
    const claims =
      recordKind === "claim"
        ? [
            create(ClaimRecordSchema, {
              key: create(ClaimKeySchema, {
                ...other,
                claimId: "activity:validate",
              }),
            }),
          ]
        : [];
    const store: RunInspectionStore = {
      getWorkflow: vi.fn(async () => workflow),
      listActivities: vi.fn(async () => activities),
      listTimers: vi.fn(async () => timers),
      listEvents: vi.fn(async () => events),
      listClaims: vi.fn(async () => claims),
    };

    await expect(inspectRun(store, key)).rejects.toThrow(
      `${recordKind} record key does not match snapshot run`,
    );
  });

  it("rejects a record with no key", async () => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "approval:export",
      runId: "run-1",
    });
    const store: RunInspectionStore = {
      getWorkflow: vi.fn(async () => undefined),
      listActivities: vi.fn(async () => [create(ActivityRecordSchema)]),
      listTimers: vi.fn(async () => []),
      listEvents: vi.fn(async () => []),
      listClaims: vi.fn(async () => []),
    };

    await expect(inspectRun(store, key)).rejects.toThrow(
      "activity record key is required",
    );
  });
});

describe("projectWorkflowRun", () => {
  it("joins exact IDs, retains unplanned records, and invents no status", () => {
    const workflowPlan = plan(
      [
        node("validate", WorkflowPlanNodeKind.ACTIVITY),
        node("delay", WorkflowPlanNodeKind.SLEEP),
        node("approve", WorkflowPlanNodeKind.WAIT_EVENT),
        node("future", WorkflowPlanNodeKind.ACTIVITY),
      ],
      [
        edge("validate", "delay"),
        edge("delay", "approve"),
        edge("approve", "future"),
      ],
    );
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "approval:export",
      runId: "run-1",
    });
    const activity = (activityId: string) =>
      create(ActivityRecordSchema, {
        key: create(ActivityKeySchema, { ...key, activityId }),
        status: ActivityStatus.COMPLETED,
      });
    const timer = (
      timerId: string,
      timerKind = TimerKind.SLEEP,
      retryActivityId = "",
    ) =>
      create(TimerRecordSchema, {
        key: create(TimerKeySchema, { ...key, timerId }),
        timerKind,
        retryActivityId,
        status: TimerStatus.SCHEDULED,
      });
    const event = (eventId: string) =>
      create(EventRecordSchema, {
        key: create(EventKeySchema, { ...key, eventId }),
      });
    const claim = (claimId: string, resourceId: string) =>
      create(ClaimRecordSchema, {
        key: create(ClaimKeySchema, { ...key, claimId }),
        resourceType: ClaimResourceType.ACTIVITY,
        resourceId,
      });
    const observedActivity = activity("validate");
    const observedTimer = timer("delay");
    const observedEvent = event("approve");
    const observedClaim = claim("activity:validate", "validate");
    const timerClaim = create(ClaimRecordSchema, {
      key: create(ClaimKeySchema, { ...key, claimId: "timer:delay" }),
      resourceType: ClaimResourceType.TIMER,
      resourceId: "delay",
    });
    const extraActivity = activity("dynamic:activity");
    const approvalPollTimer = timer("approve", TimerKind.POLL);
    const retryTimer = timer(
      "retry:validate",
      TimerKind.ACTIVITY_RETRY,
      "validate",
    );
    const extraTimer = timer("dynamic:timer");
    const extraEvent = event("external:event");
    const extraClaim = create(ClaimRecordSchema, {
      key: create(ClaimKeySchema, {
        ...key,
        claimId: "workflow:execution",
      }),
      resourceType: ClaimResourceType.WORKFLOW,
      resourceId: "approval:export",
    });
    const orphanClaim = claim("activity:dynamic", "dynamic:activity");
    const snapshot: RunSnapshot = {
      key,
      workflow: create(WorkflowRecordSchema, {
        key,
        status: WorkflowStatus.IN_PROGRESS,
      }),
      activities: [observedActivity, extraActivity],
      timers: [extraTimer, retryTimer, approvalPollTimer, observedTimer],
      events: [observedEvent, extraEvent],
      claims: [orphanClaim, timerClaim, observedClaim, extraClaim],
    };

    const projection = projectWorkflowRun(workflowPlan, snapshot);

    const projectedById = new Map(
      projection.nodes.map((projected) => [projected.node.nodeId, projected]),
    );
    expect(projectedById.get("validate")).toMatchObject({
      activities: [observedActivity],
      claims: [observedClaim],
      timers: [retryTimer],
      events: [],
    });
    expect(projectedById.get("delay")).toMatchObject({
      activities: [],
      timers: [observedTimer],
      events: [],
      claims: [timerClaim],
    });
    expect(projectedById.get("approve")).toMatchObject({
      activities: [],
      timers: [approvalPollTimer],
      events: [observedEvent],
      claims: [],
    });
    expect(projectedById.get("future")).toMatchObject({
      activities: [],
      timers: [],
      events: [],
      claims: [],
    });
    for (const projected of projection.nodes) {
      expect(projected).not.toHaveProperty("status");
      expect(projected).not.toHaveProperty("skipped");
    }
    expect(projection.unplanned).toEqual({
      activities: [extraActivity],
      timers: [extraTimer],
      events: [extraEvent],
      claims: [orphanClaim],
    });
    expect(projection.runClaims).toEqual([extraClaim]);
    expect(projection.snapshot).toBe(snapshot);
  });

  it("rejects a manually constructed snapshot containing another run", () => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "approval:export",
      runId: "run-1",
    });
    const snapshot: RunSnapshot = {
      key,
      workflow: undefined,
      activities: [
        create(ActivityRecordSchema, {
          key: create(ActivityKeySchema, {
            namespace: key.namespace,
            workflowId: key.workflowId,
            runId: "other-run",
            activityId: "validate",
          }),
        }),
      ],
      timers: [],
      events: [],
      claims: [],
    };

    expect(() => projectWorkflowRun(plan(), snapshot)).toThrow(
      "activity record key does not match snapshot run",
    );
  });

  it("rejects a manually constructed snapshot with an invalid run key", () => {
    const snapshot: RunSnapshot = {
      key: create(WorkflowKeySchema, {
        namespace: "default",
        workflowId: "_reserved",
        runId: "run-1",
      }),
      workflow: undefined,
      activities: [],
      timers: [],
      events: [],
      claims: [],
    };

    expect(() => projectWorkflowRun(plan(), snapshot)).toThrow(
      "workflow_id must not start with _",
    );
  });
});
