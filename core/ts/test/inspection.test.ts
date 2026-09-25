import { describe, expect, it } from "vitest";
import { RunInspectionService } from "../src/gen/temporaless/v1/inspection_pb.js";
import { createTemporalessInvariantServer } from "../src/invariant.js";

describe("RunInspectionService contract", () => {
  it("exposes only unary read methods", () => {
    const methods = Object.values(RunInspectionService.method).map(
      (method) => [method.name, method.methodKind] as const,
    );
    expect(methods).toEqual([
      ["GetInspectionCapabilities", "unary"],
      ["ListNamespaces", "unary"],
      ["ListWorkflowDirectory", "unary"],
      ["ListWorkflowRuns", "unary"],
      ["ListScheduledWakes", "unary"],
      ["DescribeRun", "unary"],
    ]);
  });

  it("ships in the descriptor without being registered by the store projection", () => {
    const server = createTemporalessInvariantServer();
    expect(server.parsed.services.has("temporaless.v1.RunInspectionService")).toBe(true);
    const names = server.toolCatalog().map((tool) => tool.name);
    expect(names.some((name) => name.includes("RunInspectionService"))).toBe(false);
  });
});
