import { describe, expect, it } from "vitest";
import {
  TEMPORALESS_RECORD_QUERY_SERVICE,
  TEMPORALESS_RECORD_STORE_SERVICE,
  createTemporalessInvariantHttpProxy,
  createTemporalessInvariantServer,
  registerTemporalessInvariantServices,
  temporalessDescriptorBytes,
} from "../src/invariant.js";

describe("Temporaless invariantprotocol integration", () => {
  it("loads the descriptor with Temporaless storage services", () => {
    const server = createTemporalessInvariantServer();

    expect(temporalessDescriptorBytes().byteLength).toBeGreaterThan(0);
    expect(server.parsed.services.has(TEMPORALESS_RECORD_STORE_SERVICE)).toBe(
      true,
    );
    expect(server.parsed.services.has(TEMPORALESS_RECORD_QUERY_SERVICE)).toBe(
      true,
    );
  });

  it("builds a descriptor-backed HTTP proxy tool catalog", () => {
    const server = createTemporalessInvariantHttpProxy("https://temporaless.example");
    const tools = server.toolCatalog();
    const names = tools.map((tool) => tool.name);

    expect(names).toContain("RecordStoreService.GetWorkflow");
    expect(names).toContain("RecordQueryService.ListWorkflows");
    expect(
      tools.find((tool) => tool.name === "RecordStoreService.GetWorkflow")
        ?.description,
    ).toContain("Read a single workflow record");
  });

  it("can scope invariant projection to query services only", () => {
    const server = createTemporalessInvariantHttpProxy("https://temporaless.example", {
      includeStore: false,
    });
    const names = server.toolCatalog().map((tool) => tool.name);

    expect(names).not.toContain("RecordStoreService.GetWorkflow");
    expect(names).toContain("RecordQueryService.ListWorkflows");
  });

  it("registers generated Temporaless service implementations", async () => {
    const server = createTemporalessInvariantServer({ includeQuery: false });
    registerTemporalessInvariantServices(server, {
      recordStore: {
        getWorkflow(request) {
          expect(request.key?.workflowId).toBe("prices:aapl");
          return { found: false };
        },
      },
    });

    const response = await server.invoke("RecordStoreService.GetWorkflow", {
      key: {
        namespace: "default",
        workflowId: "prices:aapl",
        runId: "2026-07-16T00:00:00Z",
      },
    });

    expect(response.found).toBe(false);
  });

  it("rejects empty generated service registration", () => {
    const server = createTemporalessInvariantServer();

    expect(() => registerTemporalessInvariantServices(server, {})).toThrow(
      "At least one Temporaless service implementation is required.",
    );
  });
});
