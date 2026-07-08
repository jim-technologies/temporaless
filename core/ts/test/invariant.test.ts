import { describe, expect, it } from "vitest";
import {
  TEMPORALESS_RECORD_QUERY_SERVICE,
  TEMPORALESS_RECORD_STORE_SERVICE,
  createTemporalessInvariantHttpProxy,
  createTemporalessInvariantServer,
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
});
