import { createServer as createNodeServer } from "node:http";

import { describe, expect, it } from "vitest";
import {
  TEMPORALESS_RECORD_QUERY_SERVICE,
  TEMPORALESS_RECORD_STORE_SERVICE,
  TEMPORALESS_READ_ONLY_QUERY_METHODS,
  TEMPORALESS_READ_ONLY_STORE_METHODS,
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
    expect(names).not.toContain("RecordStoreService.PutWorkflow");
    expect(names).not.toContain("RecordStoreService.DueTimers");
    expect(names).not.toContain("RecordStoreService.DeleteRun");
    expect(names).not.toContain("RecordQueryService.Sweep");
    expect(
      names.filter((name) => name.startsWith("RecordStoreService.")),
    ).toHaveLength(TEMPORALESS_READ_ONLY_STORE_METHODS.length);
    expect(
      names.filter((name) => name.startsWith("RecordQueryService.")),
    ).toHaveLength(TEMPORALESS_READ_ONLY_QUERY_METHODS.length);
    expect(
      tools.find((tool) => tool.name === "RecordStoreService.GetWorkflow")
        ?.description,
    ).toContain("Read a single workflow record");
  });

  it("requires an explicit operator opt-in for destructive methods", () => {
    const server = createTemporalessInvariantHttpProxy("https://temporaless.example", {
      includeOperatorMethods: true,
    });
    const names = server.toolCatalog().map((tool) => tool.name);

    expect(names).toContain("RecordStoreService.PutWorkflow");
    expect(names).toContain("RecordStoreService.DueTimers");
    expect(names).toContain("RecordStoreService.DeleteRun");
    expect(names).toContain("RecordQueryService.Sweep");
  });

  it("does not serve operator methods through the default HTTP projection", async () => {
    const server = createTemporalessInvariantHttpProxy("https://temporaless.example");
    const handler = server.httpHandler();
    const nodeServer = createNodeServer((request, response) => {
      void handler(request, response);
    });

    try {
      await new Promise<void>((resolve, reject) => {
        nodeServer.once("error", reject);
        nodeServer.listen(0, "127.0.0.1", resolve);
      });
      const address = nodeServer.address();
      if (address === null || typeof address === "string") {
        throw new Error("HTTP projection did not bind a TCP port");
      }

      const response = await fetch(
        `http://127.0.0.1:${address.port}/${TEMPORALESS_RECORD_STORE_SERVICE}/PutWorkflow`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        },
      );

      expect(response.status).toBe(404);
      await expect(response.json()).resolves.toMatchObject({ code: "not_found" });
    } finally {
      await new Promise<void>((resolve, reject) => {
        nodeServer.close((error) => (error ? reject(error) : resolve()));
      });
    }
  });

  it("can scope invariant projection to query services only", () => {
    const server = createTemporalessInvariantHttpProxy("https://temporaless.example", {
      includeStore: false,
    });
    const names = server.toolCatalog().map((tool) => tool.name);

    expect(names).not.toContain("RecordStoreService.GetWorkflow");
    expect(names).toContain("RecordQueryService.ListWorkflows");
    expect(names).not.toContain("RecordQueryService.Sweep");
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
