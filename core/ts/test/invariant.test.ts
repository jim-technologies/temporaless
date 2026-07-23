import { createServer as createNodeServer } from "node:http";

import { connectNodeAdapter } from "@connectrpc/connect-node";
import { describe, expect, it } from "vitest";
import { RecordStoreService } from "../src/gen/temporaless/v1/temporaless_pb.js";
import {
  TEMPORALESS_RECORD_QUERY_SERVICE,
  TEMPORALESS_RECORD_STORE_SERVICE,
  TEMPORALESS_READ_ONLY_QUERY_METHODS,
  TEMPORALESS_READ_ONLY_STORE_METHODS,
  MCP_PROTOCOL_VERSION,
  createTemporalessInvariantHttpProxy,
  createTemporalessInvariantServer,
  registerTemporalessInvariantServices,
  runCli,
  serveMcpStdio,
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

  it("rejects ambiguous descriptor sources before reading either one", () => {
    expect(() =>
      createTemporalessInvariantServer({
        descriptorPath: "must-not-be-read.binpb",
        descriptorBytes: new Uint8Array([0]),
      }),
    ).toThrow("descriptorPath and descriptorBytes are mutually exclusive.");
  });

  it("does not treat an explicitly supplied empty descriptor path as absent", () => {
    expect(() =>
      createTemporalessInvariantServer({ descriptorPath: "" }),
    ).toThrow();
  });

  it("builds a descriptor-backed HTTP proxy tool catalog", () => {
    const server = createTemporalessInvariantHttpProxy("https://temporaless.example");
    const tools = server.toolCatalog();
    const names = tools.map((tool) => tool.name);

    expect(names).toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.GetWorkflow`);
    expect(names).toContain(`${TEMPORALESS_RECORD_QUERY_SERVICE}.ListWorkflows`);
    expect(names).not.toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.PutWorkflow`);
    expect(names).not.toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.DeliverEvent`);
    expect(names).not.toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.DueTimers`);
    expect(names).not.toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.DeleteRun`);
    expect(names).not.toContain(`${TEMPORALESS_RECORD_QUERY_SERVICE}.Sweep`);
    expect(
      names.filter((name) => name.startsWith(`${TEMPORALESS_RECORD_STORE_SERVICE}.`)),
    ).toHaveLength(TEMPORALESS_READ_ONLY_STORE_METHODS.length);
    expect(
      names.filter((name) => name.startsWith(`${TEMPORALESS_RECORD_QUERY_SERVICE}.`)),
    ).toHaveLength(TEMPORALESS_READ_ONLY_QUERY_METHODS.length);
    expect(
      tools.find(
        (tool) => tool.name === `${TEMPORALESS_RECORD_STORE_SERVICE}.GetWorkflow`,
      )
        ?.description,
    ).toContain("Read a single workflow record");
  });

  it("requires an explicit operator opt-in for destructive methods", () => {
    const server = createTemporalessInvariantHttpProxy("https://temporaless.example", {
      includeOperatorMethods: true,
    });
    const names = server.toolCatalog().map((tool) => tool.name);

    expect(names).toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.PutWorkflow`);
    expect(names).toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.DeliverEvent`);
    expect(names).toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.DueTimers`);
    expect(names).toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.DeleteRun`);
    expect(names).toContain(`${TEMPORALESS_RECORD_QUERY_SERVICE}.Sweep`);
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

  it("proxies an allowed RPC through a generated Connect service", async () => {
    let observedWorkflowId: string | undefined;
    const handler = connectNodeAdapter({
      routes(router) {
        router.service(RecordStoreService, {
          getWorkflow(request) {
            observedWorkflowId = request.key?.workflowId;
            return { found: false };
          },
        });
      },
    });
    const nodeServer = createNodeServer(handler);

    try {
      await new Promise<void>((resolve, reject) => {
        nodeServer.once("error", reject);
        nodeServer.listen(0, "127.0.0.1", resolve);
      });
      const address = nodeServer.address();
      if (address === null || typeof address === "string") {
        throw new Error("generated Connect service did not bind a TCP port");
      }

      const proxy = createTemporalessInvariantHttpProxy(
        `http://127.0.0.1:${address.port}`,
        { includeQuery: false },
      );
      const response = await proxy.invoke(`${TEMPORALESS_RECORD_STORE_SERVICE}.GetWorkflow`, {
        key: {
          namespace: "default",
          workflowId: "prices:aapl",
          runId: "2026-07-17T00:00:00Z",
        },
      });

      expect(observedWorkflowId).toBe("prices:aapl");
      expect(response.found).toBe(false);
      expect(response.$typeName).toBe("temporaless.v1.GetWorkflowResponse");
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

    expect(names).not.toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.GetWorkflow`);
    expect(names).toContain(`${TEMPORALESS_RECORD_QUERY_SERVICE}.ListWorkflows`);
    expect(names).not.toContain(`${TEMPORALESS_RECORD_QUERY_SERVICE}.Sweep`);
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

    const response = await server.invoke(`${TEMPORALESS_RECORD_STORE_SERVICE}.GetWorkflow`, {
      key: {
        namespace: "default",
        workflowId: "prices:aapl",
        runId: "2026-07-16T00:00:00Z",
      },
    });

    expect(response.found).toBe(false);
  });

  it("executes a registered Temporaless service through the CLI projection", async () => {
    const server = createTemporalessInvariantServer({ includeQuery: false });
    let observedWorkflowId: string | undefined;
    registerTemporalessInvariantServices(server, {
      recordStore: {
        getWorkflow(request) {
          observedWorkflowId = request.key?.workflowId;
          return { found: true };
        },
      },
    });

    const output = await runCli(server, [
      TEMPORALESS_RECORD_STORE_SERVICE,
      "GetWorkflow",
      "-r",
      JSON.stringify({
        key: {
          namespace: "default",
          workflowId: "prices:aapl",
          runId: "2026-07-17T00:00:00Z",
        },
      }),
    ]);

    expect(observedWorkflowId).toBe("prices:aapl");
    expect(JSON.parse(output)).toEqual({ found: true });
  });

  it("serves the filtered Temporaless projection over MCP stdio", async () => {
    const server = createTemporalessInvariantServer({ includeQuery: false });
    let observedWorkflowId: string | undefined;
    registerTemporalessInvariantServices(server, {
      recordStore: {
        getWorkflow(request) {
          observedWorkflowId = request.key?.workflowId;
          return { found: true };
        },
      },
    });

    const requests = [
      {
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: {
          protocolVersion: MCP_PROTOCOL_VERSION,
          capabilities: {},
          clientInfo: { name: "temporaless-test", version: "1.0.0" },
        },
      },
      { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} },
      {
        jsonrpc: "2.0",
        id: 3,
        method: "tools/call",
        params: {
          name: `${TEMPORALESS_RECORD_STORE_SERVICE}.GetWorkflow`,
          arguments: {
            key: {
              namespace: "default",
              workflowId: "prices:msft",
              runId: "2026-07-17T00:00:00Z",
            },
          },
        },
      },
    ];
    async function* input(): AsyncIterable<string> {
      yield `${requests.map((request) => JSON.stringify(request)).join("\n")}\n`;
    }
    const output: string[] = [];

    await serveMcpStdio(server, input(), {
      write(chunk) {
        output.push(chunk);
      },
    });

    const responses = output
      .join("")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    const byId = new Map(responses.map((response) => [response.id, response]));

    expect(byId.get(1)?.result).toMatchObject({
      protocolVersion: MCP_PROTOCOL_VERSION,
    });
    const toolNames = byId
      .get(2)
      ?.result.tools.map((tool: { name: string }) => tool.name);
    expect(toolNames).toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.GetWorkflow`);
    expect(toolNames).not.toContain(`${TEMPORALESS_RECORD_STORE_SERVICE}.PutWorkflow`);
    expect(observedWorkflowId).toBe("prices:msft");
    expect(JSON.parse(byId.get(3)?.result.content[0].text)).toEqual({
      found: true,
    });
  });

  it("rejects empty generated service registration", () => {
    const server = createTemporalessInvariantServer();

    expect(() => registerTemporalessInvariantServices(server, {})).toThrow(
      "At least one Temporaless service implementation is required.",
    );
  });
});
