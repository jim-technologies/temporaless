import { readFileSync } from "node:fs";

import type { ServiceImpl } from "@connectrpc/connect";
import {
  Server,
  type ConnectHttpOptions,
  type HandlerContext,
  type Interceptor,
  type ToolCatalogEntry,
} from "@jim-technologies/invariant-protocol";
import {
  RecordQueryService,
  RecordStoreService,
} from "./gen/temporaless/v1/temporaless_pb.js";

export {
  InvariantError,
  MCP_PROTOCOL_VERSION,
  ParsedDescriptor,
  SchemaGenerator,
  cliHelp,
  httpHandler,
  runCli,
  serveHttp,
  serveMcpStdio,
  type Code,
  type JsonRpcRequest,
  type JsonSchema,
  type McpContextOptions,
  type McpStdioInput,
  type McpStdioOutput,
  type ToolCatalogEntry,
} from "@jim-technologies/invariant-protocol";

export const TEMPORALESS_RECORD_STORE_SERVICE = "temporaless.v1.RecordStoreService";
export const TEMPORALESS_RECORD_QUERY_SERVICE = "temporaless.v1.RecordQueryService";
export const TEMPORALESS_READ_ONLY_STORE_METHODS = [
  "GetStoreCapabilities",
  "GetWorkflow",
  "GetLatestWorkflowRun",
  "GetTimer",
  "GetActivity",
  "GetClaim",
  "GetEvent",
  "ListActivities",
  "ListTimers",
  "ListEvents",
  "ListClaims",
] as const;
export const TEMPORALESS_READ_ONLY_QUERY_METHODS = [
  "ListWorkflows",
  "ListActivities",
  "DueTimers",
] as const;
export const TEMPORALESS_DESCRIPTOR_URL = new URL(
  "./gen/temporaless/v1/temporaless_descriptor.binpb",
  import.meta.url,
);

export type TemporalessInvariantServerOptions = {
  descriptorBytes?: Uint8Array;
  descriptorPath?: string;
  includeStore?: boolean;
  includeQuery?: boolean;
  /**
   * Include mutation, claim-coordination, timer-repair, retention, and delete
   * methods. Defaults to false so an ordinary MCP/CLI projection is
   * inspection-only. Enabling this requires an operator-scoped backend
   * credential and an authenticated inbound boundary.
   */
  includeOperatorMethods?: boolean;
};

export type TemporalessInvariantHttpProxyOptions = TemporalessInvariantServerOptions &
  Omit<ConnectHttpOptions, "serviceName">;

export type TemporalessInvariantServer = Server;
export type TemporalessInvariantInterceptor = Interceptor;
export type TemporalessInvariantHandlerContext = HandlerContext;
export type TemporalessInvariantToolCatalogEntry = ToolCatalogEntry;
export type TemporalessRecordStoreImplementation = Partial<
  ServiceImpl<typeof RecordStoreService>
>;
export type TemporalessRecordQueryImplementation = Partial<
  ServiceImpl<typeof RecordQueryService>
>;
export type TemporalessInvariantServiceImplementations = {
  recordStore?: TemporalessRecordStoreImplementation;
  recordQuery?: TemporalessRecordQueryImplementation;
};

export function temporalessDescriptorBytes(): Uint8Array {
  return readFileSync(TEMPORALESS_DESCRIPTOR_URL);
}

export function createTemporalessInvariantServer(
  options: TemporalessInvariantServerOptions = {},
): TemporalessInvariantServer {
  const server = options.descriptorPath
    ? Server.fromDescriptor(options.descriptorPath)
    : Server.fromBytes(options.descriptorBytes ?? temporalessDescriptorBytes());
  includeTemporalessServices(server, options);
  return server;
}

export function createTemporalessInvariantHttpProxy(
  baseUrl: string,
  options: TemporalessInvariantHttpProxyOptions = {},
): TemporalessInvariantServer {
  const server = createTemporalessInvariantServer(options);
  const connectOptions = connectHttpOptions(options);

  if (options.includeStore ?? true) {
    server.connectHttp(baseUrl, {
      ...connectOptions,
      serviceName: TEMPORALESS_RECORD_STORE_SERVICE,
    });
  }
  if (options.includeQuery ?? true) {
    server.connectHttp(baseUrl, {
      ...connectOptions,
      serviceName: TEMPORALESS_RECORD_QUERY_SERVICE,
    });
  }
  return server;
}

export function registerTemporalessInvariantServices(
  server: TemporalessInvariantServer,
  implementations: TemporalessInvariantServiceImplementations,
): TemporalessInvariantServer {
  let registered = false;
  if (implementations.recordStore !== undefined) {
    server.register(RecordStoreService, implementations.recordStore);
    registered = true;
  }
  if (implementations.recordQuery !== undefined) {
    server.register(RecordQueryService, implementations.recordQuery);
    registered = true;
  }
  if (!registered) {
    throw new Error("At least one Temporaless service implementation is required.");
  }
  return server;
}

function includeTemporalessServices(
  server: Server,
  options: TemporalessInvariantServerOptions,
): void {
  const includes: string[] = [];
  if (options.includeStore ?? true) {
    includes.push(
      ...(options.includeOperatorMethods
        ? [`${TEMPORALESS_RECORD_STORE_SERVICE}.*`]
        : TEMPORALESS_READ_ONLY_STORE_METHODS.map(
            (method) => `${TEMPORALESS_RECORD_STORE_SERVICE}.${method}`,
          )),
    );
  }
  if (options.includeQuery ?? true) {
    includes.push(
      ...(options.includeOperatorMethods
        ? [`${TEMPORALESS_RECORD_QUERY_SERVICE}.*`]
        : TEMPORALESS_READ_ONLY_QUERY_METHODS.map(
            (method) => `${TEMPORALESS_RECORD_QUERY_SERVICE}.${method}`,
          )),
    );
  }
  if (includes.length === 0) {
    throw new Error("At least one Temporaless service must be included.");
  }
  server.include(...includes);
}

function connectHttpOptions(
  options: TemporalessInvariantHttpProxyOptions,
): Omit<ConnectHttpOptions, "serviceName"> {
  const out: Omit<ConnectHttpOptions, "serviceName"> = {};
  if (options.auth !== undefined) {
    out.auth = options.auth;
  }
  if (options.channelOptions !== undefined) {
    out.channelOptions = options.channelOptions;
  }
  if (options.observer !== undefined) {
    out.observer = options.observer;
  }
  return out;
}
