import { readFileSync } from "node:fs";

import {
  Server,
  type ConnectHttpOptions,
  type HandlerContext,
  type StreamInterceptor,
  type ToolCatalogEntry,
  type UnaryInterceptor,
} from "@jim-technologies/invariant-protocol";

export {
  InvariantError,
  ParsedDescriptor,
  SchemaGenerator,
  cliHelp,
  httpHandler,
  mcpDispatch,
  runCli,
  serveHttp,
  type Code,
  type JsonRpcRequest,
  type JsonSchema,
  type ToolCatalogEntry,
} from "@jim-technologies/invariant-protocol";

export const TEMPORALESS_RECORD_STORE_SERVICE = "temporaless.v1.RecordStoreService";
export const TEMPORALESS_RECORD_QUERY_SERVICE = "temporaless.v1.RecordQueryService";
export const TEMPORALESS_DESCRIPTOR_URL = new URL(
  "./gen/temporaless/v1/temporaless_descriptor.binpb",
  import.meta.url,
);

export type TemporalessInvariantServerOptions = {
  descriptorBytes?: Uint8Array;
  descriptorPath?: string;
  includeStore?: boolean;
  includeQuery?: boolean;
};

export type TemporalessInvariantHttpProxyOptions = TemporalessInvariantServerOptions &
  Omit<ConnectHttpOptions, "serviceName">;

export type TemporalessInvariantServer = Server;
export type TemporalessInvariantUnaryInterceptor = UnaryInterceptor;
export type TemporalessInvariantStreamInterceptor = StreamInterceptor;
export type TemporalessInvariantHandlerContext = HandlerContext;
export type TemporalessInvariantToolCatalogEntry = ToolCatalogEntry;

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

function includeTemporalessServices(
  server: Server,
  options: TemporalessInvariantServerOptions,
): void {
  const includes: string[] = [];
  if (options.includeStore ?? true) {
    includes.push(`${TEMPORALESS_RECORD_STORE_SERVICE}.*`);
  }
  if (options.includeQuery ?? true) {
    includes.push(`${TEMPORALESS_RECORD_QUERY_SERVICE}.*`);
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
