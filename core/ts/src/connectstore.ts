import { create } from "@bufbuild/protobuf";
import { durationFromMs, timestampFromDate } from "@bufbuild/protobuf/wkt";
import { createClient, type Client, type Transport } from "@connectrpc/connect";
import {
  ActivityStatus,
  DeleteActivityRequestSchema,
  DeleteClaimRequestSchema,
  DeleteEventRequestSchema,
  DeleteRunRequestSchema,
  DeleteTimerRequestSchema,
  DeleteWorkflowRequestSchema,
  DueTimersRequestSchema,
  GetActivityRequestSchema,
  GetClaimRequestSchema,
  GetEventRequestSchema,
  GetLatestWorkflowRunRequestSchema,
  GetStoreCapabilitiesRequestSchema,
  GetTimerRequestSchema,
  GetWorkflowRequestSchema,
  ListActivitiesRequestSchema,
  ListClaimsRequestSchema,
  ListEventsRequestSchema,
  ListTimersRequestSchema,
  ListWorkflowsRequestSchema,
  PutActivityRequestSchema,
  PutEventRequestSchema,
  PutTimerRequestSchema,
  PutWorkflowRequestSchema,
  RecordQueryService,
  RecordQueryServiceDueTimersRequestSchema,
  RecordQueryServiceListActivitiesRequestSchema,
  RecordStoreService,
  SweepRequestSchema,
  TimerStatus,
  TryCreateClaimRequestSchema,
  WorkflowStatus,
  type ActivityKey,
  type ActivityRecord,
  type ClaimCapability,
  type ClaimKey,
  type ClaimRecord,
  type DueTimer,
  type EventKey,
  type EventRecord,
  type LatestWorkflowRunPointer,
  type TimerKey,
  type TimerRecord,
  type WorkflowKey,
  type WorkflowRecord,
} from "./gen/temporaless/v1/temporaless_pb.js";

export type RecordStoreClient = Client<typeof RecordStoreService>;
export type RecordQueryClient = Client<typeof RecordQueryService>;

export type ListWorkflowsOptions = {
  namespace?: string;
  workflowId?: string;
  status?: WorkflowStatus;
  orderBy?: string;
  pageSize?: number;
  pageToken?: string;
};

export type ListActivitiesOptions = {
  namespace?: string;
  workflowId?: string;
  runId?: string;
  status?: ActivityStatus;
  orderBy?: string;
  pageSize?: number;
  pageToken?: string;
};

export type SweepOptions = {
  namespace?: string;
  now: Date;
  maxAgeMs: number;
};

export type ListResult<T> = {
  records: T[];
  nextPageToken: string;
};

export class ConnectStore {
  constructor(private readonly client: RecordStoreClient) {}

  static fromTransport(transport: Transport): ConnectStore {
    return new ConnectStore(createClient(RecordStoreService, transport));
  }

  async getWorkflow(key: WorkflowKey): Promise<WorkflowRecord | undefined> {
    const response = await this.client.getWorkflow(
      create(GetWorkflowRequestSchema, { key }),
    );
    return response.found ? response.record : undefined;
  }

  async putWorkflow(record: WorkflowRecord): Promise<void> {
    await this.client.putWorkflow(create(PutWorkflowRequestSchema, { record }));
  }

  async getLatestWorkflowRun(
    namespace: string,
    workflowId: string,
  ): Promise<LatestWorkflowRunPointer | undefined> {
    const response = await this.client.getLatestWorkflowRun(
      create(GetLatestWorkflowRunRequestSchema, { namespace, workflowId }),
    );
    return response.found ? response.pointer : undefined;
  }

  async deleteWorkflow(key: WorkflowKey): Promise<boolean> {
    const response = await this.client.deleteWorkflow(
      create(DeleteWorkflowRequestSchema, { key }),
    );
    return response.deleted;
  }

  async deleteRun(key: WorkflowKey): Promise<number> {
    const response = await this.client.deleteRun(
      create(DeleteRunRequestSchema, { key }),
    );
    return response.deleted;
  }

  async getActivity(key: ActivityKey): Promise<ActivityRecord | undefined> {
    const response = await this.client.getActivity(
      create(GetActivityRequestSchema, { key }),
    );
    return response.found ? response.record : undefined;
  }

  async putActivity(record: ActivityRecord): Promise<void> {
    await this.client.putActivity(create(PutActivityRequestSchema, { record }));
  }

  async listActivities(key: WorkflowKey): Promise<ActivityRecord[]> {
    const response = await this.client.listActivities(
      create(ListActivitiesRequestSchema, { key }),
    );
    return response.records;
  }

  async deleteActivity(key: ActivityKey): Promise<boolean> {
    const response = await this.client.deleteActivity(
      create(DeleteActivityRequestSchema, { key }),
    );
    return response.deleted;
  }

  async getTimer(key: TimerKey): Promise<TimerRecord | undefined> {
    const response = await this.client.getTimer(
      create(GetTimerRequestSchema, { key }),
    );
    return response.found ? response.record : undefined;
  }

  async putTimer(record: TimerRecord): Promise<void> {
    await this.client.putTimer(create(PutTimerRequestSchema, { record }));
  }

  async listTimers(
    key: WorkflowKey,
    status: TimerStatus = TimerStatus.UNSPECIFIED,
  ): Promise<TimerRecord[]> {
    const response = await this.client.listTimers(
      create(ListTimersRequestSchema, { key, status }),
    );
    return response.records;
  }

  async deleteTimer(key: TimerKey): Promise<boolean> {
    const response = await this.client.deleteTimer(
      create(DeleteTimerRequestSchema, { key }),
    );
    return response.deleted;
  }

  async dueTimers(namespace: string, now: Date): Promise<DueTimer[]> {
    const response = await this.client.dueTimers(
      create(DueTimersRequestSchema, {
        namespace,
        now: timestampFromDate(now),
      }),
    );
    return response.due;
  }

  async getEvent(key: EventKey): Promise<EventRecord | undefined> {
    const response = await this.client.getEvent(
      create(GetEventRequestSchema, { key }),
    );
    return response.found ? response.record : undefined;
  }

  async putEvent(record: EventRecord): Promise<void> {
    await this.client.putEvent(create(PutEventRequestSchema, { record }));
  }

  async listEvents(key: WorkflowKey): Promise<EventRecord[]> {
    const response = await this.client.listEvents(
      create(ListEventsRequestSchema, { key }),
    );
    return response.records;
  }

  async deleteEvent(key: EventKey): Promise<boolean> {
    const response = await this.client.deleteEvent(
      create(DeleteEventRequestSchema, { key }),
    );
    return response.deleted;
  }

  async getClaim(key: ClaimKey): Promise<ClaimRecord | undefined> {
    const response = await this.client.getClaim(
      create(GetClaimRequestSchema, { key }),
    );
    return response.found ? response.record : undefined;
  }

  async listClaims(key: WorkflowKey): Promise<ClaimRecord[]> {
    const response = await this.client.listClaims(
      create(ListClaimsRequestSchema, { key }),
    );
    return response.records;
  }

  async tryCreateClaim(record: ClaimRecord): Promise<boolean> {
    const response = await this.client.tryCreateClaim(
      create(TryCreateClaimRequestSchema, { record }),
    );
    return response.created;
  }

  async deleteClaim(key: ClaimKey): Promise<boolean> {
    const response = await this.client.deleteClaim(
      create(DeleteClaimRequestSchema, { key }),
    );
    return response.deleted;
  }

  async claimCapability(): Promise<ClaimCapability> {
    const response = await this.client.getStoreCapabilities(
      create(GetStoreCapabilitiesRequestSchema),
    );
    return response.claimCapability;
  }
}

export class ConnectQueryStore {
  constructor(private readonly client: RecordQueryClient) {}

  static fromTransport(transport: Transport): ConnectQueryStore {
    return new ConnectQueryStore(createClient(RecordQueryService, transport));
  }

  async listWorkflows(
    options: ListWorkflowsOptions = {},
  ): Promise<ListResult<WorkflowRecord>> {
    const response = await this.client.listWorkflows(
      create(ListWorkflowsRequestSchema, {
        namespace: options.namespace ?? "",
        workflowId: options.workflowId ?? "",
        status: options.status ?? WorkflowStatus.UNSPECIFIED,
        orderBy: options.orderBy ?? "",
        pageSize: options.pageSize ?? 0,
        pageToken: options.pageToken ?? "",
      }),
    );
    return {
      records: response.records,
      nextPageToken: response.nextPageToken,
    };
  }

  async listActivities(
    options: ListActivitiesOptions = {},
  ): Promise<ListResult<ActivityRecord>> {
    const response = await this.client.listActivities(
      create(RecordQueryServiceListActivitiesRequestSchema, {
        namespace: options.namespace ?? "",
        workflowId: options.workflowId ?? "",
        runId: options.runId ?? "",
        status: options.status ?? ActivityStatus.UNSPECIFIED,
        orderBy: options.orderBy ?? "",
        pageSize: options.pageSize ?? 0,
        pageToken: options.pageToken ?? "",
      }),
    );
    return {
      records: response.records,
      nextPageToken: response.nextPageToken,
    };
  }

  async dueTimers(namespace: string, now: Date): Promise<DueTimer[]> {
    const response = await this.client.dueTimers(
      create(RecordQueryServiceDueTimersRequestSchema, {
        namespace,
        now: timestampFromDate(now),
      }),
    );
    return response.due;
  }

  async sweep(options: SweepOptions): Promise<number> {
    const response = await this.client.sweep(
      create(SweepRequestSchema, {
        namespace: options.namespace ?? "",
        now: timestampFromDate(options.now),
        maxAge: durationFromMs(options.maxAgeMs),
      }),
    );
    return response.deleted;
  }
}
