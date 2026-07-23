import { create } from "@bufbuild/protobuf";
import { AnySchema, timestampFromDate } from "@bufbuild/protobuf/wkt";
import { describe, expect, it, vi } from "vitest";
import { ConnectQueryStore, ConnectStore } from "../src/connectstore.js";
import {
  ActivityRecordSchema,
  ClaimCapability,
  ClaimKeySchema,
  ClaimRecordSchema,
  DeliverEventResponseSchema,
  EventDeliveryCapability,
  EventDeliveryDisposition,
  EventKeySchema,
  EventRecordSchema,
  GetStoreCapabilitiesResponseSchema,
  GetWorkflowResponseSchema,
  ListClaimsResponseSchema,
  ListWorkflowsResponseSchema,
  RecordQueryServiceDueTimersResponseSchema,
  RecordSchemaVersion,
  SweepResponseSchema,
  TimerKeySchema,
  WorkflowKeySchema,
  WorkflowRecordSchema,
  WorkflowStatus,
  type RecordQueryClient,
  type RecordStoreClient,
} from "../src/index.js";

describe("ConnectStore", () => {
  it("returns undefined for missing workflow records", async () => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "refresh:prices",
      runId: "run-1",
    });
    const getWorkflow = vi.fn(async () =>
      create(GetWorkflowResponseSchema, { found: false }),
    );
    const store = new ConnectStore({ getWorkflow } as unknown as RecordStoreClient);

    await expect(store.getWorkflow(key)).resolves.toBeUndefined();
    expect(getWorkflow).toHaveBeenCalledWith(expect.objectContaining({ key }));
  });

  it("returns workflow records when found", async () => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "refresh:prices",
      runId: "run-1",
    });
    const record = create(WorkflowRecordSchema, {
      key,
      status: WorkflowStatus.COMPLETED,
    });
    const getWorkflow = vi.fn(async () =>
      create(GetWorkflowResponseSchema, { found: true, record }),
    );
    const store = new ConnectStore({ getWorkflow } as unknown as RecordStoreClient);

    await expect(store.getWorkflow(key)).resolves.toEqual(record);
  });

  it("exposes store claim capability", async () => {
    const getStoreCapabilities = vi.fn(async () =>
      create(GetStoreCapabilitiesResponseSchema, {
        claimCapability: ClaimCapability.CREATE_ONLY_CLAIMS,
      }),
    );
    const store = new ConnectStore({
      getStoreCapabilities,
    } as unknown as RecordStoreClient);

    await expect(store.claimCapability()).resolves.toBe(
      ClaimCapability.CREATE_ONLY_CLAIMS,
    );
  });

  it.each([
    {
      name: "maps an unspecified capability to no atomic create",
      remote: EventDeliveryCapability.UNSPECIFIED,
      expected: EventDeliveryCapability.NO_ATOMIC_CREATE,
    },
    {
      name: "returns no atomic create",
      remote: EventDeliveryCapability.NO_ATOMIC_CREATE,
      expected: EventDeliveryCapability.NO_ATOMIC_CREATE,
    },
    {
      name: "returns create-only delivery",
      remote: EventDeliveryCapability.CREATE_ONLY,
      expected: EventDeliveryCapability.CREATE_ONLY,
    },
  ])("$name", async ({ remote, expected }) => {
    const getStoreCapabilities = vi.fn(async () =>
      create(GetStoreCapabilitiesResponseSchema, {
        eventDeliveryCapability: remote,
      }),
    );
    const store = new ConnectStore({
      getStoreCapabilities,
    } as unknown as RecordStoreClient);

    await expect(store.eventDeliveryCapability()).resolves.toBe(expected);
  });

  it("rejects an unknown event delivery capability", async () => {
    const getStoreCapabilities = vi.fn(async () =>
      create(GetStoreCapabilitiesResponseSchema, {
        eventDeliveryCapability: 99 as EventDeliveryCapability,
      }),
    );
    const store = new ConnectStore({
      getStoreCapabilities,
    } as unknown as RecordStoreClient);

    await expect(store.eventDeliveryCapability()).rejects.toThrow(
      "invalid event delivery capability 99",
    );
  });

  it.each([
    EventDeliveryDisposition.CREATED,
    EventDeliveryDisposition.IDEMPOTENT,
  ])("delivers an event with disposition %s", async (disposition) => {
    const record = deliverableEventRecord();
    const deliverEvent = vi.fn(async () =>
      create(DeliverEventResponseSchema, { disposition }),
    );
    const store = new ConnectStore({ deliverEvent } as unknown as RecordStoreClient);

    await expect(store.deliverEvent(record)).resolves.toBe(disposition);
    expect(deliverEvent).toHaveBeenCalledWith(expect.objectContaining({ record }));
  });

  it("rejects an invalid event delivery disposition", async () => {
    const deliverEvent = vi.fn(async () =>
      create(DeliverEventResponseSchema, {
        disposition: EventDeliveryDisposition.UNSPECIFIED,
      }),
    );
    const store = new ConnectStore({ deliverEvent } as unknown as RecordStoreClient);

    await expect(store.deliverEvent(deliverableEventRecord())).rejects.toThrow(
      "invalid event delivery disposition 0",
    );
  });

  it.each([
    {
      name: "schema version",
      record: create(EventRecordSchema, {
        ...deliverableEventRecord(),
        schemaVersion: RecordSchemaVersion.UNSPECIFIED,
      }),
      message: "schema version",
    },
    {
      name: "key",
      record: create(EventRecordSchema, {
        ...deliverableEventRecord(),
        key: undefined,
      }),
      message: "key is required",
    },
    {
      name: "payload",
      record: create(EventRecordSchema, {
        ...deliverableEventRecord(),
        payload: undefined,
      }),
      message: "payload is required",
    },
    {
      name: "received_at",
      record: create(EventRecordSchema, {
        ...deliverableEventRecord(),
        receivedAt: undefined,
      }),
      message: "received_at is required",
    },
  ])("rejects an event record with an invalid $name", async ({ record, message }) => {
    const deliverEvent = vi.fn();
    const store = new ConnectStore({ deliverEvent } as unknown as RecordStoreClient);

    await expect(store.deliverEvent(record)).rejects.toThrow(message);
    expect(deliverEvent).not.toHaveBeenCalled();
  });

  it("lists claims within one workflow run", async () => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "refresh:prices",
      runId: "run-1",
    });
    const record = create(ClaimRecordSchema, {
      key: create(ClaimKeySchema, {
        namespace: key.namespace,
        workflowId: key.workflowId,
        runId: key.runId,
        claimId: "workflow:execution",
      }),
    });
    const listClaims = vi.fn(async () =>
      create(ListClaimsResponseSchema, { records: [record] }),
    );
    const store = new ConnectStore({ listClaims } as unknown as RecordStoreClient);

    await expect(store.listClaims(key)).resolves.toEqual([record]);
    expect(listClaims).toHaveBeenCalledWith(expect.objectContaining({ key }));
  });
});

function deliverableEventRecord() {
  return create(EventRecordSchema, {
    schemaVersion: RecordSchemaVersion.EVENT,
    key: create(EventKeySchema, {
      namespace: "default",
      workflowId: "refresh:prices",
      runId: "run-1",
      eventId: "approval",
    }),
    payload: create(AnySchema, {
      typeUrl: "type.googleapis.com/google.protobuf.StringValue",
      value: new Uint8Array([10, 8, 97, 112, 112, 114, 111, 118, 101, 100]),
    }),
    receivedAt: timestampFromDate(new Date("2026-07-22T00:00:00Z")),
  });
}

describe("ConnectQueryStore", () => {
  it("lists indexed workflows with pagination metadata", async () => {
    const key = create(WorkflowKeySchema, {
      namespace: "default",
      workflowId: "refresh:prices",
      runId: "run-1",
    });
    const record = create(WorkflowRecordSchema, {
      key,
      status: WorkflowStatus.IN_PROGRESS,
    });
    const listWorkflows = vi.fn(async () =>
      create(ListWorkflowsResponseSchema, {
        records: [record],
        nextPageToken: "page-2",
      }),
    );
    const query = new ConnectQueryStore({
      listWorkflows,
    } as unknown as RecordQueryClient);

    await expect(
      query.listWorkflows({
        namespace: "default",
        status: WorkflowStatus.IN_PROGRESS,
        pageSize: 25,
      }),
    ).resolves.toEqual({
      records: [record],
      nextPageToken: "page-2",
    });
    expect(listWorkflows).toHaveBeenCalledWith(
      expect.objectContaining({
        namespace: "default",
        status: WorkflowStatus.IN_PROGRESS,
        pageSize: 25,
      }),
    );
  });

  it("converts Date and millisecond duration inputs for query RPCs", async () => {
    const now = new Date("2026-07-07T12:00:00.000Z");
    const seconds = BigInt(Math.floor(now.getTime() / 1000));
    const sweep = vi.fn(async () =>
      create(SweepResponseSchema, { deleted: 3 }),
    );
    const dueTimers = vi.fn(async () =>
      create(RecordQueryServiceDueTimersResponseSchema, {
        due: [
          {
            key: create(TimerKeySchema, {
              namespace: "default",
              workflowId: "refresh:prices",
              runId: "run-1",
              timerId: "timer-1",
            }),
          },
        ],
      }),
    );
    const query = new ConnectQueryStore({
      sweep,
      dueTimers,
    } as unknown as RecordQueryClient);

    await expect(
      query.sweep({ namespace: "default", now, maxAgeMs: 60_000 }),
    ).resolves.toBe(3);
    await expect(query.dueTimers("default", now)).resolves.toHaveLength(1);
    expect(sweep).toHaveBeenCalledWith(
      expect.objectContaining({
        namespace: "default",
        now: expect.objectContaining({ seconds }),
        maxAge: expect.objectContaining({ seconds: 60n }),
      }),
    );
    expect(dueTimers).toHaveBeenCalledWith(
      expect.objectContaining({
        namespace: "default",
        now: expect.objectContaining({ seconds }),
      }),
    );
  });

  it("keeps generated record construction available to callers", () => {
    const activity = create(ActivityRecordSchema, {});

    expect(activity).toBeDefined();
  });
});
