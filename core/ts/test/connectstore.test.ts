import { create } from "@bufbuild/protobuf";
import { describe, expect, it, vi } from "vitest";
import { ConnectQueryStore, ConnectStore } from "../src/connectstore.js";
import {
  ActivityRecordSchema,
  ClaimCapability,
  GetStoreCapabilitiesResponseSchema,
  GetWorkflowResponseSchema,
  ListWorkflowsResponseSchema,
  RecordQueryServiceDueTimersResponseSchema,
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
});

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
