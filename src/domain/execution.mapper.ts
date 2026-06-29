import {
    StepExecutionStatus,
    type StepExecution,
    type StepExecutionRecord,
    type WorkflowRunRecord,
    WorkflowRunStatus,
} from "../interfaces/workflow.interface";

const EMPTY = "";

export function parseRedisFields(fields: string[] | Record<string, string>): Record<string, string> {
    if (!Array.isArray(fields)) {
        return fields ?? {};
    }
    const out: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
        out[fields[i]!] = fields[i + 1] ?? EMPTY;
    }
    return out;
}

function decodeJson<T>(value: string | null | undefined): T | null {
    if (value == null || value === EMPTY) return null;
    try {
        return JSON.parse(value) as T;
    } catch {
        return null;
    }
}

function parseOptionalInt(value: string | null | undefined): number | null {
    if (value == null || value === EMPTY || value === "null") return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

export function fromRedisHash(hash: Record<string, string>): StepExecutionRecord | null {
    if (!hash.id) return null;
    const leaseOwner = hash.lease_owner;
    return {
        id: hash.id,
        workflow_run_id: hash.workflow_run_id ?? "",
        step_name: hash.step_name ?? "",
        queue: hash.queue ?? "default",
        status: (hash.status as StepExecutionStatus) ?? StepExecutionStatus.PENDING,
        input: decodeJson<Record<string, any>>(hash.input),
        output: decodeJson<Record<string, any>>(hash.output),
        error: decodeJson<Record<string, any>>(hash.error),
        join_step_id: hash.join_step_id && hash.join_step_id !== "null" ? hash.join_step_id : null,
        fan_out_index: parseOptionalInt(hash.fan_out_index),
        join_total: parseOptionalInt(hash.join_total),
        join_remaining: parseOptionalInt(hash.join_remaining),
        attempts: Number(hash.attempts ?? 0),
        version: Number(hash.version ?? 0),
        updated_at: hash.updated_at ?? new Date(0).toISOString(),
        queued: hash.queued === "1",
        lease_owner: leaseOwner && leaseOwner !== "" ? leaseOwner : null,
        lease_until: Number(hash.lease_until ?? 0),
    };
}

export function fromRedisRunHash(hash: Record<string, string>, workflowRunId: string): WorkflowRunRecord | null {
    if (!hash.id) return null;
    return {
        id: hash.id ?? workflowRunId,
        workflow_name: hash.workflow_name ?? "",
        status: (hash.status as WorkflowRunStatus) ?? WorkflowRunStatus.RUNNING,
        input: decodeJson<Record<string, any>>(hash.input) ?? {},
        output: decodeJson<Record<string, any>>(hash.output),
        active_count: Number(hash.active_count ?? 0),
        version: Number(hash.version ?? 0),
    };
}

export function fromPgRow(row: Record<string, unknown>): StepExecution {
    return {
        id: row.id as string,
        workflowRunId: row.workflow_run_id as string,
        stepName: row.step_name as string,
        queue: (row.queue as string) ?? "default",
        status: row.status as StepExecutionStatus,
        input: row.input as Record<string, any> | null,
        output: row.output as Record<string, any> | null,
        error: row.error as Record<string, any> | null,
        joinStepId: row.join_step_id as string | null,
        fanOutIndex: row.fan_out_index as number | null,
        attempts: (row.attempts as number) ?? 0,
        createdAt: row.created_at as Date,
        updatedAt: row.updated_at as Date,
        finishedAt: row.finished_at as Date | null,
    };
}

export function pgRowToExecutionRecord(row: Record<string, unknown>): StepExecutionRecord {
    return {
        id: row.id as string,
        workflow_run_id: row.workflow_run_id as string,
        step_name: row.step_name as string,
        queue: (row.queue as string) ?? "default",
        status: row.status as StepExecutionStatus,
        input: row.input as Record<string, any> | null,
        output: row.output as Record<string, any> | null,
        error: row.error as Record<string, any> | null,
        join_step_id: row.join_step_id as string | null,
        fan_out_index: row.fan_out_index as number | null,
        join_total: row.join_total as number | null,
        join_remaining: row.join_remaining as number | null,
        attempts: (row.attempts as number) ?? 0,
        version: (row.version as number) ?? 0,
        updated_at:
            row.updated_at instanceof Date
                ? row.updated_at.toISOString()
                : String(row.updated_at ?? new Date(0).toISOString()),
        queued: false,
        lease_owner: null,
        lease_until: 0,
    };
}

export function toStepExecution(record: StepExecutionRecord): StepExecution {
    return {
        id: record.id,
        workflowRunId: record.workflow_run_id,
        stepName: record.step_name,
        queue: record.queue,
        status: record.status,
        input: record.input,
        output: record.output,
        error: record.error,
        joinStepId: record.join_step_id,
        fanOutIndex: record.fan_out_index,
        attempts: record.attempts,
        createdAt: new Date(record.updated_at),
        updatedAt: new Date(record.updated_at),
        finishedAt:
            record.status === StepExecutionStatus.COMPLETED ||
            record.status === StepExecutionStatus.FAILED ||
            record.status === StepExecutionStatus.CANCELLED
                ? new Date(record.updated_at)
                : null,
    };
}
