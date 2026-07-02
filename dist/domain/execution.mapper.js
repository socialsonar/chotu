import { StepExecutionStatus, WorkflowRunStatus, } from "../interfaces/workflow.interface";
const EMPTY = "";
export function parseRedisFields(fields) {
    if (!Array.isArray(fields)) {
        return fields ?? {};
    }
    const out = {};
    for (let i = 0; i < fields.length; i += 2) {
        out[fields[i]] = fields[i + 1] ?? EMPTY;
    }
    return out;
}
function decodeJson(value) {
    if (value == null || value === EMPTY)
        return null;
    try {
        return JSON.parse(value);
    }
    catch {
        return null;
    }
}
function parseOptionalInt(value) {
    if (value == null || value === EMPTY || value === "null")
        return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}
export function fromRedisHash(hash) {
    if (!hash.id)
        return null;
    const leaseOwner = hash.lease_owner;
    return {
        id: hash.id,
        workflow_run_id: hash.workflow_run_id ?? "",
        step_name: hash.step_name ?? "",
        queue: hash.queue ?? "default",
        status: hash.status ?? StepExecutionStatus.PENDING,
        input: decodeJson(hash.input),
        output: decodeJson(hash.output),
        error: decodeJson(hash.error),
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
export function fromRedisRunHash(hash, workflowRunId) {
    if (!hash.id)
        return null;
    return {
        id: hash.id ?? workflowRunId,
        workflow_name: hash.workflow_name ?? "",
        status: hash.status ?? WorkflowRunStatus.RUNNING,
        input: decodeJson(hash.input) ?? {},
        output: decodeJson(hash.output),
        active_count: Number(hash.active_count ?? 0),
        version: Number(hash.version ?? 0),
    };
}
export function fromPgRow(row) {
    return {
        id: row.id,
        workflowRunId: row.workflow_run_id,
        stepName: row.step_name,
        queue: row.queue ?? "default",
        status: row.status,
        input: row.input,
        output: row.output,
        error: row.error,
        joinStepId: row.join_step_id,
        fanOutIndex: row.fan_out_index,
        attempts: row.attempts ?? 0,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        finishedAt: row.finished_at,
    };
}
export function pgRowToExecutionRecord(row) {
    return {
        id: row.id,
        workflow_run_id: row.workflow_run_id,
        step_name: row.step_name,
        queue: row.queue ?? "default",
        status: row.status,
        input: row.input,
        output: row.output,
        error: row.error,
        join_step_id: row.join_step_id,
        fan_out_index: row.fan_out_index,
        join_total: row.join_total,
        join_remaining: row.join_remaining,
        attempts: row.attempts ?? 0,
        version: row.version ?? 0,
        updated_at: row.updated_at instanceof Date
            ? row.updated_at.toISOString()
            : String(row.updated_at ?? new Date(0).toISOString()),
        queued: false,
        lease_owner: null,
        lease_until: 0,
    };
}
export function toStepExecution(record) {
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
        finishedAt: record.status === StepExecutionStatus.COMPLETED ||
            record.status === StepExecutionStatus.FAILED ||
            record.status === StepExecutionStatus.CANCELLED
            ? new Date(record.updated_at)
            : null,
    };
}
//# sourceMappingURL=execution.mapper.js.map