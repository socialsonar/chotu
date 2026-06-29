import type { RedisClient } from "bun";
import {
    fromRedisHash,
    fromRedisRunHash,
    parseRedisFields,
} from "../../domain/execution.mapper";
import type { IStateStore } from "../../interfaces/state-store.interface";
import {
    StepExecutionStatus,
    WorkflowRunStatus,
    type StepExecutionRecord,
    type WorkflowRunRecord,
} from "../../interfaces/workflow.interface";
import {
    activeStepKey,
    joinBranchesKey,
    RECOVERY_LEADER_KEY,
    RECOVERY_LEADER_TTL_SEC,
    runKey,
    runLockKey,
    runStepsKey,
    STARTUP_RECONCILE_KEY,
    STARTUP_RECONCILE_TTL_SEC,
    stepKey,
    SYNC_STREAM,
} from "./keys";
import {
    ACQUIRE_LEADER_LOCK_SCRIPT,
    ACQUIRE_RUN_LOCK_SCRIPT,
    CLAIM_STEP_SCRIPT,
    COMPLETE_RUN_SCRIPT,
    CREATE_STEP_SCRIPT,
    DECR_JOIN_SCRIPT,
    FAIL_RUN_SCRIPT,
    INCREMENT_ATTEMPTS_SCRIPT,
    RELEASE_RUN_LOCK_SCRIPT,
    RENEW_LEASE_SCRIPT,
    RESET_EXPIRED_LEASE_SCRIPT,
    ROLLBACK_STEP_SCRIPT,
    SET_STEP_STATUS_SCRIPT,
} from "./scripts";

const EMPTY = "";

function encodeJson(value: unknown): string {
    if (value == null) return EMPTY;
    return JSON.stringify(value);
}

function decodeJson<T>(value: string | null | undefined): T | null {
    if (value == null || value === EMPTY) return null;
    try {
        return JSON.parse(value) as T;
    } catch {
        return null;
    }
}

async function readHash(
    redis: RedisClient,
    key: string,
): Promise<Record<string, string>> {
    const fields = (await redis.send("HGETALL", [key])) as string[] | Record<string, string>;
    if (!fields || (Array.isArray(fields) && fields.length === 0)) return {};
    if (!Array.isArray(fields) && Object.keys(fields).length === 0) return {};
    return parseRedisFields(fields);
}

export class RedisStateStore implements IStateStore {
    constructor(private readonly redis: RedisClient) {}

    private nowIso(): string {
        return new Date().toISOString();
    }

    async existsStep(stepExecId: string): Promise<boolean> {
        const n = (await this.redis.send("EXISTS", [stepKey(stepExecId)])) as number;
        return n === 1;
    }

    async existsRun(workflowRunId: string): Promise<boolean> {
        const n = (await this.redis.send("EXISTS", [runKey(workflowRunId)])) as number;
        return n === 1;
    }

    async loadStep(stepExecId: string): Promise<StepExecutionRecord | null> {
        const hash = await readHash(this.redis, stepKey(stepExecId));
        if (!hash.id) return null;
        return fromRedisHash(hash);
    }

    async loadRun(workflowRunId: string): Promise<WorkflowRunRecord | null> {
        const hash = await readHash(this.redis, runKey(workflowRunId));
        return fromRedisRunHash(hash, workflowRunId);
    }

    async getActiveCount(workflowRunId: string): Promise<number> {
        const count = (await this.redis.send("HGET", [
            runKey(workflowRunId),
            "active_count",
        ])) as string | null;
        return Number(count ?? 0);
    }

    async claimStep(
        stepExecId: string,
        leaseOwner: string,
        leaseTtlMs: number,
    ): Promise<StepExecutionRecord | null> {
        const now = this.nowIso();
        const leaseUntil = String(Date.now() + leaseTtlMs);
        const result = (await this.redis.send("EVAL", [
            CLAIM_STEP_SCRIPT,
            "2",
            stepKey(stepExecId),
            SYNC_STREAM,
            now,
            leaseOwner,
            leaseUntil,
            stepExecId,
        ])) as string[] | null;

        if (!result?.length) return null;
        return fromRedisHash(parseRedisFields(result));
    }

    async renewLease(stepExecId: string, leaseOwner: string, leaseTtlMs: number): Promise<boolean> {
        const result = (await this.redis.send("EVAL", [
            RENEW_LEASE_SCRIPT,
            "1",
            stepKey(stepExecId),
            leaseOwner,
            String(Date.now() + leaseTtlMs),
            this.nowIso(),
        ])) as number;
        return result === 1;
    }

    async resetExpiredLease(stepExecId: string): Promise<boolean> {
        const result = (await this.redis.send("EVAL", [
            RESET_EXPIRED_LEASE_SCRIPT,
            "2",
            stepKey(stepExecId),
            SYNC_STREAM,
            String(Date.now()),
            this.nowIso(),
            stepExecId,
        ])) as number;
        return result === 1;
    }

    async setStepStatus(stepExecId: string, status: StepExecutionStatus): Promise<boolean> {
        const row = await this.loadStep(stepExecId);
        if (!row) return false;

        const result = (await this.redis.send("EVAL", [
            SET_STEP_STATUS_SCRIPT,
            "4",
            stepKey(stepExecId),
            runKey(row.workflow_run_id),
            activeStepKey(row.workflow_run_id, row.step_name),
            SYNC_STREAM,
            status,
            this.nowIso(),
            stepExecId,
        ])) as number;

        return result === 1;
    }

    async incrementAttempts(stepExecId: string): Promise<number> {
        const result = (await this.redis.send("EVAL", [
            INCREMENT_ATTEMPTS_SCRIPT,
            "2",
            stepKey(stepExecId),
            SYNC_STREAM,
            this.nowIso(),
            stepExecId,
        ])) as number | null;

        return result ?? 0;
    }

    async decrementJoinRemaining(joinStepId: string): Promise<number | null> {
        const result = (await this.redis.send("EVAL", [
            DECR_JOIN_SCRIPT,
            "1",
            stepKey(joinStepId),
            this.nowIso(),
        ])) as number | null;

        return result;
    }

    async rollbackStep(stepExecId: string, workflowRunId: string, stepName: string): Promise<void> {
        await this.redis.send("EVAL", [
            ROLLBACK_STEP_SCRIPT,
            "3",
            stepKey(stepExecId),
            runKey(workflowRunId),
            activeStepKey(workflowRunId, stepName),
            stepExecId,
        ]);
        await this.redis.send("SREM", [runStepsKey(workflowRunId), stepExecId]);
    }

    async rollbackRun(workflowRunId: string): Promise<void> {
        await this.redis.send("DEL", [runKey(workflowRunId)]);
        const prefix = `chotu:run:${workflowRunId}:`;
        let cursor = "0";
        do {
            const [nextCursor, keys] = (await this.redis.send("SCAN", [
                cursor,
                "MATCH",
                `${prefix}*`,
                "COUNT",
                "100",
            ])) as [string, string[]];
            cursor = nextCursor;
            if (keys.length) await this.redis.send("DEL", keys);
        } while (cursor !== "0");
    }

    async createRun(params: {
        id: string;
        workflowName: string;
        input: Record<string, any>;
    }): Promise<void> {
        const now = this.nowIso();
        await this.redis.send("HSET", [
            runKey(params.id),
            "id",
            params.id,
            "workflow_name",
            params.workflowName,
            "status",
            WorkflowRunStatus.RUNNING,
            "input",
            encodeJson(params.input),
            "output",
            EMPTY,
            "active_count",
            "0",
            "version",
            "0",
            "created_at",
            now,
            "updated_at",
            now,
        ]);
    }

    async createStep(params: {
        id: string;
        workflowRunId: string;
        stepName: string;
        queue: string;
        status?: StepExecutionStatus;
        input: Record<string, any> | null;
        joinStepId?: string | null;
        fanOutIndex?: number | null;
        joinTotal?: number | null;
        joinRemaining?: number | null;
    }): Promise<boolean> {
        const status = params.status ?? StepExecutionStatus.PENDING;
        const now = this.nowIso();
        const joinStepId = params.joinStepId ?? null;

        const result = (await this.redis.send("EVAL", [
            CREATE_STEP_SCRIPT,
            "4",
            stepKey(params.id),
            runKey(params.workflowRunId),
            activeStepKey(params.workflowRunId, params.stepName),
            joinStepId ? joinBranchesKey(joinStepId) : stepKey("_noop_branches"),
            params.id,
            params.workflowRunId,
            params.stepName,
            params.queue,
            status,
            encodeJson(params.input),
            joinStepId ?? "null",
            params.fanOutIndex != null ? String(params.fanOutIndex) : "null",
            params.joinTotal != null ? String(params.joinTotal) : "null",
            params.joinRemaining != null ? String(params.joinRemaining) : "null",
            "0",
            now,
            now,
        ])) as number;

        if (result === 1) {
            await this.redis.send("SADD", [runStepsKey(params.workflowRunId), params.id]);
        }

        return result === 1;
    }

    async completeStep(stepExecId: string, output: Record<string, any>): Promise<StepExecutionRecord | null> {
        const row = await this.loadStep(stepExecId);
        if (!row) return null;

        const now = this.nowIso();
        await this.applyTerminalTransition(row, StepExecutionStatus.COMPLETED, now, {
            output: encodeJson(output),
            error: EMPTY,
            finished_at: now,
        });

        return this.loadStep(stepExecId);
    }

    async failStep(stepExecId: string, error: Record<string, any>): Promise<StepExecutionRecord | null> {
        const row = await this.loadStep(stepExecId);
        if (!row) return null;

        const now = this.nowIso();
        await this.applyTerminalTransition(row, StepExecutionStatus.FAILED, now, {
            output: EMPTY,
            error: encodeJson(error),
            finished_at: now,
        });

        return this.loadStep(stepExecId);
    }

    private async applyTerminalTransition(
        row: StepExecutionRecord,
        status: StepExecutionStatus,
        now: string,
        extra: Record<string, string>,
    ): Promise<void> {
        if (
            row.status === StepExecutionStatus.COMPLETED ||
            row.status === StepExecutionStatus.FAILED
        ) {
            return;
        }

        const activeStatuses = new Set([
            StepExecutionStatus.PENDING,
            StepExecutionStatus.RUNNING,
            StepExecutionStatus.WAITING,
        ]);

        if (activeStatuses.has(row.status)) {
            await this.redis.send("SREM", [activeStepKey(row.workflow_run_id, row.step_name), row.id]);
            await this.redis.send("HINCRBY", [runKey(row.workflow_run_id), "active_count", "-1"]);
        }

        const version = String(row.version + 1);
        await this.redis.send("HSET", [
            stepKey(row.id),
            "status",
            status,
            "updated_at",
            now,
            "version",
            version,
            "queued",
            "0",
            "lease_owner",
            "",
            "lease_until",
            "0",
            ...Object.entries(extra).flat(),
        ]);
    }

    async finalizeJoinStep(
        joinStepId: string,
        input: Record<string, any>[],
    ): Promise<StepExecutionRecord | null> {
        const row = await this.loadStep(joinStepId);
        if (!row) return null;

        const now = this.nowIso();
        const wasWaiting = row.status === StepExecutionStatus.WAITING;

        if (wasWaiting) {
            await this.redis.send("SREM", [activeStepKey(row.workflow_run_id, row.step_name), joinStepId]);
        }

        const version = String(row.version + 1);
        await this.redis.send("HSET", [
            stepKey(joinStepId),
            "status",
            StepExecutionStatus.PENDING,
            "input",
            encodeJson(input),
            "join_remaining",
            "null",
            "join_total",
            "null",
            "queued",
            "0",
            "updated_at",
            now,
            "version",
            version,
        ]);

        if (wasWaiting) {
            await this.redis.send("SADD", [activeStepKey(row.workflow_run_id, row.step_name), joinStepId]);
        } else {
            await this.redis.send("HINCRBY", [runKey(row.workflow_run_id), "active_count", "1"]);
            await this.redis.send("SADD", [activeStepKey(row.workflow_run_id, row.step_name), joinStepId]);
        }

        return this.loadStep(joinStepId);
    }

    async setJoinRemaining(joinStepId: string, remaining: number): Promise<void> {
        await this.redis.send("HSET", [
            stepKey(joinStepId),
            "join_remaining",
            String(remaining),
            "updated_at",
            this.nowIso(),
        ]);
    }

    async rebuildJoinRemainingFromBranches(joinStepId: string): Promise<number | null> {
        const joinRow = await this.loadStep(joinStepId);
        if (!joinRow || joinRow.status !== StepExecutionStatus.WAITING) return null;
        if (joinRow.join_remaining != null) return joinRow.join_remaining;

        const branches = await this.getJoinBranches(joinStepId);
        let remaining = 0;
        for (const branch of branches) {
            if (
                branch.status === StepExecutionStatus.PENDING ||
                branch.status === StepExecutionStatus.RUNNING
            ) {
                remaining++;
            }
        }
        await this.setJoinRemaining(joinStepId, remaining);
        return remaining;
    }

    async getJoinBranches(joinStepId: string): Promise<StepExecutionRecord[]> {
        const branchIds = (await this.redis.send("LRANGE", [
            joinBranchesKey(joinStepId),
            "0",
            "-1",
        ])) as string[];

        const rows: StepExecutionRecord[] = [];
        for (const id of branchIds ?? []) {
            const row = await this.loadStep(id);
            if (row) rows.push(row);
        }
        return rows.sort((a, b) => (a.fan_out_index ?? 0) - (b.fan_out_index ?? 0));
    }

    async getRunStatus(workflowRunId: string): Promise<WorkflowRunStatus | null> {
        const status = (await this.redis.send("HGET", [
            runKey(workflowRunId),
            "status",
        ])) as string | null;
        return (status as WorkflowRunStatus) ?? null;
    }

    private async getStepsForRun(workflowRunId: string): Promise<StepExecutionRecord[]> {
        const ids = (await this.redis.send("SMEMBERS", [runStepsKey(workflowRunId)])) as string[];
        const rows: StepExecutionRecord[] = [];
        for (const id of ids ?? []) {
            const row = await this.loadStep(id);
            if (row) rows.push(row);
        }
        return rows;
    }

    async recomputeRunActiveCount(workflowRunId: string): Promise<number> {
        const steps = await this.getStepsForRun(workflowRunId);
        const activeStatuses = new Set([
            StepExecutionStatus.PENDING,
            StepExecutionStatus.RUNNING,
            StepExecutionStatus.WAITING,
        ]);
        let count = 0;
        for (const step of steps) {
            if (activeStatuses.has(step.status)) count++;
        }
        await this.redis.send("HSET", [runKey(workflowRunId), "active_count", String(count)]);
        return count;
    }

    async findStepByName(
        workflowRunId: string,
        stepName: string,
    ): Promise<StepExecutionRecord | null> {
        const steps = await this.getStepsForRun(workflowRunId);
        let latest: StepExecutionRecord | null = null;
        for (const step of steps) {
            if (step.step_name !== stepName) continue;
            if (!latest || step.updated_at > latest.updated_at) {
                latest = step;
            }
        }
        return latest;
    }

    async countUnabsorbedFailures(workflowRunId: string): Promise<number> {
        const steps = await this.getStepsForRun(workflowRunId);
        let count = 0;

        for (const step of steps) {
            if (step.status !== StepExecutionStatus.FAILED) continue;
            if (step.join_step_id) {
                const join = await this.loadStep(step.join_step_id);
                if (join?.status === StepExecutionStatus.COMPLETED) continue;
            }
            count++;
        }

        return count;
    }

    async tryCompleteRun(
        workflowRunId: string,
        output: Record<string, any> | null,
    ): Promise<number | null> {
        const result = (await this.redis.send("EVAL", [
            COMPLETE_RUN_SCRIPT,
            "1",
            runKey(workflowRunId),
            this.nowIso(),
            encodeJson(output),
        ])) as number;
        return result > 0 ? result : null;
    }

    async tryFailRun(workflowRunId: string, reason?: string): Promise<number | null> {
        const output = encodeJson(reason ? { reason } : null);
        const result = (await this.redis.send("EVAL", [
            FAIL_RUN_SCRIPT,
            "1",
            runKey(workflowRunId),
            this.nowIso(),
            output,
        ])) as number;
        return result > 0 ? result : null;
    }

    async acquireRunLock(workflowRunId: string, token: string, ttlSec = 30): Promise<boolean> {
        const result = (await this.redis.send("EVAL", [
            ACQUIRE_RUN_LOCK_SCRIPT,
            "1",
            runLockKey(workflowRunId),
            token,
            String(ttlSec),
        ])) as number;
        return result === 1;
    }

    async releaseRunLock(workflowRunId: string, token: string): Promise<void> {
        await this.redis.send("EVAL", [
            RELEASE_RUN_LOCK_SCRIPT,
            "1",
            runLockKey(workflowRunId),
            token,
        ]);
    }

    async tryAcquireRecoveryLeader(instanceId: string): Promise<boolean> {
        const result = (await this.redis.send("EVAL", [
            ACQUIRE_LEADER_LOCK_SCRIPT,
            "1",
            RECOVERY_LEADER_KEY,
            instanceId,
            String(RECOVERY_LEADER_TTL_SEC),
        ])) as number;
        return result === 1;
    }

    async tryAcquireStartupReconcile(instanceId: string): Promise<boolean> {
        const result = (await this.redis.send("EVAL", [
            ACQUIRE_LEADER_LOCK_SCRIPT,
            "1",
            STARTUP_RECONCILE_KEY,
            instanceId,
            String(STARTUP_RECONCILE_TTL_SEC),
        ])) as number;
        return result === 1;
    }

    async hydrateRunIfMissing(row: Record<string, unknown>): Promise<boolean> {
        const id = row.id as string;
        if (await this.existsRun(id)) return false;
        await this.hydrateRun(row);
        return true;
    }

    async hydrateStepIfMissing(row: Record<string, unknown>): Promise<boolean> {
        const id = row.id as string;
        if (await this.existsStep(id)) return false;
        await this.hydrateStep(row);
        return true;
    }

    private async hydrateRun(row: Record<string, unknown>): Promise<void> {
        const id = row.id as string;
        const now = (row.updated_at as Date)?.toISOString?.() ?? this.nowIso();
        await this.redis.send("HSET", [
            runKey(id),
            "id",
            id,
            "workflow_name",
            row.workflow_name as string,
            "status",
            row.status as string,
            "input",
            encodeJson(row.input),
            "output",
            encodeJson(row.output),
            "active_count",
            "0",
            "version",
            String((row.version as number) ?? 0),
            "created_at",
            (row.created_at as Date)?.toISOString?.() ?? now,
            "updated_at",
            now,
        ]);
    }

    private async hydrateStep(row: Record<string, unknown>): Promise<void> {
        const id = row.id as string;
        const workflowRunId = row.workflow_run_id as string;
        const stepName = row.step_name as string;
        const status = row.status as StepExecutionStatus;
        const now = (row.updated_at as Date)?.toISOString?.() ?? this.nowIso();

        await this.redis.send("HSET", [
            stepKey(id),
            "id",
            id,
            "workflow_run_id",
            workflowRunId,
            "step_name",
            stepName,
            "queue",
            (row.queue as string) ?? "default",
            "status",
            status,
            "input",
            encodeJson(row.input),
            "output",
            encodeJson(row.output),
            "error",
            encodeJson(row.error),
            "join_step_id",
            row.join_step_id ? String(row.join_step_id) : "null",
            "fan_out_index",
            row.fan_out_index != null ? String(row.fan_out_index) : "null",
            "join_total",
            row.join_total != null ? String(row.join_total) : "null",
            "join_remaining",
            row.join_remaining != null ? String(row.join_remaining) : "null",
            "attempts",
            String((row.attempts as number) ?? 0),
            "queued",
            "0",
            "lease_owner",
            "",
            "lease_until",
            "0",
            "version",
            String((row.version as number) ?? 0),
            "created_at",
            (row.created_at as Date)?.toISOString?.() ?? now,
            "updated_at",
            now,
        ]);

        const activeStatuses = new Set([
            StepExecutionStatus.PENDING,
            StepExecutionStatus.RUNNING,
            StepExecutionStatus.WAITING,
        ]);

        if (activeStatuses.has(status)) {
            await this.redis.send("SADD", [activeStepKey(workflowRunId, stepName), id]);
        }

        const joinStepId = row.join_step_id as string | null;
        if (joinStepId) {
            await this.redis.send("RPUSH", [joinBranchesKey(joinStepId), id]);
        }

        await this.redis.send("SADD", [runStepsKey(workflowRunId), id]);
    }

    async scanStepIds(pattern: string): Promise<string[]> {
        const ids: string[] = [];
        let cursor = "0";
        do {
            const [nextCursor, keys] = (await this.redis.send("SCAN", [
                cursor,
                "MATCH",
                pattern,
                "COUNT",
                "100",
            ])) as [string, string[]];
            cursor = nextCursor;
            for (const key of keys) {
                const prefix = "chotu:step:";
                if (key.startsWith(prefix)) {
                    const id = key.slice(prefix.length);
                    if (id.startsWith("_")) continue;
                    ids.push(id);
                }
            }
        } while (cursor !== "0");
        return ids;
    }
}
