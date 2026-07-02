import { fromRedisHash, fromRedisRunHash, parseRedisFields, } from "../../domain/execution.mapper";
import { StepExecutionStatus, WorkflowRunStatus, } from "../../interfaces/workflow.interface";
import { activeStepKey, joinBranchesKey, RECOVERY_LEADER_KEY, RECOVERY_LEADER_TTL_SEC, runKey, runLockKey, runStepsKey, STARTUP_RECONCILE_KEY, STARTUP_RECONCILE_TTL_SEC, stepKey, SYNC_STREAM, } from "./keys";
import { ACQUIRE_LEADER_LOCK_SCRIPT, ACQUIRE_RUN_LOCK_SCRIPT, CLAIM_STEP_SCRIPT, COMPLETE_RUN_SCRIPT, CANCEL_RUN_SCRIPT, CREATE_STEP_SCRIPT, DECR_JOIN_SCRIPT, FAIL_RUN_SCRIPT, INCREMENT_ATTEMPTS_SCRIPT, RELEASE_RUN_LOCK_SCRIPT, RENEW_LEASE_SCRIPT, RESET_EXPIRED_LEASE_SCRIPT, ROLLBACK_STEP_SCRIPT, SET_STEP_STATUS_SCRIPT, } from "./scripts";
const EMPTY = "";
function encodeJson(value) {
    if (value == null)
        return EMPTY;
    return JSON.stringify(value);
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
async function readHash(redis, key) {
    const fields = (await redis.send("HGETALL", [key]));
    if (!fields || (Array.isArray(fields) && fields.length === 0))
        return {};
    if (!Array.isArray(fields) && Object.keys(fields).length === 0)
        return {};
    return parseRedisFields(fields);
}
export class RedisStateStore {
    redis;
    constructor(redis) {
        this.redis = redis;
    }
    nowIso() {
        return new Date().toISOString();
    }
    async existsStep(stepExecId) {
        const n = (await this.redis.send("EXISTS", [stepKey(stepExecId)]));
        return n === 1;
    }
    async existsRun(workflowRunId) {
        const n = (await this.redis.send("EXISTS", [runKey(workflowRunId)]));
        return n === 1;
    }
    async loadStep(stepExecId) {
        const hash = await readHash(this.redis, stepKey(stepExecId));
        if (!hash.id)
            return null;
        return fromRedisHash(hash);
    }
    async loadRun(workflowRunId) {
        const hash = await readHash(this.redis, runKey(workflowRunId));
        return fromRedisRunHash(hash, workflowRunId);
    }
    async getActiveCount(workflowRunId) {
        const count = (await this.redis.send("HGET", [
            runKey(workflowRunId),
            "active_count",
        ]));
        return Number(count ?? 0);
    }
    async claimStep(stepExecId, leaseOwner, leaseTtlMs) {
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
        ]));
        if (!result?.length)
            return null;
        return fromRedisHash(parseRedisFields(result));
    }
    async renewLease(stepExecId, leaseOwner, leaseTtlMs) {
        const result = (await this.redis.send("EVAL", [
            RENEW_LEASE_SCRIPT,
            "1",
            stepKey(stepExecId),
            leaseOwner,
            String(Date.now() + leaseTtlMs),
            this.nowIso(),
        ]));
        return result === 1;
    }
    async resetExpiredLease(stepExecId) {
        const result = (await this.redis.send("EVAL", [
            RESET_EXPIRED_LEASE_SCRIPT,
            "2",
            stepKey(stepExecId),
            SYNC_STREAM,
            String(Date.now()),
            this.nowIso(),
            stepExecId,
        ]));
        return result === 1;
    }
    async setStepStatus(stepExecId, status) {
        const row = await this.loadStep(stepExecId);
        if (!row)
            return false;
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
        ]));
        return result === 1;
    }
    async incrementAttempts(stepExecId) {
        const result = (await this.redis.send("EVAL", [
            INCREMENT_ATTEMPTS_SCRIPT,
            "2",
            stepKey(stepExecId),
            SYNC_STREAM,
            this.nowIso(),
            stepExecId,
        ]));
        return result ?? 0;
    }
    async decrementJoinRemaining(joinStepId) {
        const result = (await this.redis.send("EVAL", [
            DECR_JOIN_SCRIPT,
            "1",
            stepKey(joinStepId),
            this.nowIso(),
        ]));
        return result;
    }
    async rollbackStep(stepExecId, workflowRunId, stepName) {
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
    async rollbackRun(workflowRunId) {
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
            ]));
            cursor = nextCursor;
            if (keys.length)
                await this.redis.send("DEL", keys);
        } while (cursor !== "0");
    }
    async createRun(params) {
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
    async createStep(params) {
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
        ]));
        if (result === 1) {
            await this.redis.send("SADD", [runStepsKey(params.workflowRunId), params.id]);
        }
        return result === 1;
    }
    async completeStep(stepExecId, output) {
        const row = await this.loadStep(stepExecId);
        if (!row)
            return null;
        const now = this.nowIso();
        await this.applyTerminalTransition(row, StepExecutionStatus.COMPLETED, now, {
            output: encodeJson(output),
            error: EMPTY,
            finished_at: now,
        });
        return this.loadStep(stepExecId);
    }
    async failStep(stepExecId, error) {
        const row = await this.loadStep(stepExecId);
        if (!row)
            return null;
        const now = this.nowIso();
        await this.applyTerminalTransition(row, StepExecutionStatus.FAILED, now, {
            output: EMPTY,
            error: encodeJson(error),
            finished_at: now,
        });
        return this.loadStep(stepExecId);
    }
    async cancelStep(stepExecId, reason) {
        const row = await this.loadStep(stepExecId);
        if (!row)
            return null;
        const now = this.nowIso();
        await this.applyTerminalTransition(row, StepExecutionStatus.CANCELLED, now, {
            output: EMPTY,
            error: encodeJson(reason ? { reason } : null),
            finished_at: now,
        });
        return this.loadStep(stepExecId);
    }
    async applyTerminalTransition(row, status, now, extra) {
        if (row.status === StepExecutionStatus.COMPLETED ||
            row.status === StepExecutionStatus.FAILED ||
            row.status === StepExecutionStatus.CANCELLED) {
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
    async finalizeJoinStep(joinStepId, input) {
        const row = await this.loadStep(joinStepId);
        if (!row)
            return null;
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
        }
        else {
            await this.redis.send("HINCRBY", [runKey(row.workflow_run_id), "active_count", "1"]);
            await this.redis.send("SADD", [activeStepKey(row.workflow_run_id, row.step_name), joinStepId]);
        }
        return this.loadStep(joinStepId);
    }
    async setJoinRemaining(joinStepId, remaining) {
        await this.redis.send("HSET", [
            stepKey(joinStepId),
            "join_remaining",
            String(remaining),
            "updated_at",
            this.nowIso(),
        ]);
    }
    async rebuildJoinRemainingFromBranches(joinStepId) {
        const joinRow = await this.loadStep(joinStepId);
        if (!joinRow || joinRow.status !== StepExecutionStatus.WAITING)
            return null;
        if (joinRow.join_remaining != null)
            return joinRow.join_remaining;
        const branches = await this.getJoinBranches(joinStepId);
        let remaining = 0;
        for (const branch of branches) {
            if (branch.status === StepExecutionStatus.PENDING ||
                branch.status === StepExecutionStatus.RUNNING) {
                remaining++;
            }
        }
        await this.setJoinRemaining(joinStepId, remaining);
        return remaining;
    }
    async getJoinBranches(joinStepId) {
        const branchIds = (await this.redis.send("LRANGE", [
            joinBranchesKey(joinStepId),
            "0",
            "-1",
        ]));
        const rows = [];
        for (const id of branchIds ?? []) {
            const row = await this.loadStep(id);
            if (row)
                rows.push(row);
        }
        return rows.sort((a, b) => (a.fan_out_index ?? 0) - (b.fan_out_index ?? 0));
    }
    async getRunStatus(workflowRunId) {
        const status = (await this.redis.send("HGET", [
            runKey(workflowRunId),
            "status",
        ]));
        return status ?? null;
    }
    async getStepsForRun(workflowRunId) {
        const ids = (await this.redis.send("SMEMBERS", [runStepsKey(workflowRunId)]));
        const rows = [];
        for (const id of ids ?? []) {
            const row = await this.loadStep(id);
            if (row)
                rows.push(row);
        }
        return rows;
    }
    async listStepsForRun(workflowRunId) {
        return this.getStepsForRun(workflowRunId);
    }
    async markAbortRequested(workflowRunId) {
        await this.redis.send("HSET", [runKey(workflowRunId), "abort_requested", "1"]);
    }
    async isAbortRequested(workflowRunId) {
        const flag = (await this.redis.send("HGET", [
            runKey(workflowRunId),
            "abort_requested",
        ]));
        return flag === "1";
    }
    async recomputeRunActiveCount(workflowRunId) {
        const steps = await this.getStepsForRun(workflowRunId);
        const activeStatuses = new Set([
            StepExecutionStatus.PENDING,
            StepExecutionStatus.RUNNING,
            StepExecutionStatus.WAITING,
        ]);
        let count = 0;
        for (const step of steps) {
            if (activeStatuses.has(step.status))
                count++;
        }
        await this.redis.send("HSET", [runKey(workflowRunId), "active_count", String(count)]);
        return count;
    }
    async findStepByName(workflowRunId, stepName) {
        const steps = await this.getStepsForRun(workflowRunId);
        let latest = null;
        for (const step of steps) {
            if (step.step_name !== stepName)
                continue;
            if (!latest || step.updated_at > latest.updated_at) {
                latest = step;
            }
        }
        return latest;
    }
    async countUnabsorbedFailures(workflowRunId) {
        const steps = await this.getStepsForRun(workflowRunId);
        let count = 0;
        for (const step of steps) {
            if (step.status !== StepExecutionStatus.FAILED)
                continue;
            if (step.join_step_id) {
                const join = await this.loadStep(step.join_step_id);
                if (join?.status === StepExecutionStatus.COMPLETED)
                    continue;
            }
            count++;
        }
        return count;
    }
    async tryCompleteRun(workflowRunId, output) {
        const result = (await this.redis.send("EVAL", [
            COMPLETE_RUN_SCRIPT,
            "1",
            runKey(workflowRunId),
            this.nowIso(),
            encodeJson(output),
        ]));
        return result > 0 ? result : null;
    }
    async tryFailRun(workflowRunId, reason) {
        const output = encodeJson(reason ? { reason } : null);
        const result = (await this.redis.send("EVAL", [
            FAIL_RUN_SCRIPT,
            "1",
            runKey(workflowRunId),
            this.nowIso(),
            output,
        ]));
        return result > 0 ? result : null;
    }
    async tryCancelRun(workflowRunId, reason) {
        const output = encodeJson(reason ? { reason } : null);
        const result = (await this.redis.send("EVAL", [
            CANCEL_RUN_SCRIPT,
            "1",
            runKey(workflowRunId),
            this.nowIso(),
            output,
        ]));
        return result > 0 ? result : null;
    }
    async acquireRunLock(workflowRunId, token, ttlSec = 30) {
        const result = (await this.redis.send("EVAL", [
            ACQUIRE_RUN_LOCK_SCRIPT,
            "1",
            runLockKey(workflowRunId),
            token,
            String(ttlSec),
        ]));
        return result === 1;
    }
    async releaseRunLock(workflowRunId, token) {
        await this.redis.send("EVAL", [
            RELEASE_RUN_LOCK_SCRIPT,
            "1",
            runLockKey(workflowRunId),
            token,
        ]);
    }
    async tryAcquireRecoveryLeader(instanceId) {
        const result = (await this.redis.send("EVAL", [
            ACQUIRE_LEADER_LOCK_SCRIPT,
            "1",
            RECOVERY_LEADER_KEY,
            instanceId,
            String(RECOVERY_LEADER_TTL_SEC),
        ]));
        return result === 1;
    }
    async tryAcquireStartupReconcile(instanceId) {
        const result = (await this.redis.send("EVAL", [
            ACQUIRE_LEADER_LOCK_SCRIPT,
            "1",
            STARTUP_RECONCILE_KEY,
            instanceId,
            String(STARTUP_RECONCILE_TTL_SEC),
        ]));
        return result === 1;
    }
    async hydrateRunIfMissing(row) {
        const id = row.id;
        if (await this.existsRun(id))
            return false;
        await this.hydrateRun(row);
        return true;
    }
    async hydrateStepIfMissing(row) {
        const id = row.id;
        if (await this.existsStep(id))
            return false;
        await this.hydrateStep(row);
        return true;
    }
    async hydrateRun(row) {
        const id = row.id;
        const now = row.updated_at?.toISOString?.() ?? this.nowIso();
        await this.redis.send("HSET", [
            runKey(id),
            "id",
            id,
            "workflow_name",
            row.workflow_name,
            "status",
            row.status,
            "input",
            encodeJson(row.input),
            "output",
            encodeJson(row.output),
            "active_count",
            "0",
            "version",
            String(row.version ?? 0),
            "created_at",
            row.created_at?.toISOString?.() ?? now,
            "updated_at",
            now,
        ]);
    }
    async hydrateStep(row) {
        const id = row.id;
        const workflowRunId = row.workflow_run_id;
        const stepName = row.step_name;
        const status = row.status;
        const now = row.updated_at?.toISOString?.() ?? this.nowIso();
        await this.redis.send("HSET", [
            stepKey(id),
            "id",
            id,
            "workflow_run_id",
            workflowRunId,
            "step_name",
            stepName,
            "queue",
            row.queue ?? "default",
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
            String(row.attempts ?? 0),
            "queued",
            "0",
            "lease_owner",
            "",
            "lease_until",
            "0",
            "version",
            String(row.version ?? 0),
            "created_at",
            row.created_at?.toISOString?.() ?? now,
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
        const joinStepId = row.join_step_id;
        if (joinStepId) {
            await this.redis.send("RPUSH", [joinBranchesKey(joinStepId), id]);
        }
        await this.redis.send("SADD", [runStepsKey(workflowRunId), id]);
    }
    async scanStepIds(pattern) {
        const ids = [];
        let cursor = "0";
        do {
            const [nextCursor, keys] = (await this.redis.send("SCAN", [
                cursor,
                "MATCH",
                pattern,
                "COUNT",
                "100",
            ]));
            cursor = nextCursor;
            for (const key of keys) {
                const prefix = "chotu:step:";
                if (key.startsWith(prefix)) {
                    const id = key.slice(prefix.length);
                    if (id.startsWith("_"))
                        continue;
                    ids.push(id);
                }
            }
        } while (cursor !== "0");
        return ids;
    }
}
//# sourceMappingURL=state-store.js.map