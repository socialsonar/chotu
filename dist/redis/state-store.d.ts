import type { RedisClient } from "bun";
import { StepExecutionStatus, WorkflowRunStatus } from "../interfaces/workflow.interface";
export interface StepExecutionRow {
    id: string;
    workflow_run_id: string;
    step_name: string;
    queue: string;
    status: StepExecutionStatus;
    input: Record<string, any> | null;
    output: Record<string, any> | null;
    error: Record<string, any> | null;
    join_step_id: string | null;
    fan_out_index: number | null;
    join_total: number | null;
    join_remaining: number | null;
    attempts: number;
    version: number;
    updated_at: string;
    queued: boolean;
    lease_owner: string | null;
    lease_until: number;
}
export interface WorkflowRunRow {
    id: string;
    workflow_name: string;
    status: WorkflowRunStatus;
    input: Record<string, any>;
    output: Record<string, any> | null;
    active_count: number;
    version: number;
}
export declare function mapStepHash(hash: Record<string, string>): StepExecutionRow | null;
export declare class RedisStateStore {
    private readonly redis;
    constructor(redis: RedisClient);
    private nowIso;
    existsStep(stepExecId: string): Promise<boolean>;
    existsRun(workflowRunId: string): Promise<boolean>;
    loadStep(stepExecId: string): Promise<StepExecutionRow | null>;
    loadRun(workflowRunId: string): Promise<WorkflowRunRow | null>;
    getActiveCount(workflowRunId: string): Promise<number>;
    getStepUpdatedAt(stepExecId: string): Promise<Date>;
    claimStep(stepExecId: string, leaseOwner: string, leaseTtlMs: number): Promise<StepExecutionRow | null>;
    renewLease(stepExecId: string, leaseOwner: string, leaseTtlMs: number): Promise<boolean>;
    resetExpiredLease(stepExecId: string): Promise<boolean>;
    setStepStatus(stepExecId: string, status: StepExecutionStatus): Promise<boolean>;
    incrementAttempts(stepExecId: string): Promise<number>;
    decrementJoinRemaining(joinStepId: string): Promise<number | null>;
    rollbackStep(stepExecId: string, workflowRunId: string, stepName: string): Promise<void>;
    rollbackRun(workflowRunId: string): Promise<void>;
    createRun(params: {
        id: string;
        workflowName: string;
        input: Record<string, any>;
    }): Promise<void>;
    createStep(params: {
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
    }): Promise<boolean>;
    completeStep(stepExecId: string, output: Record<string, any>): Promise<StepExecutionRow | null>;
    failStep(stepExecId: string, error: Record<string, any>): Promise<StepExecutionRow | null>;
    private applyTerminalTransition;
    finalizeJoinStep(joinStepId: string, input: Record<string, any>[]): Promise<StepExecutionRow | null>;
    setJoinRemaining(joinStepId: string, remaining: number): Promise<void>;
    rebuildJoinRemainingFromBranches(joinStepId: string): Promise<number | null>;
    getJoinBranches(joinStepId: string): Promise<StepExecutionRow[]>;
    getRunStatus(workflowRunId: string): Promise<WorkflowRunStatus | null>;
    getStepsForRun(workflowRunId: string): Promise<StepExecutionRow[]>;
    countUnabsorbedFailures(workflowRunId: string): Promise<number>;
    tryCompleteRun(workflowRunId: string, output: Record<string, any> | null): Promise<number | null>;
    tryFailRun(workflowRunId: string, reason?: string): Promise<number | null>;
    completeWorkflow(workflowRunId: string, output: Record<string, any> | null): Promise<void>;
    failWorkflow(workflowRunId: string, reason?: string): Promise<void>;
    acquireRunLock(workflowRunId: string, token: string, ttlSec?: number): Promise<boolean>;
    releaseRunLock(workflowRunId: string, token: string): Promise<void>;
    tryAcquireRecoveryLeader(instanceId: string): Promise<boolean>;
    tryAcquireStartupReconcile(instanceId: string): Promise<boolean>;
    hydrateRunIfMissing(row: Record<string, unknown>): Promise<boolean>;
    hydrateStepIfMissing(row: Record<string, unknown>): Promise<boolean>;
    hydrateRun(row: Record<string, unknown>): Promise<void>;
    hydrateStep(row: Record<string, unknown>): Promise<void>;
    rebuildActiveCount(workflowRunId: string): Promise<number>;
    scanStepIds(pattern: string): Promise<string[]>;
}
//# sourceMappingURL=state-store.d.ts.map