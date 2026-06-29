import type {
    StepExecutionRecord,
    StepExecutionStatus,
    WorkflowRunRecord,
    WorkflowRunStatus,
} from "./workflow.interface";

export interface IStateStore {
    existsStep(stepExecId: string): Promise<boolean>;
    existsRun(workflowRunId: string): Promise<boolean>;
    loadStep(stepExecId: string): Promise<StepExecutionRecord | null>;
    loadRun(workflowRunId: string): Promise<WorkflowRunRecord | null>;
    getActiveCount(workflowRunId: string): Promise<number>;
    claimStep(
        stepExecId: string,
        leaseOwner: string,
        leaseTtlMs: number,
    ): Promise<StepExecutionRecord | null>;
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
    completeStep(
        stepExecId: string,
        output: Record<string, any>,
    ): Promise<StepExecutionRecord | null>;
    failStep(stepExecId: string, error: Record<string, any>): Promise<StepExecutionRecord | null>;
    cancelStep(stepExecId: string, reason?: string): Promise<StepExecutionRecord | null>;
    finalizeJoinStep(
        joinStepId: string,
        input: Record<string, any>[],
    ): Promise<StepExecutionRecord | null>;
    getJoinBranches(joinStepId: string): Promise<StepExecutionRecord[]>;
    getRunStatus(workflowRunId: string): Promise<WorkflowRunStatus | null>;
    countUnabsorbedFailures(workflowRunId: string): Promise<number>;
    tryCompleteRun(
        workflowRunId: string,
        output: Record<string, any> | null,
    ): Promise<number | null>;
    tryFailRun(workflowRunId: string, reason?: string): Promise<number | null>;
    tryCancelRun(workflowRunId: string, reason?: string): Promise<number | null>;
    markAbortRequested(workflowRunId: string): Promise<void>;
    isAbortRequested(workflowRunId: string): Promise<boolean>;
    listStepsForRun(workflowRunId: string): Promise<StepExecutionRecord[]>;
    acquireRunLock(workflowRunId: string, token: string, ttlSec?: number): Promise<boolean>;
    releaseRunLock(workflowRunId: string, token: string): Promise<void>;
    tryAcquireRecoveryLeader(instanceId: string): Promise<boolean>;
    tryAcquireStartupReconcile(instanceId: string): Promise<boolean>;
    hydrateRunIfMissing(row: Record<string, unknown>): Promise<boolean>;
    hydrateStepIfMissing(row: Record<string, unknown>): Promise<boolean>;
    rebuildJoinRemainingFromBranches(joinStepId: string): Promise<number | null>;
    scanStepIds(pattern: string): Promise<string[]>;
    recomputeRunActiveCount(workflowRunId: string): Promise<number>;
    findStepByName(workflowRunId: string, stepName: string): Promise<StepExecutionRecord | null>;
}
