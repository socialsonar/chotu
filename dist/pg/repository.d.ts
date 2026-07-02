import type { SQL } from "bun";
import { StepExecutionStatus, WorkflowRunStatus, type StepExecution, type WorkflowCompleteInput, type WorkflowRun } from "../interfaces/workflow.interface";
import type { StepExecutionRow } from "../redis/state-store";
export declare class PgRepository {
    private readonly sql;
    constructor(sql: SQL);
    getWorkflowRun(id: string): Promise<WorkflowRun | null>;
    getStepExecutions(workflowRunId: string): Promise<StepExecution[]>;
    insertWorkflowRunWithFirstStep(params: {
        workflowRunId: string;
        workflowName: string;
        input: Record<string, any>;
        firstStepId: string;
        firstStepName: string;
        queue: string;
    }): Promise<void>;
    insertWorkflowRun(params: {
        id: string;
        workflowName: string;
        input: Record<string, any>;
    }): Promise<void>;
    insertStep(params: {
        id: string;
        workflowRunId: string;
        stepName: string;
        queue: string;
        status: StepExecutionStatus;
        input: Record<string, any> | null;
        joinStepId?: string | null;
        fanOutIndex?: number | null;
        joinTotal?: number | null;
        joinRemaining?: number | null;
    }): Promise<void>;
    syncStepTerminal(params: {
        id: string;
        status: StepExecutionStatus.COMPLETED | StepExecutionStatus.FAILED;
        output?: Record<string, any> | null;
        error?: Record<string, any> | null;
        version: number;
    }): Promise<void>;
    syncJoinFinalize(params: {
        id: string;
        input: Record<string, any>[];
        version: number;
    }): Promise<void>;
    syncJoinRemaining(id: string, remaining: number): Promise<void>;
    syncWorkflowTerminal(params: {
        id: string;
        status: WorkflowRunStatus.COMPLETED | WorkflowRunStatus.FAILED;
        output: Record<string, any> | null;
        version: number;
    }): Promise<boolean>;
    syncStepAttempts(params: {
        id: string;
        attempts: number;
        updatedAt: string;
        version: number;
    }): Promise<boolean>;
    syncStepStatus(params: {
        id: string;
        status: StepExecutionStatus;
        updatedAt: string;
        attempts?: number;
        version: number;
    }): Promise<boolean>;
    resetStaleRunning(threshold: Date): Promise<void>;
    resetStaleRunningReturning(threshold: Date): Promise<{
        id: string;
        workflow_run_id: string;
        queue: string;
    }[]>;
    resurrectRetriableFailedSteps(maxAttemptsByQueue: Map<string, number>): Promise<void>;
    reopenFailedRunsWithPendingSteps(): Promise<void>;
    listPendingSteps(): Promise<{
        id: string;
        workflow_run_id: string;
        queue: string;
    }[]>;
    listOrphanedPendingSteps(threshold: Date): Promise<{
        id: string;
        workflow_run_id: string;
        queue: string;
    }[]>;
    listNonTerminalRuns(): Promise<Record<string, unknown>[]>;
    listStepsForRunningWorkflows(): Promise<Record<string, unknown>[]>;
    listWaitingJoins(): Promise<{
        id: string;
        workflow_run_id: string;
        join_remaining: number | null;
    }[]>;
    listJoinBranches(joinId: string): Promise<Record<string, unknown>[]>;
    getRunRow(workflowRunId: string): Promise<Record<string, unknown> | null>;
    getCompleteStepRow(workflowRunId: string, completeStepName: string): Promise<Record<string, unknown> | null>;
    insertCompleteStep(params: {
        id: string;
        workflowRunId: string;
        stepName: string;
        queue: string;
        input: WorkflowCompleteInput;
    }): Promise<void>;
    completeWorkflowFromCompleteStep(params: {
        workflowRunId: string;
        output: Record<string, any> | null;
        version?: number;
    }): Promise<boolean>;
    getStepRow(id: string): Promise<Record<string, unknown> | null>;
    getRunForHydrate(id: string): Promise<Record<string, unknown> | null>;
    getTerminalStepOutputs(workflowRunId: string, terminalNames: string[]): Promise<{
        step_name: string;
        output: Record<string, any> | null;
    }[]>;
    mapWorkflowRun(row: Record<string, unknown>): WorkflowRun;
    mapStepExecution(row: Record<string, unknown>): StepExecution;
    mapStepRow(row: Record<string, unknown>): StepExecutionRow;
}
//# sourceMappingURL=repository.d.ts.map