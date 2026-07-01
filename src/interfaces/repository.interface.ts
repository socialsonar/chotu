import type {
    StepExecution,
    StepExecutionStatus,
    WorkflowCompleteInput,
    WorkflowRun,
    WorkflowRunStatus,
} from "./workflow.interface";

export interface IWorkflowRepository {
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
        status: StepExecutionStatus.COMPLETED | StepExecutionStatus.FAILED | StepExecutionStatus.CANCELLED;
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
        status: WorkflowRunStatus.COMPLETED | WorkflowRunStatus.FAILED | WorkflowRunStatus.CANCELLED;
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
    listPendingSteps(): Promise<{ id: string; workflow_run_id: string; queue: string }[]>;
    getRunRow(workflowRunId: string): Promise<Record<string, unknown> | null>;
    getCompleteStepRow(
        workflowRunId: string,
        completeStepName: string,
    ): Promise<Record<string, unknown> | null>;
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
    getTerminalStepOutputs(
        workflowRunId: string,
        terminalNames: string[],
    ): Promise<{ step_name: string; output: Record<string, any> | null }[]>;
    deleteStepsForRun(workflowRunId: string): Promise<void>;
}
