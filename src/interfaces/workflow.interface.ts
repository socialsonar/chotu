export enum WorkflowRunStatus {
    RUNNING = "running",
    COMPLETED = "completed",
    FAILED = "failed",
}

export enum StepExecutionStatus {
    PENDING = "pending",
    RUNNING = "running",
    COMPLETED = "completed",
    FAILED = "failed",
    WAITING = "waiting",
}

export interface WorkflowCompleteInput<I = unknown> {
    workflowInput: I;
    workflowRunId: string;
}

export interface WorkflowRun {
    id: string;
    workflowName: string;
    status: WorkflowRunStatus;
    input: Record<string, any>;
    output: Record<string, any> | null;
    createdAt: Date;
    updatedAt: Date;
    finishedAt: Date | null;
}

export interface StepExecution {
    id: string;
    workflowRunId: string;
    stepName: string;
    queue: string;
    status: StepExecutionStatus;
    input: Record<string, any> | null;
    output: Record<string, any> | null;
    error: Record<string, any> | null;
    joinStepId: string | null;
    fanOutIndex: number | null;
    attempts: number;
    createdAt: Date;
    updatedAt: Date;
    finishedAt: Date | null;
}

export interface CreatedWorkflowRun {
    id: string;
}

export interface StepExecutionRecord {
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

export interface WorkflowRunRecord {
    id: string;
    workflow_name: string;
    status: WorkflowRunStatus;
    input: Record<string, any>;
    output: Record<string, any> | null;
    active_count: number;
    version: number;
}
