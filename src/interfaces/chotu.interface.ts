import type { CreatedWorkflowRun, StepExecution, WorkflowRun } from "./workflow.interface";
import type { WorkflowDefinition } from "../domain/workflow";
import type { QueueConfig } from "./queue.interface";
import type { ChotuLogger } from "../logger";
import type { ChotuHooks } from "./hooks.interface";

export interface ChotuHealth {
    postgres: boolean;
    redis: boolean;
    workers: boolean;
}

export interface ChotuConfig {
    postgresUrl: string;
    redisUrl: string;
    postgresMaxConnections?: number;
    flushIntervalMs?: number;
    leaseTtlMs?: number;
    queues: QueueConfig[];
    stepQueues: Record<string, string>;
    workflows: WorkflowDefinition[];
    logger?: ChotuLogger;
    hooks?: ChotuHooks;
}

export interface Chotu {
    listen(options?: { deferWorkers?: boolean }): Promise<void>;
    startWorkers(): Promise<void>;
    shutdown(): Promise<void>;
    runWorkflow<I>(name: string, input: I): Promise<CreatedWorkflowRun>;
    getWorkflowRun(id: string): Promise<WorkflowRun | null>;
    getStepExecutions(workflowRunId: string): Promise<StepExecution[]>;
    health(): Promise<ChotuHealth>;
    isStarted(): boolean;
    areWorkersStarted(): boolean;
}
