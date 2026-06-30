import type { Chotu, ChotuConfig, ChotuHealth } from "./interfaces/chotu.interface";
import ChotuImpl from "./chotu.impl";

export type { Chotu, ChotuConfig, ChotuHealth, StepResolver } from "./interfaces/chotu.interface";
export type {
    ChotuHooks,
    StepCancelledContext,
    StepCompletedContext,
    StepFailedContext,
    StepHookContext,
    WorkflowCompletedContext,
    WorkflowCancelledContext,
    WorkflowErrorContext,
    WorkflowHookContext,
} from "./interfaces/hooks.interface";
export type { ChotuLogger } from "./logger";
export type { QueueConfig, RateLimitConfig } from "./interfaces/queue.interface";
export type {
    CreatedWorkflowRun,
    StepExecution,
    StepExecutionRecord,
    WorkflowCompleteInput,
    WorkflowRun,
    WorkflowRunRecord,
} from "./interfaces/workflow.interface";
export {
    StepExecutionStatus,
    WorkflowRunStatus,
} from "./interfaces/workflow.interface";

export {
    Step,
    createStepError,
    getStepName,
    isChotuStepError,
    next,
    parallel,
    isNextStep,
    isParallelSpec,
} from "./domain/step";
export type {
    ChotuStepError,
    NextStep,
    NextStepsResult,
    ParallelSpec,
    StepClass,
} from "./domain/step";
export { StepRegistry } from "./engine/step-registry";
export type { StepRegistryOptions } from "./engine/step-registry";
export {
    computeLeaseTtlMs,
    DEFAULT_LEASE_BUFFER_MS,
    DEFAULT_STEP_TIMEOUT_MS,
    resolveStepTimeoutMs,
} from "./domain/timeout";
export { Workflow, defineWorkflow, validateConfig, validateStepQueues } from "./domain/workflow";
export type { WorkflowClass } from "./domain/workflow";

export { FAIR_ENQUEUE_SCRIPT } from "./persistence/redis/scripts";
export {
    queueRotationKey,
    queueWfKey,
    queueWorkflowsKey,
    stepKey,
    inflightKey,
} from "./persistence/redis/keys";

let instance: Chotu | undefined;

export function resetChotu(): void {
    instance = undefined;
}

export function createChotu(config: ChotuConfig): Chotu {
    if (instance?.isStarted()) {
        throw new Error("[chotu] Already started; call shutdown() first");
    }
    if (instance) {
        resetChotu();
    }
    instance = new ChotuImpl(config, resetChotu);
    return instance;
}

export function getChotu(): Chotu | undefined {
    return instance;
}
