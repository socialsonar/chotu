import type { StepResolver } from "../interfaces/chotu.interface";
import type { StepHookContext } from "../interfaces/hooks.interface";
import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { QueueConfig } from "../interfaces/queue.interface";
import { StepExecutionStatus, type StepExecutionRecord } from "../interfaces/workflow.interface";
import type { ChotuLogger } from "../logger";
import { ChotuHookRunner } from "./hook-runner";
import { StepRegistry } from "./step-registry";
import { WorkflowLifecycle } from "./workflow-lifecycle";
export declare class StepExecutor {
    private readonly lifecycle;
    private readonly registry;
    private readonly fairQueue;
    private readonly logger;
    private readonly hookRunner;
    private readonly resolveStep?;
    constructor(lifecycle: WorkflowLifecycle, registry: StepRegistry, fairQueue: IFairQueue, logger: ChotuLogger, hookRunner: ChotuHookRunner, resolveStep?: StepResolver | undefined);
    processStepExecution(row: StepExecutionRecord, queue: QueueConfig, signal: AbortSignal): Promise<boolean>;
    recoverFromWorkerError(stepExecId: string, row: StepExecutionRecord, queue: QueueConfig): Promise<void>;
    normalizeInput(input: Record<string, any> | any[] | string | null): any;
    throwIfAborted(signal: AbortSignal): void;
    private throwIfRunAborted;
    private createStepSignal;
    private raceWithStepTimeout;
    loadStep(stepExecId: string): Promise<StepExecutionRecord | null>;
    setStepStatus(stepExecId: string, status: StepExecutionStatus): Promise<boolean>;
    buildStepHookContext(row: StepExecutionRecord): Promise<StepHookContext>;
    handleAbortedStep(row: StepExecutionRecord): Promise<void>;
}
//# sourceMappingURL=step-executor.d.ts.map