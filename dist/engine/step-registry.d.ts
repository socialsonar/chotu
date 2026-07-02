import { type StepClass } from "../domain/step";
import { type Workflow } from "../domain/workflow";
import type { QueueConfig } from "../interfaces/queue.interface";
export interface StepRegistryOptions {
    defaultStepTimeoutMs?: number;
    leaseBufferMs?: number;
}
export declare class StepRegistry {
    private readonly stepClasses;
    private readonly workflows;
    private readonly queues;
    readonly stepQueues: Record<string, string>;
    private readonly defaultStepTimeoutMs;
    private readonly leaseBufferMs;
    constructor(queueConfigs: QueueConfig[], stepQueues: Record<string, string>, workflowDefinitions: Workflow[], options?: StepRegistryOptions);
    getWorkflow(name: string): Workflow | undefined;
    getStepClass(stepName: string): StepClass<any, any> | undefined;
    getQueue(name: string): QueueConfig | undefined;
    queueNames(): Iterable<string>;
    allQueues(): QueueConfig[];
    resolveQueue(stepName: string): string;
    getStepTimeoutOverrideMs(stepName: string): number | undefined;
    getEffectiveStepTimeoutMs(stepName: string): number;
    getLeaseTtlMs(stepName: string): number;
    getEffectiveMaxAttempts(queue: QueueConfig, error: Error): number;
    private isTransientDbError;
}
//# sourceMappingURL=step-registry.d.ts.map