import { getStepName, getStepTimeoutMs, type StepClass } from "../domain/step";
import {
    computeLeaseTtlMs,
    DEFAULT_LEASE_BUFFER_MS,
    DEFAULT_STEP_TIMEOUT_MS,
    resolveStepTimeoutMs,
} from "../domain/timeout";
import { validateConfig, type Workflow } from "../domain/workflow";
import type { QueueConfig } from "../interfaces/queue.interface";

export interface StepRegistryOptions {
    defaultStepTimeoutMs?: number;
    leaseBufferMs?: number;
}

export class StepRegistry {
    private readonly stepClasses = new Map<string, StepClass<any, any>>();
    private readonly workflows = new Map<string, Workflow>();
    private readonly queues = new Map<string, QueueConfig>();
    readonly stepQueues: Record<string, string>;
    private readonly defaultStepTimeoutMs: number;
    private readonly leaseBufferMs: number;

    constructor(
        queueConfigs: QueueConfig[],
        stepQueues: Record<string, string>,
        workflowDefinitions: Workflow[],
        options: StepRegistryOptions = {},
    ) {
        const defaultStepTimeoutMs = options.defaultStepTimeoutMs ?? DEFAULT_STEP_TIMEOUT_MS;
        const leaseBufferMs = options.leaseBufferMs ?? DEFAULT_LEASE_BUFFER_MS;

        if (defaultStepTimeoutMs < 1) {
            throw new Error("[chotu] defaultStepTimeoutMs must be >= 1");
        }
        if (leaseBufferMs < 0) {
            throw new Error("[chotu] leaseBufferMs must be >= 0");
        }

        this.defaultStepTimeoutMs = defaultStepTimeoutMs;
        this.leaseBufferMs = leaseBufferMs;

        validateConfig(queueConfigs, stepQueues, workflowDefinitions);

        for (const queue of queueConfigs) {
            this.queues.set(queue.name, queue);
        }
        this.stepQueues = stepQueues;

        for (const workflow of workflowDefinitions) {
            this.workflows.set(workflow.name, workflow);
            for (const stepClass of workflow.steps) {
                this.stepClasses.set(getStepName(stepClass), stepClass);
            }
        }
    }

    getWorkflow(name: string): Workflow | undefined {
        return this.workflows.get(name);
    }

    getStepClass(stepName: string): StepClass<any, any> | undefined {
        return this.stepClasses.get(stepName);
    }

    getQueue(name: string): QueueConfig | undefined {
        return this.queues.get(name);
    }

    queueNames(): Iterable<string> {
        return this.queues.keys();
    }

    allQueues(): QueueConfig[] {
        return [...this.queues.values()];
    }

    resolveQueue(stepName: string): string {
        const queueName = this.stepQueues[stepName] ?? "default";
        if (!this.queues.has(queueName)) {
            throw new Error(`[chotu] Queue "${queueName}" not configured (step "${stepName}")`);
        }
        return queueName;
    }

    getStepTimeoutOverrideMs(stepName: string): number | undefined {
        const stepClass = this.stepClasses.get(stepName);
        if (!stepClass) return undefined;
        return getStepTimeoutMs(stepClass);
    }

    getEffectiveStepTimeoutMs(stepName: string): number {
        return resolveStepTimeoutMs(this.getStepTimeoutOverrideMs(stepName), this.defaultStepTimeoutMs);
    }

    getLeaseTtlMs(stepName: string): number {
        return computeLeaseTtlMs(this.getEffectiveStepTimeoutMs(stepName), this.leaseBufferMs);
    }

    getEffectiveMaxAttempts(queue: QueueConfig, error: Error): number {
        const maxRetries = queue.maxRetries ?? 3;
        const baseAttempts = maxRetries + 1;
        if (this.isTransientDbError(error)) {
            return baseAttempts + 2;
        }
        return baseAttempts;
    }

    private isTransientDbError(error: Error): boolean {
        const msg = error.message.toLowerCase();
        return (
            msg.includes("failed to read data") ||
            msg.includes("connection") ||
            msg.includes("timeout") ||
            (error as NodeJS.ErrnoException).code === "ERR_POSTGRES_INVALID_MESSAGE"
        );
    }
}