import { getStepName, getStepTimeoutMs, type StepClass } from "../domain/step";
import { validateConfig, type WorkflowDefinition } from "../domain/workflow";
import type { QueueConfig } from "../interfaces/queue.interface";

export class StepRegistry {
    private readonly stepClasses = new Map<string, StepClass<any, any>>();
    private readonly workflows = new Map<string, WorkflowDefinition>();
    private readonly queues = new Map<string, QueueConfig>();
    readonly stepQueues: Record<string, string>;

    constructor(
        queueConfigs: QueueConfig[],
        stepQueues: Record<string, string>,
        workflowDefinitions: WorkflowDefinition[],
    ) {
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

    getWorkflow(name: string): WorkflowDefinition | undefined {
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

    getStepTimeoutMs(stepName: string): number | undefined {
        const stepClass = this.stepClasses.get(stepName);
        if (!stepClass) return undefined;
        return getStepTimeoutMs(stepClass);
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