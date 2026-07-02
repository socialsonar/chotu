import { getStepName, getStepTimeoutMs } from "../domain/step";
import { computeLeaseTtlMs, DEFAULT_LEASE_BUFFER_MS, DEFAULT_STEP_TIMEOUT_MS, resolveStepTimeoutMs, } from "../domain/timeout";
import { validateConfig } from "../domain/workflow";
export class StepRegistry {
    stepClasses = new Map();
    workflows = new Map();
    queues = new Map();
    stepQueues;
    defaultStepTimeoutMs;
    leaseBufferMs;
    constructor(queueConfigs, stepQueues, workflowDefinitions, options = {}) {
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
    getWorkflow(name) {
        return this.workflows.get(name);
    }
    getStepClass(stepName) {
        return this.stepClasses.get(stepName);
    }
    getQueue(name) {
        return this.queues.get(name);
    }
    queueNames() {
        return this.queues.keys();
    }
    allQueues() {
        return [...this.queues.values()];
    }
    resolveQueue(stepName) {
        const queueName = this.stepQueues[stepName] ?? "default";
        if (!this.queues.has(queueName)) {
            throw new Error(`[chotu] Queue "${queueName}" not configured (step "${stepName}")`);
        }
        return queueName;
    }
    getStepTimeoutOverrideMs(stepName) {
        const stepClass = this.stepClasses.get(stepName);
        if (!stepClass)
            return undefined;
        return getStepTimeoutMs(stepClass);
    }
    getEffectiveStepTimeoutMs(stepName) {
        return resolveStepTimeoutMs(this.getStepTimeoutOverrideMs(stepName), this.defaultStepTimeoutMs);
    }
    getLeaseTtlMs(stepName) {
        return computeLeaseTtlMs(this.getEffectiveStepTimeoutMs(stepName), this.leaseBufferMs);
    }
    getEffectiveMaxAttempts(queue, error) {
        const maxRetries = queue.maxRetries ?? 3;
        const baseAttempts = maxRetries + 1;
        if (this.isTransientDbError(error)) {
            return baseAttempts + 2;
        }
        return baseAttempts;
    }
    isTransientDbError(error) {
        const msg = error.message.toLowerCase();
        return (msg.includes("failed to read data") ||
            msg.includes("connection") ||
            msg.includes("timeout") ||
            error.code === "ERR_POSTGRES_INVALID_MESSAGE");
    }
}
//# sourceMappingURL=step-registry.js.map