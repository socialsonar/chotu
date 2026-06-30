import type { WorkflowHookContext } from "../interfaces/hooks.interface";
import { getStepName, getStepTimeoutMs, type StepClass } from "./step";
import type { QueueConfig } from "../interfaces/queue.interface";

export abstract class Workflow<I = any, O = any> {
    abstract readonly name: string;
    abstract readonly firstStep: StepClass<I, any>;
    abstract readonly steps: StepClass<any, any>[];
    readonly completeStep?: StepClass<any, any>;
    readonly terminalSteps?: StepClass<any, any>[];

    async onBeforeStart(
        _input: I,
        _ctx: WorkflowHookContext,
        _signal: AbortSignal,
    ): Promise<I | void> {}

    async onAfterCompleted(
        _input: I,
        _output: O | null,
        _ctx: WorkflowHookContext,
        _signal: AbortSignal,
    ): Promise<void> {}
}

export type WorkflowClass<I = any, O = any> = new () => Workflow<I, O>;

function stepInList(step: StepClass<any, any>, steps: StepClass<any, any>[]): boolean {
    const name = getStepName(step);
    return steps.some((s) => getStepName(s) === name);
}

function validateWorkflowInstance<I, O>(instance: Workflow<I, O>): void {
    if (!instance.name?.trim()) {
        throw new Error("[chotu] Workflow name is required");
    }

    if (!stepInList(instance.firstStep, instance.steps)) {
        throw new Error(
            `[chotu] Workflow "${instance.name}": firstStep "${getStepName(instance.firstStep)}" must be in steps`,
        );
    }

    if (instance.completeStep && !stepInList(instance.completeStep, instance.steps)) {
        throw new Error(
            `[chotu] Workflow "${instance.name}": completeStep "${getStepName(instance.completeStep)}" must be in steps`,
        );
    }

    if (instance.terminalSteps) {
        for (const terminal of instance.terminalSteps) {
            if (!stepInList(terminal, instance.steps)) {
                throw new Error(
                    `[chotu] Workflow "${instance.name}": terminalStep "${getStepName(terminal)}" must be in steps`,
                );
            }
        }
    }

    const names = instance.steps.map(getStepName);
    const duplicates = names.filter((n, i) => names.indexOf(n) !== i);
    if (duplicates.length > 0) {
        throw new Error(
            `[chotu] Workflow "${instance.name}": duplicate step names: ${[...new Set(duplicates)].join(", ")}`,
        );
    }

    if (!instance.completeStep && !instance.terminalSteps?.length) {
        throw new Error(
            `[chotu] Workflow "${instance.name}": define completeStep or terminalSteps for workflow completion output`,
        );
    }
}

export function defineWorkflow<I, O>(WorkflowCls: WorkflowClass<I, O>): Workflow<I, O> {
    const instance = new WorkflowCls();
    validateWorkflowInstance(instance);
    return instance;
}

export function validateStepQueues(
    stepQueues: Record<string, string>,
    workflows: Workflow[],
): void {
    validateConfig([], stepQueues, workflows);
}

export function validateConfig(
    queues: QueueConfig[],
    stepQueues: Record<string, string>,
    workflows: Workflow[],
): void {
    if (!queues.length) {
        throw new Error("[chotu] At least one queue must be configured");
    }

    const queueNames = new Set<string>();
    for (const queue of queues) {
        if (!queue.name?.trim()) {
            throw new Error("[chotu] Queue name is required");
        }
        if (queue.concurrency < 1) {
            throw new Error(`[chotu] Queue "${queue.name}": concurrency must be >= 1`);
        }
        if (queue.maxRetries != null && queue.maxRetries < 0) {
            throw new Error(`[chotu] Queue "${queue.name}": maxRetries must be >= 0`);
        }
        queueNames.add(queue.name);
    }

    const registered = new Set<string>();
    const stepToWorkflow = new Map<string, string>();

    for (const workflow of workflows) {
        for (const step of workflow.steps) {
            const stepName = getStepName(step);
            registered.add(stepName);

            const existingWorkflow = stepToWorkflow.get(stepName);
            if (existingWorkflow && existingWorkflow !== workflow.name) {
                throw new Error(
                    `[chotu] Duplicate step name "${stepName}" in workflows "${existingWorkflow}" and "${workflow.name}"`,
                );
            }
            stepToWorkflow.set(stepName, workflow.name);

            const resolvedQueue = stepQueues[stepName] ?? "default";
            if (!queueNames.has(resolvedQueue)) {
                throw new Error(
                    `[chotu] Step "${stepName}" (workflow "${workflow.name}") resolves to queue "${resolvedQueue}" which is not configured`,
                );
            }

            const timeoutMs = getStepTimeoutMs(step);
            if (timeoutMs != null && timeoutMs < 1) {
                throw new Error(
                    `[chotu] Step "${stepName}" (workflow "${workflow.name}"): timeoutMs must be >= 1`,
                );
            }
        }
    }

    for (const stepName of Object.keys(stepQueues)) {
        if (!registered.has(stepName)) {
            throw new Error(`[chotu] stepQueues key "${stepName}" is not a registered step`);
        }
        if (!queueNames.has(stepQueues[stepName])) {
            throw new Error(
                `[chotu] stepQueues["${stepName}"] references unconfigured queue "${stepQueues[stepName]}"`,
            );
        }
    }
}
