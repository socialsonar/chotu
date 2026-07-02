import { getStepName, getStepTimeoutMs } from "./step";
function stepInList(step, steps) {
    const name = getStepName(step);
    return steps.some((s) => getStepName(s) === name);
}
export function defineWorkflow(config) {
    if (!config.name?.trim()) {
        throw new Error("[chotu] Workflow name is required");
    }
    if (!stepInList(config.firstStep, config.steps)) {
        throw new Error(`[chotu] Workflow "${config.name}": firstStep "${getStepName(config.firstStep)}" must be in steps`);
    }
    if (config.completeStep && !stepInList(config.completeStep, config.steps)) {
        throw new Error(`[chotu] Workflow "${config.name}": completeStep "${getStepName(config.completeStep)}" must be in steps`);
    }
    if (config.terminalSteps) {
        for (const terminal of config.terminalSteps) {
            if (!stepInList(terminal, config.steps)) {
                throw new Error(`[chotu] Workflow "${config.name}": terminalStep "${getStepName(terminal)}" must be in steps`);
            }
        }
    }
    const names = config.steps.map(getStepName);
    const duplicates = names.filter((n, i) => names.indexOf(n) !== i);
    if (duplicates.length > 0) {
        throw new Error(`[chotu] Workflow "${config.name}": duplicate step names: ${[...new Set(duplicates)].join(", ")}`);
    }
    if (!config.completeStep && !config.terminalSteps?.length) {
        throw new Error(`[chotu] Workflow "${config.name}": define completeStep or terminalSteps for workflow completion output`);
    }
    return config;
}
export function validateStepQueues(stepQueues, workflows) {
    validateConfig([], stepQueues, workflows);
}
export function validateConfig(queues, stepQueues, workflows) {
    if (!queues.length) {
        throw new Error("[chotu] At least one queue must be configured");
    }
    const queueNames = new Set();
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
    const registered = new Set();
    const stepToWorkflow = new Map();
    for (const workflow of workflows) {
        for (const step of workflow.steps) {
            const stepName = getStepName(step);
            registered.add(stepName);
            const existingWorkflow = stepToWorkflow.get(stepName);
            if (existingWorkflow && existingWorkflow !== workflow.name) {
                throw new Error(`[chotu] Duplicate step name "${stepName}" in workflows "${existingWorkflow}" and "${workflow.name}"`);
            }
            stepToWorkflow.set(stepName, workflow.name);
            const resolvedQueue = stepQueues[stepName] ?? "default";
            if (!queueNames.has(resolvedQueue)) {
                throw new Error(`[chotu] Step "${stepName}" (workflow "${workflow.name}") resolves to queue "${resolvedQueue}" which is not configured`);
            }
            const timeoutMs = getStepTimeoutMs(step);
            if (timeoutMs != null && timeoutMs < 1) {
                throw new Error(`[chotu] Step "${stepName}" (workflow "${workflow.name}"): timeoutMs must be >= 1`);
            }
        }
    }
    for (const stepName of Object.keys(stepQueues)) {
        if (!registered.has(stepName)) {
            throw new Error(`[chotu] stepQueues key "${stepName}" is not a registered step`);
        }
        if (!queueNames.has(stepQueues[stepName])) {
            throw new Error(`[chotu] stepQueues["${stepName}"] references unconfigured queue "${stepQueues[stepName]}"`);
        }
    }
}
//# sourceMappingURL=workflow.js.map