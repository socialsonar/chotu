export const SYNC_STREAM = "chotu:sync:stream";
export const SYNC_CONSUMER_GROUP = "chotu-flusher";

export const RECOVERY_LEADER_KEY = "chotu:recovery:leader";
export const STARTUP_RECONCILE_KEY = "chotu:startup:reconcile";

export const DEFAULT_LEASE_TTL_MS = 60_000;
export const RECOVERY_LEADER_TTL_SEC = 30;
export const STARTUP_RECONCILE_TTL_SEC = 300;

export function stepKey(stepExecId: string): string {
    return `chotu:step:${stepExecId}`;
}

export function runKey(workflowRunId: string): string {
    return `chotu:run:${workflowRunId}`;
}

export function runStepsKey(workflowRunId: string): string {
    return `chotu:run:${workflowRunId}:steps`;
}

export function activeStepKey(workflowRunId: string, stepName: string): string {
    return `chotu:run:${workflowRunId}:active:${stepName}`;
}

export function joinBranchesKey(joinStepId: string): string {
    return `chotu:run:branches:${joinStepId}`;
}

export function runLockKey(workflowRunId: string): string {
    return `chotu:sync:lock:${workflowRunId}`;
}

export function inflightKey(queueName: string): string {
    return `chotu:queue:${queueName}:inflight`;
}

export function queueWfKey(queueName: string, workflowRunId: string): string {
    return `chotu:queue:${queueName}:wf:${workflowRunId}`;
}

export function queueWorkflowsKey(queueName: string): string {
    return `chotu:queue:${queueName}:workflows`;
}

export function queueRotationKey(queueName: string): string {
    return `chotu:queue:${queueName}:rotation`;
}

export function rateLimitKey(queueName: string, windowKey: number): string {
    return `chotu:ratelimit:${queueName}:${windowKey}`;
}
