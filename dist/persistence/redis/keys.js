export const SYNC_STREAM = "chotu:sync:stream";
export const SYNC_CONSUMER_GROUP = "chotu-flusher";
export const RECOVERY_LEADER_KEY = "chotu:recovery:leader";
export const STARTUP_RECONCILE_KEY = "chotu:startup:reconcile";
export const RECOVERY_LEADER_TTL_SEC = 20;
export const RECOVERY_INTERVAL_MS = 30_000;
export const STARTUP_RECONCILE_TTL_SEC = 300;
export function stepKey(stepExecId) {
    return `chotu:step:${stepExecId}`;
}
export function runKey(workflowRunId) {
    return `chotu:run:${workflowRunId}`;
}
export function runStepsKey(workflowRunId) {
    return `chotu:run:${workflowRunId}:steps`;
}
export function activeStepKey(workflowRunId, stepName) {
    return `chotu:run:${workflowRunId}:active:${stepName}`;
}
export function joinBranchesKey(joinStepId) {
    return `chotu:run:branches:${joinStepId}`;
}
export function runLockKey(workflowRunId) {
    return `chotu:sync:lock:${workflowRunId}`;
}
export function inflightKey(queueName) {
    return `chotu:queue:${queueName}:inflight`;
}
export function queueWfKey(queueName, workflowRunId) {
    return `chotu:queue:${queueName}:wf:${workflowRunId}`;
}
export function queueWorkflowsKey(queueName) {
    return `chotu:queue:${queueName}:workflows`;
}
export function queueRotationKey(queueName) {
    return `chotu:queue:${queueName}:rotation`;
}
export function rateLimitKey(queueName, windowKey) {
    return `chotu:ratelimit:${queueName}:${windowKey}`;
}
//# sourceMappingURL=keys.js.map