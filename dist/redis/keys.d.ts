export declare const SYNC_STREAM = "chotu:sync:stream";
export declare const SYNC_CONSUMER_GROUP = "chotu-flusher";
export declare const RECOVERY_LEADER_KEY = "chotu:recovery:leader";
export declare const STARTUP_RECONCILE_KEY = "chotu:startup:reconcile";
export declare const DEFAULT_LEASE_TTL_MS = 60000;
export declare const RECOVERY_LEADER_TTL_SEC = 30;
export declare const STARTUP_RECONCILE_TTL_SEC = 300;
export declare function stepKey(stepExecId: string): string;
export declare function runKey(workflowRunId: string): string;
export declare function runStepsKey(workflowRunId: string): string;
export declare function activeStepKey(workflowRunId: string, stepName: string): string;
export declare function joinBranchesKey(joinStepId: string): string;
export declare function runLockKey(workflowRunId: string): string;
export declare function inflightKey(queueName: string): string;
export declare function queueWfKey(queueName: string, workflowRunId: string): string;
export declare function queueWorkflowsKey(queueName: string): string;
export declare function queueRotationKey(queueName: string): string;
export declare function rateLimitKey(queueName: string, windowKey: number): string;
//# sourceMappingURL=keys.d.ts.map