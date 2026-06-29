import type { QueueConfig } from "./queue.interface";

export interface IFairQueue {
    pop(queueName: string): Promise<string | null>;
    ack(queueName: string, stepExecId: string): Promise<void>;
    requeue(queueName: string, stepExecId: string, workflowRunId: string): Promise<void>;
    enqueue(stepExecId: string, queueName: string, workflowRunId: string): Promise<void>;
    enqueueWithRetry(
        stepExecId: string,
        queueName: string,
        workflowRunId: string,
        maxAttempts?: number,
    ): Promise<void>;
    acquireRateLimit(queue: QueueConfig): Promise<boolean>;
    rateLimitBackoffMs(queue: QueueConfig): number;
    isStepInAnyInflight(stepExecId: string, queueNames: Iterable<string>): Promise<boolean>;
}
