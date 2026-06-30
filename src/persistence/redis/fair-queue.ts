import type { ChotuRedis } from "../../platform";
import { sleep } from "../../platform/sleep";
import type { IFairQueue } from "../../interfaces/fair-queue.interface";
import type { QueueConfig } from "../../interfaces/queue.interface";
import {
    inflightKey,
    queueRotationKey,
    queueWfKey,
    queueWorkflowsKey,
    rateLimitKey,
    stepKey,
} from "./keys";
import {
    ACK_INFLIGHT_SCRIPT,
    CANCEL_FROM_QUEUE_SCRIPT,
    FAIR_ENQUEUE_SCRIPT,
    FAIR_POP_SCRIPT,
    RATE_LIMIT_SCRIPT,
    REQUEUE_INFLIGHT_SCRIPT,
} from "./scripts";

export class RedisFairQueue implements IFairQueue {
    constructor(private readonly redis: ChotuRedis) {}

    async pop(queueName: string): Promise<string | null> {
        return (await this.redis.send("EVAL", [
            FAIR_POP_SCRIPT,
            "3",
            queueRotationKey(queueName),
            queueWorkflowsKey(queueName),
            inflightKey(queueName),
            `chotu:queue:${queueName}:wf:`,
        ])) as string | null;
    }

    async ack(queueName: string, stepExecId: string): Promise<void> {
        await this.redis.send("EVAL", [
            ACK_INFLIGHT_SCRIPT,
            "2",
            inflightKey(queueName),
            stepKey(stepExecId),
            stepExecId,
        ]);
    }

    async requeue(queueName: string, stepExecId: string, workflowRunId: string): Promise<void> {
        await this.redis.send("EVAL", [
            REQUEUE_INFLIGHT_SCRIPT,
            "5",
            inflightKey(queueName),
            queueWfKey(queueName, workflowRunId),
            queueWorkflowsKey(queueName),
            queueRotationKey(queueName),
            stepKey(stepExecId),
            stepExecId,
            workflowRunId,
        ]);
    }

    async enqueue(stepExecId: string, queueName: string, workflowRunId: string): Promise<void> {
        await this.redis.send("EVAL", [
            FAIR_ENQUEUE_SCRIPT,
            "4",
            queueWfKey(queueName, workflowRunId),
            queueWorkflowsKey(queueName),
            queueRotationKey(queueName),
            stepKey(stepExecId),
            stepExecId,
            workflowRunId,
        ]);
    }

    async enqueueWithRetry(
        stepExecId: string,
        queueName: string,
        workflowRunId: string,
        maxAttempts = 3,
    ): Promise<void> {
        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                await this.enqueue(stepExecId, queueName, workflowRunId);
                return;
            } catch (err) {
                if (attempt === maxAttempts - 1) {
                    throw err;
                }
                await sleep(100 * (attempt + 1));
            }
        }
    }

    async cancelFromQueue(
        queueName: string,
        stepExecId: string,
        workflowRunId: string,
    ): Promise<void> {
        await this.redis.send("EVAL", [
            CANCEL_FROM_QUEUE_SCRIPT,
            "3",
            inflightKey(queueName),
            queueWfKey(queueName, workflowRunId),
            stepKey(stepExecId),
            stepExecId,
        ]);
    }

    async acquireRateLimit(queue: QueueConfig): Promise<boolean> {
        if (!queue.rateLimit) return true;

        const windowKey = Math.floor(Date.now() / queue.rateLimit.windowMs);
        const key = rateLimitKey(queue.name, windowKey);
        const allowed = (await this.redis.send("EVAL", [
            RATE_LIMIT_SCRIPT,
            "1",
            key,
            String(Math.ceil(queue.rateLimit.windowMs / 1000)),
            String(queue.rateLimit.max),
        ])) as number;

        return allowed === 1;
    }

    rateLimitBackoffMs(queue: QueueConfig): number {
        const base = queue.pollIntervalMs ?? 500;
        return base + Math.floor(Math.random() * base);
    }

    async isStepInAnyInflight(
        stepExecId: string,
        queueNames: Iterable<string>,
    ): Promise<boolean> {
        for (const queueName of queueNames) {
            const pos = (await this.redis.send("LPOS", [
                inflightKey(queueName),
                stepExecId,
            ])) as number | null;
            if (pos != null) return true;
        }
        return false;
    }
}
