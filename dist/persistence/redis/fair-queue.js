import { sleep } from "../../platform/sleep";
import { inflightKey, queueRotationKey, queueWfKey, queueWorkflowsKey, rateLimitKey, stepKey, } from "./keys";
import { ACK_INFLIGHT_SCRIPT, CANCEL_FROM_QUEUE_SCRIPT, FAIR_ENQUEUE_SCRIPT, FAIR_POP_SCRIPT, RATE_LIMIT_SCRIPT, REQUEUE_INFLIGHT_SCRIPT, } from "./scripts";
export class RedisFairQueue {
    redis;
    constructor(redis) {
        this.redis = redis;
    }
    async pop(queueName) {
        return (await this.redis.send("EVAL", [
            FAIR_POP_SCRIPT,
            "3",
            queueRotationKey(queueName),
            queueWorkflowsKey(queueName),
            inflightKey(queueName),
            `chotu:queue:${queueName}:wf:`,
        ]));
    }
    async ack(queueName, stepExecId) {
        await this.redis.send("EVAL", [
            ACK_INFLIGHT_SCRIPT,
            "2",
            inflightKey(queueName),
            stepKey(stepExecId),
            stepExecId,
        ]);
    }
    async requeue(queueName, stepExecId, workflowRunId) {
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
    async enqueue(stepExecId, queueName, workflowRunId) {
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
    async enqueueWithRetry(stepExecId, queueName, workflowRunId, maxAttempts = 3) {
        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                await this.enqueue(stepExecId, queueName, workflowRunId);
                return;
            }
            catch (err) {
                if (attempt === maxAttempts - 1) {
                    throw err;
                }
                await sleep(100 * (attempt + 1));
            }
        }
    }
    async cancelFromQueue(queueName, stepExecId, workflowRunId) {
        await this.redis.send("EVAL", [
            CANCEL_FROM_QUEUE_SCRIPT,
            "3",
            inflightKey(queueName),
            queueWfKey(queueName, workflowRunId),
            stepKey(stepExecId),
            stepExecId,
        ]);
    }
    async acquireRateLimit(queue) {
        if (!queue.rateLimit)
            return true;
        const windowKey = Math.floor(Date.now() / queue.rateLimit.windowMs);
        const key = rateLimitKey(queue.name, windowKey);
        const allowed = (await this.redis.send("EVAL", [
            RATE_LIMIT_SCRIPT,
            "1",
            key,
            String(Math.ceil(queue.rateLimit.windowMs / 1000)),
            String(queue.rateLimit.max),
        ]));
        return allowed === 1;
    }
    rateLimitBackoffMs(queue) {
        const base = queue.pollIntervalMs ?? 500;
        return base + Math.floor(Math.random() * base);
    }
    async isStepInAnyInflight(stepExecId, queueNames) {
        for (const queueName of queueNames) {
            const pos = (await this.redis.send("LPOS", [
                inflightKey(queueName),
                stepExecId,
            ]));
            if (pos != null)
                return true;
        }
        return false;
    }
}
//# sourceMappingURL=fair-queue.js.map