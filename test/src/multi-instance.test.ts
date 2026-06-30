import { RedisClient } from "bun";
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
    createChotu,
    defineWorkflow,
    FAIR_ENQUEUE_SCRIPT,
    queueRotationKey,
    queueWfKey,
    queueWorkflowsKey,
    resetChotu,
    Step,
    stepKey,
    StepExecutionStatus,
    Workflow,
    type StepHookContext,
    WorkflowRunStatus,
} from "chotu";
import { HAS_ENV } from "./test-env";

async function deleteRedisRunState(redis: RedisClient, runId: string): Promise<void> {
    const prefix = `chotu:run:${runId}`;
    await redis.send("DEL", [`${prefix}`, `${prefix}:steps`]);

    let cursor = "0";
    do {
        const [nextCursor, keys] = (await redis.send("SCAN", [
            cursor,
            "MATCH",
            `${prefix}:*`,
            "COUNT",
            "100",
        ])) as [string, string[]];
        cursor = nextCursor;
        if (keys.length) await redis.send("DEL", keys);
    } while (cursor !== "0");

    cursor = "0";
    do {
        const [nextCursor, keys] = (await redis.send("SCAN", [
            cursor,
            "MATCH",
            "chotu:step:*",
            "COUNT",
            "100",
        ])) as [string, string[]];
        cursor = nextCursor;
        for (const key of keys) {
            const wfRunId = (await redis.send("HGET", [key, "workflow_run_id"])) as string | null;
            if (wfRunId === runId) await redis.send("DEL", [key]);
        }
    } while (cursor !== "0");
}

class OnceStep extends Step<{ v: number }, { done: true }> {
    static stepName = "OnceStep";

    async run(_input: { v: number }) {
        return { done: true as const };
    }

    async getNextSteps() {
        return "END" as const;
    }
}

class OnceWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "once-test";
    readonly firstStep = OnceStep;
    readonly steps = [OnceStep];
    readonly terminalSteps = [OnceStep];
}

const onceWorkflow = defineWorkflow(OnceWorkflow);

class ThrowOnceStep extends Step<{ v: number }, { done: true }> {
    static stepName = "ThrowOnceStep";
    private static attempts = 0;

    static resetAttempts() {
        ThrowOnceStep.attempts = 0;
    }

    async run(_input: { v: number }) {
        return { done: true as const };
    }

    async onAfterRun(_input: { v: number }, _output: { done: true }, _ctx: StepHookContext) {
        ThrowOnceStep.attempts++;
        if (ThrowOnceStep.attempts === 1) {
            throw new Error("transient onAfterRun failure");
        }
    }

    getNextSteps() {
        return "END" as const;
    }
}

class ThrowOnceWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "throw-once-test";
    readonly firstStep = ThrowOnceStep;
    readonly steps = [ThrowOnceStep];
    readonly terminalSteps = [ThrowOnceStep];
}

const throwOnceWorkflow = defineWorkflow(ThrowOnceWorkflow);

describe.skipIf(!HAS_ENV)("multi-instance integration", () => {
    let redis: RedisClient;

    beforeAll(async () => {
        redis = new RedisClient(process.env.REDIS_URL!);
        await redis.connect();
    });

    afterAll(async () => {
        resetChotu();
        redis.close();
    });

    test("listen does not wipe pre-existing Redis queue keys", async () => {
        const marker = `chotu:queue:default:wf:marker-${crypto.randomUUID()}`;
        await redis.send("LPUSH", [marker, "keep-me"]);

        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1 }],
            stepQueues: { OnceStep: "default" },
            workflows: [onceWorkflow],
        });

        await chotu.listen({ deferWorkers: true });

        const len = (await redis.send("LLEN", [marker])) as number;
        expect(len).toBe(1);

        await redis.send("DEL", [marker]);
        await chotu.shutdown();
        resetChotu();
    });

    test("duplicate enqueue is idempotent", async () => {
        const workflowRunId = crypto.randomUUID();
        const stepExecId = crypto.randomUUID();
        const queueName = "default";

        await redis.send("HSET", [
            stepKey(stepExecId),
            "id",
            stepExecId,
            "workflow_run_id",
            workflowRunId,
            "step_name",
            "EchoStep",
            "queue",
            queueName,
            "status",
            "pending",
            "queued",
            "0",
            "attempts",
            "0",
            "version",
            "0",
        ]);

        const enqueue = () =>
            redis.send("EVAL", [
                FAIR_ENQUEUE_SCRIPT,
                "4",
                queueWfKey(queueName, workflowRunId),
                queueWorkflowsKey(queueName),
                queueRotationKey(queueName),
                stepKey(stepExecId),
                stepExecId,
                workflowRunId,
            ]);

        await enqueue();
        await enqueue();

        const wfLen = (await redis.send("LLEN", [
            queueWfKey(queueName, workflowRunId),
        ])) as number;
        expect(wfLen).toBe(1);

        const queued = (await redis.send("HGET", [stepKey(stepExecId), "queued"])) as string;
        expect(queued).toBe("1");

        await redis.send("DEL", [
            stepKey(stepExecId),
            queueWfKey(queueName, workflowRunId),
            queueWorkflowsKey(queueName),
            queueRotationKey(queueName),
        ]);
    });

    test("workflow completes with PG API reads", async () => {
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1 }],
            stepQueues: { OnceStep: "default" },
            workflows: [onceWorkflow],
            flushIntervalMs: 200,
        });

        await chotu.listen();

        const { id } = await chotu.runWorkflow("once-test", { v: 0 });

        const deadline = Date.now() + 30_000;
        let run = await chotu.getWorkflowRun(id);
        while (run?.status === WorkflowRunStatus.RUNNING && Date.now() < deadline) {
            await Bun.sleep(200);
            run = await chotu.getWorkflowRun(id);
        }

        expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);

        const steps = await chotu.getStepExecutions(id);
        const completed = steps.filter((s) => s.status === StepExecutionStatus.COMPLETED);
        expect(completed.length).toBeGreaterThanOrEqual(1);

        await chotu.shutdown();
        resetChotu();
    }, 30_000);

    test("cold hydrate recomputes active_count and does not prematurely complete", async () => {
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, maxRetries: 2 }],
            stepQueues: { OnceStep: "default" },
            workflows: [onceWorkflow],
        });

        await chotu.listen({ deferWorkers: true });
        const { id: runId } = await chotu.runWorkflow("once-test", { v: 1 });
        await chotu.shutdown();
        resetChotu();

        await deleteRedisRunState(redis, runId);

        const chotu2 = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1 }],
            stepQueues: { OnceStep: "default" },
            workflows: [onceWorkflow],
        });

        await chotu2.listen({ deferWorkers: true });

        const activeCount = (await redis.send("HGET", [
            `chotu:run:${runId}`,
            "active_count",
        ])) as string | null;
        expect(Number(activeCount ?? 0)).toBeGreaterThan(0);

        const runBeforeWorkers = await chotu2.getWorkflowRun(runId);
        expect(runBeforeWorkers?.status).toBe(WorkflowRunStatus.RUNNING);

        await chotu2.startWorkers();

        const deadline = Date.now() + 30_000;
        let run = await chotu2.getWorkflowRun(runId);
        while (run?.status === WorkflowRunStatus.RUNNING && Date.now() < deadline) {
            await Bun.sleep(200);
            run = await chotu2.getWorkflowRun(runId);
        }

        expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);

        await chotu2.shutdown();
        resetChotu();
    }, 30_000);

    test("worker recovers from unexpected step error and completes", async () => {
        ThrowOnceStep.resetAttempts();
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, maxRetries: 3, pollIntervalMs: 100 }],
            stepQueues: { ThrowOnceStep: "default" },
            workflows: [throwOnceWorkflow],
            flushIntervalMs: 200,
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("throw-once-test", { v: 0 });

        const deadline = Date.now() + 30_000;
        let run = await chotu.getWorkflowRun(id);
        while (run?.status === WorkflowRunStatus.RUNNING && Date.now() < deadline) {
            await Bun.sleep(200);
            run = await chotu.getWorkflowRun(id);
        }

        expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);
        expect(ThrowOnceStep["attempts"]).toBeGreaterThanOrEqual(2);

        await chotu.shutdown();
        resetChotu();
    });
});
