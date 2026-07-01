import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import {
    createChotu,
    defineWorkflow,
    resetChotu,
    Step,
    Workflow,
    WorkflowRunStatus,
} from "chotu";
import { HAS_ENV } from "./test-env";

class PurgeStep extends Step<{ v: number }, { done: true }> {
    static stepName = "PurgeStep";

    async run() {
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class PurgeWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "purge-terminal";
    readonly firstStep = PurgeStep;
    readonly steps = [PurgeStep];
    readonly terminalSteps = [PurgeStep];
}

const purgeWorkflow = defineWorkflow(PurgeWorkflow);

async function waitForRun(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    expected: WorkflowRunStatus,
    timeoutMs = 30_000,
) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const run = await chotu.getWorkflowRun(runId);
        if (run?.status === expected) return run;
        await Bun.sleep(100);
    }
    const run = await chotu.getWorkflowRun(runId);
    throw new Error(`Expected run ${runId} to reach ${expected}, got ${run?.status ?? "missing"}`);
}

async function redisHasStepForRun(redis: RedisClient, runId: string): Promise<boolean> {
    let cursor = "0";
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
            if (wfRunId === runId) return true;
        }
    } while (cursor !== "0");
    return false;
}

describe.skipIf(!HAS_ENV)("terminal purge", () => {
    let redis: RedisClient;

    beforeAll(async () => {
        redis = new RedisClient(process.env.REDIS_URL!);
        await redis.connect();
    });

    afterAll(() => {
        resetChotu();
        redis.close();
    });

    test("purges Redis and Postgres steps after workflow completes", async () => {
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1 }],
            stepQueues: {},
            workflows: [purgeWorkflow],
        });
        await chotu.listen();

        const { id: runId } = await chotu.runWorkflow("purge-terminal", { v: 1 });
        const run = await waitForRun(chotu, runId, WorkflowRunStatus.COMPLETED);
        expect(run?.output).toEqual({ done: true });

        const steps = await chotu.getStepExecutions(runId);
        expect(steps).toEqual([]);

        const runExists = (await redis.send("EXISTS", [`chotu:run:${runId}`])) as number;
        expect(runExists).toBe(0);

        expect(await redisHasStepForRun(redis, runId)).toBe(false);

        await chotu.shutdown();
    }, 60_000);
});
