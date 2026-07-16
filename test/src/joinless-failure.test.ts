import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import {
    createChotu,
    defineWorkflow,
    next,
    parallel,
    resetChotu,
    runKey,
    runLockKey,
    Step,
    StepExecutionStatus,
    Workflow,
    WorkflowRunStatus,
    type NextStepsResult,
} from "chotu";
import { HAS_ENV } from "./test-env";

class FanOutStep extends Step<{ v: number }, { v: number }> {
    static stepName = "FanOutStep";

    async run(input: { v: number }) {
        return input;
    }

    getNextSteps(input: { v: number }): NextStepsResult {
        return parallel([
            next(FailBranchStep, { v: input.v }),
            next(SlowOkBranchStep, { v: input.v }),
        ]);
    }
}

class FailBranchStep extends Step<{ v: number }, never> {
    static stepName = "FailBranchStep";

    async run(): Promise<never> {
        throw new Error("joinless branch failure");
    }

    getNextSteps(): NextStepsResult {
        return "END";
    }
}

class SlowOkBranchStep extends Step<{ v: number }, { ok: true }> {
    static stepName = "SlowOkBranchStep";

    async run(_input: { v: number }, signal: AbortSignal) {
        await Bun.sleep(1500, { signal });
        return { ok: true as const };
    }

    getNextSteps(): NextStepsResult {
        return "END";
    }
}

class JoinlessFailWorkflow extends Workflow<{ v: number }, { ok: true }> {
    readonly name = "joinless-fail";
    readonly firstStep = FanOutStep;
    readonly steps = [FanOutStep, FailBranchStep, SlowOkBranchStep];
    readonly terminalSteps = [FailBranchStep, SlowOkBranchStep];
}

const joinlessFailWorkflow = defineWorkflow(JoinlessFailWorkflow);

class LinearFailStep extends Step<{ v: number }, never> {
    static stepName = "LinearFailStep";

    async run(): Promise<never> {
        throw new Error("linear failure under held run lock");
    }

    getNextSteps(): NextStepsResult {
        return "END";
    }
}

class LinearFailWorkflow extends Workflow<{ v: number }, never> {
    readonly name = "linear-fail-lock-race";
    readonly firstStep = LinearFailStep;
    readonly steps = [LinearFailStep];
    readonly terminalSteps = [LinearFailStep];
}

const linearFailWorkflow = defineWorkflow(LinearFailWorkflow);

async function waitForStepStatus(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    stepName: string,
    expected: StepExecutionStatus,
    timeoutMs = 30_000,
) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const step = (await chotu.getStepExecutions(runId)).find((s) => s.stepName === stepName);
        if (step?.status === expected) return step;
        await Bun.sleep(50);
    }
    const step = (await chotu.getStepExecutions(runId)).find((s) => s.stepName === stepName);
    throw new Error(
        `Expected step "${stepName}" to reach ${expected}, got ${step?.status ?? "missing"}`,
    );
}

async function waitForRunStatus(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    expected: WorkflowRunStatus,
    timeoutMs = 30_000,
) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const run = await chotu.getWorkflowRun(runId);
        if (run?.status === expected) return run;
        await Bun.sleep(50);
    }
    const run = await chotu.getWorkflowRun(runId);
    throw new Error(`Expected run ${runId} to reach ${expected}, got ${run?.status ?? "missing"}`);
}

async function waitForRedisActiveCount(
    redis: RedisClient,
    runId: string,
    expected: number,
    timeoutMs = 15_000,
) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const raw = (await redis.send("HGET", [runKey(runId), "active_count"])) as string | null;
        if (Number(raw ?? -1) === expected) return;
        await Bun.sleep(50);
    }
    const raw = (await redis.send("HGET", [runKey(runId), "active_count"])) as string | null;
    throw new Error(`Expected active_count=${expected}, got ${raw ?? "missing"}`);
}

describe.skipIf(!HAS_ENV)("joinless parallel failure", () => {
    let redis: RedisClient;

    beforeAll(async () => {
        redis = new RedisClient(process.env.REDIS_URL!);
        await redis.connect();
    });

    afterAll(() => {
        resetChotu();
        redis.close();
    });

    test("stays running after one joinless branch fails while sibling is active, then fails when idle", async () => {
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 2, maxRetries: 0, pollIntervalMs: 50 }],
            stepQueues: {
                FanOutStep: "default",
                FailBranchStep: "default",
                SlowOkBranchStep: "default",
            },
            workflows: [joinlessFailWorkflow],
            purgeOnTerminal: false,
            flushIntervalMs: 100,
        });

        try {
            await chotu.listen();
            const { id } = await chotu.runWorkflow("joinless-fail", { v: 1 });

            await waitForStepStatus(chotu, id, "FailBranchStep", StepExecutionStatus.FAILED);

            const whileSiblingActive = await chotu.getWorkflowRun(id);
            expect(whileSiblingActive?.status).toBe(WorkflowRunStatus.RUNNING);

            const activeCount = Number(
                ((await redis.send("HGET", [runKey(id), "active_count"])) as string | null) ?? "0",
            );
            expect(activeCount).toBeGreaterThan(0);

            const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.FAILED, 10_000);
            expect(run?.status).toBe(WorkflowRunStatus.FAILED);

            const slow = (await chotu.getStepExecutions(id)).find(
                (s) => s.stepName === "SlowOkBranchStep",
            );
            expect(slow?.status).toBe(StepExecutionStatus.COMPLETED);
        } finally {
            await chotu.shutdown();
            resetChotu();
        }
    }, 20_000);

    test("checkCompletion retries after run-lock contention and fails the idle run", async () => {
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, maxRetries: 0, pollIntervalMs: 50 }],
            stepQueues: { LinearFailStep: "default" },
            workflows: [linearFailWorkflow],
            purgeOnTerminal: false,
            flushIntervalMs: 100,
        });

        const lockToken = "test-held-completion-lock";
        let runId = "";

        try {
            await chotu.listen();
            const created = await chotu.runWorkflow("linear-fail-lock-race", { v: 1 });
            runId = created.id;

            const lockAcquired = (await redis.send("SET", [
                runLockKey(runId),
                lockToken,
                "NX",
                "EX",
                "60",
            ])) as string | null;
            expect(lockAcquired).toBe("OK");

            await waitForStepStatus(chotu, runId, "LinearFailStep", StepExecutionStatus.FAILED);
            await waitForRedisActiveCount(redis, runId, 0);

            // While the lock is held, completion cannot finish — run stays running.
            await Bun.sleep(300);
            expect((await chotu.getWorkflowRun(runId))?.status).toBe(WorkflowRunStatus.RUNNING);

            // Releasing the lock lets the retried checkCompletion mark the run failed.
            await redis.send("DEL", [runLockKey(runId)]);
            const run = await waitForRunStatus(chotu, runId, WorkflowRunStatus.FAILED, 5_000);
            expect(run?.status).toBe(WorkflowRunStatus.FAILED);
        } finally {
            if (runId) {
                await redis.send("DEL", [runLockKey(runId)]);
            }
            await chotu.shutdown();
            resetChotu();
        }
    }, 20_000);
});
