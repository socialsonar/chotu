import { RedisClient } from "bun";
import { spawn, type Subprocess } from "bun";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import {
    createChotu,
    resetChotu,
    StepExecutionStatus,
    WorkflowRunStatus,
    type StepExecution,
} from "chotu";
import {
    durabilityBaseConfig,
    type DurabilityInput,
    type DurabilityOutput,
    timeoutProbeConfig,
} from "./durability.workflow";
import { HAS_ENV } from "./test-env";

const TASK_COUNT = 8;
const PERMANENT_FAIL_INDEX = 5;
const WORKER_COUNT = 1;

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

async function waitForRunStatus(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    expected: WorkflowRunStatus,
    timeoutMs = 90_000,
) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const run = await chotu.getWorkflowRun(runId);
        if (run?.status === expected) return run;
        if (
            expected === WorkflowRunStatus.COMPLETED &&
            run?.status === WorkflowRunStatus.FAILED
        ) {
            throw new Error(`Workflow ${runId} failed unexpectedly`);
        }
        await Bun.sleep(200);
    }
    const run = await chotu.getWorkflowRun(runId);
    throw new Error(`Expected run ${runId} to reach ${expected}, got ${run?.status ?? "missing"}`);
}

async function waitForActiveTasks(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    minActive: number,
    timeoutMs = 20_000,
): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const steps = await chotu.getStepExecutions(runId);
        const active = steps.filter(
            (s) =>
                s.stepName === "TaskDurabilityStep" &&
                (s.status === StepExecutionStatus.RUNNING ||
                    s.status === StepExecutionStatus.PENDING),
        ).length;
        if (active >= minActive) return;
        await Bun.sleep(100);
    }
    throw new Error(`Expected at least ${minActive} active task steps`);
}

function countByName(steps: StepExecution[], name: string) {
    const matched = steps.filter((s) => s.stepName === name);
    return {
        completed: matched.filter((s) => s.status === StepExecutionStatus.COMPLETED).length,
        failed: matched.filter((s) => s.status === StepExecutionStatus.FAILED).length,
        active: matched.filter(
            (s) =>
                s.status === StepExecutionStatus.PENDING ||
                s.status === StepExecutionStatus.RUNNING ||
                s.status === StepExecutionStatus.WAITING,
        ).length,
        total: matched.length,
    };
}

async function waitForDurabilitySteps(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    timeoutMs = 30_000,
): Promise<StepExecution[]> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const steps = await chotu.getStepExecutions(runId);
        const tasks = countByName(steps, "TaskDurabilityStep");
        if (
            tasks.total === TASK_COUNT &&
            tasks.completed === TASK_COUNT - 1 &&
            tasks.failed === 1 &&
            tasks.active === 0
        ) {
            return steps;
        }
        await Bun.sleep(300);
    }
    return chotu.getStepExecutions(runId);
}

function assertDurabilityOutput(
    output: DurabilityOutput | null | undefined,
    steps: StepExecution[],
): void {
    expect(output?.totalTasks).toBe(TASK_COUNT);
    expect(output?.failedTasks).toBe(1);
    expect(output?.completedTasks).toBe(TASK_COUNT - 1);

    expect(countByName(steps, "OrchestratorDurabilityStep").completed).toBeGreaterThanOrEqual(1);
    expect(countByName(steps, "JoinDurabilityStep").completed).toBeGreaterThanOrEqual(1);
    expect(countByName(steps, "SummaryDurabilityStep").completed).toBeGreaterThanOrEqual(1);

    const tasks = countByName(steps, "TaskDurabilityStep");
    expect(tasks.total).toBeGreaterThanOrEqual(TASK_COUNT - 1);
    expect(tasks.completed + tasks.failed).toBeGreaterThanOrEqual(TASK_COUNT - 1);
    expect(tasks.failed).toBeGreaterThanOrEqual(1);
}

async function spawnDurabilityWorkers(): Promise<Subprocess[]> {
    const workers: Subprocess[] = [];
    for (let i = 0; i < WORKER_COUNT; i++) {
        workers.push(
            spawn({
                cmd: ["bun", "run", "src/durability-worker.ts", `--id=${i}`],
                cwd: `${import.meta.dir}/..`,
                env: process.env,
                stdout: "ignore",
                stderr: "ignore",
            }),
        );
    }
    await Bun.sleep(2000);
    return workers;
}

async function killWorkers(workers: Subprocess[], signal: "SIGTERM" | "SIGKILL" = "SIGKILL") {
    for (const w of workers) {
        w.kill(signal);
    }
    await Bun.sleep(signal === "SIGKILL" ? 1500 : 1000);
}

function defaultInput(): DurabilityInput {
    return {
        taskCount: TASK_COUNT,
        seed: 42,
        permanentFailIndex: PERMANENT_FAIL_INDEX,
    };
}

describe.skipIf(!HAS_ENV)("durability corner cases", () => {
    let redis: RedisClient;
    let activeWorkers: Subprocess[] = [];

    beforeAll(async () => {
        redis = new RedisClient(process.env.REDIS_URL!);
        await redis.connect();
    });

    afterAll(() => {
        resetChotu();
        redis.close();
    });

    async function trackWorkers(workers: Subprocess[]): Promise<Subprocess[]> {
        activeWorkers.push(...workers);
        return workers;
    }

    afterEach(async () => {
        if (activeWorkers.length) {
            await killWorkers(activeWorkers, "SIGKILL");
            activeWorkers = [];
        }
        resetChotu();
        await Bun.sleep(2000);
    });

    test(
        "multi-instance survives worker crashes and submitter restart",
        async () => {
            let workers = await trackWorkers(await spawnDurabilityWorkers());

            resetChotu();
            const submitter = createChotu(durabilityBaseConfig());
            await submitter.listen({ deferWorkers: true });

            const { id: runId } = await submitter.runWorkflow("durability", defaultInput());

            await Bun.sleep(3000);
            await killWorkers(workers, "SIGKILL");
            workers = await trackWorkers(await spawnDurabilityWorkers());

            await Bun.sleep(3000);
            await killWorkers(workers, "SIGKILL");
            workers = await trackWorkers(await spawnDurabilityWorkers());

            await Bun.sleep(1500);
            await submitter.shutdown();
            resetChotu();

            const submitter2 = createChotu(durabilityBaseConfig());
            await submitter2.listen({ deferWorkers: true });

            const run = await waitForRunStatus(
                submitter2,
                runId,
                WorkflowRunStatus.COMPLETED,
                120_000,
            );
            assertDurabilityOutput(
                run?.output as DurabilityOutput,
                await waitForDurabilitySteps(submitter2, runId),
            );

            await submitter2.shutdown();
        },
        180_000,
    );

    test("cold hydrate after Redis wipe completes workflow", async () => {
        resetChotu();
        const chotu = createChotu(durabilityBaseConfig());
        await chotu.listen({ deferWorkers: true });

        const { id: runId } = await chotu.runWorkflow("durability", defaultInput());
        await chotu.shutdown();
        resetChotu();

        await deleteRedisRunState(redis, runId);

        const chotu2 = createChotu(durabilityBaseConfig());
        await chotu2.listen({ deferWorkers: true });

        const activeCount = (await redis.send("HGET", [
            `chotu:run:${runId}`,
            "active_count",
        ])) as string | null;
        expect(Number(activeCount ?? 0)).toBeGreaterThan(0);
        expect((await chotu2.getWorkflowRun(runId))?.status).toBe(WorkflowRunStatus.RUNNING);

        await chotu2.startWorkers();

        const run = await waitForRunStatus(chotu2, runId, WorkflowRunStatus.COMPLETED, 90_000);
        assertDurabilityOutput(
            run?.output as DurabilityOutput,
            await waitForDurabilitySteps(chotu2, runId),
        );

        await chotu2.shutdown();
    }, 120_000);

    test("shutdown mid-flight recovers and completes workflow", async () => {
        resetChotu();
        const chotu = createChotu(durabilityBaseConfig());
        await chotu.listen({ deferWorkers: true });

        const { id: runId } = await chotu.runWorkflow("durability", defaultInput());
        await chotu.startWorkers();
        await waitForActiveTasks(chotu, runId, 2);

        await chotu.shutdown();
        resetChotu();

        const chotu2 = createChotu(durabilityBaseConfig());
        await chotu2.listen();

        const run = await waitForRunStatus(chotu2, runId, WorkflowRunStatus.COMPLETED, 90_000);
        assertDurabilityOutput(
            run?.output as DurabilityOutput,
            await waitForDurabilitySteps(chotu2, runId),
        );

        await chotu2.shutdown();
    }, 120_000);

    test("SIGKILL worker during hung step, new worker completes workflow", async () => {
        let workers = await trackWorkers(await spawnDurabilityWorkers());

        resetChotu();
        const submitter = createChotu({
            ...durabilityBaseConfig(),
            leaseTtlMs: 1_500,
        });
        await submitter.listen({ deferWorkers: true });

        const { id: runId } = await submitter.runWorkflow("durability", {
            ...defaultInput(),
            hangTaskIndex: 0,
        });
        await waitForActiveTasks(submitter, runId, 1, 30_000);

        await killWorkers(workers, "SIGKILL");
        workers = await trackWorkers(await spawnDurabilityWorkers());

        const run = await waitForRunStatus(submitter, runId, WorkflowRunStatus.COMPLETED, 120_000);
        assertDurabilityOutput(
            run?.output as DurabilityOutput,
            await waitForDurabilitySteps(submitter, runId),
        );

        await submitter.shutdown();
    }, 180_000);

    test("step timeout is retried and workflow completes", async () => {
        resetChotu();
        const chotu = createChotu(timeoutProbeConfig());
        await chotu.listen();

        const { id: runId } = await chotu.runWorkflow("durability-timeout-probe", { v: 1 });

        const run = await waitForRunStatus(chotu, runId, WorkflowRunStatus.COMPLETED, 30_000);
        expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);

        const step = (await chotu.getStepExecutions(runId)).find(
            (s) => s.stepName === "TimeoutProbeStep",
        );
        expect(step?.status).toBe(StepExecutionStatus.COMPLETED);

        await chotu.shutdown();
    }, 60_000);
});
