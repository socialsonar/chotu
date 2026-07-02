import { afterAll, describe, expect, test } from "bun:test";
import { RedisClient } from "bun";
import {
    createChotu,
    defineWorkflow,
    resetChotu,
    Step,
    stepKey,
    Workflow,
    type ChotuHooks,
    StepExecutionStatus,
    WorkflowRunStatus,
} from "chotu";
import { HAS_ENV } from "./test-env";

class PendingStep extends Step<{ v: number }, { done: true }> {
    static stepName = "PendingStep";

    async run(_input: { v: number }) {
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class SlowStep extends Step<{ v: number }, { done: true }> {
    static stepName = "SlowStep";

    async run(_input: { v: number }, signal: AbortSignal) {
        for (let i = 0; i < 50; i++) {
            if (signal.aborted) throw new Error("Aborted");
            await Bun.sleep(100);
        }
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class FailStep extends Step<{ v: number }, never> {
    static stepName = "FailStep";

    async run() {
        throw new Error("intentional failure");
    }

    getNextSteps(): "END" {
        return "END";
    }
}

class FastIgnoringSignalStep extends Step<{ v: number }, { done: true }> {
    static stepName = "FastIgnoringSignalStep";

    async run(_input: { v: number }, _signal: AbortSignal) {
        await Bun.sleep(50);
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class HangIgnoringSignalStep extends Step<{ v: number }, { done: true }> {
    static stepName = "HangIgnoringSignalStep";
    static timeoutMs = 2_000;

    async run(_input: { v: number }, _signal: AbortSignal) {
        await Bun.sleep(120_000);
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class FastIgnoringSignalWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "abort-fast-ignore";
    readonly firstStep = FastIgnoringSignalStep;
    readonly steps = [FastIgnoringSignalStep];
    readonly terminalSteps = [FastIgnoringSignalStep];
}

class HangIgnoringSignalWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "abort-hang-ignore";
    readonly firstStep = HangIgnoringSignalStep;
    readonly steps = [HangIgnoringSignalStep];
    readonly terminalSteps = [HangIgnoringSignalStep];
}

class PendingWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "abort-pending";
    readonly firstStep = PendingStep;
    readonly steps = [PendingStep];
    readonly terminalSteps = [PendingStep];
}

class SlowWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "abort-slow";
    readonly firstStep = SlowStep;
    readonly steps = [SlowStep];
    readonly terminalSteps = [SlowStep];
}

class FailWorkflow extends Workflow<{ v: number }, never> {
    readonly name = "abort-fail";
    readonly firstStep = FailStep;
    readonly steps = [FailStep];
    readonly terminalSteps = [FailStep];
}

const pendingWorkflow = defineWorkflow(PendingWorkflow);
const slowWorkflow = defineWorkflow(SlowWorkflow);
const failWorkflow = defineWorkflow(FailWorkflow);
const fastIgnoringSignalWorkflow = defineWorkflow(FastIgnoringSignalWorkflow);
const hangIgnoringSignalWorkflow = defineWorkflow(HangIgnoringSignalWorkflow);

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
        await Bun.sleep(100);
    }

    const run = await chotu.getWorkflowRun(runId);
    throw new Error(`Expected run ${runId} to reach ${expected}, got ${run?.status ?? "missing"}`);
}

describe.skipIf(!HAS_ENV)("abortWorkflow", () => {
    afterAll(() => {
        resetChotu();
    });

    test("aborts pending workflow with cancelled status", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { PendingStep: "default" },
            workflows: [pendingWorkflow],
        });

        await chotu.listen({ deferWorkers: true });
        const { id } = await chotu.runWorkflow("abort-pending", { v: 1 });

        const aborted = await chotu.abortWorkflow(id, "user abort");
        expect(aborted).toBe(true);

        await chotu.startWorkers();
        const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.CANCELLED);
        expect(run?.status).toBe(WorkflowRunStatus.CANCELLED);
        expect(run?.output).toEqual({ reason: "user abort" });

        const steps = await chotu.getStepExecutions(id);
        expect(steps).toEqual([]);

        await chotu.shutdown();
        resetChotu();
    });

    test("aborts running workflow", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { SlowStep: "default" },
            workflows: [slowWorkflow],
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("abort-slow", { v: 1 });
        await Bun.sleep(150);

        const aborted = await chotu.abortWorkflow(id, "stopped");
        expect(aborted).toBe(true);

        const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.CANCELLED);
        expect(run?.status).toBe(WorkflowRunStatus.CANCELLED);

        const steps = await chotu.getStepExecutions(id);
        expect(steps).toEqual([]);

        await chotu.shutdown();
        resetChotu();
    });

    test("second abort is idempotent", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { PendingStep: "default" },
            workflows: [pendingWorkflow],
        });

        await chotu.listen({ deferWorkers: true });
        const { id } = await chotu.runWorkflow("abort-pending", { v: 1 });

        expect(await chotu.abortWorkflow(id, "first")).toBe(true);
        expect(await chotu.abortWorkflow(id, "second")).toBe(false);

        await chotu.startWorkers();
        await waitForRunStatus(chotu, id, WorkflowRunStatus.CANCELLED);

        await chotu.shutdown();
        resetChotu();
    });

    test("fires onWorkflowCancelled not onWorkflowError", async () => {
        resetChotu();
        const calls: string[] = [];
        const hooks: ChotuHooks = {
            onWorkflowCancelled: () => {
                calls.push("onWorkflowCancelled");
            },
            onWorkflowError: () => {
                calls.push("onWorkflowError");
            },
            onStepCancelled: () => {
                calls.push("onStepCancelled");
            },
        };

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { PendingStep: "default" },
            workflows: [pendingWorkflow],
            hooks,
        });

        await chotu.listen({ deferWorkers: true });
        const { id } = await chotu.runWorkflow("abort-pending", { v: 1 });
        await chotu.abortWorkflow(id, "hook test");
        await chotu.startWorkers();
        await waitForRunStatus(chotu, id, WorkflowRunStatus.CANCELLED);
        await chotu.shutdown();
        resetChotu();

        expect(calls).toContain("onWorkflowCancelled");
        expect(calls).toContain("onStepCancelled");
        expect(calls).not.toContain("onWorkflowError");
    });

    test("failure path still yields failed not cancelled", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, maxRetries: 0, pollIntervalMs: 50 }],
            stepQueues: { FailStep: "default" },
            workflows: [failWorkflow],
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("abort-fail", { v: 1 });
        const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.FAILED);

        expect(run?.status).toBe(WorkflowRunStatus.FAILED);
        expect(await chotu.abortWorkflow(id)).toBe(false);

        await chotu.shutdown();
        resetChotu();
    });

    test("aborts fast-completing step that ignores signal", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { FastIgnoringSignalStep: "default" },
            workflows: [fastIgnoringSignalWorkflow],
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("abort-fast-ignore", { v: 1 });
        await Bun.sleep(20);

        const aborted = await chotu.abortWorkflow(id, "fast ignore");
        expect(aborted).toBe(true);

        const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.CANCELLED);
        expect(run?.status).toBe(WorkflowRunStatus.CANCELLED);

        await chotu.shutdown();
        resetChotu();
    });

    test("recovery cancels orphaned pending on aborted run", async () => {
        resetChotu();
        const redis = new RedisClient(process.env.REDIS_URL!);
        await redis.connect();

        try {
            const chotu = createChotu({
                postgresUrl: process.env.POSTGRES_URL!,
                redisUrl: process.env.REDIS_URL!,
                queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
                stepQueues: { PendingStep: "default" },
                workflows: [pendingWorkflow],
                purgeOnTerminal: false,
            });

            await chotu.listen({ deferWorkers: true });
            const { id } = await chotu.runWorkflow("abort-pending", { v: 1 });
            await chotu.abortWorkflow(id, "orphan pending");

            const runStepsKey = `chotu:run:${id}:steps`;
            const stepIds = (await redis.send("SMEMBERS", [runStepsKey])) as string[];
            expect(stepIds.length).toBeGreaterThan(0);

            const orphanStepId = crypto.randomUUID();
            await redis.send("HSET", [
                stepKey(orphanStepId),
                "id",
                orphanStepId,
                "workflow_run_id",
                id,
                "step_name",
                "PendingStep",
                "queue",
                "default",
                "status",
                StepExecutionStatus.PENDING,
                "queued",
                "0",
                "attempts",
                "0",
                "version",
                "0",
            ]);
            await redis.send("SADD", [runStepsKey, orphanStepId]);
            await redis.send("HSET", [
                `chotu:run:${id}`,
                "status",
                WorkflowRunStatus.RUNNING,
                "active_count",
                "1",
                "abort_requested",
                "1",
            ]);

            const recovered = await chotu.recoverAbortingRuns();
            expect(recovered).toBeGreaterThan(0);

            const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.CANCELLED);
            expect(run?.status).toBe(WorkflowRunStatus.CANCELLED);

            await chotu.shutdown();
            await redis.send("DEL", [stepKey(orphanStepId), `chotu:run:${id}`, runStepsKey]);
        } finally {
            redis.close();
            resetChotu();
        }
    });

    test("recovery cancels running step with expired lease on aborted run", async () => {
        resetChotu();
        const redis = new RedisClient(process.env.REDIS_URL!);
        await redis.connect();

        try {
            const chotu = createChotu({
                postgresUrl: process.env.POSTGRES_URL!,
                redisUrl: process.env.REDIS_URL!,
                leaseBufferMs: 500,
                queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
                stepQueues: { HangIgnoringSignalStep: "default" },
                workflows: [hangIgnoringSignalWorkflow],
                purgeOnTerminal: false,
            });

            await chotu.listen({ deferWorkers: true });
            const { id } = await chotu.runWorkflow("abort-hang-ignore", { v: 1 });
            const runStepsKey = `chotu:run:${id}:steps`;
            const stepIds = (await redis.send("SMEMBERS", [runStepsKey])) as string[];
            const stepExecId = stepIds[0]!;
            expect(stepExecId).toBeDefined();

            const expiredLeaseUntil = String(Date.now() - 1_000);
            await redis.send("HSET", [
                stepKey(stepExecId),
                "status",
                StepExecutionStatus.RUNNING,
                "lease_owner",
                "dead-worker",
                "lease_until",
                expiredLeaseUntil,
            ]);
            await redis.send("HSET", [
                `chotu:run:${id}`,
                "status",
                WorkflowRunStatus.RUNNING,
                "active_count",
                "1",
            ]);
            await chotu.abortWorkflow(id, "expired lease");

            const recovered = await chotu.recoverAbortingRuns();
            expect(recovered).toBeGreaterThan(0);

            const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.CANCELLED);
            expect(run?.status).toBe(WorkflowRunStatus.CANCELLED);

            await chotu.shutdown();
            await redis.send("DEL", [stepKey(stepExecId), `chotu:run:${id}`, runStepsKey]);
        } finally {
            redis.close();
            resetChotu();
        }
    });
});
