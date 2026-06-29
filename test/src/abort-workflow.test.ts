import { afterAll, describe, expect, test } from "bun:test";
import {
    createChotu,
    defineWorkflow,
    resetChotu,
    Step,
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

const pendingWorkflow = defineWorkflow({
    name: "abort-pending",
    firstStep: PendingStep,
    steps: [PendingStep],
    terminalSteps: [PendingStep],
});

const slowWorkflow = defineWorkflow({
    name: "abort-slow",
    firstStep: SlowStep,
    steps: [SlowStep],
    terminalSteps: [SlowStep],
});

const failWorkflow = defineWorkflow({
    name: "abort-fail",
    firstStep: FailStep,
    steps: [FailStep],
    terminalSteps: [FailStep],
});

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
        expect(steps.every((s) => s.status === StepExecutionStatus.CANCELLED)).toBe(true);

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
        expect(steps.some((s) => s.status === StepExecutionStatus.CANCELLED)).toBe(true);

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
});
