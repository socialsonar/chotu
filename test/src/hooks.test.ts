import { afterAll, describe, expect, test } from "bun:test";
import {
    createChotu,
    defineWorkflow,
    resetChotu,
    Step,
    type ChotuHooks,
    type StepHookContext,
    WorkflowRunStatus,
} from "chotu";
import { HAS_ENV } from "./test-env";

const HAS_ENV_LOADED = HAS_ENV;

type HookCall = { name: string; ctx: unknown };

function createHookSpy(): { hooks: ChotuHooks; calls: HookCall[] } {
    const calls: HookCall[] = [];
    const record = (name: string, ctx: unknown) => {
        calls.push({ name, ctx });
    };

    const hooks: ChotuHooks = {
        onWorkflowStarted: (ctx) => record("onWorkflowStarted", ctx),
        onWorkflowCompleted: (ctx) => record("onWorkflowCompleted", ctx),
        onWorkflowError: (ctx) => record("onWorkflowError", ctx),
        onStepStarted: (ctx) => record("onStepStarted", ctx),
        onStepCompleted: (ctx) => record("onStepCompleted", ctx),
        onStepFailed: (ctx) => record("onStepFailed", ctx),
    };

    return { hooks, calls };
}

let capturedStepCtx: StepHookContext | undefined;

class HappyStep extends Step<{ v: number }, { done: true }> {
    static stepName = "HappyStep";

    async onBeforeRun(_input: { v: number }, ctx: StepHookContext) {
        capturedStepCtx = ctx;
    }

    async run(_input: { v: number }) {
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

const happyWorkflow = defineWorkflow({
    name: "hooks-happy",
    firstStep: HappyStep,
    steps: [HappyStep],
    terminalSteps: [HappyStep],
});

const failWorkflow = defineWorkflow({
    name: "hooks-fail",
    firstStep: FailStep,
    steps: [FailStep],
    terminalSteps: [FailStep],
});

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

describe.skipIf(!HAS_ENV_LOADED)("hooks", () => {
    afterAll(() => {
        resetChotu();
    });

    test("fires workflow and step hooks on success", async () => {
        resetChotu();
        capturedStepCtx = undefined;
        const { hooks, calls } = createHookSpy();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { HappyStep: "default" },
            workflows: [happyWorkflow],
            hooks,
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("hooks-happy", { v: 1 });
        await waitForRun(chotu, id, WorkflowRunStatus.COMPLETED);
        await chotu.shutdown();
        resetChotu();

        expect(calls.some((c) => c.name === "onWorkflowStarted")).toBe(true);
        expect(calls.some((c) => c.name === "onWorkflowCompleted")).toBe(true);
        expect(calls.some((c) => c.name === "onStepStarted")).toBe(true);
        expect(calls.some((c) => c.name === "onStepCompleted")).toBe(true);
        expect(calls.some((c) => c.name === "onWorkflowError")).toBe(false);
        expect(calls.some((c) => c.name === "onStepFailed")).toBe(false);

        const started = calls.find((c) => c.name === "onWorkflowStarted")?.ctx as {
            workflowRunId: string;
            workflowName: string;
            input: { v: number };
        };
        expect(started.workflowRunId).toBe(id);
        expect(started.workflowName).toBe("hooks-happy");
        expect(started.input).toEqual({ v: 1 });

        expect(capturedStepCtx?.workflowRunId).toBe(id);
        expect(capturedStepCtx?.workflowName).toBe("hooks-happy");
        expect(capturedStepCtx?.stepName).toBe("HappyStep");
        expect(capturedStepCtx?.attempt).toBe(1);
    });

    test("fires onWorkflowError and onStepFailed on terminal failure", async () => {
        resetChotu();
        const { hooks, calls } = createHookSpy();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, maxRetries: 0, pollIntervalMs: 50 }],
            stepQueues: { FailStep: "default" },
            workflows: [failWorkflow],
            hooks,
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("hooks-fail", { v: 1 });
        await waitForRun(chotu, id, WorkflowRunStatus.FAILED);
        await chotu.shutdown();
        resetChotu();

        const stepFailed = calls.filter((c) => c.name === "onStepFailed");
        expect(stepFailed.length).toBeGreaterThanOrEqual(1);
        expect(stepFailed.some((c) => (c.ctx as { willRetry: boolean }).willRetry === false)).toBe(
            true,
        );
        expect(calls.some((c) => c.name === "onWorkflowError")).toBe(true);
        expect(calls.some((c) => c.name === "onWorkflowCompleted")).toBe(false);
    });

    test("hook throw does not break workflow execution", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { HappyStep: "default" },
            workflows: [happyWorkflow],
            hooks: {
                onWorkflowStarted() {
                    throw new Error("hook blew up");
                },
            },
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("hooks-happy", { v: 2 });
        const run = await waitForRun(chotu, id, WorkflowRunStatus.COMPLETED);
        await chotu.shutdown();
        resetChotu();

        expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);
    });
});
