import { afterAll, describe, expect, test } from "bun:test";
import {
    createChotu,
    defineWorkflow,
    resetChotu,
    Step,
    Workflow,
    StepExecutionStatus,
    WorkflowRunStatus,
} from "chotu";
import { HAS_ENV } from "./test-env";

class SlowStep extends Step<{ v: number }, { done: true }> {
    static stepName = "SlowStep";
    static timeoutMs = 50;

    async run(_input: { v: number }) {
        await Bun.sleep(500);
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class FastStep extends Step<{ v: number }, { done: true }> {
    static stepName = "FastStep";
    static timeoutMs = 1000;

    async run(_input: { v: number }) {
        await Bun.sleep(10);
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class SlowTimeoutWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "slow-timeout-test";
    readonly firstStep = SlowStep;
    readonly steps = [SlowStep];
    readonly terminalSteps = [SlowStep];
}

class FastTimeoutWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "fast-timeout-test";
    readonly firstStep = FastStep;
    readonly steps = [FastStep];
    readonly terminalSteps = [FastStep];
}

const slowWorkflow = defineWorkflow(SlowTimeoutWorkflow);
const fastWorkflow = defineWorkflow(FastTimeoutWorkflow);

async function waitForStepStatus(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    stepName: string,
    expected: StepExecutionStatus,
    timeoutMs = 30_000,
) {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
        const steps = await chotu.getStepExecutions(runId);
        const step = steps.find((s) => s.stepName === stepName);
        if (step?.status === expected) {
            return step;
        }
        await Bun.sleep(100);
    }

    const steps = await chotu.getStepExecutions(runId);
    const step = steps.find((s) => s.stepName === stepName);
    throw new Error(
        `Expected step "${stepName}" to reach ${expected}, got ${step?.status ?? "missing"}`,
    );
}

async function waitForRunStatus(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    expected: WorkflowRunStatus,
    timeoutMs = 15_000,
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

describe.skipIf(!HAS_ENV)("step timeout", () => {
    afterAll(() => {
        resetChotu();
    });

    test("fails step when run exceeds static timeoutMs", async () => {
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, maxRetries: 0, pollIntervalMs: 50 }],
            stepQueues: { SlowStep: "default" },
            workflows: [slowWorkflow],
            flushIntervalMs: 200,
        });

        try {
            await chotu.listen();
            const { id } = await chotu.runWorkflow("slow-timeout-test", { v: 1 });

            const slow = await waitForStepStatus(
                chotu,
                id,
                "SlowStep",
                StepExecutionStatus.FAILED,
            );
            expect(slow.error?.message).toContain("timed out after 50ms");

            const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.FAILED);
            expect(run?.status).toBe(WorkflowRunStatus.FAILED);
        } finally {
            await chotu.shutdown();
            resetChotu();
        }
    }, 15_000);

    test("completes step when run finishes within static timeoutMs", async () => {
        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, maxRetries: 0, pollIntervalMs: 50 }],
            stepQueues: { FastStep: "default" },
            workflows: [fastWorkflow],
            flushIntervalMs: 200,
        });

        try {
            await chotu.listen();
            const { id } = await chotu.runWorkflow("fast-timeout-test", { v: 1 });

            await waitForStepStatus(chotu, id, "FastStep", StepExecutionStatus.COMPLETED);

            const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.COMPLETED);
            expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);
        } finally {
            await chotu.shutdown();
            resetChotu();
        }
    });

    test("fails step when run exceeds defaultStepTimeoutMs", async () => {
        class DefaultSlowStep extends Step<{ v: number }, { done: true }> {
            static stepName = "DefaultSlowStep";

            async run() {
                await Bun.sleep(300);
                return { done: true as const };
            }

            getNextSteps() {
                return "END" as const;
            }
        }

        class DefaultSlowTimeoutWorkflow extends Workflow<{ v: number }, { done: true }> {
            readonly name = "default-slow-timeout-test";
            readonly firstStep = DefaultSlowStep;
            readonly steps = [DefaultSlowStep];
            readonly terminalSteps = [DefaultSlowStep];
        }

        const defaultSlowWorkflow = defineWorkflow(DefaultSlowTimeoutWorkflow);

        resetChotu();
        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            defaultStepTimeoutMs: 80,
            leaseBufferMs: 100,
            queues: [{ name: "default", concurrency: 1, maxRetries: 0, pollIntervalMs: 50 }],
            stepQueues: { DefaultSlowStep: "default" },
            workflows: [defaultSlowWorkflow],
            flushIntervalMs: 200,
        });

        try {
            await chotu.listen();
            const { id } = await chotu.runWorkflow("default-slow-timeout-test", { v: 1 });

            const step = await waitForStepStatus(
                chotu,
                id,
                "DefaultSlowStep",
                StepExecutionStatus.FAILED,
            );
            expect(step.error?.message).toContain("timed out after 80ms");

            const run = await waitForRunStatus(chotu, id, WorkflowRunStatus.FAILED);
            expect(run?.status).toBe(WorkflowRunStatus.FAILED);
        } finally {
            await chotu.shutdown();
            resetChotu();
        }
    }, 15_000);
});
