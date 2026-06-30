import { afterAll, describe, expect, test } from "bun:test";
import {
    createChotu,
    defineWorkflow,
    resetChotu,
    Step,
    Workflow,
    type ChotuHooks,
    WorkflowRunStatus,
} from "chotu";
import { HAS_ENV } from "./test-env";

const HAS_ENV_LOADED = HAS_ENV;

const hookCalls: string[] = [];
let transformedInput: { v: number } | undefined;
let completedOutput: { done: true } | null | undefined;

class TransformStep extends Step<{ v: number }, { done: true }> {
    static stepName = "TransformStep";

    async run(input: { v: number }) {
        transformedInput = input;
        return { done: true as const };
    }

    getNextSteps() {
        return "END" as const;
    }
}

class TransformWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "wf-hooks-transform";
    readonly firstStep = TransformStep;
    readonly steps = [TransformStep];
    readonly terminalSteps = [TransformStep];

    async onBeforeStart(input: { v: number }) {
        hookCalls.push("onBeforeStart");
        return { v: input.v + 10 };
    }

    async onAfterCompleted(_input: { v: number }, output: { done: true } | null) {
        hookCalls.push("onAfterCompleted");
        completedOutput = output;
    }
}

class RejectWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "wf-hooks-reject";
    readonly firstStep = TransformStep;
    readonly steps = [TransformStep];
    readonly terminalSteps = [TransformStep];

    async onBeforeStart() {
        throw new Error("before start rejected");
    }
}

class AfterCompletedThrowWorkflow extends Workflow<{ v: number }, { done: true }> {
    readonly name = "wf-hooks-after-throw";
    readonly firstStep = TransformStep;
    readonly steps = [TransformStep];
    readonly terminalSteps = [TransformStep];

    async onAfterCompleted() {
        throw new Error("after completed blew up");
    }
}

const transformWorkflow = defineWorkflow(TransformWorkflow);
const rejectWorkflow = defineWorkflow(RejectWorkflow);
const afterThrowWorkflow = defineWorkflow(AfterCompletedThrowWorkflow);

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

describe.skipIf(!HAS_ENV_LOADED)("workflow hooks", () => {
    afterAll(() => {
        resetChotu();
    });

    test("onBeforeStart transforms input before persistence", async () => {
        resetChotu();
        hookCalls.length = 0;
        transformedInput = undefined;
        completedOutput = undefined;

        const globalCalls: string[] = [];
        const hooks: ChotuHooks = {
            onWorkflowStarted: () => {
                globalCalls.push("onWorkflowStarted");
            },
            onWorkflowCompleted: () => {
                globalCalls.push("onWorkflowCompleted");
            },
        };

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { TransformStep: "default" },
            workflows: [transformWorkflow],
            hooks,
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("wf-hooks-transform", { v: 1 });
        await waitForRun(chotu, id, WorkflowRunStatus.COMPLETED);
        const run = await chotu.getWorkflowRun(id);
        await chotu.shutdown();
        resetChotu();

        expect(hookCalls).toEqual(["onBeforeStart", "onAfterCompleted"]);
        expect(globalCalls).toEqual(["onWorkflowStarted", "onWorkflowCompleted"]);
        expect(transformedInput).toEqual({ v: 11 });
        expect(run?.input).toEqual({ v: 11 });
        expect(completedOutput).toEqual({ done: true });
    });

    test("onBeforeStart throw prevents run creation", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { TransformStep: "default" },
            workflows: [rejectWorkflow],
        });

        await chotu.listen();
        await expect(chotu.runWorkflow("wf-hooks-reject", { v: 1 })).rejects.toThrow(
            "before start rejected",
        );
        await chotu.shutdown();
        resetChotu();
    });

    test("onAfterCompleted throw does not fail completed workflow", async () => {
        resetChotu();

        const chotu = createChotu({
            postgresUrl: process.env.POSTGRES_URL!,
            redisUrl: process.env.REDIS_URL!,
            queues: [{ name: "default", concurrency: 1, pollIntervalMs: 50 }],
            stepQueues: { TransformStep: "default" },
            workflows: [afterThrowWorkflow],
        });

        await chotu.listen();
        const { id } = await chotu.runWorkflow("wf-hooks-after-throw", { v: 2 });
        const run = await waitForRun(chotu, id, WorkflowRunStatus.COMPLETED);
        await chotu.shutdown();
        resetChotu();

        expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);
    });
});
