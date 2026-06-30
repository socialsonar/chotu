import {
    createChotu,
    defineWorkflow,
    next,
    Step,
    Workflow,
    WorkflowRunStatus,
    type NextStepsResult,
} from "chotu";
import { FailWorkflow, MonitorWorkflow } from "./monitor.workflow";

class AsyncRouteStep extends Step<{ value: number }, { value: number }> {
    async run(input: { value: number }, _signal: AbortSignal) {
        return { value: input.value + 1 };
    }

    async getNextSteps(
        _input: { value: number },
        output: { value: number },
        _signal: AbortSignal,
    ): Promise<NextStepsResult> {
        await Bun.sleep(1);
        return next(AsyncTargetStep, output);
    }
}

class AsyncTargetStep extends Step<{ value: number }, { value: number }> {
    async run(input: { value: number }, _signal: AbortSignal) {
        return { value: input.value * 2 };
    }

    getNextSteps(_input: { value: number }, _output: { value: number }, _signal: AbortSignal): NextStepsResult {
        return "END";
    }
}

class AsyncWorkflowClass extends Workflow<{ value: number }, { value: number }> {
    readonly name = "async-route";
    readonly firstStep = AsyncRouteStep;
    readonly steps = [AsyncRouteStep, AsyncTargetStep];
    readonly terminalSteps = [AsyncTargetStep];
}

const AsyncWorkflow = defineWorkflow(AsyncWorkflowClass);

const baseConfig = {
    postgresUrl: process.env.POSTGRES_URL!,
    redisUrl: process.env.REDIS_URL!,
    queues: [
        { name: "search", concurrency: 2, maxRetries: 1, pollIntervalMs: 100 },
        { name: "fetch", concurrency: 5, maxRetries: 1, pollIntervalMs: 100 },
        { name: "aggregate", concurrency: 2, maxRetries: 1, pollIntervalMs: 100 },
    ],
    stepQueues: {
        SearchStep: "search",
        GoogleFetchStep: "fetch",
        BingFetchStep: "fetch",
        AggregateStep: "aggregate",
        FailStep: "search",
        AsyncRouteStep: "search",
        AsyncTargetStep: "fetch",
    },
};

function makeChotu() {
    return createChotu({
        ...baseConfig,
        workflows: [MonitorWorkflow, FailWorkflow, AsyncWorkflow],
    });
}

async function waitForRun(
    chotu: ReturnType<typeof makeChotu>,
    runId: string,
    expected: WorkflowRunStatus,
    timeoutMs = 30_000,
): Promise<void> {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
        const run = await chotu.getWorkflowRun(runId);
        if (run?.status === expected) {
            return;
        }
        if (
            expected !== WorkflowRunStatus.FAILED &&
            run?.status === WorkflowRunStatus.FAILED
        ) {
            throw new Error(`Workflow ${runId} failed unexpectedly`);
        }
        await Bun.sleep(200);
    }

    throw new Error(`Timed out waiting for workflow ${runId} to reach ${expected}`);
}

let chotu = makeChotu();

// Health before listen
console.log("[test] Scenario 0: health before listen");
const healthBefore = await chotu.health();
console.assert(healthBefore.postgres === true, "postgres should be reachable before listen");
console.assert(healthBefore.redis === true, "redis should be reachable before listen");
console.assert(healthBefore.workers === false, "workers should not be started yet");

await chotu.listen();

// 1. Happy path
console.log("[test] Scenario 1: happy path");
const happy = await chotu.runWorkflow("monitor", { url: "https://example.com" });
await waitForRun(chotu, happy.id, WorkflowRunStatus.COMPLETED);
const happyRun = await chotu.getWorkflowRun(happy.id);
const happySteps = await chotu.getStepExecutions(happy.id);
console.log("[test] Happy path output:", happyRun?.output);
console.assert(happySteps.some((s) => s.stepName === "AggregateStep" && s.queue === "aggregate"));
console.assert(happySteps.every((s) => s.queue.length > 0));

// 2. Partial join — one branch fails, workflow still completes
console.log("[test] Scenario 2: partial join failure");
const partial = await chotu.runWorkflow("monitor", {
    url: "https://example.com",
    failPlatform: "google",
});
await waitForRun(chotu, partial.id, WorkflowRunStatus.COMPLETED);
const partialRun = await chotu.getWorkflowRun(partial.id);
console.log("[test] Partial join output:", partialRun?.output);
console.assert(
    (partialRun?.output as { errors?: string[] })?.errors?.length === 1,
    "expected one branch error",
);

// 3. Linear failure — workflow failed
console.log("[test] Scenario 3: linear failure");
const failed = await chotu.runWorkflow("fail", { url: "https://example.com" });
await waitForRun(chotu, failed.id, WorkflowRunStatus.FAILED);
const failedRun = await chotu.getWorkflowRun(failed.id);
console.log("[test] Failed workflow status:", failedRun?.status);
console.assert(failedRun?.status === WorkflowRunStatus.FAILED);

// 4. Query API shape
console.log("[test] Scenario 4: query API");
const steps = await chotu.getStepExecutions(happy.id);
console.assert(steps.length > 0);
console.assert(steps[0].workflowRunId === happy.id);
console.assert(typeof steps[0].queue === "string");

// 5. Async getNextSteps
console.log("[test] Scenario 5: async getNextSteps");
const asyncRun = await chotu.runWorkflow("async-route", { value: 3 });
await waitForRun(chotu, asyncRun.id, WorkflowRunStatus.COMPLETED);
const asyncResult = await chotu.getWorkflowRun(asyncRun.id);
console.assert((asyncResult?.output as { value?: number })?.value === 8);

// 6. Health API
console.log("[test] Scenario 6: health");
const health = await chotu.health();
console.assert(health.postgres === true);
console.assert(health.redis === true);
console.assert(health.workers === true);

// 7. createChotu while started throws
console.log("[test] Scenario 7: createChotu while started throws");
let createWhileStartedThrew = false;
try {
    createChotu(baseConfig);
} catch (err) {
    createWhileStartedThrew =
        err instanceof Error && err.message.includes("Already started");
}
console.assert(createWhileStartedThrew, "createChotu should throw while started");

// 8. Singleton restart with startup recovery
console.log("[test] Scenario 8: singleton restart");
const inFlightRun = await chotu.runWorkflow("async-route", { value: 1 });
await Bun.sleep(300);
await chotu.shutdown();
chotu = makeChotu();
await chotu.listen();
await waitForRun(chotu, inFlightRun.id, WorkflowRunStatus.COMPLETED);
const recoveredRun = await chotu.getWorkflowRun(inFlightRun.id);
console.assert(
    (recoveredRun?.output as { value?: number })?.value === 4,
    "in-flight workflow should complete after restart",
);

// 10. Terminal read consistency (Option B — completed/failed visible in Postgres immediately)
console.log("[test] Scenario 10: terminal read consistency");
const terminalRun = await chotu.runWorkflow("async-route", { value: 10 });
await waitForRun(chotu, terminalRun.id, WorkflowRunStatus.COMPLETED);
const terminalSteps = await chotu.getStepExecutions(terminalRun.id);
console.assert(
    terminalSteps.every(
        (s) =>
            s.status === "completed" ||
            s.status === "failed" ||
            s.status === "waiting",
    ),
    "all step executions should be in terminal or waiting state after workflow completes",
);
console.assert(
    terminalSteps.some((s) => s.status === "completed"),
    "expected at least one completed step",
);

const restartRun = await chotu.runWorkflow("monitor", { url: "https://example.com" });
await waitForRun(chotu, restartRun.id, WorkflowRunStatus.COMPLETED);

// 9. Validation — missing queue for step
console.log("[test] Scenario 9: validation rejects missing queue");
await chotu.shutdown();
let validationThrew = false;
try {
    createChotu({
        ...baseConfig,
        queues: [{ name: "search", concurrency: 1 }],
        stepQueues: {
            SearchStep: "search",
            GoogleFetchStep: "fetch",
            BingFetchStep: "fetch",
            AggregateStep: "aggregate",
            FailStep: "search",
        },
        workflows: [MonitorWorkflow],
    });
} catch (err) {
    validationThrew = err instanceof Error && err.message.includes("not configured");
}
console.assert(validationThrew, "expected validation error for unconfigured queue");

console.log("[test] All scenarios passed");
