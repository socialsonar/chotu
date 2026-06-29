import { afterAll, describe, expect, test } from "bun:test";
import { createChotu, resetChotu, StepExecutionStatus, WorkflowRunStatus } from "chotu";
import { InMemoryHookCounter } from "./hook-metrics";
import { stressBaseConfig, type ScrapeStressInput } from "./scrape-stress.workflow";
import { HAS_ENV } from "./test-env";

const queries = ["hooks stress query a", "hooks stress query b"];
const bqsPerQuery = 3;
const urlsPerBq = 2;
const totalBqs = queries.length * bqsPerQuery;
const totalUrls = totalBqs * urlsPerBq;

/** Expected completed step executions: planner + fetch + scrape + analyse + aggregate */
const expectedCompletedSteps = 1 + totalBqs + totalUrls + totalUrls + 1;

async function waitForRun(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    timeoutMs = 300_000,
) {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
        const run = await chotu.getWorkflowRun(runId);
        if (run?.status === WorkflowRunStatus.FAILED) {
            return run;
        }
        if (run?.status === WorkflowRunStatus.COMPLETED) {
            return run;
        }
        await Bun.sleep(200);
    }

    const run = await chotu.getWorkflowRun(runId);
    throw new Error(`Expected run ${runId} to complete, got ${run?.status ?? "missing"}`);
}

async function waitForHook(
    counter: InMemoryHookCounter,
    field: string,
    min: number,
    timeoutMs = 60_000,
) {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
        if ((counter.counts[field] ?? 0) >= min) return;
        await Bun.sleep(100);
    }

    throw new Error(`Expected hook ${field} >= ${min}, got ${counter.counts[field] ?? 0}`);
}

function countCompleted(steps: Awaited<ReturnType<typeof createChotu>["getStepExecutions"]>) {
    return steps.filter((s) => s.status === StepExecutionStatus.COMPLETED).length;
}

describe.skipIf(!HAS_ENV)("hooks stress", () => {
    afterAll(() => {
        resetChotu();
    });

    test("scrape-stress workflow fires hooks correctly under load", async () => {
        resetChotu();
        const counter = new InMemoryHookCounter();

        const chotu = createChotu({
            ...stressBaseConfig(),
            hooks: counter.hooks(),
        });

        await chotu.listen();

        try {
            const input: ScrapeStressInput = {
                queries,
                bqsPerQuery,
                urlsPerBq,
                seed: 99,
            };

            const { id: runId } = await chotu.runWorkflow("scrape-stress", input);
            const run = await waitForRun(chotu, runId);
            await waitForHook(counter, "workflowCompleted", 1);
            await waitForHook(counter, "stepCompleted", expectedCompletedSteps);
            const steps = await chotu.getStepExecutions(runId);

            expect(run?.status).toBe(WorkflowRunStatus.COMPLETED);
            expect(counter.counts.workflowStarted).toBe(1);
            expect(counter.counts.workflowCompleted).toBe(1);
            expect(counter.counts.workflowError ?? 0).toBe(0);
            expect(counter.counts.cacheMissOnStep ?? 0).toBe(0);
            expect(counter.runCaches.size).toBe(0);

            const completedInDb = countCompleted(steps);
            expect(completedInDb).toBe(expectedCompletedSteps);
            expect(counter.counts.stepCompleted).toBe(completedInDb);
            expect(counter.counts.stepStarted).toBeGreaterThanOrEqual(completedInDb);

            for (const sample of counter.stepCtxSamples) {
                expect(sample.workflowRunId).toBe(runId);
            }

            const stepNames = new Set(counter.stepCtxSamples.map((s) => s.stepName));
            expect(stepNames.has("PlannerStressStep")).toBe(true);
            expect(stepNames.has("FetchStressStep")).toBe(true);
            expect(stepNames.has("ScrapeStressStep")).toBe(true);
            expect(stepNames.has("AnalyseStressStep")).toBe(true);
            expect(stepNames.has("AggregateStressStep")).toBe(true);
        } finally {
            await chotu.shutdown();
            resetChotu();
        }
    }, 300_000);
});
