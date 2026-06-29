/**
 * Multi-instance stress + crash recovery test.
 *
 * queries[] → 50 BQs/query → 20 urls/BQ → scrape → analyse
 *
 * Usage:
 *   POSTGRES_URL=... REDIS_URL=... bun run src/stress-runner.ts
 *
 * Env:
 *   STRESS_QUERIES          pipe-separated or JSON array (default: 2 queries)
 *   STRESS_BQS_PER_QUERY    default 50
 *   STRESS_URLS_PER_BQ      default 20
 *   STRESS_INSTANCES        default 3
 *   STRESS_CRASHES          default 3 (SIGKILL worker cycles)
 *   STRESS_CRASH_AFTER_MS   default 45000 (ms between crash cycles)
 *   STRESS_SEED             default 42
 */
import { spawn, type Subprocess } from "bun";
import { RedisClient } from "bun";
import {
    createChotu,
    resetChotu,
    StepExecutionStatus,
    WorkflowRunStatus,
    type StepExecution,
} from "chotu";
import {
    clearHookMetrics,
    createRedisHooks,
    DEFAULT_HOOKS_PREFIX,
    readHookMetrics,
    validateHookMetrics,
} from "./hook-metrics";
import {
    parseQueries,
    stressBaseConfig,
    type ScrapeStressInput,
    type StressOutput,
} from "./scrape-stress.workflow";

const queries = parseQueries(process.env.STRESS_QUERIES);
const bqsPerQuery = Number(process.env.STRESS_BQS_PER_QUERY ?? 50);
const urlsPerBq = Number(process.env.STRESS_URLS_PER_BQ ?? 20);
const instanceCount = Number(process.env.STRESS_INSTANCES ?? 3);
const crashCount = Number(process.env.STRESS_CRASHES ?? 3);
const crashAfterMs = Number(process.env.STRESS_CRASH_AFTER_MS ?? 45_000);
const seed = Number(process.env.STRESS_SEED ?? 42);
const hooksEnabled = process.env.STRESS_HOOKS === "1";
const hooksPrefix = process.env.STRESS_HOOKS_PREFIX ?? DEFAULT_HOOKS_PREFIX;

if (!process.env.POSTGRES_URL || !process.env.REDIS_URL) {
    console.error("POSTGRES_URL and REDIS_URL required");
    process.exit(1);
}

const totalBqs = queries.length * bqsPerQuery;
const totalUrls = totalBqs * urlsPerBq;

interface Progress {
    status: string;
    planner: number;
    fetch: { completed: number; failed: number; active: number };
    scrape: { completed: number; failed: number; active: number };
    analyse: { completed: number; failed: number; active: number };
}

function countSteps(steps: StepExecution[], name: string) {
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

function progressFromSteps(steps: StepExecution[], status: string): Progress {
    const planner = countSteps(steps, "PlannerStressStep");
    const fetch = countSteps(steps, "FetchStressStep");
    const scrape = countSteps(steps, "ScrapeStressStep");
    const analyse = countSteps(steps, "AnalyseStressStep");
    return {
        status,
        planner: planner.completed,
        fetch: { completed: fetch.completed, failed: fetch.failed, active: fetch.active },
        scrape: { completed: scrape.completed, failed: scrape.failed, active: scrape.active },
        analyse: { completed: analyse.completed, failed: analyse.failed, active: analyse.active },
    };
}

function analyzeSteps(steps: StepExecution[]): string[] {
    const issues: string[] = [];
    const planner = countSteps(steps, "PlannerStressStep");
    const fetch = countSteps(steps, "FetchStressStep");
    const scrape = countSteps(steps, "ScrapeStressStep");
    const analyse = countSteps(steps, "AnalyseStressStep");

    if (planner.completed !== 1) {
        issues.push(`PlannerStressStep completed=${planner.completed} expected=1`);
    }

    const fetchDone = fetch.completed + fetch.failed;
    if (fetchDone < totalBqs * 0.95) {
        issues.push(
            `FetchStressStep done=${fetchDone}/${totalBqs} (completed=${fetch.completed} failed=${fetch.failed} active=${fetch.active})`,
        );
    }

    const scrapeDone = scrape.completed + scrape.failed;
    if (scrapeDone < totalUrls * 0.9) {
        issues.push(
            `ScrapeStressStep done=${scrapeDone}/${totalUrls} (completed=${scrape.completed} failed=${scrape.failed})`,
        );
    }

    const analyseDone = analyse.completed + analyse.failed;
    if (analyseDone < totalUrls * 0.9) {
        issues.push(
            `AnalyseStressStep done=${analyseDone}/${totalUrls} (completed=${analyse.completed} failed=${analyse.failed})`,
        );
    }

    const activeByName = new Map<string, number>();
    for (const s of steps) {
        if (
            s.status === StepExecutionStatus.PENDING ||
            s.status === StepExecutionStatus.RUNNING ||
            s.status === StepExecutionStatus.WAITING
        ) {
            activeByName.set(s.stepName, (activeByName.get(s.stepName) ?? 0) + 1);
        }
    }
    if (activeByName.size > 0) {
        issues.push(
            `Still active: ${[...activeByName.entries()].map(([n, c]) => `${n}=${c}`).join(", ")}`,
        );
    }

    return issues;
}

async function spawnWorkers(): Promise<Subprocess[]> {
    const workers: Subprocess[] = [];
    for (let i = 0; i < instanceCount; i++) {
        workers.push(
            spawn({
                cmd: ["bun", "run", "src/stress-worker.ts", `--id=${i}`],
                cwd: `${import.meta.dir}/..`,
                env: {
                    ...process.env,
                    STRESS_HOOKS: hooksEnabled ? "1" : "",
                    STRESS_HOOKS_PREFIX: hooksPrefix,
                },
                stdout: "inherit",
                stderr: "inherit",
            }),
        );
    }
    await Bun.sleep(3000);
    return workers;
}

async function killWorkers(workers: Subprocess[], signal: "SIGTERM" | "SIGKILL" = "SIGKILL"): Promise<void> {
    for (const w of workers) {
        w.kill(signal);
    }
    await Bun.sleep(signal === "SIGKILL" ? 2000 : 1500);
}

async function waitForRun(
    chotu: ReturnType<typeof createChotu>,
    runId: string,
    timeoutMs: number,
): Promise<{ status: string; output: Record<string, unknown> | null }> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const run = await chotu.getWorkflowRun(runId);
        if (
            run?.status === WorkflowRunStatus.COMPLETED ||
            run?.status === WorkflowRunStatus.FAILED
        ) {
            return { status: run.status, output: run.output };
        }
        await Bun.sleep(1000);
    }
    const run = await chotu.getWorkflowRun(runId);
    return { status: run?.status ?? "timeout", output: run?.output ?? null };
}

function logProgress(label: string, p: Progress, elapsedSec: number): void {
    console.log(
        `[${elapsedSec.toFixed(0)}s] ${label} status=${p.status} ` +
            `fetch=${p.fetch.completed}+${p.fetch.failed}f/${totalBqs} ` +
            `scrape=${p.scrape.completed}+${p.scrape.failed}f/${totalUrls} ` +
            `analyse=${p.analyse.completed}+${p.analyse.failed}f/${totalUrls} ` +
            `active=f${p.fetch.active}s${p.scrape.active}a${p.analyse.active}`,
    );
}

console.log("=".repeat(72));
console.log("CHOTU MULTI-INSTANCE STRESS + CRASH RECOVERY");
console.log("=".repeat(72));
console.log({
    queries,
    bqsPerQuery,
    urlsPerBq,
    totalBqs,
    totalUrls,
    instanceCount,
    crashCount,
    crashAfterMs,
    seed,
    hooksEnabled,
    perInstanceConcurrency: { planner: 50, fetch: 20, scrape: 300, analyse: 50 },
});
console.log("=".repeat(72));

let workers = await spawnWorkers();
let runId = "";
const start = Date.now();
let hooksRedis: RedisClient | undefined;

if (hooksEnabled) {
    hooksRedis = new RedisClient(process.env.REDIS_URL!);
    await hooksRedis.connect();
    await clearHookMetrics(hooksRedis, hooksPrefix);
}

try {
    resetChotu();
    const submitterHooks = hooksEnabled && hooksRedis
        ? createRedisHooks(hooksRedis, hooksPrefix)
        : undefined;

    const submitter = createChotu({
        ...stressBaseConfig(),
        hooks: submitterHooks,
        logger: {
            info: (...args: unknown[]) => console.log("[submitter]", ...args),
            warn: (...args: unknown[]) => console.warn("[submitter]", ...args),
            error: (...args: unknown[]) => console.error("[submitter]", ...args),
        },
    });

    await submitter.listen({ deferWorkers: true });

    const input: ScrapeStressInput = {
        queries,
        bqsPerQuery,
        urlsPerBq,
        seed,
    };

    const { id } = await submitter.runWorkflow("scrape-stress", input);
    runId = id;
    console.log(`[submitter] started run ${runId}`);

    let crashesDone = 0;
    while (crashesDone < crashCount) {
        const crashAt = Date.now() + crashAfterMs;
        while (Date.now() < crashAt) {
            const run = await submitter.getWorkflowRun(runId);
            if (
                run?.status === WorkflowRunStatus.COMPLETED ||
                run?.status === WorkflowRunStatus.FAILED
            ) {
                break;
            }
            const steps = await submitter.getStepExecutions(runId);
            logProgress("monitor", progressFromSteps(steps, run?.status ?? "running"), (Date.now() - start) / 1000);
            await Bun.sleep(5000);
        }

        const run = await submitter.getWorkflowRun(runId);
        if (
            run?.status === WorkflowRunStatus.COMPLETED ||
            run?.status === WorkflowRunStatus.FAILED
        ) {
            console.log(`[submitter] run finished before crash #${crashesDone + 1}`);
            break;
        }

        crashesDone++;
        console.log("\n" + "!".repeat(72));
        console.log(`CRASH #${crashesDone}/${crashCount} — SIGKILL all ${instanceCount} workers`);
        console.log("!".repeat(72));
        await killWorkers(workers, "SIGKILL");
        workers = await spawnWorkers();
    }

    const timeoutMs = Math.max(900_000, totalUrls * 200);
    console.log(`\n[submitter] waiting for completion (timeout ${Math.round(timeoutMs / 1000)}s)...`);

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const run = await submitter.getWorkflowRun(runId);
        if (
            run?.status === WorkflowRunStatus.COMPLETED ||
            run?.status === WorkflowRunStatus.FAILED
        ) {
            break;
        }
        const steps = await submitter.getStepExecutions(runId);
        logProgress("recovery", progressFromSteps(steps, run?.status ?? "running"), (Date.now() - start) / 1000);
        await Bun.sleep(5000);
    }

    const { status, output } = await waitForRun(submitter, runId, 5000);
    const durationMs = Date.now() - start;
    const steps = await submitter.getStepExecutions(runId);
    const issues = analyzeSteps(steps);

    if (status !== WorkflowRunStatus.COMPLETED) {
        issues.unshift(`Workflow ended with status=${status}`);
    } else if (output) {
        const out = output as unknown as StressOutput;
        if (out.totalBqs !== totalBqs || out.totalUrls !== totalUrls) {
            issues.push(
                `Output totals mismatch: bqs=${out.totalBqs}/${totalBqs} urls=${out.totalUrls}/${totalUrls}`,
            );
        }
    }

    await submitter.shutdown();
    resetChotu();

    console.log("\n" + "=".repeat(72));
    console.log("RESULTS");
    console.log("=".repeat(72));
    console.log(`runId=${runId}`);
    console.log(`status=${status} duration=${(durationMs / 1000).toFixed(1)}s crashes=${crashesDone}`);
    console.log(`steps=${steps.length}`);

    const fetch = countSteps(steps, "FetchStressStep");
    const scrape = countSteps(steps, "ScrapeStressStep");
    const analyse = countSteps(steps, "AnalyseStressStep");
    console.log(
        `fetch: ${fetch.completed} ok, ${fetch.failed} fail / ${totalBqs}`,
    );
    console.log(
        `scrape: ${scrape.completed} ok, ${scrape.failed} fail / ${totalUrls}`,
    );
    console.log(
        `analyse: ${analyse.completed} ok, ${analyse.failed} fail / ${totalUrls}`,
    );

    if (hooksEnabled && hooksRedis) {
        const hookMetrics = await readHookMetrics(hooksRedis, hooksPrefix);
        console.log("hook metrics:", hookMetrics);

        const completedSteps = steps.filter(
            (s) => s.status === StepExecutionStatus.COMPLETED,
        ).length;

        const hookIssues = validateHookMetrics(hookMetrics, {
            workflowStarted: 1,
            workflowCompleted: 1,
            workflowError: 0,
            stepCompletedMin: completedSteps,
            cacheMissOnStep: 0,
            cacheLeakOnComplete: 0,
        });

        for (const issue of hookIssues) {
            issues.push(`hooks: ${issue}`);
        }
    }

    if (issues.length) {
        for (const issue of issues) console.log(`  ⚠ ${issue}`);
        console.log(`\nFAILED: ${issues.length} issue(s)`);
        process.exit(1);
    }

    console.log("\nPASSED: workflow completed after crash recovery");
    process.exit(0);
} finally {
    await killWorkers(workers, "SIGTERM");
    hooksRedis?.close();
}
