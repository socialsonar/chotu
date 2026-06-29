import { defineWorkflow, next, parallel, Step, type NextStepsResult } from "chotu";

/** queries[] → 50 BQs/query → 20 urls/BQ → scrape → analyse */

export interface ScrapeStressInput {
    queries: string[];
    bqsPerQuery?: number;
    urlsPerBq?: number;
    seed?: number;
}

export interface BqTask {
    query: string;
    queryIndex: number;
    bqIndex: number;
    bqId: string;
    seed: number;
    urlsPerBq: number;
}

export interface FetchResult {
    bq: BqTask;
    urls: string[];
}

export interface ScrapeTask {
    bq: BqTask;
    url: string;
    urlIndex: number;
}

export interface ScrapeResult {
    bq: BqTask;
    url: string;
    urlIndex: number;
    content: string;
    htmlBytes: number;
    latencyMs: number;
}

export interface AnalyseInput {
    scrape: ScrapeResult;
}

export interface AnalyseResult {
    bq: BqTask;
    url: string;
    score: number;
    keywords: string[];
}

export interface StressOutput {
    queries: string[];
    bqsPerQuery: number;
    urlsPerBq: number;
    totalBqs: number;
    totalUrls: number;
}

export interface WorkflowCompleteInput {
    workflowInput: ScrapeStressInput;
    workflowRunId: string;
}

function rng(seed: number, n: number): number {
    const x = Math.sin(seed * 9999 + n * 7919) * 10000;
    return x - Math.floor(x);
}

function taskKey(bq: BqTask, urlIndex: number, kind: string): number {
    return bq.queryIndex * 10_000 + bq.bqIndex * 100 + urlIndex + kind.length;
}

function shouldFail(_seed: number, _key: number, _kind: "fetch" | "scrape" | "analyse"): boolean {
    return false;
}

function shouldTimeout(seed: number, key: number): boolean {
    return rng(seed, key + 2000) < 0.03;
}

function delayMs(seed: number, key: number, kind: "plan" | "fetch" | "scrape" | "analyse"): number {
    const r = rng(seed, key + kind.length);
    if (kind === "plan") return 30 + Math.floor(r * 120);
    if (kind === "fetch") {
        if (r < 0.08) return 400 + Math.floor(r * 1200);
        return 40 + Math.floor(r * 180);
    }
    if (kind === "scrape") {
        if (r < 0.06) return 600 + Math.floor(r * 2400);
        if (r < 0.2) return 150 + Math.floor(r * 350);
        return 20 + Math.floor(r * 90);
    }
    if (r < 0.1) return 200 + Math.floor(r * 500);
    return 25 + Math.floor(r * 75);
}

async function sleep(ms: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted) throw new Error("Aborted");
    await Bun.sleep(ms);
    if (signal.aborted) throw new Error("Aborted");
}

class PlannerStressStep extends Step<ScrapeStressInput, { bqs: BqTask[]; seed: number }> {
    static stepName = "PlannerStressStep";

    async run(input: ScrapeStressInput, signal: AbortSignal): Promise<{ bqs: BqTask[]; seed: number }> {
        const seed = input.seed ?? 42;
        const bqsPerQuery = input.bqsPerQuery ?? 50;
        const urlsPerBq = input.urlsPerBq ?? 20;
        await sleep(delayMs(seed, 0, "plan"), signal);

        const bqs: BqTask[] = [];
        for (let qi = 0; qi < input.queries.length; qi++) {
            const query = input.queries[qi]!;
            for (let bi = 0; bi < bqsPerQuery; bi++) {
                bqs.push({
                    query,
                    queryIndex: qi,
                    bqIndex: bi,
                    bqId: `q${qi}-bq${bi}`,
                    seed,
                    urlsPerBq,
                });
            }
        }
        return { bqs, seed };
    }

    getNextSteps(input: ScrapeStressInput, output: { bqs: BqTask[] }, _signal: AbortSignal): NextStepsResult {
        return parallel(
            output.bqs.map((bq) => next(FetchStressStep, bq)),
        );
    }
}

class FetchStressStep extends Step<BqTask, FetchResult> {
    static stepName = "FetchStressStep";

    async run(bq: BqTask, signal: AbortSignal): Promise<FetchResult> {
        const key = taskKey(bq, 0, "fetch");
        const ms = delayMs(bq.seed, key, "fetch");
        if (shouldTimeout(bq.seed, key)) {
            await sleep(ms + 8000, signal);
        } else {
            await sleep(ms, signal);
        }
        if (shouldFail(bq.seed, key, "fetch")) {
            throw new Error(`Simulated fetch failure: ${bq.bqId}`);
        }

        const urlsPerBq = bq.urlsPerBq;
        const urls: string[] = [];
        for (let u = 0; u < urlsPerBq; u++) {
            urls.push(
                `https://example.com/${encodeURIComponent(bq.query)}/${bq.bqId}/u${u}`,
            );
        }
        return { bq, urls };
    }

    getNextSteps(_input: BqTask, output: FetchResult, _signal: AbortSignal): NextStepsResult {
        return parallel(
            output.urls.map((url, urlIndex) =>
                next(ScrapeStressStep, { bq: output.bq, url, urlIndex }),
            ),
        );
    }
}

class ScrapeStressStep extends Step<ScrapeTask, ScrapeResult> {
    static stepName = "ScrapeStressStep";

    async run(task: ScrapeTask, signal: AbortSignal): Promise<ScrapeResult> {
        const key = taskKey(task.bq, task.urlIndex, "scrape");
        const ms = delayMs(task.bq.seed, key, "scrape");
        if (shouldTimeout(task.bq.seed, key)) {
            await sleep(ms + 6000, signal);
        } else {
            await sleep(ms, signal);
        }
        if (shouldFail(task.bq.seed, key, "scrape")) {
            throw new Error(`Simulated scrape failure: ${task.url}`);
        }

        return {
            bq: task.bq,
            url: task.url,
            urlIndex: task.urlIndex,
            content: `<html>${task.url}</html>`,
            htmlBytes: 900 + task.urlIndex * 11,
            latencyMs: ms,
        };
    }

    getNextSteps(_input: ScrapeTask, output: ScrapeResult, _signal: AbortSignal): NextStepsResult {
        return next(AnalyseStressStep, { scrape: output });
    }
}

class AnalyseStressStep extends Step<AnalyseInput, AnalyseResult> {
    static stepName = "AnalyseStressStep";

    async run(input: AnalyseInput, signal: AbortSignal): Promise<AnalyseResult> {
        const { scrape } = input;
        const key = taskKey(scrape.bq, scrape.urlIndex, "analyse");
        await sleep(delayMs(scrape.bq.seed, key, "analyse"), signal);
        if (shouldFail(scrape.bq.seed, key, "analyse")) {
            throw new Error(`Simulated analyse failure: ${scrape.url}`);
        }

        return {
            bq: scrape.bq,
            url: scrape.url,
            score: 0.4 + rng(scrape.bq.seed, key) * 0.6,
            keywords: [scrape.bq.query, scrape.bq.bqId, "stress"],
        };
    }

    getNextSteps(_input: AnalyseInput, _output: AnalyseResult, _signal: AbortSignal): NextStepsResult {
        return "END";
    }
}

class AggregateStressStep extends Step<WorkflowCompleteInput, StressOutput> {
    static stepName = "AggregateStressStep";

    async run(input: WorkflowCompleteInput, _signal: AbortSignal): Promise<StressOutput> {
        const wf = input.workflowInput;
        const bqsPerQuery = wf.bqsPerQuery ?? 50;
        const urlsPerBq = wf.urlsPerBq ?? 20;
        const totalBqs = wf.queries.length * bqsPerQuery;
        return {
            queries: wf.queries,
            bqsPerQuery,
            urlsPerBq,
            totalBqs,
            totalUrls: totalBqs * urlsPerBq,
        };
    }

    getNextSteps(_input: WorkflowCompleteInput, _output: StressOutput, _signal: AbortSignal): NextStepsResult {
        return "END";
    }
}

export const ScrapeStressWorkflow = defineWorkflow<ScrapeStressInput>({
    name: "scrape-stress",
    firstStep: PlannerStressStep,
    steps: [
        PlannerStressStep,
        FetchStressStep,
        ScrapeStressStep,
        AnalyseStressStep,
        AggregateStressStep,
    ],
    completeStep: AggregateStressStep,
});

export function stressBaseConfig() {
    return {
        postgresUrl: process.env.POSTGRES_URL!,
        redisUrl: process.env.REDIS_URL!,
        defaultStepTimeoutMs: 30_000,
        leaseBufferMs: 5_000,
        flushIntervalMs: 500,
        queues: [
            { name: "planner", concurrency: 50, maxRetries: 2, pollIntervalMs: 50 },
            { name: "fetch", concurrency: 20, maxRetries: 3, pollIntervalMs: 50 },
            { name: "scrape", concurrency: 300, maxRetries: 3, pollIntervalMs: 50 },
            { name: "analyse", concurrency: 50, maxRetries: 3, pollIntervalMs: 50 },
            { name: "aggregate", concurrency: 2, maxRetries: 2, pollIntervalMs: 50 },
        ],
        stepQueues: {
            PlannerStressStep: "planner",
            FetchStressStep: "fetch",
            ScrapeStressStep: "scrape",
            AnalyseStressStep: "analyse",
            AggregateStressStep: "aggregate",
        },
        workflows: [ScrapeStressWorkflow],
    };
}

export function parseQueries(raw: string | undefined): string[] {
    if (!raw?.trim()) {
        return ["best running shoes 2026", "wireless headphones review"];
    }
    if (raw.startsWith("[")) {
        return JSON.parse(raw) as string[];
    }
    return raw.split("|").map((q) => q.trim()).filter(Boolean);
}
