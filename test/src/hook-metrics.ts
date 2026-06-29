import { RedisClient } from "bun";
import type { ChotuHooks } from "chotu";

export const DEFAULT_HOOKS_PREFIX = "chotu:stress:hooks";

function countersKey(prefix: string): string {
    return `${prefix}:counters`;
}

function cacheKey(prefix: string, workflowRunId: string): string {
    return `${prefix}:cache:${workflowRunId}`;
}

function parseHash(raw: string[] | Record<string, string> | null): Record<string, number> {
    const out: Record<string, number> = {};
    if (!raw) return out;

    if (Array.isArray(raw)) {
        for (let i = 0; i < raw.length; i += 2) {
            const field = raw[i];
            const value = raw[i + 1];
            if (field != null && value != null) {
                out[field] = Number(value);
            }
        }
        return out;
    }

    for (const [field, value] of Object.entries(raw)) {
        out[field] = Number(value);
    }
    return out;
}

export async function clearHookMetrics(redis: RedisClient, prefix: string): Promise<void> {
    await redis.send("DEL", [countersKey(prefix)]);

    let cursor = "0";
    do {
        const [nextCursor, keys] = (await redis.send("SCAN", [
            cursor,
            "MATCH",
            `${prefix}:cache:*`,
            "COUNT",
            "100",
        ])) as [string, string[]];
        cursor = nextCursor;
        if (keys.length) await redis.send("DEL", keys);
    } while (cursor !== "0");
}

export function createRedisHooks(redis: RedisClient, prefix: string): ChotuHooks {
    const incr = (field: string) =>
        redis.send("HINCRBY", [countersKey(prefix), field, "1"]) as Promise<number>;

    return {
        onWorkflowStarted: async (ctx) => {
            await incr("workflowStarted");
            await redis.send("SET", [`${prefix}:runId`, ctx.workflowRunId]);
            await redis.send("SET", [cacheKey(prefix, ctx.workflowRunId), "1"]);
        },
        onWorkflowCompleted: async (ctx) => {
            await incr("workflowCompleted");
            const exists = await redis.send("GET", [cacheKey(prefix, ctx.workflowRunId)]);
            if (!exists) await incr("cacheLeakOnComplete");
            await redis.send("DEL", [cacheKey(prefix, ctx.workflowRunId)]);
        },
        onWorkflowError: async (ctx) => {
            await incr("workflowError");
            await redis.send("DEL", [cacheKey(prefix, ctx.workflowRunId)]);
        },
        onStepStarted: async (ctx) => {
            await incr("stepStarted");
            const exists = await redis.send("GET", [cacheKey(prefix, ctx.workflowRunId)]);
            if (!exists) await incr("cacheMissOnStep");
        },
        onStepCompleted: async () => {
            await incr("stepCompleted");
        },
        onStepFailed: async (ctx) => {
            if (ctx.willRetry) await incr("stepFailedRetry");
            else await incr("stepFailedTerminal");
        },
    };
}

export async function readHookMetrics(
    redis: RedisClient,
    prefix: string,
): Promise<Record<string, number>> {
    const raw = (await redis.send("HGETALL", [countersKey(prefix)])) as
        | string[]
        | Record<string, string>
        | null;
    return parseHash(raw);
}

export interface HookMetricsExpectations {
    workflowStarted: number;
    workflowCompleted: number;
    workflowError: number;
    stepCompletedMin: number;
    cacheMissOnStep: number;
    cacheLeakOnComplete: number;
}

export function validateHookMetrics(
    metrics: Record<string, number>,
    expected: HookMetricsExpectations,
): string[] {
    const issues: string[] = [];
    const n = (k: string) => metrics[k] ?? 0;

    if (n("workflowStarted") !== expected.workflowStarted) {
        issues.push(
            `workflowStarted=${n("workflowStarted")}, expected ${expected.workflowStarted}`,
        );
    }
    if (n("workflowCompleted") !== expected.workflowCompleted) {
        issues.push(
            `workflowCompleted=${n("workflowCompleted")}, expected ${expected.workflowCompleted}`,
        );
    }
    if (n("workflowError") !== expected.workflowError) {
        issues.push(`workflowError=${n("workflowError")}, expected ${expected.workflowError}`);
    }
    if (n("stepCompleted") < expected.stepCompletedMin) {
        issues.push(
            `stepCompleted=${n("stepCompleted")}, expected >= ${expected.stepCompletedMin}`,
        );
    }
    if (n("cacheMissOnStep") !== expected.cacheMissOnStep) {
        issues.push(`cacheMissOnStep=${n("cacheMissOnStep")}, expected ${expected.cacheMissOnStep}`);
    }
    if (n("cacheLeakOnComplete") !== expected.cacheLeakOnComplete) {
        issues.push(
            `cacheLeakOnComplete=${n("cacheLeakOnComplete")}, expected ${expected.cacheLeakOnComplete}`,
        );
    }

    return issues;
}

export class InMemoryHookCounter {
    readonly counts: Record<string, number> = {};
    readonly runCaches = new Map<string, unknown>();
    readonly stepCtxSamples: Array<{ workflowRunId: string; stepName: string }> = [];

    hooks(): ChotuHooks {
        const incr = (field: string) => {
            this.counts[field] = (this.counts[field] ?? 0) + 1;
        };

        return {
            onWorkflowStarted: (ctx) => {
                incr("workflowStarted");
                this.runCaches.set(ctx.workflowRunId, { created: Date.now() });
            },
            onWorkflowCompleted: (ctx) => {
                incr("workflowCompleted");
                this.runCaches.delete(ctx.workflowRunId);
            },
            onWorkflowError: (ctx) => {
                incr("workflowError");
                this.runCaches.delete(ctx.workflowRunId);
            },
            onStepStarted: (ctx) => {
                incr("stepStarted");
                this.stepCtxSamples.push({
                    workflowRunId: ctx.workflowRunId,
                    stepName: ctx.stepName,
                });
                if (!this.runCaches.has(ctx.workflowRunId)) {
                    incr("cacheMissOnStep");
                }
            },
            onStepCompleted: () => {
                incr("stepCompleted");
            },
            onStepFailed: (ctx) => {
                if (ctx.willRetry) incr("stepFailedRetry");
                else incr("stepFailedTerminal");
            },
        };
    }
}
