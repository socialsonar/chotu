import type { RedisClient } from "bun";
import { StepExecutionStatus } from "../../interfaces/workflow.interface";
import { parseRedisFields } from "../../domain/execution.mapper";
import type { IWorkflowRepository } from "../../interfaces/repository.interface";
import type { ChotuLogger } from "../../logger";
import { defaultLogger } from "../../logger";
import { SYNC_CONSUMER_GROUP, SYNC_STREAM } from "../redis/keys";

const PEL_RECLAIM_MS = 30_000;

export class PgFlusher {
    private running = false;
    private loopPromise: Promise<void> | null = null;
    private readonly consumerId = `flusher-${crypto.randomUUID().slice(0, 8)}`;

    constructor(
        private readonly redis: RedisClient,
        private readonly repository: IWorkflowRepository,
        private readonly flushIntervalMs: number,
        private readonly logger: ChotuLogger = defaultLogger,
    ) {}

    async start(): Promise<void> {
        if (this.running) return;
        this.running = true;

        try {
            await this.redis.send("XGROUP", [
                "CREATE",
                SYNC_STREAM,
                SYNC_CONSUMER_GROUP,
                "0",
                "MKSTREAM",
            ]);
        } catch {
            // group already exists
        }

        this.loopPromise = this.runLoop();
    }

    async stop(drainTimeoutMs = 5000): Promise<void> {
        this.running = false;
        if (!this.loopPromise) return;

        await Promise.race([
            this.loopPromise,
            Bun.sleep(drainTimeoutMs),
        ]);
        this.loopPromise = null;
    }

    private async runLoop(): Promise<void> {
        while (this.running) {
            try {
                await this.flushBatch();
            } catch (err) {
                this.logger.error("[chotu] Flusher error:", err);
            }
            await Bun.sleep(this.flushIntervalMs);
        }

        try {
            await this.flushBatch();
        } catch (err) {
            this.logger.error("[chotu] Flusher drain error:", err);
        }
    }

    private async reclaimStalePending(): Promise<[string, string[]][]> {
        const minIdle = String(PEL_RECLAIM_MS);
        const result = (await this.redis.send("XAUTOCLAIM", [
            SYNC_STREAM,
            SYNC_CONSUMER_GROUP,
            this.consumerId,
            minIdle,
            "0-0",
            "COUNT",
            "100",
        ])) as [string, [string, string[]][], string[]] | null;

        if (!result) return [];
        return result[1] ?? [];
    }

    private async flushBatch(): Promise<void> {
        const reclaimed = await this.reclaimStalePending();
        await this.processEntries(reclaimed);

        const result = (await this.redis.send("XREADGROUP", [
            "GROUP",
            SYNC_CONSUMER_GROUP,
            this.consumerId,
            "COUNT",
            "100",
            "BLOCK",
            "100",
            "STREAMS",
            SYNC_STREAM,
            ">",
        ])) as [string, [string, string[]][]][] | null;

        if (!result?.length) return;

        const [, entries] = result[0]!;
        await this.processEntries(entries);
    }

    private async processEntries(entries: [string, string[]][]): Promise<void> {
        const ackIds: string[] = [];

        for (const [entryId, flatFields] of entries) {
            const fields = parseRedisFields(flatFields);
            try {
                await this.applyEvent(fields);
                ackIds.push(entryId);
            } catch (err) {
                this.logger.error(`[chotu] Failed to flush event ${entryId}:`, err);
            }
        }

        if (ackIds.length) {
            await this.redis.send("XACK", [SYNC_STREAM, SYNC_CONSUMER_GROUP, ...ackIds]);
        }
    }

    private async applyEvent(fields: Record<string, string>): Promise<void> {
        const type = fields.type;
        const version = Number(fields.version ?? 0);

        if (type === "step.status") {
            await this.repository.syncStepStatus({
                id: fields.id!,
                status: fields.status as StepExecutionStatus,
                updatedAt: fields.updated_at ?? new Date().toISOString(),
                version,
            });
            return;
        }

        if (type === "step.attempts") {
            await this.repository.syncStepAttempts({
                id: fields.id!,
                attempts: Number(fields.attempts ?? 0),
                updatedAt: fields.updated_at ?? new Date().toISOString(),
                version,
            });
        }
    }
}
