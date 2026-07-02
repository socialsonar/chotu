import { sleep } from "../../platform/sleep";
import { parseRedisFields } from "../../domain/execution.mapper";
import { defaultLogger } from "../../logger";
import { SYNC_CONSUMER_GROUP, SYNC_STREAM } from "../redis/keys";
const PEL_RECLAIM_MS = 30_000;
export class PgFlusher {
    redis;
    repository;
    flushIntervalMs;
    logger;
    running = false;
    loopPromise = null;
    consumerId = `flusher-${crypto.randomUUID().slice(0, 8)}`;
    constructor(redis, repository, flushIntervalMs, logger = defaultLogger) {
        this.redis = redis;
        this.repository = repository;
        this.flushIntervalMs = flushIntervalMs;
        this.logger = logger;
    }
    async start() {
        if (this.running)
            return;
        this.running = true;
        try {
            await this.redis.send("XGROUP", [
                "CREATE",
                SYNC_STREAM,
                SYNC_CONSUMER_GROUP,
                "0",
                "MKSTREAM",
            ]);
        }
        catch {
            // group already exists
        }
        this.loopPromise = this.runLoop();
    }
    async stop(drainTimeoutMs = 5000) {
        this.running = false;
        if (!this.loopPromise)
            return;
        await Promise.race([
            this.loopPromise,
            sleep(drainTimeoutMs),
        ]);
        this.loopPromise = null;
    }
    async runLoop() {
        while (this.running) {
            try {
                await this.flushBatch();
            }
            catch (err) {
                this.logger.error("[chotu] Flusher error:", err);
            }
            await sleep(this.flushIntervalMs);
        }
        try {
            await this.flushBatch();
        }
        catch (err) {
            this.logger.error("[chotu] Flusher drain error:", err);
        }
    }
    async reclaimStalePending() {
        const minIdle = String(PEL_RECLAIM_MS);
        const result = (await this.redis.send("XAUTOCLAIM", [
            SYNC_STREAM,
            SYNC_CONSUMER_GROUP,
            this.consumerId,
            minIdle,
            "0-0",
            "COUNT",
            "100",
        ]));
        if (!result)
            return [];
        return result[1] ?? [];
    }
    async flushBatch() {
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
        ]));
        if (!result?.length)
            return;
        const [, entries] = result[0];
        await this.processEntries(entries);
    }
    async processEntries(entries) {
        const ackIds = [];
        for (const [entryId, flatFields] of entries) {
            const fields = parseRedisFields(flatFields);
            try {
                await this.applyEvent(fields);
                ackIds.push(entryId);
            }
            catch (err) {
                this.logger.error(`[chotu] Failed to flush event ${entryId}:`, err);
            }
        }
        if (ackIds.length) {
            await this.redis.send("XACK", [SYNC_STREAM, SYNC_CONSUMER_GROUP, ...ackIds]);
        }
    }
    async applyEvent(fields) {
        const type = fields.type;
        const version = Number(fields.version ?? 0);
        if (type === "step.status") {
            await this.repository.syncStepStatus({
                id: fields.id,
                status: fields.status,
                updatedAt: fields.updated_at ?? new Date().toISOString(),
                version,
            });
            return;
        }
        if (type === "step.attempts") {
            await this.repository.syncStepAttempts({
                id: fields.id,
                attempts: Number(fields.attempts ?? 0),
                updatedAt: fields.updated_at ?? new Date().toISOString(),
                version,
            });
        }
    }
}
//# sourceMappingURL=flusher.js.map