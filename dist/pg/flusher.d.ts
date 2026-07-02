import type { RedisClient } from "bun";
import type { ChotuLogger } from "../logger";
import { PgRepository } from "./repository";
export declare class PgFlusher {
    private readonly redis;
    private readonly repository;
    private readonly flushIntervalMs;
    private readonly logger;
    private running;
    private loopPromise;
    private readonly consumerId;
    constructor(redis: RedisClient, repository: PgRepository, flushIntervalMs: number, logger?: ChotuLogger);
    start(): Promise<void>;
    stop(drainTimeoutMs?: number): Promise<void>;
    private runLoop;
    private reclaimStalePending;
    private flushBatch;
    private processEntries;
    private parseFields;
    private applyEvent;
}
//# sourceMappingURL=flusher.d.ts.map