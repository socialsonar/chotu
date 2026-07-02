import type { ChotuRedis } from "../../platform";
import type { IWorkflowRepository } from "../../interfaces/repository.interface";
import type { ChotuLogger } from "../../logger";
export declare class PgFlusher {
    private readonly redis;
    private readonly repository;
    private readonly flushIntervalMs;
    private readonly logger;
    private running;
    private loopPromise;
    private readonly consumerId;
    constructor(redis: ChotuRedis, repository: IWorkflowRepository, flushIntervalMs: number, logger?: ChotuLogger);
    start(): Promise<void>;
    stop(drainTimeoutMs?: number): Promise<void>;
    private runLoop;
    private reclaimStalePending;
    private flushBatch;
    private processEntries;
    private applyEvent;
}
//# sourceMappingURL=flusher.d.ts.map