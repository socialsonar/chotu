export interface LatencyStats {
    count: number;
    avgMs: number;
    maxMs: number;
    p50Ms: number;
    p95Ms: number;
}
export declare class EngineMetrics {
    private static dbLatencies;
    private static redisLatencies;
    private static stepLatencies;
    static dbOps: number;
    static redisOps: number;
    static stepsCompleted: number;
    static recordDb(latencyMs: number): void;
    static recordRedis(latencyMs: number): void;
    static recordStep(stepName: string, latencyMs: number): void;
    static snapshot(elapsedSec: number): {
        elapsedSec: number;
        db: {
            count: number;
            avgMs: number;
            maxMs: number;
            p50Ms: number;
            p95Ms: number;
            totalOps: number;
            opsPerSec: number;
        };
        redis: {
            count: number;
            avgMs: number;
            maxMs: number;
            p50Ms: number;
            p95Ms: number;
            totalOps: number;
            opsPerSec: number;
        };
        steps: {
            totalCompleted: number;
            stepsPerSec: number;
            byStep: {
                [k: string]: {
                    count: number;
                    avgMs: number;
                    maxMs: number;
                    p50Ms: number;
                    p95Ms: number;
                    step: string;
                };
            };
        };
    };
    static reset(): void;
    private static stats;
}
export declare function timedDb<T>(fn: () => Promise<T>): Promise<T>;
export declare function timedRedis<T>(fn: () => Promise<T>): Promise<T>;
//# sourceMappingURL=metrics.d.ts.map