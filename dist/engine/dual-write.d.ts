export declare function writeWithPgFallback<T>(params: {
    redisWrite: () => Promise<T>;
    pgWrite: (result: T) => Promise<void>;
    rollback: (result: T) => Promise<void>;
}): Promise<T>;
//# sourceMappingURL=dual-write.d.ts.map