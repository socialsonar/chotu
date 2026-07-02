export declare class Semaphore {
    private readonly max;
    private current;
    private readonly queue;
    constructor(max: number);
    acquire(): Promise<void>;
    release(): void;
    run<T>(fn: () => Promise<T>): Promise<T>;
}
//# sourceMappingURL=semaphore.d.ts.map