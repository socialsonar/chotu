export declare const DEFAULT_STEP_TIMEOUT_MS = 60000;
export declare const DEFAULT_LEASE_BUFFER_MS = 30000;
export declare function computeLeaseTtlMs(stepTimeoutMs: number, leaseBufferMs: number): number;
export declare function resolveStepTimeoutMs(stepTimeoutMs: number | undefined, defaultStepTimeoutMs: number): number;
//# sourceMappingURL=timeout.d.ts.map