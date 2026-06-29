export const DEFAULT_STEP_TIMEOUT_MS = 60_000;
export const DEFAULT_LEASE_BUFFER_MS = 30_000;

export function computeLeaseTtlMs(stepTimeoutMs: number, leaseBufferMs: number): number {
    return stepTimeoutMs + leaseBufferMs;
}

export function resolveStepTimeoutMs(
    stepTimeoutMs: number | undefined,
    defaultStepTimeoutMs: number,
): number {
    return stepTimeoutMs ?? defaultStepTimeoutMs;
}
