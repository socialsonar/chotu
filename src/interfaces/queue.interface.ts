export interface RateLimitConfig {
    max: number;
    windowMs: number;
}

export interface QueueConfig {
    name: string;
    concurrency: number;
    maxRetries?: number;
    rateLimit?: RateLimitConfig;
    pollIntervalMs?: number;
}
