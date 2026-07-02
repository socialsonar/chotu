export interface ChotuRedis {
    connect(): Promise<void>;
    ping(): Promise<string>;
    close(): void;
    send(command: string, args: string[]): Promise<unknown>;
}
export declare function createRedis(url: string): ChotuRedis;
//# sourceMappingURL=redis.d.ts.map