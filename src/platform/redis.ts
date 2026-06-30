import { Redis } from "ioredis";

export interface ChotuRedis {
    connect(): Promise<void>;
    ping(): Promise<string>;
    close(): void;
    send(command: string, args: string[]): Promise<unknown>;
}

export function createRedis(url: string): ChotuRedis {
    const client = new Redis(url, {
        lazyConnect: true,
        maxRetriesPerRequest: null,
    });

    return {
        async connect() {
            if (client.status === "wait" || client.status === "end") {
                await client.connect();
            }
        },
        ping() {
            return client.ping();
        },
        close() {
            client.disconnect();
        },
        send(command, args) {
            return client.call(command, ...args);
        },
    };
}
