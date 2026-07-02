import Redis from "ioredis";
export function createRedis(url) {
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
//# sourceMappingURL=redis.js.map