/** Worker process — full listen() with workers. Used by stress-runner for multi-instance. */
import { RedisClient } from "bun";
import { createChotu } from "chotu";
import { createRedisHooks, DEFAULT_HOOKS_PREFIX } from "./hook-metrics";
import { stressBaseConfig } from "./scrape-stress.workflow";

const id = process.argv.find((a) => a.startsWith("--id="))?.split("=")[1] ?? "0";
const hooksEnabled = process.env.STRESS_HOOKS === "1";
const hooksPrefix = process.env.STRESS_HOOKS_PREFIX ?? DEFAULT_HOOKS_PREFIX;

const config = stressBaseConfig();
let hooks: ReturnType<typeof createRedisHooks> | undefined;
let hooksRedis: RedisClient | undefined;

if (hooksEnabled) {
    hooksRedis = new RedisClient(process.env.REDIS_URL!);
    await hooksRedis.connect();
    hooks = createRedisHooks(hooksRedis, hooksPrefix);
}

const chotu = createChotu({
    ...config,
    hooks,
    logger: {
        info: (...args: unknown[]) => console.log(`[worker-${id}]`, ...args),
        warn: (...args: unknown[]) => console.warn(`[worker-${id}]`, ...args),
        error: (...args: unknown[]) => console.error(`[worker-${id}]`, ...args),
    },
});

console.log(`[worker-${id}] starting${hooksEnabled ? " (hooks enabled)" : ""}`);
await chotu.listen();
console.log(`[worker-${id}] ready`);

process.on("SIGTERM", async () => {
    await chotu.shutdown();
    hooksRedis?.close();
    process.exit(0);
});

await new Promise(() => {});
