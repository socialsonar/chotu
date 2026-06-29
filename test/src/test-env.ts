import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const envPath = resolve(import.meta.dir, "../.env");

try {
    const text = readFileSync(envPath, "utf8");
    for (const line of text.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) continue;
        const eq = trimmed.indexOf("=");
        if (eq === -1) continue;
        const key = trimmed.slice(0, eq).trim();
        const value = trimmed.slice(eq + 1).trim();
        if (process.env[key] === undefined) {
            process.env[key] = value;
        }
    }
} catch {
    // ignore — rely on process env
}

export const HAS_ENV = Boolean(process.env.POSTGRES_URL && process.env.REDIS_URL);
