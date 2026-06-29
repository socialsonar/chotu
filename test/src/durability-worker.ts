/** Worker process for durability multi-instance tests. */
import { createChotu } from "chotu";
import { durabilityBaseConfig } from "./durability.workflow";

const id = process.argv.find((a) => a.startsWith("--id="))?.split("=")[1] ?? "0";

const chotu = createChotu(durabilityBaseConfig());

console.log(`[durability-worker-${id}] starting`);
await chotu.listen();
console.log(`[durability-worker-${id}] ready`);

process.on("SIGTERM", async () => {
    await chotu.shutdown();
    process.exit(0);
});

await new Promise(() => {});
