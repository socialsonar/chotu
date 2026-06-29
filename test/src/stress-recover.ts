/**
 * Resume monitoring an in-flight run after manual worker restart.
 * Usage: STRESS_RUN_ID=<uuid> bun run src/stress-recover.ts
 */
import { spawn, type Subprocess } from "bun";
import { createChotu, resetChotu, WorkflowRunStatus } from "chotu";
import { stressBaseConfig } from "./scrape-stress.workflow";

const instanceCount = Number(process.env.STRESS_INSTANCES ?? 3);
const runId = process.env.STRESS_RUN_ID;

if (!runId) {
    console.error("STRESS_RUN_ID required");
    process.exit(1);
}

async function spawnWorkers(): Promise<Subprocess[]> {
    const workers: Subprocess[] = [];
    for (let i = 0; i < instanceCount; i++) {
        workers.push(
            spawn({
                cmd: ["bun", "run", "src/stress-worker.ts", `--id=${i}`],
                cwd: `${import.meta.dir}/..`,
                env: { ...process.env },
                stdout: "inherit",
                stderr: "inherit",
            }),
        );
    }
    await Bun.sleep(3000);
    return workers;
}

console.log("=".repeat(60));
console.log("CRASH RECOVERY — restart workers for run", runId);
console.log("=".repeat(60));

const workers = await spawnWorkers();
const start = Date.now();

resetChotu();
const monitor = createChotu({
    ...stressBaseConfig(),
    logger: { info: () => {}, warn: console.warn, error: console.error },
});
await monitor.listen({ deferWorkers: true });

const deadline = Date.now() + 900_000;
let status = "unknown";

while (Date.now() < deadline) {
    const run = await monitor.getWorkflowRun(runId);
    const steps = await monitor.getStepExecutions(runId);
    const fetch = steps.filter((s) => s.stepName === "FetchStressStep" && s.status === "completed").length;
    const scrape = steps.filter((s) => s.stepName === "ScrapeStressStep" && s.status === "completed").length;
    const analyse = steps.filter((s) => s.stepName === "AnalyseStressStep" && s.status === "completed").length;
    const pending = steps.filter((s) => s.status === "pending" || s.status === "running").length;

    console.log(
        `[${((Date.now() - start) / 1000).toFixed(0)}s] status=${run?.status} fetch=${fetch} scrape=${scrape} analyse=${analyse} active=${pending}`,
    );

    if (run?.status === WorkflowRunStatus.COMPLETED || run?.status === WorkflowRunStatus.FAILED) {
        status = run.status;
        console.log("  →", run.status, JSON.stringify(run.output));
        break;
    }
    await Bun.sleep(3000);
}

await monitor.shutdown();
resetChotu();
for (const w of workers) w.kill("SIGTERM");

console.log(`\nRECOVERY RESULT: ${status} (${((Date.now() - start) / 1000).toFixed(1)}s)`);
process.exit(status === WorkflowRunStatus.COMPLETED ? 0 : 1);
