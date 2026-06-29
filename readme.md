# chotu

Lightweight workflow orchestrator for Bun.

## Install

```bash
bun add chotu
```

## Usage

```ts
import { createChotu, defineWorkflow, Step, next } from "chotu";

const chotu = createChotu({
  postgresUrl: process.env.POSTGRES_URL!,
  redisUrl: process.env.REDIS_URL!,
  queues: [{ name: "default", concurrency: 2, maxRetries: 3 }],
  stepQueues: { MyStep: "default" },
  workflows: [MyWorkflow],
  defaultStepTimeoutMs: 60_000, // optional — max step runtime (default: 60000)
  leaseBufferMs: 30_000, // optional — added above timeout for execution lease (default: 30000)
});

await chotu.listen();
const { id } = await chotu.runWorkflow("my-workflow", { foo: "bar" });
const run = await chotu.getWorkflowRun(id);
await chotu.shutdown();
```

`createChotu` returns a process-wide singleton. Calling it again while started throws — call `shutdown()` first. After `shutdown()`, `createChotu()` creates a fresh instance automatically. Use `resetChotu()` in tests when you need to clear the singleton without a full shutdown.

## Multi-instance

Run **multiple Chotu processes** against the same Postgres and Redis URLs. Workers across instances share fair queues and compete for steps safely.

- **`concurrency` is per instance** — total workers = sum of `concurrency` across all instances.
- **Startup is non-destructive** — `listen()` never wipes shared Redis queues or live state.
- **Leader-elected recovery** — one instance at a time reclaims inflight/orphaned steps and hydrates missing Redis keys from Postgres.
- **Steps should be idempotent** — at-least-once delivery is possible under retries or lease expiry.

## Architecture

Chotu uses a **Redis execution layer** with **Postgres as the API source of truth**:

- **Redis** — fair queues, inflight tracking, rate limits, claims, leases, join counters, completion CAS
- **Postgres** — API reads (`getWorkflowRun`, `getStepExecutions`), durable audit, cold hydrate when Redis keys are missing

**Consistency:** terminal step/workflow states sync to Postgres immediately (after a Redis CAS). Non-terminal updates (`running`, retry attempts) flush asynchronously via a Redis Stream outbox.

**API reads:** always from Postgres. In-flight status may lag Redis by up to `flushIntervalMs` (accepted tradeoff).

Bun loads `.env` automatically — no `dotenv` needed.

## Config

| Field | Description |
|-------|-------------|
| `postgresUrl` | Postgres connection URL (API reads + durable audit) |
| `redisUrl` | Redis connection URL (live execution state and queues) |
| `postgresMaxConnections` | Postgres pool size (default: 10) |
| `flushIntervalMs` | Async flush interval for non-terminal PG updates (default: 1000) |
| `defaultStepTimeoutMs` | Default max step runtime in ms; every step is timed out (default: 60000) |
| `leaseBufferMs` | Added above step timeout when computing execution lease (default: 30000) |
| `queues` | Worker queue configs (concurrency per instance, retries, rate limits) |
| `stepQueues` | Map step names to queue names |
| `workflows` | Workflow definitions |
| `logger` | Optional `{ info, warn, error }` logger (default: `console`) |
| `hooks` | Optional global lifecycle hooks (see below) |

Every registered step must resolve to a configured queue. Steps omitted from `stepQueues` use the `"default"` queue, which must exist in `queues`.

Workflows must define `completeStep` or `terminalSteps` for completion output.

### Hooks

Pass `hooks` on `createChotu` for cross-cutting workflow/step lifecycle side effects (caching, metrics, tracing). Hooks are awaited; errors are logged and do not fail the run.

**Workflow hooks**

| Hook | When |
|------|------|
| `onWorkflowStarted` | After the run is created and the first step is enqueued |
| `onWorkflowCompleted` | After the workflow is marked completed in Postgres |
| `onWorkflowError` | After the workflow is marked failed in Postgres |

Each receives `{ workflowRunId, workflowName, input }`. Completed adds `output`; error adds optional `reason`.

**Global step hooks**

| Hook | When |
|------|------|
| `onStepStarted` | Worker claimed the step and passed rate limiting |
| `onStepCompleted` | Step finished successfully |
| `onStepFailed` | Step failed; `willRetry` is `true` before requeue, `false` on terminal failure |

Each receives `StepHookContext`: `{ stepExecId, stepName, queue, workflowRunId, workflowName, attempt }`.

**Per-run cache example**

```ts
const runCache = new Map<string, MyCache>();

const chotu = createChotu({
  // ...
  hooks: {
    onWorkflowStarted({ workflowRunId }) {
      runCache.set(workflowRunId, new MyCache());
    },
    onWorkflowCompleted({ workflowRunId }) {
      runCache.delete(workflowRunId);
    },
    onWorkflowError({ workflowRunId }) {
      runCache.delete(workflowRunId);
    },
  },
});
```

Steps can read the cache via `workflowRunId` from `StepHookContext` in `onBeforeRun` / `onAfterRun` / `onError`.

### Queue options

| Field | Description |
|-------|-------------|
| `name` | Queue name |
| `concurrency` | Concurrent workers **per instance** |
| `maxRetries` | Retries after first attempt (default: 3) |
| `rateLimit` | `{ max, windowMs }` rate limit (global per queue via Redis) |
| `pollIntervalMs` | Idle poll interval (default: 500) |

## Steps

Step methods receive an `AbortSignal` that aborts on `shutdown()` or step timeout. Check `signal.aborted` in long-running work.

Every step has a **timeout** (default: `defaultStepTimeoutMs`, 60s). Override per step with `static timeoutMs`. The execution **lease** is derived automatically as `timeout + leaseBufferMs` so crash recovery does not fire before the in-process timeout.

```ts
class MyStep extends Step<I, O> {
  static timeoutMs = 120_000; // optional — overrides defaultStepTimeoutMs for this step

  abstract run(input: I, signal: AbortSignal): Promise<O>;
  abstract getNextSteps(input: I, output: O, signal: AbortSignal): NextStepsResult | Promise<NextStepsResult>;
}

// Optional instance hooks — ctx includes workflowRunId, stepName, attempt, etc.
async onBeforeRun(input: I, ctx: StepHookContext, signal: AbortSignal): Promise<void>;
async onAfterRun(input: I, output: O, ctx: StepHookContext, signal: AbortSignal): Promise<void>;
async onError(input: I, error: Error, ctx: StepHookContext, signal: AbortSignal): Promise<void>;
```

`getNextSteps` may be sync or async. **Implement steps idempotently** where side effects matter.

## Limitations

- One active (`pending`, `running`, or `waiting`) execution per `(workflow_run_id, step_name)` — enforced by Redis active-step keys and a DB unique index.
- Implications: a workflow cannot loop back to the same step name within one run, and parallel branches cannot merge into the same step name.

## API

| Method | Description |
|--------|-------------|
| `listen()` | Connect DB/Redis, migrate schema, leader cold-reconcile if needed, start workers |
| `startWorkers()` | Start workers after `listen({ deferWorkers: true })` |
| `runWorkflow(name, input)` | Start a workflow run |
| `getWorkflowRun(id)` | Fetch workflow run state (Postgres) |
| `getStepExecutions(workflowRunId)` | List step executions (Postgres) |
| `health()` | `{ postgres, redis, workers }` for **this instance** |
| `shutdown()` | Stop workers, abort in-flight steps, close connections |
| `resetChotu()` | Clear singleton (for tests) |

## Build

```bash
bun install
bun run build
```

## Tests

```bash
bun install
cp test/.env.example test/.env   # set POSTGRES_URL and REDIS_URL
bun run test:unit                # integration tests
bun run test                   # example app
bun run test:dev               # example watch mode
```
