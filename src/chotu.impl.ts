import { RedisClient, SQL } from "bun";
import { ChotuEngine } from "./engine/engine";
import { ChotuHookRunner } from "./engine/hook-runner";
import { StepRegistry } from "./engine/step-registry";
import { WorkflowLifecycle } from "./engine/workflow-lifecycle";
import { StepExecutor } from "./engine/step-executor";
import { RecoveryService } from "./engine/recovery.service";
import { QueueWorkerPool } from "./engine/queue-worker";
import { Chotu, ChotuConfig, ChotuHealth } from "./interfaces/chotu.interface";
import { StepExecution, WorkflowRun } from "./interfaces/workflow.interface";
import { ensureSchema } from "./schema";
import { defaultLogger } from "./logger";
import { PgFlusher } from "./persistence/pg/flusher";
import { PgRepository } from "./persistence/pg/repository";
import { RedisFairQueue } from "./persistence/redis/fair-queue";
import { RedisStateStore } from "./persistence/redis/state-store";

export default class ChotuImpl implements Chotu {
    private readonly sql: SQL;
    private readonly redis: RedisClient;
    private readonly repository: PgRepository;
    private readonly stateStore: RedisStateStore;
    private readonly fairQueue: RedisFairQueue;
    private readonly flusher: PgFlusher;
    private readonly engine: ChotuEngine;
    private readonly logger;
    private readonly instanceId = `chotu-${crypto.randomUUID().slice(0, 8)}`;
    private started = false;
    private workersStarted = false;

    constructor(
        private readonly config: ChotuConfig,
        private readonly onShutdown?: () => void,
    ) {
        this.logger = config.logger ?? defaultLogger;
        this.sql = new SQL(config.postgresUrl, {
            max: config.postgresMaxConnections ?? 10,
        });
        this.redis = new RedisClient(config.redisUrl);
        this.repository = new PgRepository(this.sql);
        this.stateStore = new RedisStateStore(this.redis);
        this.fairQueue = new RedisFairQueue(this.redis);
        this.flusher = new PgFlusher(
            this.redis,
            this.repository,
            config.flushIntervalMs ?? 1000,
            this.logger,
        );

        const registry = new StepRegistry(
            config.queues,
            config.stepQueues,
            config.workflows,
            {
                defaultStepTimeoutMs: config.defaultStepTimeoutMs,
                leaseBufferMs: config.leaseBufferMs,
            },
        );
        const hookRunner = new ChotuHookRunner(config.hooks, this.logger);
        const lifecycle = new WorkflowLifecycle(
            this.stateStore,
            this.repository,
            this.fairQueue,
            registry,
            this.logger,
            hookRunner,
        );
        const stepExecutor = new StepExecutor(
            lifecycle,
            registry,
            this.fairQueue,
            this.logger,
            hookRunner,
        );
        const recovery = new RecoveryService(
            this.stateStore,
            this.repository,
            this.fairQueue,
            lifecycle,
            registry,
            this.logger,
            this.redis,
            this.instanceId,
        );
        const workerPool = new QueueWorkerPool(
            this.fairQueue,
            this.stateStore,
            stepExecutor,
            recovery,
            registry,
            this.logger,
            this.instanceId,
            hookRunner,
        );

        this.engine = new ChotuEngine(
            this.flusher,
            workerPool,
            lifecycle,
            recovery,
        );
    }

    isStarted(): boolean {
        return this.started;
    }

    areWorkersStarted(): boolean {
        return this.workersStarted;
    }

    private assertStarted() {
        if (!this.started) {
            throw new Error("[chotu] Call listen() before running workflows");
        }
    }

    async listen(options?: { deferWorkers?: boolean }): Promise<void> {
        if (this.started) return;
        this.started = true;

        this.logger.info("[chotu] Hello from chotu!");

        await this.sql`SELECT 1`;
        this.logger.info("[chotu] Postgres connected");

        await ensureSchema(this.sql, this.logger);

        await this.redis.connect();
        await this.redis.ping();
        this.logger.info("[chotu] Redis connected");

        await this.engine.recoverOnStartup();

        if (!options?.deferWorkers) {
            await this.startWorkers();
        }
    }

    async startWorkers(): Promise<void> {
        if (this.workersStarted) return;
        this.workersStarted = true;
        this.engine.setWorkersStarted(true);
        await this.engine.start();
        this.logger.info("[chotu] Ready.");
    }

    async shutdown(): Promise<void> {
        if (!this.started) return;

        await this.engine.stop();

        this.started = false;
        this.workersStarted = false;
        this.engine.setWorkersStarted(false);

        await this.sql.close();
        this.redis.close();

        this.logger.info("[chotu] Stopped.");
        this.onShutdown?.();
    }

    async runWorkflow<I>(name: string, input: I): Promise<{ id: string }> {
        this.assertStarted();
        return this.engine.runWorkflow(name, input);
    }

    async getWorkflowRun(id: string): Promise<WorkflowRun | null> {
        this.assertStarted();
        return this.engine.getWorkflowRun(id);
    }

    async getStepExecutions(workflowRunId: string): Promise<StepExecution[]> {
        this.assertStarted();
        return this.engine.getStepExecutions(workflowRunId);
    }

    async abortWorkflow(workflowRunId: string, reason?: string): Promise<boolean> {
        this.assertStarted();
        return this.engine.abortWorkflow(workflowRunId, reason);
    }

    async health(): Promise<ChotuHealth> {
        let postgres = false;
        let redis = false;

        try {
            await this.sql`SELECT 1`;
            postgres = true;
        } catch {
            postgres = false;
        }

        try {
            await this.redis.connect();
            await this.redis.ping();
            redis = true;
        } catch {
            redis = false;
        }

        return {
            postgres,
            redis,
            workers: this.workersStarted && this.engine.areWorkersStarted(),
        };
    }
}
