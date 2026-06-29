import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { QueueConfig } from "../interfaces/queue.interface";
import type { IStateStore } from "../interfaces/state-store.interface";
import { StepExecutionStatus } from "../interfaces/workflow.interface";
import type { ChotuLogger } from "../logger";
import { ChotuHookRunner } from "./hook-runner";
import { StepRegistry } from "./step-registry";
import { RecoveryService } from "./recovery.service";
import { StepExecutor } from "./step-executor";

export class QueueWorkerPool {
    private started = false;
    private workers: Promise<void>[] = [];
    private readonly inFlight = new Set<Promise<unknown>>();
    private readonly inFlightStepIds = new Set<string>();
    private lastRecoveryAt = 0;
    private readonly abortControllers = new Map<string, AbortController>();

    constructor(
        private readonly fairQueue: IFairQueue,
        private readonly stateStore: IStateStore,
        private readonly stepExecutor: StepExecutor,
        private readonly recovery: RecoveryService,
        private readonly registry: StepRegistry,
        private readonly logger: ChotuLogger,
        private readonly instanceId: string,
        private readonly leaseTtlMs: number,
        private readonly hookRunner: ChotuHookRunner,
    ) {}

    isStarted(): boolean {
        return this.started;
    }

    async start(): Promise<void> {
        if (this.started) return;
        this.started = true;

        for (const queue of this.registry.allQueues()) {
            for (let i = 0; i < queue.concurrency; i++) {
                this.workers.push(this.runWorker(queue));
            }
            this.logger.info(
                `[chotu] Queue "${queue.name}" started (concurrency=${queue.concurrency})`,
            );
        }
    }

    async stop(): Promise<void> {
        if (!this.started) return;
        this.started = false;

        for (const controller of this.abortControllers.values()) {
            controller.abort();
        }

        await Promise.allSettled([...this.inFlight]);
        await Promise.allSettled(this.workers);
        this.workers = [];
        this.inFlight.clear();
        this.abortControllers.clear();
    }

    private async runWorker(queue: QueueConfig): Promise<void> {
        const pollMs = queue.pollIntervalMs ?? 500;

        while (this.started) {
            try {
                if (Date.now() - this.lastRecoveryAt > 60_000) {
                    this.lastRecoveryAt = Date.now();
                    if (await this.stateStore.tryAcquireRecoveryLeader(this.instanceId)) {
                        await this.runLeaderRecovery();
                    }
                }

                await this.renewInFlightLeases();

                const stepExecId = await this.fairQueue.pop(queue.name);
                if (!stepExecId) {
                    await Bun.sleep(pollMs);
                    continue;
                }

                const row = await this.stepExecutor.loadStep(stepExecId);
                if (!row) {
                    this.logger.warn(`[chotu] Step execution ${stepExecId} not found after pop`);
                    await this.fairQueue.ack(queue.name, stepExecId);
                    continue;
                }

                const claimed = await this.stateStore.claimStep(
                    stepExecId,
                    this.instanceId,
                    this.leaseTtlMs,
                );
                if (!claimed) {
                    await this.handleFailedClaim(stepExecId, queue.name);
                    continue;
                }

                let inflightHandled = false;
                try {
                    if (!(await this.fairQueue.acquireRateLimit(queue))) {
                        if (
                            !(await this.stepExecutor.setStepStatus(
                                stepExecId,
                                StepExecutionStatus.PENDING,
                            ))
                        ) {
                            await this.fairQueue.ack(queue.name, stepExecId);
                            inflightHandled = true;
                            continue;
                        }
                        await this.fairQueue.requeue(
                            queue.name,
                            stepExecId,
                            claimed.workflow_run_id,
                        );
                        inflightHandled = true;
                        await Bun.sleep(this.fairQueue.rateLimitBackoffMs(queue));
                        continue;
                    }

                    const stepCtx = await this.stepExecutor.buildStepHookContext(claimed);
                    await this.hookRunner.stepStarted(stepCtx);

                    const controller = new AbortController();
                    this.abortControllers.set(stepExecId, controller);
                    this.inFlightStepIds.add(stepExecId);

                    const work = this.stepExecutor.processStepExecution(
                        claimed,
                        queue,
                        controller.signal,
                    );
                    this.inFlight.add(work);
                    try {
                        try {
                            inflightHandled = await work;
                        } catch (err) {
                            this.logger.error(
                                `[chotu] Unexpected error processing step ${stepExecId} ("${claimed.step_name}"):`,
                                err,
                            );
                            await this.stepExecutor.recoverFromWorkerError(
                                stepExecId,
                                claimed,
                                queue,
                            );
                            inflightHandled = true;
                        }
                    } finally {
                        this.inFlight.delete(work);
                        this.inFlightStepIds.delete(stepExecId);
                        this.abortControllers.delete(stepExecId);
                    }
                } finally {
                    if (!inflightHandled) {
                        await this.fairQueue.ack(queue.name, stepExecId);
                    }
                }
            } catch (err) {
                this.logger.error(`[chotu] Worker error on queue "${queue.name}":`, err);
                await Bun.sleep(1000);
            }
        }
    }

    private async renewInFlightLeases(): Promise<void> {
        for (const stepExecId of this.inFlightStepIds) {
            await this.stateStore.renewLease(stepExecId, this.instanceId, this.leaseTtlMs);
        }
    }

    private async runLeaderRecovery(): Promise<void> {
        await this.recovery.recoverInflightSteps();
        await this.recovery.recoverStaleRunningSteps();
        await this.recovery.recoverOrphanedPendingSteps();
        await this.recovery.rebuildJoinStateFromRedis();
    }

    private async handleFailedClaim(stepExecId: string, queueName: string): Promise<void> {
        const row = await this.stepExecutor.loadStep(stepExecId);
        if (!row) {
            this.logger.warn(`[chotu] Claim failed for missing step ${stepExecId}`);
            await this.fairQueue.ack(queueName, stepExecId);
            return;
        }

        if (row.status === StepExecutionStatus.RUNNING) {
            await this.fairQueue.ack(queueName, stepExecId);
            return;
        }

        if (row.status === StepExecutionStatus.PENDING) {
            await this.fairQueue.requeue(queueName, stepExecId, row.workflow_run_id);
            return;
        }

        await this.fairQueue.ack(queueName, stepExecId);
    }
}
