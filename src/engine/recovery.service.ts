import type { ChotuRedis } from "../platform";
import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { IWorkflowRepository } from "../interfaces/repository.interface";
import type { IStateStore } from "../interfaces/state-store.interface";
import { StepExecutionStatus, WorkflowRunStatus } from "../interfaces/workflow.interface";
import type { ChotuLogger } from "../logger";
import { inflightKey, STARTUP_RECONCILE_KEY } from "../persistence/redis/keys";
import { StepRegistry } from "./step-registry";
import { WorkflowLifecycle } from "./workflow-lifecycle";

export class RecoveryService {
    constructor(
        private readonly stateStore: IStateStore,
        private readonly repository: IWorkflowRepository,
        private readonly fairQueue: IFairQueue,
        private readonly lifecycle: WorkflowLifecycle,
        private readonly registry: StepRegistry,
        private readonly logger: ChotuLogger,
        private readonly redis: ChotuRedis,
        private readonly instanceId: string,
    ) {}

    async recoverOnStartup(): Promise<number> {
        this.logger.info("[chotu] Multi-instance startup (non-destructive)");
        const isLeader = await this.stateStore.tryAcquireStartupReconcile(this.instanceId);
        try {
            return await this.coldStartupReconcile(isLeader);
        } finally {
            if (isLeader) {
                await this.redis.send("DEL", [STARTUP_RECONCILE_KEY]);
            }
        }
    }

    async coldStartupReconcile(isLeader: boolean): Promise<number> {
        let hydrated = 0;
        let enqueued = 0;
        const affectedRunIds = new Set<string>();

        const pendingRows = await this.repository.listPendingSteps();
        for (let i = 0; i < pendingRows.length; i++) {
            const row = pendingRows[i]!;
            affectedRunIds.add(row.workflow_run_id);

            if (i > 0 && i % 50 === 0 && isLeader) {
                await this.stateStore.tryAcquireStartupReconcile(this.instanceId);
            }

            if (!(await this.stateStore.existsStep(row.id))) {
                const stepRow = await this.repository.getStepRow(row.id);
                if (stepRow) {
                    if (!(await this.stateStore.existsRun(row.workflow_run_id))) {
                        const runRow = await this.repository.getRunForHydrate(row.workflow_run_id);
                        if (runRow) {
                            await this.stateStore.hydrateRunIfMissing(runRow);
                            hydrated++;
                        }
                    }
                    await this.stateStore.hydrateStepIfMissing(stepRow);
                    hydrated++;
                }
            }

            if (
                isLeader &&
                (await this.reEnqueueIfPending(row.id, row.queue ?? "default", row.workflow_run_id))
            ) {
                enqueued++;
            }
        }

        for (const runId of affectedRunIds) {
            await this.stateStore.recomputeRunActiveCount(runId);
        }

        if (hydrated > 0 || enqueued > 0) {
            this.logger.info(
                `[chotu] Cold reconcile hydrated=${hydrated} re-enqueued=${enqueued}`,
            );
        }

        return enqueued;
    }

    async recoverStaleRunningSteps(): Promise<number> {
        let recovered = 0;
        const stepIds = await this.stateStore.scanStepIds("chotu:step:*");

        for (const stepExecId of stepIds) {
            const row = await this.lifecycle.loadStep(stepExecId);
            if (!row || row.status !== StepExecutionStatus.RUNNING) continue;
            if (await this.shouldSkipRun(row.workflow_run_id)) continue;
            if (row.lease_until > Date.now()) continue;

            const reset = await this.stateStore.resetExpiredLease(stepExecId);
            if (!reset) continue;

            await this.fairQueue.enqueueWithRetry(
                stepExecId,
                row.queue ?? "default",
                row.workflow_run_id,
            );
            recovered++;
        }

        if (recovered > 0) {
            this.logger.info(`[chotu] Recovered ${recovered} stale running step(s)`);
        }

        return recovered;
    }

    async recoverInflightSteps(): Promise<number> {
        let recovered = 0;

        for (const queueName of this.registry.queueNames()) {
            const key = inflightKey(queueName);
            const items = (await this.redis.send("LRANGE", [key, "0", "-1"])) as string[];
            if (!items?.length) continue;

            for (const stepExecId of items) {
                const row = await this.lifecycle.loadStep(stepExecId);
                if (!row) {
                    await this.fairQueue.ack(queueName, stepExecId);
                    continue;
                }

                if (await this.shouldSkipRun(row.workflow_run_id)) {
                    await this.fairQueue.ack(queueName, stepExecId);
                    continue;
                }

                if (row.status === StepExecutionStatus.PENDING) {
                    await this.fairQueue.requeue(queueName, stepExecId, row.workflow_run_id);
                    recovered++;
                    continue;
                }

                if (row.status === StepExecutionStatus.RUNNING && row.lease_until <= Date.now()) {
                    await this.stateStore.resetExpiredLease(stepExecId);
                    await this.fairQueue.requeue(queueName, stepExecId, row.workflow_run_id);
                    recovered++;
                    continue;
                }

                if (
                    row.status === StepExecutionStatus.COMPLETED ||
                    row.status === StepExecutionStatus.FAILED ||
                    row.status === StepExecutionStatus.CANCELLED ||
                    row.status === StepExecutionStatus.WAITING ||
                    row.status === StepExecutionStatus.RUNNING
                ) {
                    await this.fairQueue.ack(queueName, stepExecId);
                }
            }
        }

        if (recovered > 0) {
            this.logger.info(`[chotu] Recovered ${recovered} inflight step(s)`);
        }

        return recovered;
    }

    async recoverOrphanedPendingSteps(): Promise<number> {
        let recovered = 0;
        const stepIds = await this.stateStore.scanStepIds("chotu:step:*");

        for (const stepExecId of stepIds) {
            const row = await this.lifecycle.loadStep(stepExecId);
            if (!row || row.status !== StepExecutionStatus.PENDING) continue;
            if (await this.shouldSkipRun(row.workflow_run_id)) continue;
            if (row.queued) continue;
            if (
                await this.fairQueue.isStepInAnyInflight(stepExecId, this.registry.queueNames())
            ) {
                continue;
            }

            await this.fairQueue.enqueueWithRetry(
                stepExecId,
                row.queue ?? "default",
                row.workflow_run_id,
            );
            recovered++;
        }

        if (recovered > 0) {
            this.logger.info(`[chotu] Recovered ${recovered} orphaned pending step(s)`);
        }

        return recovered;
    }

    async rebuildJoinStateFromRedis(): Promise<void> {
        const stepIds = await this.stateStore.scanStepIds("chotu:step:*");
        const affectedRunIds = new Set<string>();

        for (const stepExecId of stepIds) {
            const row = await this.lifecycle.loadStep(stepExecId);
            if (!row || row.status !== StepExecutionStatus.WAITING) continue;
            if (row.join_remaining != null) continue;

            const remaining = await this.stateStore.rebuildJoinRemainingFromBranches(stepExecId);
            if (remaining == null) continue;

            await this.repository.syncJoinRemaining(stepExecId, remaining);
            affectedRunIds.add(row.workflow_run_id);

            if (remaining === 0) {
                await this.lifecycle.finalizeJoin(stepExecId, row.workflow_run_id);
            }
        }

        for (const runId of affectedRunIds) {
            await this.stateStore.recomputeRunActiveCount(runId);
        }
    }

    async reEnqueueIfPending(
        stepExecId: string,
        queueName: string,
        workflowRunId: string,
    ): Promise<boolean> {
        if (await this.shouldSkipRun(workflowRunId)) return false;

        const step = await this.lifecycle.loadStep(stepExecId);
        if (
            step?.status === StepExecutionStatus.PENDING &&
            !step.queued &&
            !(await this.fairQueue.isStepInAnyInflight(stepExecId, this.registry.queueNames()))
        ) {
            await this.fairQueue.enqueueWithRetry(stepExecId, queueName, workflowRunId);
            return true;
        }
        return false;
    }

    private async shouldSkipRun(workflowRunId: string): Promise<boolean> {
        if (await this.stateStore.isAbortRequested(workflowRunId)) return true;
        const status = await this.stateStore.getRunStatus(workflowRunId);
        return status !== WorkflowRunStatus.RUNNING;
    }
}
