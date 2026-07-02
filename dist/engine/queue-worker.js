import { StepExecutionStatus } from "../interfaces/workflow.interface";
import { sleep } from "../platform/sleep";
import { RECOVERY_INTERVAL_MS } from "../persistence/redis/keys";
export class QueueWorkerPool {
    fairQueue;
    stateStore;
    stepExecutor;
    recovery;
    registry;
    logger;
    instanceId;
    hookRunner;
    started = false;
    workers = [];
    inFlight = new Set();
    inFlightStepIds = new Set();
    inFlightStepNames = new Map();
    lastRecoveryAt = 0;
    abortControllers = new Map();
    inFlightRunIds = new Map();
    constructor(fairQueue, stateStore, stepExecutor, recovery, registry, logger, instanceId, hookRunner) {
        this.fairQueue = fairQueue;
        this.stateStore = stateStore;
        this.stepExecutor = stepExecutor;
        this.recovery = recovery;
        this.registry = registry;
        this.logger = logger;
        this.instanceId = instanceId;
        this.hookRunner = hookRunner;
    }
    isStarted() {
        return this.started;
    }
    async start() {
        if (this.started)
            return;
        this.started = true;
        this.lastRecoveryAt = Date.now();
        for (const queue of this.registry.allQueues()) {
            for (let i = 0; i < queue.concurrency; i++) {
                this.workers.push(this.runWorker(queue));
            }
            this.logger.info(`[chotu] Queue "${queue.name}" started (concurrency=${queue.concurrency})`);
        }
    }
    async stop() {
        if (!this.started)
            return;
        this.started = false;
        for (const controller of this.abortControllers.values()) {
            controller.abort();
        }
        await Promise.allSettled([...this.inFlight]);
        await Promise.allSettled(this.workers);
        this.workers = [];
        this.inFlight.clear();
        this.inFlightStepNames.clear();
        this.abortControllers.clear();
        this.inFlightRunIds.clear();
    }
    abortInFlightForRun(workflowRunId) {
        for (const [stepExecId, runId] of this.inFlightRunIds) {
            if (runId !== workflowRunId)
                continue;
            const controller = this.abortControllers.get(stepExecId);
            controller?.abort(new Error("workflow abort"));
        }
    }
    async runWorker(queue) {
        const pollMs = queue.pollIntervalMs ?? 500;
        while (this.started) {
            try {
                if (Date.now() - this.lastRecoveryAt > RECOVERY_INTERVAL_MS) {
                    this.lastRecoveryAt = Date.now();
                    if (await this.stateStore.tryAcquireRecoveryLeader(this.instanceId)) {
                        await this.runLeaderRecovery();
                    }
                }
                await this.renewInFlightLeases();
                const stepExecId = await this.fairQueue.pop(queue.name);
                if (!stepExecId) {
                    await sleep(pollMs);
                    continue;
                }
                const row = await this.stepExecutor.loadStep(stepExecId);
                if (!row) {
                    this.logger.warn(`[chotu] Step execution ${stepExecId} not found after pop`);
                    await this.fairQueue.ack(queue.name, stepExecId);
                    continue;
                }
                if (row.status === StepExecutionStatus.CANCELLED ||
                    row.status === StepExecutionStatus.COMPLETED ||
                    row.status === StepExecutionStatus.FAILED) {
                    await this.fairQueue.ack(queue.name, stepExecId);
                    continue;
                }
                if (await this.stateStore.isAbortRequested(row.workflow_run_id)) {
                    await this.stepExecutor.handleAbortedStep(row);
                    await this.fairQueue.ack(queue.name, stepExecId);
                    continue;
                }
                const claimed = await this.stateStore.claimStep(stepExecId, this.instanceId, this.registry.getLeaseTtlMs(row.step_name));
                if (!claimed) {
                    await this.handleFailedClaim(stepExecId, queue.name);
                    continue;
                }
                let inflightHandled = false;
                try {
                    if (!(await this.fairQueue.acquireRateLimit(queue))) {
                        if (!(await this.stepExecutor.setStepStatus(stepExecId, StepExecutionStatus.PENDING))) {
                            await this.fairQueue.ack(queue.name, stepExecId);
                            inflightHandled = true;
                            continue;
                        }
                        await this.fairQueue.requeue(queue.name, stepExecId, claimed.workflow_run_id);
                        inflightHandled = true;
                        await sleep(this.fairQueue.rateLimitBackoffMs(queue));
                        continue;
                    }
                    const stepCtx = await this.stepExecutor.buildStepHookContext(claimed);
                    await this.hookRunner.stepStarted(stepCtx);
                    const controller = new AbortController();
                    this.abortControllers.set(stepExecId, controller);
                    this.inFlightRunIds.set(stepExecId, claimed.workflow_run_id);
                    this.inFlightStepIds.add(stepExecId);
                    this.inFlightStepNames.set(stepExecId, claimed.step_name);
                    const work = this.stepExecutor.processStepExecution(claimed, queue, controller.signal);
                    this.inFlight.add(work);
                    try {
                        try {
                            inflightHandled = await work;
                        }
                        catch (err) {
                            this.logger.error(`[chotu] Unexpected error processing step ${stepExecId} ("${claimed.step_name}"):`, err);
                            await this.stepExecutor.recoverFromWorkerError(stepExecId, claimed, queue);
                            inflightHandled = true;
                        }
                    }
                    finally {
                        this.inFlight.delete(work);
                        this.inFlightStepIds.delete(stepExecId);
                        this.inFlightStepNames.delete(stepExecId);
                        this.abortControllers.delete(stepExecId);
                        this.inFlightRunIds.delete(stepExecId);
                    }
                }
                finally {
                    if (!inflightHandled) {
                        await this.fairQueue.ack(queue.name, stepExecId);
                    }
                }
            }
            catch (err) {
                this.logger.error(`[chotu] Worker error on queue "${queue.name}":`, err);
                await sleep(1000);
            }
        }
    }
    async renewInFlightLeases() {
        for (const stepExecId of this.inFlightStepIds) {
            const stepName = this.inFlightStepNames.get(stepExecId);
            if (!stepName)
                continue;
            await this.stateStore.renewLease(stepExecId, this.instanceId, this.registry.getLeaseTtlMs(stepName));
        }
    }
    async runLeaderRecovery() {
        const steps = [
            async () => {
                await this.recovery.recoverInflightSteps();
            },
            async () => {
                await this.recovery.recoverStaleRunningSteps();
            },
            async () => {
                await this.recovery.recoverOrphanedPendingSteps();
            },
            () => this.recovery.rebuildJoinStateFromRedis(),
        ];
        for (const step of steps) {
            if (!(await this.stateStore.tryAcquireRecoveryLeader(this.instanceId))) {
                return;
            }
            await step();
        }
    }
    async handleFailedClaim(stepExecId, queueName) {
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
//# sourceMappingURL=queue-worker.js.map