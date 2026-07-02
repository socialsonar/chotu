import { StepExecutionStatus } from "../interfaces/workflow.interface";
export class StepExecutor {
    lifecycle;
    registry;
    fairQueue;
    logger;
    hookRunner;
    constructor(lifecycle, registry, fairQueue, logger, hookRunner) {
        this.lifecycle = lifecycle;
        this.registry = registry;
        this.fairQueue = fairQueue;
        this.logger = logger;
        this.hookRunner = hookRunner;
    }
    async processStepExecution(row, queue, signal) {
        const stepExecId = row.id;
        const stepClass = this.registry.getStepClass(row.step_name);
        if (!stepClass) {
            this.logger.error(`[chotu] Step class "${row.step_name}" not registered`);
            await this.lifecycle.failStep(stepExecId, row, new Error(`Step "${row.step_name}" not registered`));
            return false;
        }
        const runRow = await this.lifecycle.loadRun(row.workflow_run_id);
        const workflowName = runRow?.workflow_name ?? "unknown";
        const step = new stepClass();
        const input = this.normalizeInput(row.input);
        const attempts = row.attempts;
        const ctx = {
            stepExecId,
            stepName: row.step_name,
            queue: row.queue,
            workflowRunId: row.workflow_run_id,
            workflowName,
            attempt: attempts + 1,
        };
        const shutdownSignal = signal;
        const timeoutMs = this.registry.getEffectiveStepTimeoutMs(row.step_name);
        const { stepSignal, cleanup } = this.createStepSignal(shutdownSignal, timeoutMs, row.step_name);
        try {
            const execute = async () => {
                await this.throwIfRunAborted(row.workflow_run_id);
                this.throwIfAborted(shutdownSignal);
                await step.onBeforeRun(input, ctx, stepSignal);
                await this.throwIfRunAborted(row.workflow_run_id);
                this.throwIfAborted(shutdownSignal);
                const output = await step.run(input, stepSignal);
                await this.throwIfRunAborted(row.workflow_run_id);
                this.throwIfAborted(shutdownSignal);
                await step.onAfterRun(input, output, ctx, stepSignal);
                const nextSteps = await step.getNextSteps(input, output, stepSignal);
                if (!(await this.lifecycle.canScheduleForRun(row.workflow_run_id))) {
                    return;
                }
                await this.lifecycle.completeStep(stepExecId, output);
                if (nextSteps === "END") {
                    if (row.join_step_id) {
                        await this.lifecycle.scheduleNext(nextSteps, row.workflow_run_id, stepExecId, output);
                    }
                    await this.hookRunner.stepCompleted({ ...ctx, output });
                    await this.lifecycle.checkCompletion(row.workflow_run_id);
                }
                else {
                    await this.lifecycle.scheduleNext(nextSteps, row.workflow_run_id, stepExecId, output);
                    await this.hookRunner.stepCompleted({ ...ctx, output });
                }
            };
            await this.raceWithStepTimeout(execute, stepSignal, shutdownSignal, timeoutMs, row.step_name);
            return false;
        }
        catch (err) {
            if (shutdownSignal.aborted) {
                if (await this.lifecycle.isAbortRequested(row.workflow_run_id)) {
                    await this.lifecycle.cancelStep(stepExecId, row);
                    return false;
                }
                if (await this.lifecycle.setStepStatus(stepExecId, StepExecutionStatus.PENDING)) {
                    await this.fairQueue.requeue(row.queue, stepExecId, row.workflow_run_id);
                }
                return true;
            }
            if (await this.lifecycle.isAbortRequested(row.workflow_run_id)) {
                await this.lifecycle.cancelStep(stepExecId, row);
                return false;
            }
            const error = err instanceof Error ? err : new Error(String(err));
            this.logger.error(`[chotu] Step ${stepExecId} ("${row.step_name}") failed (attempt ${attempts + 1}):`, error);
            await step.onError(input, error, ctx, stepSignal).catch((onErrorErr) => {
                this.logger.error(`[chotu] Step ${stepExecId} ("${row.step_name}") onError handler failed:`, onErrorErr);
            });
            const effectiveMax = this.registry.getEffectiveMaxAttempts(queue, error);
            if (attempts + 1 < effectiveMax) {
                await this.hookRunner.stepFailed({ ...ctx, error, willRetry: true });
                await this.lifecycle.incrementAttempts(stepExecId);
                if (await this.lifecycle.setStepStatus(stepExecId, StepExecutionStatus.PENDING)) {
                    await this.fairQueue.requeue(row.queue, stepExecId, row.workflow_run_id);
                }
                return true;
            }
            await this.hookRunner.stepFailed({ ...ctx, error, willRetry: false });
            await this.lifecycle.failStep(stepExecId, row, error);
            return false;
        }
        finally {
            cleanup();
        }
    }
    async recoverFromWorkerError(stepExecId, row, queue) {
        const current = await this.lifecycle.loadStep(stepExecId);
        if (!current)
            return;
        if (current.status === StepExecutionStatus.RUNNING) {
            await this.lifecycle.setStepStatus(stepExecId, StepExecutionStatus.PENDING);
        }
        await this.fairQueue.requeue(queue.name, stepExecId, row.workflow_run_id);
    }
    normalizeInput(input) {
        if (input == null)
            return {};
        if (typeof input === "string") {
            try {
                return JSON.parse(input);
            }
            catch {
                return input;
            }
        }
        return input;
    }
    throwIfAborted(signal) {
        if (signal.aborted) {
            const err = new Error("Aborted");
            err.name = "AbortError";
            throw err;
        }
    }
    async throwIfRunAborted(workflowRunId) {
        if (await this.lifecycle.isAbortRequested(workflowRunId)) {
            const err = new Error("Aborted");
            err.name = "AbortError";
            throw err;
        }
    }
    createStepSignal(shutdownSignal, timeoutMs, stepName) {
        const timeoutController = new AbortController();
        const stepSignal = AbortSignal.any([shutdownSignal, timeoutController.signal]);
        const timer = setTimeout(() => {
            timeoutController.abort(new Error(`Step "${stepName}" timed out after ${timeoutMs}ms`));
        }, timeoutMs);
        return {
            stepSignal,
            cleanup: () => clearTimeout(timer),
        };
    }
    raceWithStepTimeout(fn, stepSignal, shutdownSignal, timeoutMs, stepName) {
        if (stepSignal.aborted) {
            this.throwIfAborted(shutdownSignal);
            throw new Error(`Step "${stepName}" timed out after ${timeoutMs}ms`);
        }
        const fnPromise = fn();
        return Promise.race([
            fnPromise,
            new Promise((_, reject) => {
                stepSignal.addEventListener("abort", () => {
                    if (shutdownSignal.aborted) {
                        const err = new Error("Aborted");
                        err.name = "AbortError";
                        reject(err);
                        return;
                    }
                    const reason = stepSignal.reason;
                    reject(reason instanceof Error
                        ? reason
                        : new Error(`Step "${stepName}" timed out after ${timeoutMs}ms`));
                }, { once: true });
            }),
        ]).finally(() => {
            fnPromise.catch(() => { });
        });
    }
    loadStep(stepExecId) {
        return this.lifecycle.loadStep(stepExecId);
    }
    setStepStatus(stepExecId, status) {
        return this.lifecycle.setStepStatus(stepExecId, status);
    }
    async buildStepHookContext(row) {
        const runRow = await this.lifecycle.loadRun(row.workflow_run_id);
        return {
            stepExecId: row.id,
            stepName: row.step_name,
            queue: row.queue,
            workflowRunId: row.workflow_run_id,
            workflowName: runRow?.workflow_name ?? "unknown",
            attempt: row.attempts + 1,
        };
    }
    async handleAbortedStep(row) {
        if (!(await this.lifecycle.isAbortRequested(row.workflow_run_id)))
            return;
        const active = new Set([
            StepExecutionStatus.PENDING,
            StepExecutionStatus.RUNNING,
            StepExecutionStatus.WAITING,
        ]);
        if (!active.has(row.status))
            return;
        await this.fairQueue.cancelFromQueue(row.queue, row.id, row.workflow_run_id);
        await this.lifecycle.cancelStep(row.id, row);
    }
}
//# sourceMappingURL=step-executor.js.map