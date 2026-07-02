import { createStepError, getStepName, isChotuStepError, isNextStep, isParallelSpec, } from "../domain/step";
import { StepExecutionStatus, WorkflowRunStatus, } from "../interfaces/workflow.interface";
import { sleep } from "../platform/sleep";
async function syncWithRetry(fn, label, logger, maxAttempts = 3) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await fn();
            return;
        }
        catch (err) {
            if (attempt === maxAttempts) {
                logger.error(`[chotu] ${label} failed after ${maxAttempts} attempts:`, err);
                return;
            }
            await sleep(100 * attempt);
        }
    }
}
export class WorkflowLifecycle {
    stateStore;
    repository;
    fairQueue;
    registry;
    logger;
    hookRunner;
    constructor(stateStore, repository, fairQueue, registry, logger, hookRunner) {
        this.stateStore = stateStore;
        this.repository = repository;
        this.fairQueue = fairQueue;
        this.registry = registry;
        this.logger = logger;
        this.hookRunner = hookRunner;
    }
    async runWorkflow(name, input) {
        const workflow = this.registry.getWorkflow(name);
        if (!workflow) {
            throw new Error(`[chotu] Workflow "${name}" not registered`);
        }
        const workflowRunId = crypto.randomUUID();
        const firstStepId = crypto.randomUUID();
        const firstStepName = getStepName(workflow.firstStep);
        const queue = this.registry.resolveQueue(firstStepName);
        await this.stateStore.createRun({
            id: workflowRunId,
            workflowName: name,
            input: input,
        });
        const created = await this.stateStore.createStep({
            id: firstStepId,
            workflowRunId,
            stepName: firstStepName,
            queue,
            input: input,
        });
        if (!created) {
            await this.stateStore.rollbackRun(workflowRunId);
            throw new Error(`[chotu] Failed to create first step in Redis for workflow ${workflowRunId}`);
        }
        try {
            await this.repository.insertWorkflowRunWithFirstStep({
                workflowRunId,
                workflowName: name,
                input: input,
                firstStepId,
                firstStepName,
                queue,
            });
        }
        catch (err) {
            await this.stateStore.rollbackStep(firstStepId, workflowRunId, firstStepName);
            await this.stateStore.rollbackRun(workflowRunId);
            throw err;
        }
        await this.enqueueStep(firstStepId, firstStepName, workflowRunId);
        this.logger.info(`[chotu] Workflow run ${workflowRunId} ("${name}") started`);
        await this.hookRunner.workflowStarted({
            workflowRunId,
            workflowName: name,
            input: input,
        });
        return { id: workflowRunId };
    }
    async getWorkflowRun(id) {
        return this.repository.getWorkflowRun(id);
    }
    async getStepExecutions(workflowRunId) {
        return this.repository.getStepExecutions(workflowRunId);
    }
    async loadStep(stepExecId) {
        return this.stateStore.loadStep(stepExecId);
    }
    async loadRun(workflowRunId) {
        return this.stateStore.loadRun(workflowRunId);
    }
    async setStepStatus(stepExecId, status) {
        const ok = await this.stateStore.setStepStatus(stepExecId, status);
        if (!ok) {
            this.logger.warn(`[chotu] setStepStatus(${stepExecId}, ${status}) rejected — step missing`);
        }
        return ok;
    }
    async incrementAttempts(stepExecId) {
        return this.stateStore.incrementAttempts(stepExecId);
    }
    async completeStep(stepExecId, output) {
        const updated = await this.stateStore.completeStep(stepExecId, output);
        if (!updated)
            return;
        await syncWithRetry(() => this.repository.syncStepTerminal({
            id: stepExecId,
            status: StepExecutionStatus.COMPLETED,
            output,
            version: updated.version,
        }), `syncStepTerminal(completed, ${stepExecId})`, this.logger);
    }
    async failStep(stepExecId, row, error) {
        if (await this.isAbortRequested(row.workflow_run_id)) {
            await this.cancelStep(stepExecId, row);
            return;
        }
        const updated = await this.stateStore.failStep(stepExecId, { message: error.message });
        if (!updated)
            return;
        await syncWithRetry(() => this.repository.syncStepTerminal({
            id: stepExecId,
            status: StepExecutionStatus.FAILED,
            error: { message: error.message },
            version: updated.version,
        }), `syncStepTerminal(failed, ${stepExecId})`, this.logger);
        if (row.join_step_id && row.fan_out_index != null) {
            await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
        }
        await this.checkCompletion(row.workflow_run_id);
        this.logger.info(`[chotu] Step ${stepExecId} failed (workflow ${row.workflow_run_id} continues)`);
    }
    async cancelStep(stepExecId, row, reason) {
        const updated = await this.stateStore.cancelStep(stepExecId, reason);
        if (!updated)
            return;
        await syncWithRetry(() => this.repository.syncStepTerminal({
            id: stepExecId,
            status: StepExecutionStatus.CANCELLED,
            error: reason ? { reason } : null,
            version: updated.version,
        }), `syncStepTerminal(cancelled, ${stepExecId})`, this.logger);
        const runRow = await this.stateStore.loadRun(row.workflow_run_id);
        if (runRow) {
            await this.hookRunner.stepCancelled({
                stepExecId,
                stepName: row.step_name,
                queue: row.queue,
                workflowRunId: row.workflow_run_id,
                workflowName: runRow.workflow_name,
                attempt: row.attempts + 1,
                reason,
            });
        }
        if (row.join_step_id && row.fan_out_index != null) {
            await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
        }
        await this.finalizeCancelIfReady(row.workflow_run_id, reason);
        this.logger.info(`[chotu] Step ${stepExecId} cancelled (workflow ${row.workflow_run_id})`);
    }
    async isAbortRequested(workflowRunId) {
        return this.stateStore.isAbortRequested(workflowRunId);
    }
    async canScheduleForRun(workflowRunId) {
        const status = await this.stateStore.getRunStatus(workflowRunId);
        if (status !== WorkflowRunStatus.RUNNING)
            return false;
        if (await this.isAbortRequested(workflowRunId))
            return false;
        return true;
    }
    async beginCancelWorkflow(workflowRunId, reason) {
        const run = await this.stateStore.loadRun(workflowRunId);
        if (!run)
            return false;
        if (run.status !== WorkflowRunStatus.RUNNING)
            return false;
        if (await this.isAbortRequested(workflowRunId))
            return false;
        await this.stateStore.markAbortRequested(workflowRunId);
        const steps = await this.stateStore.listStepsForRun(workflowRunId);
        const cancellable = new Set([
            StepExecutionStatus.PENDING,
            StepExecutionStatus.WAITING,
        ]);
        for (const step of steps) {
            if (!cancellable.has(step.status))
                continue;
            await this.fairQueue.cancelFromQueue(step.queue, step.id, workflowRunId);
            await this.cancelStep(step.id, step, reason);
        }
        return true;
    }
    async finalizeCancelIfReady(workflowRunId, reason) {
        if (!(await this.isAbortRequested(workflowRunId)))
            return false;
        const activeCount = await this.stateStore.getActiveCount(workflowRunId);
        if (activeCount > 0)
            return false;
        await this.finalizeCancelledRun(workflowRunId, reason);
        return true;
    }
    async finalizeCancelledRun(workflowRunId, reason) {
        const output = reason ? { reason } : null;
        const version = await this.stateStore.tryCancelRun(workflowRunId, reason);
        if (version == null)
            return;
        await syncWithRetry(async () => {
            const synced = await this.repository.syncWorkflowTerminal({
                id: workflowRunId,
                status: WorkflowRunStatus.CANCELLED,
                output,
                version,
            });
            if (!synced) {
                throw new Error(`syncWorkflowTerminal(cancelled) returned false for ${workflowRunId}`);
            }
        }, `syncWorkflowTerminal(cancelled, ${workflowRunId})`, this.logger);
        this.logger.info(`[chotu] Workflow run ${workflowRunId} cancelled${reason ? `: ${reason}` : ""}`);
        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (runRow) {
            await this.hookRunner.workflowCancelled({
                workflowRunId,
                workflowName: runRow.workflow_name,
                input: runRow.input,
                reason,
            });
        }
    }
    async scheduleNext(nextSteps, workflowRunId, currentStepExecId, stepOutput) {
        if (!(await this.canScheduleForRun(workflowRunId)))
            return;
        const currentRow = await this.loadStep(currentStepExecId);
        if (!currentRow)
            return;
        if (nextSteps === "END") {
            if (currentRow.join_step_id) {
                await this.completeBranch(currentRow);
            }
            return;
        }
        if (isNextStep(nextSteps)) {
            const stepName = getStepName(nextSteps.step);
            const nextStepId = await this.createStepExecution({
                workflowRunId,
                stepName,
                queue: this.registry.resolveQueue(stepName),
                input: nextSteps.input,
                joinStepId: currentRow.join_step_id,
                fanOutIndex: currentRow.fan_out_index,
            });
            await this.enqueueStep(nextStepId, stepName, workflowRunId);
            return;
        }
        if (isParallelSpec(nextSteps)) {
            await this.spawnParallel(nextSteps, workflowRunId);
        }
    }
    async checkCompletion(workflowRunId) {
        const lockToken = crypto.randomUUID();
        const acquired = await this.stateStore.acquireRunLock(workflowRunId, lockToken, 30);
        if (!acquired)
            return;
        try {
            await this.doCheckCompletion(workflowRunId);
        }
        finally {
            await this.stateStore.releaseRunLock(workflowRunId, lockToken);
        }
    }
    async enqueueStep(stepExecId, stepName, workflowRunId) {
        if (!(await this.canScheduleForRun(workflowRunId)))
            return;
        const queueName = this.registry.resolveQueue(stepName);
        await this.fairQueue.enqueueWithRetry(stepExecId, queueName, workflowRunId);
    }
    async decrementJoinRemaining(joinStepId, workflowRunId) {
        if (!(await this.canScheduleForRun(workflowRunId))) {
            await this.finalizeCancelIfReady(workflowRunId);
            return;
        }
        const remaining = await this.stateStore.decrementJoinRemaining(joinStepId);
        if (remaining == null)
            return;
        await this.repository.syncJoinRemaining(joinStepId, remaining);
        if (remaining > 0) {
            await this.checkCompletion(workflowRunId);
            return;
        }
        await this.finalizeJoin(joinStepId, workflowRunId);
    }
    async finalizeJoin(joinStepId, workflowRunId) {
        if (!(await this.canScheduleForRun(workflowRunId))) {
            await this.finalizeCancelIfReady(workflowRunId);
            return;
        }
        const joinRow = await this.loadStep(joinStepId);
        if (!joinRow) {
            this.logger.error(`[chotu] Join step ${joinStepId} missing when finalizing workflow ${workflowRunId}`);
            await this.failWorkflowRun(workflowRunId, "Join step missing during finalize");
            return;
        }
        const branches = await this.stateStore.getJoinBranches(joinStepId);
        const outputs = branches.map((branch) => {
            if (branch.status === StepExecutionStatus.FAILED) {
                const branchError = branch.error;
                return createStepError(branchError?.message ?? "Branch failed", branch.step_name ?? "unknown");
            }
            const output = branch.output;
            if (output && isChotuStepError(output)) {
                return output;
            }
            return output ?? {};
        });
        const updated = await this.stateStore.finalizeJoinStep(joinStepId, outputs);
        if (!updated)
            return;
        await syncWithRetry(() => this.repository.syncJoinFinalize({
            id: joinStepId,
            input: outputs,
            version: updated.version,
        }), `syncJoinFinalize(${joinStepId})`, this.logger);
        await this.enqueueStep(joinStepId, joinRow.step_name, workflowRunId);
    }
    async failWorkflowRun(workflowRunId, reason) {
        const output = reason ? { reason } : null;
        const version = await this.stateStore.tryFailRun(workflowRunId, reason);
        if (version == null)
            return;
        await syncWithRetry(async () => {
            const synced = await this.repository.syncWorkflowTerminal({
                id: workflowRunId,
                status: WorkflowRunStatus.FAILED,
                output,
                version,
            });
            if (!synced) {
                throw new Error(`syncWorkflowTerminal(failed) returned false for ${workflowRunId}`);
            }
        }, `syncWorkflowTerminal(failed, ${workflowRunId})`, this.logger);
        this.logger.info(`[chotu] Workflow run ${workflowRunId} failed${reason ? `: ${reason}` : ""}`);
        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (runRow) {
            await this.hookRunner.workflowError({
                workflowRunId,
                workflowName: runRow.workflow_name,
                input: runRow.input,
                reason,
            });
        }
    }
    async createStepExecution(params) {
        if (!(await this.canScheduleForRun(params.workflowRunId))) {
            throw new Error(`[chotu] Cannot create step for non-running workflow ${params.workflowRunId}`);
        }
        const id = crypto.randomUUID();
        const status = params.status ?? StepExecutionStatus.PENDING;
        const created = await this.stateStore.createStep({
            id,
            workflowRunId: params.workflowRunId,
            stepName: params.stepName,
            queue: params.queue,
            status,
            input: params.input,
            joinStepId: params.joinStepId,
            fanOutIndex: params.fanOutIndex,
            joinTotal: params.joinTotal,
            joinRemaining: params.joinRemaining,
        });
        if (!created) {
            throw new Error(`[chotu] Active step already exists for "${params.stepName}" in run ${params.workflowRunId}`);
        }
        try {
            await this.repository.insertStep({
                id,
                workflowRunId: params.workflowRunId,
                stepName: params.stepName,
                queue: params.queue,
                status,
                input: params.input,
                joinStepId: params.joinStepId,
                fanOutIndex: params.fanOutIndex,
                joinTotal: params.joinTotal,
                joinRemaining: params.joinRemaining,
            });
        }
        catch (err) {
            await this.stateStore.rollbackStep(id, params.workflowRunId, params.stepName);
            throw err;
        }
        return id;
    }
    async completeBranch(row) {
        if (!row.join_step_id || row.fan_out_index == null)
            return;
        await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
    }
    async spawnParallel(spec, workflowRunId) {
        let joinStepId = null;
        if (spec.join) {
            const joinStepName = getStepName(spec.join);
            joinStepId = await this.createStepExecution({
                workflowRunId,
                stepName: joinStepName,
                queue: this.registry.resolveQueue(joinStepName),
                input: null,
                status: StepExecutionStatus.WAITING,
                joinTotal: spec.branches.length,
                joinRemaining: spec.branches.length,
            });
        }
        for (let i = 0; i < spec.branches.length; i++) {
            const branch = spec.branches[i];
            const stepName = getStepName(branch.step);
            const branchStepId = await this.createStepExecution({
                workflowRunId,
                stepName,
                queue: this.registry.resolveQueue(stepName),
                input: branch.input,
                joinStepId,
                fanOutIndex: i,
            });
            await this.enqueueStep(branchStepId, stepName, workflowRunId);
        }
    }
    async doCheckCompletion(workflowRunId) {
        const runStatus = await this.stateStore.getRunStatus(workflowRunId);
        if (runStatus === WorkflowRunStatus.COMPLETED ||
            runStatus === WorkflowRunStatus.FAILED ||
            runStatus === WorkflowRunStatus.CANCELLED) {
            return;
        }
        if (await this.isAbortRequested(workflowRunId)) {
            await this.finalizeCancelIfReady(workflowRunId);
            return;
        }
        const activeCount = await this.stateStore.getActiveCount(workflowRunId);
        if (activeCount > 0)
            return;
        const unabsorbedFailures = await this.stateStore.countUnabsorbedFailures(workflowRunId);
        if (unabsorbedFailures > 0) {
            await this.failWorkflowRun(workflowRunId);
            return;
        }
        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (!runRow)
            return;
        const workflow = this.registry.getWorkflow(runRow.workflow_name);
        if (workflow?.completeStep) {
            await this.handleCompleteStep(workflowRunId, workflow, runRow);
            return;
        }
        await this.completeWorkflowWithoutCompleteStep(workflowRunId, workflow);
    }
    async handleCompleteStep(workflowRunId, workflow, runRow) {
        const completeStepName = getStepName(workflow.completeStep);
        let completeStepId = null;
        const completeRow = await this.stateStore.findStepByName(workflowRunId, completeStepName);
        if (!completeRow) {
            const queue = this.registry.resolveQueue(completeStepName);
            const input = {
                workflowInput: runRow.input,
                workflowRunId,
            };
            completeStepId = await this.createStepExecution({
                workflowRunId,
                stepName: completeStepName,
                queue,
                input,
            });
        }
        else if (completeRow.status === StepExecutionStatus.COMPLETED) {
            const version = await this.stateStore.tryCompleteRun(workflowRunId, completeRow.output ?? null);
            if (version == null)
                return;
            await syncWithRetry(async () => {
                const synced = await this.repository.completeWorkflowFromCompleteStep({
                    workflowRunId,
                    output: completeRow.output ?? null,
                    version,
                });
                if (!synced) {
                    throw new Error(`completeWorkflowFromCompleteStep returned false for ${workflowRunId}`);
                }
            }, `completeWorkflowFromCompleteStep(${workflowRunId})`, this.logger);
            this.logger.info(`[chotu] Workflow run ${workflowRunId} completed`);
            await this.hookRunner.workflowCompleted({
                workflowRunId,
                workflowName: workflow.name,
                input: runRow.input,
                output: completeRow.output ?? null,
            });
            return;
        }
        else if (completeRow.status === StepExecutionStatus.PENDING ||
            completeRow.status === StepExecutionStatus.RUNNING) {
            completeStepId = completeRow.id;
        }
        if (completeStepId) {
            await this.enqueueStep(completeStepId, completeStepName, workflowRunId);
        }
    }
    async completeWorkflowWithoutCompleteStep(workflowRunId, workflow) {
        let output = null;
        if (workflow?.terminalSteps?.length) {
            const terminalNames = workflow.terminalSteps.map(getStepName);
            const terminalRows = [];
            for (const name of terminalNames) {
                const row = await this.stateStore.findStepByName(workflowRunId, name);
                if (row?.status === StepExecutionStatus.COMPLETED) {
                    terminalRows.push({ step_name: name, output: row.output ?? null });
                }
            }
            if (terminalRows.length === 1) {
                output = terminalRows[0]?.output ?? null;
            }
            else if (terminalRows.length > 1) {
                output = Object.fromEntries(terminalRows.map((r) => [r.step_name, r.output ?? null]));
            }
        }
        const version = await this.stateStore.tryCompleteRun(workflowRunId, output);
        if (version == null)
            return;
        await syncWithRetry(async () => {
            const synced = await this.repository.syncWorkflowTerminal({
                id: workflowRunId,
                status: WorkflowRunStatus.COMPLETED,
                output,
                version,
            });
            if (!synced) {
                throw new Error(`syncWorkflowTerminal(completed) returned false for ${workflowRunId}`);
            }
        }, `syncWorkflowTerminal(completed, ${workflowRunId})`, this.logger);
        this.logger.info(`[chotu] Workflow run ${workflowRunId} completed`);
        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (runRow) {
            await this.hookRunner.workflowCompleted({
                workflowRunId,
                workflowName: runRow.workflow_name,
                input: runRow.input,
                output,
            });
        }
    }
}
//# sourceMappingURL=workflow-lifecycle.js.map