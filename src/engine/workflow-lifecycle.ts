import {
    createStepError,
    getStepName,
    isChotuStepError,
    isNextStep,
    isParallelSpec,
    type NextStep,
    type ParallelSpec,
} from "../domain/step";
import type { Workflow } from "../domain/workflow";
import type { WorkflowHookContext } from "../interfaces/hooks.interface";
import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { ChotuHookRunner } from "./hook-runner";
import type { IWorkflowRepository } from "../interfaces/repository.interface";
import type { IStateStore } from "../interfaces/state-store.interface";
import {
    StepExecutionStatus,
    WorkflowRunStatus,
    type StepExecution,
    type StepExecutionRecord,
    type WorkflowCompleteInput,
    type WorkflowRun,
    type WorkflowRunRecord,
} from "../interfaces/workflow.interface";
import type { ChotuLogger } from "../logger";
import { sleep } from "../platform/sleep";
import type { RunPurger } from "./run-purger";
import { StepRegistry } from "./step-registry";

const DEFAULT_BEFORE_START_TIMEOUT_MS = 30_000;

async function syncWithRetry(
    fn: () => Promise<void>,
    label: string,
    logger: ChotuLogger,
    maxAttempts = 3,
): Promise<void> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await fn();
            return;
        } catch (err) {
            if (attempt === maxAttempts) {
                logger.error(`[chotu] ${label} failed after ${maxAttempts} attempts:`, err);
                return;
            }
            await sleep(100 * attempt);
        }
    }
}

export class WorkflowLifecycle {
    constructor(
        private readonly stateStore: IStateStore,
        private readonly repository: IWorkflowRepository,
        private readonly fairQueue: IFairQueue,
        private readonly registry: StepRegistry,
        private readonly logger: ChotuLogger,
        private readonly hookRunner: ChotuHookRunner,
        private readonly runPurger: RunPurger,
    ) {}

    async runWorkflow<I>(name: string, input: I): Promise<{ id: string }> {
        const workflow = this.registry.getWorkflow(name);
        if (!workflow) {
            throw new Error(`[chotu] Workflow "${name}" not registered`);
        }

        const workflowRunId = crypto.randomUUID();
        const hookCtx: WorkflowHookContext = {
            workflowRunId,
            workflowName: name,
            input: input as Record<string, any>,
        };

        let effectiveInput = input;
        const beforeStartResult = await workflow.onBeforeStart(
            input,
            hookCtx,
            AbortSignal.timeout(DEFAULT_BEFORE_START_TIMEOUT_MS),
        );
        if (beforeStartResult !== undefined) {
            effectiveInput = beforeStartResult;
        }
        const effectiveInputRecord = effectiveInput as Record<string, any>;

        const firstStepId = crypto.randomUUID();
        const firstStepName = getStepName(workflow.firstStep);
        const queue = this.registry.resolveQueue(firstStepName);

        await this.stateStore.createRun({
            id: workflowRunId,
            workflowName: name,
            input: effectiveInputRecord,
        });

        const created = await this.stateStore.createStep({
            id: firstStepId,
            workflowRunId,
            stepName: firstStepName,
            queue,
            input: effectiveInputRecord,
        });

        if (!created) {
            await this.stateStore.rollbackRun(workflowRunId);
            throw new Error(
                `[chotu] Failed to create first step in Redis for workflow ${workflowRunId}`,
            );
        }

        try {
            await this.repository.insertWorkflowRunWithFirstStep({
                workflowRunId,
                workflowName: name,
                input: effectiveInputRecord,
                firstStepId,
                firstStepName,
                queue,
            });
        } catch (err) {
            await this.stateStore.rollbackStep(firstStepId, workflowRunId, firstStepName);
            await this.stateStore.rollbackRun(workflowRunId);
            throw err;
        }

        await this.enqueueStep(firstStepId, firstStepName, workflowRunId);

        this.logger.info(`[chotu] Workflow run ${workflowRunId} ("${name}") started`);

        await this.hookRunner.workflowStarted({
            workflowRunId,
            workflowName: name,
            input: effectiveInputRecord,
        });

        return { id: workflowRunId };
    }

    async getWorkflowRun(id: string): Promise<WorkflowRun | null> {
        return this.repository.getWorkflowRun(id);
    }

    async getStepExecutions(workflowRunId: string): Promise<StepExecution[]> {
        return this.repository.getStepExecutions(workflowRunId);
    }

    async loadStep(stepExecId: string): Promise<StepExecutionRecord | null> {
        return this.stateStore.loadStep(stepExecId);
    }

    async loadRun(workflowRunId: string): Promise<WorkflowRunRecord | null> {
        return this.stateStore.loadRun(workflowRunId);
    }

    async setStepStatus(stepExecId: string, status: StepExecutionStatus): Promise<boolean> {
        const ok = await this.stateStore.setStepStatus(stepExecId, status);
        if (!ok) {
            this.logger.warn(
                `[chotu] setStepStatus(${stepExecId}, ${status}) rejected — step missing`,
            );
        }
        return ok;
    }

    async incrementAttempts(stepExecId: string): Promise<number> {
        return this.stateStore.incrementAttempts(stepExecId);
    }

    async completeStep(stepExecId: string, output: Record<string, any>): Promise<void> {
        const updated = await this.stateStore.completeStep(stepExecId, output);
        if (!updated) return;

        await syncWithRetry(
            () =>
                this.repository.syncStepTerminal({
                    id: stepExecId,
                    status: StepExecutionStatus.COMPLETED,
                    output,
                    version: updated.version,
                }),
            `syncStepTerminal(completed, ${stepExecId})`,
            this.logger,
        );
    }

    async failStep(
        stepExecId: string,
        row: StepExecutionRecord,
        error: Error,
    ): Promise<void> {
        if (await this.isAbortRequested(row.workflow_run_id)) {
            await this.cancelStep(stepExecId, row);
            return;
        }

        const updated = await this.stateStore.failStep(stepExecId, { message: error.message });
        if (!updated) return;

        await syncWithRetry(
            () =>
                this.repository.syncStepTerminal({
                    id: stepExecId,
                    status: StepExecutionStatus.FAILED,
                    error: { message: error.message },
                    version: updated.version,
                }),
            `syncStepTerminal(failed, ${stepExecId})`,
            this.logger,
        );

        if (row.join_step_id && row.fan_out_index != null) {
            await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
        }

        await this.checkCompletion(row.workflow_run_id);
        this.logger.info(
            `[chotu] Step ${stepExecId} failed (workflow ${row.workflow_run_id} continues)`,
        );
    }

    async cancelStep(
        stepExecId: string,
        row: StepExecutionRecord,
        reason?: string,
    ): Promise<void> {
        const updated = await this.stateStore.cancelStep(stepExecId, reason);
        if (!updated) return;

        await syncWithRetry(
            () =>
                this.repository.syncStepTerminal({
                    id: stepExecId,
                    status: StepExecutionStatus.CANCELLED,
                    error: reason ? { reason } : null,
                    version: updated.version,
                }),
            `syncStepTerminal(cancelled, ${stepExecId})`,
            this.logger,
        );

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
        this.logger.info(
            `[chotu] Step ${stepExecId} cancelled (workflow ${row.workflow_run_id})`,
        );
    }

    async isAbortRequested(workflowRunId: string): Promise<boolean> {
        return this.stateStore.isAbortRequested(workflowRunId);
    }

    async canScheduleForRun(workflowRunId: string): Promise<boolean> {
        const status = await this.stateStore.getRunStatus(workflowRunId);
        if (status !== WorkflowRunStatus.RUNNING) return false;
        if (await this.isAbortRequested(workflowRunId)) return false;
        return true;
    }

    async beginCancelWorkflow(workflowRunId: string, reason?: string): Promise<boolean> {
        const run = await this.stateStore.loadRun(workflowRunId);
        if (!run) return false;
        if (run.status !== WorkflowRunStatus.RUNNING) return false;
        if (await this.isAbortRequested(workflowRunId)) return false;

        await this.stateStore.markAbortRequested(workflowRunId);

        const steps = await this.stateStore.listStepsForRun(workflowRunId);
        const cancellable = new Set([
            StepExecutionStatus.PENDING,
            StepExecutionStatus.WAITING,
        ]);

        for (const step of steps) {
            if (!cancellable.has(step.status)) continue;

            await this.fairQueue.cancelFromQueue(step.queue, step.id, workflowRunId);
            await this.cancelStep(step.id, step, reason);
        }

        return true;
    }

    async finalizeCancelIfReady(workflowRunId: string, reason?: string): Promise<boolean> {
        if (!(await this.isAbortRequested(workflowRunId))) return false;

        const activeCount = await this.stateStore.getActiveCount(workflowRunId);
        if (activeCount > 0) return false;

        await this.finalizeCancelledRun(workflowRunId, reason);
        return true;
    }

    private async finalizeCancelledRun(workflowRunId: string, reason?: string): Promise<void> {
        const output = reason ? { reason } : null;
        const version = await this.stateStore.tryCancelRun(workflowRunId, reason);
        if (version == null) return;

        await syncWithRetry(
            async () => {
                const synced = await this.repository.syncWorkflowTerminal({
                    id: workflowRunId,
                    status: WorkflowRunStatus.CANCELLED,
                    output,
                    version,
                });
                if (!synced) {
                    throw new Error(
                        `syncWorkflowTerminal(cancelled) returned false for ${workflowRunId}`,
                    );
                }
            },
            `syncWorkflowTerminal(cancelled, ${workflowRunId})`,
            this.logger,
        );

        this.logger.info(
            `[chotu] Workflow run ${workflowRunId} cancelled${reason ? `: ${reason}` : ""}`,
        );

        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (runRow) {
            await this.hookRunner.workflowCancelled({
                workflowRunId,
                workflowName: runRow.workflow_name,
                input: runRow.input,
                reason,
            });
        }

        await this.runPurger.purgeTerminalRun(workflowRunId);
    }

    async scheduleNext(
        nextSteps: "END" | NextStep<any> | ParallelSpec,
        workflowRunId: string,
        currentStepExecId: string,
        stepOutput?: Record<string, any>,
    ): Promise<void> {
        if (!(await this.canScheduleForRun(workflowRunId))) return;

        const currentRow = await this.loadStep(currentStepExecId);
        if (!currentRow) return;

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
                input: nextSteps.input as Record<string, any>,
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

    async checkCompletion(workflowRunId: string): Promise<void> {
        const lockToken = crypto.randomUUID();
        const acquired = await this.stateStore.acquireRunLock(workflowRunId, lockToken, 30);
        if (!acquired) return;

        try {
            await this.doCheckCompletion(workflowRunId);
        } finally {
            await this.stateStore.releaseRunLock(workflowRunId, lockToken);
        }
    }

    async enqueueStep(
        stepExecId: string,
        stepName: string,
        workflowRunId: string,
    ): Promise<void> {
        if (!(await this.canScheduleForRun(workflowRunId))) return;

        const queueName = this.registry.resolveQueue(stepName);
        await this.fairQueue.enqueueWithRetry(stepExecId, queueName, workflowRunId);
    }

    async decrementJoinRemaining(joinStepId: string, workflowRunId: string): Promise<void> {
        if (!(await this.canScheduleForRun(workflowRunId))) {
            await this.finalizeCancelIfReady(workflowRunId);
            return;
        }

        const remaining = await this.stateStore.decrementJoinRemaining(joinStepId);
        if (remaining == null) return;

        await this.repository.syncJoinRemaining(joinStepId, remaining);

        if (remaining > 0) {
            await this.checkCompletion(workflowRunId);
            return;
        }

        await this.finalizeJoin(joinStepId, workflowRunId);
    }

    async finalizeJoin(joinStepId: string, workflowRunId: string): Promise<void> {
        if (!(await this.canScheduleForRun(workflowRunId))) {
            await this.finalizeCancelIfReady(workflowRunId);
            return;
        }

        const joinRow = await this.loadStep(joinStepId);
        if (!joinRow) {
            this.logger.error(
                `[chotu] Join step ${joinStepId} missing when finalizing workflow ${workflowRunId}`,
            );
            await this.failWorkflowRun(workflowRunId, "Join step missing during finalize");
            return;
        }

        const branches = await this.stateStore.getJoinBranches(joinStepId);

        const outputs: Record<string, any>[] = branches.map((branch) => {
            if (branch.status === StepExecutionStatus.FAILED) {
                const branchError = branch.error as { message?: string } | null;
                return createStepError(
                    branchError?.message ?? "Branch failed",
                    branch.step_name ?? "unknown",
                );
            }
            const output = branch.output as Record<string, any> | null;
            if (output && isChotuStepError(output)) {
                return output;
            }
            return output ?? {};
        });

        const updated = await this.stateStore.finalizeJoinStep(joinStepId, outputs);
        if (!updated) return;

        await syncWithRetry(
            () =>
                this.repository.syncJoinFinalize({
                    id: joinStepId,
                    input: outputs,
                    version: updated.version,
                }),
            `syncJoinFinalize(${joinStepId})`,
            this.logger,
        );

        await this.enqueueStep(joinStepId, joinRow.step_name, workflowRunId);
    }

    async failWorkflowRun(workflowRunId: string, reason?: string): Promise<void> {
        const output = reason ? { reason } : null;
        const version = await this.stateStore.tryFailRun(workflowRunId, reason);
        if (version == null) return;

        await syncWithRetry(
            async () => {
                const synced = await this.repository.syncWorkflowTerminal({
                    id: workflowRunId,
                    status: WorkflowRunStatus.FAILED,
                    output,
                    version,
                });
                if (!synced) {
                    throw new Error(`syncWorkflowTerminal(failed) returned false for ${workflowRunId}`);
                }
            },
            `syncWorkflowTerminal(failed, ${workflowRunId})`,
            this.logger,
        );

        this.logger.info(
            `[chotu] Workflow run ${workflowRunId} failed${reason ? `: ${reason}` : ""}`,
        );

        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (runRow) {
            await this.hookRunner.workflowError({
                workflowRunId,
                workflowName: runRow.workflow_name,
                input: runRow.input,
                reason,
            });
        }

        await this.runPurger.purgeTerminalRun(workflowRunId);
    }

    async createStepExecution(params: {
        workflowRunId: string;
        stepName: string;
        queue: string;
        input: Record<string, any> | null;
        status?: StepExecutionStatus;
        joinStepId?: string | null;
        fanOutIndex?: number | null;
        joinTotal?: number | null;
        joinRemaining?: number | null;
    }): Promise<string> {
        if (!(await this.canScheduleForRun(params.workflowRunId))) {
            throw new Error(
                `[chotu] Cannot create step for non-running workflow ${params.workflowRunId}`,
            );
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
            throw new Error(
                `[chotu] Active step already exists for "${params.stepName}" in run ${params.workflowRunId}`,
            );
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
        } catch (err) {
            await this.stateStore.rollbackStep(id, params.workflowRunId, params.stepName);
            throw err;
        }

        return id;
    }

    private async completeBranch(row: StepExecutionRecord): Promise<void> {
        if (!row.join_step_id || row.fan_out_index == null) return;
        await this.decrementJoinRemaining(row.join_step_id, row.workflow_run_id);
    }

    private async spawnParallel(spec: ParallelSpec, workflowRunId: string): Promise<void> {
        let joinStepId: string | null = null;

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
            const branch = spec.branches[i]!;
            const stepName = getStepName(branch.step);
            const branchStepId = await this.createStepExecution({
                workflowRunId,
                stepName,
                queue: this.registry.resolveQueue(stepName),
                input: branch.input as Record<string, any>,
                joinStepId,
                fanOutIndex: i,
            });
            await this.enqueueStep(branchStepId, stepName, workflowRunId);
        }
    }

    private async doCheckCompletion(workflowRunId: string): Promise<void> {
        const runStatus = await this.stateStore.getRunStatus(workflowRunId);

        if (
            runStatus === WorkflowRunStatus.COMPLETED ||
            runStatus === WorkflowRunStatus.FAILED ||
            runStatus === WorkflowRunStatus.CANCELLED
        ) {
            return;
        }

        if (await this.isAbortRequested(workflowRunId)) {
            await this.finalizeCancelIfReady(workflowRunId);
            return;
        }

        const activeCount = await this.stateStore.getActiveCount(workflowRunId);
        if (activeCount > 0) return;

        const unabsorbedFailures = await this.stateStore.countUnabsorbedFailures(workflowRunId);
        if (unabsorbedFailures > 0) {
            await this.failWorkflowRun(workflowRunId);
            return;
        }

        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (!runRow) return;

        const workflow = this.registry.getWorkflow(runRow.workflow_name);

        if (workflow?.completeStep) {
            await this.handleCompleteStep(workflowRunId, workflow, runRow);
            return;
        }

        await this.completeWorkflowWithoutCompleteStep(workflowRunId, workflow);
    }

    private async handleCompleteStep(
        workflowRunId: string,
        workflow: Workflow,
        runRow: { input: Record<string, any> },
    ): Promise<void> {
        const completeStepName = getStepName(workflow.completeStep!);
        let completeStepId: string | null = null;

        const completeRow = await this.stateStore.findStepByName(workflowRunId, completeStepName);

        if (!completeRow) {
            const queue = this.registry.resolveQueue(completeStepName);
            const input = {
                workflowInput: runRow.input,
                workflowRunId,
            } satisfies WorkflowCompleteInput;

            completeStepId = await this.createStepExecution({
                workflowRunId,
                stepName: completeStepName,
                queue,
                input,
            });
        } else if (completeRow.status === StepExecutionStatus.COMPLETED) {
            const version = await this.stateStore.tryCompleteRun(
                workflowRunId,
                completeRow.output ?? null,
            );
            if (version == null) return;

            await syncWithRetry(
                async () => {
                    const synced = await this.repository.completeWorkflowFromCompleteStep({
                        workflowRunId,
                        output: completeRow.output ?? null,
                        version,
                    });
                    if (!synced) {
                        throw new Error(
                            `completeWorkflowFromCompleteStep returned false for ${workflowRunId}`,
                        );
                    }
                },
                `completeWorkflowFromCompleteStep(${workflowRunId})`,
                this.logger,
            );

            this.logger.info(`[chotu] Workflow run ${workflowRunId} completed`);
            const output = completeRow.output ?? null;
            await this.invokeWorkflowHook("onAfterCompleted", () =>
                workflow.onAfterCompleted(
                    runRow.input,
                    output,
                    {
                        workflowRunId,
                        workflowName: workflow.name,
                        input: runRow.input,
                    },
                    AbortSignal.timeout(DEFAULT_BEFORE_START_TIMEOUT_MS),
                ),
            );
            await this.hookRunner.workflowCompleted({
                workflowRunId,
                workflowName: workflow.name,
                input: runRow.input,
                output,
            });
            await this.runPurger.purgeTerminalRun(workflowRunId);
            return;
        } else if (
            completeRow.status === StepExecutionStatus.PENDING ||
            completeRow.status === StepExecutionStatus.RUNNING
        ) {
            completeStepId = completeRow.id;
        }

        if (completeStepId) {
            await this.enqueueStep(completeStepId, completeStepName, workflowRunId);
        }
    }

    private async completeWorkflowWithoutCompleteStep(
        workflowRunId: string,
        workflow: Workflow | undefined,
    ): Promise<void> {
        let output: Record<string, any> | null = null;

        if (workflow?.terminalSteps?.length) {
            const terminalNames = workflow.terminalSteps.map(getStepName);
            const terminalRows: { step_name: string; output: Record<string, any> | null }[] = [];

            for (const name of terminalNames) {
                const row = await this.stateStore.findStepByName(workflowRunId, name);
                if (row?.status === StepExecutionStatus.COMPLETED) {
                    terminalRows.push({ step_name: name, output: row.output ?? null });
                }
            }

            if (terminalRows.length === 1) {
                output = terminalRows[0]?.output ?? null;
            } else if (terminalRows.length > 1) {
                output = Object.fromEntries(
                    terminalRows.map((r) => [r.step_name, r.output ?? null]),
                );
            }
        }

        const version = await this.stateStore.tryCompleteRun(workflowRunId, output);
        if (version == null) return;

        await syncWithRetry(
            async () => {
                const synced = await this.repository.syncWorkflowTerminal({
                    id: workflowRunId,
                    status: WorkflowRunStatus.COMPLETED,
                    output,
                    version,
                });
                if (!synced) {
                    throw new Error(`syncWorkflowTerminal(completed) returned false for ${workflowRunId}`);
                }
            },
            `syncWorkflowTerminal(completed, ${workflowRunId})`,
            this.logger,
        );

        this.logger.info(`[chotu] Workflow run ${workflowRunId} completed`);

        const runRow = await this.stateStore.loadRun(workflowRunId);
        if (runRow) {
            if (workflow) {
                await this.invokeWorkflowHook("onAfterCompleted", () =>
                    workflow.onAfterCompleted(
                        runRow.input,
                        output,
                        {
                            workflowRunId,
                            workflowName: runRow.workflow_name,
                            input: runRow.input,
                        },
                        AbortSignal.timeout(DEFAULT_BEFORE_START_TIMEOUT_MS),
                    ),
                );
            }
            await this.hookRunner.workflowCompleted({
                workflowRunId,
                workflowName: runRow.workflow_name,
                input: runRow.input,
                output,
            });
        }

        await this.runPurger.purgeTerminalRun(workflowRunId);
    }

    private async invokeWorkflowHook(
        name: string,
        fn: () => Promise<void> | void,
    ): Promise<void> {
        try {
            await fn();
        } catch (err) {
            this.logger.error(`[chotu] Workflow hook ${name} failed:`, err);
        }
    }
}
