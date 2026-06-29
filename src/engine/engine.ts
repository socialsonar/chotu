import type { PgFlusher } from "../persistence/pg/flusher";
import type { StepExecution, WorkflowRun } from "../interfaces/workflow.interface";
import type { QueueWorkerPool } from "./queue-worker";
import type { RecoveryService } from "./recovery.service";
import type { WorkflowLifecycle } from "./workflow-lifecycle";

export class ChotuEngine {
    private workersStarted = false;
    private engineStarted = false;

    constructor(
        private readonly flusher: PgFlusher,
        private readonly workerPool: QueueWorkerPool,
        private readonly lifecycle: WorkflowLifecycle,
        private readonly recovery: RecoveryService,
    ) {}

    setWorkersStarted(value: boolean): void {
        this.workersStarted = value;
    }

    areWorkersStarted(): boolean {
        return this.workersStarted;
    }

    async start(): Promise<void> {
        if (this.engineStarted) return;
        this.engineStarted = true;
        this.workersStarted = true;

        await this.flusher.start();
        await this.workerPool.start();
    }

    async stop(): Promise<void> {
        if (!this.engineStarted) return;
        this.engineStarted = false;
        this.workersStarted = false;

        await this.workerPool.stop();
        await this.flusher.stop();
    }

    async recoverOnStartup(): Promise<number> {
        return this.recovery.recoverOnStartup();
    }

    async runWorkflow<I>(name: string, input: I): Promise<{ id: string }> {
        return this.lifecycle.runWorkflow(name, input);
    }

    async getWorkflowRun(id: string): Promise<WorkflowRun | null> {
        return this.lifecycle.getWorkflowRun(id);
    }

    async getStepExecutions(workflowRunId: string): Promise<StepExecution[]> {
        return this.lifecycle.getStepExecutions(workflowRunId);
    }

    async abortWorkflow(workflowRunId: string, reason?: string): Promise<boolean> {
        const started = await this.lifecycle.beginCancelWorkflow(workflowRunId, reason);
        if (!started) return false;
        this.workerPool.abortInFlightForRun(workflowRunId);
        await this.lifecycle.finalizeCancelIfReady(workflowRunId, reason);
        return true;
    }

    async recoverStaleRunningSteps(): Promise<number> {
        return this.recovery.recoverStaleRunningSteps();
    }

    async recoverInflightSteps(): Promise<number> {
        return this.recovery.recoverInflightSteps();
    }

    async recoverOrphanedPendingSteps(): Promise<number> {
        return this.recovery.recoverOrphanedPendingSteps();
    }
}
