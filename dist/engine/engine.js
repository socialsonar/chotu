export class ChotuEngine {
    flusher;
    workerPool;
    lifecycle;
    recovery;
    workersStarted = false;
    engineStarted = false;
    constructor(flusher, workerPool, lifecycle, recovery) {
        this.flusher = flusher;
        this.workerPool = workerPool;
        this.lifecycle = lifecycle;
        this.recovery = recovery;
    }
    setWorkersStarted(value) {
        this.workersStarted = value;
    }
    areWorkersStarted() {
        return this.workersStarted;
    }
    async start() {
        if (this.engineStarted)
            return;
        this.engineStarted = true;
        this.workersStarted = true;
        await this.flusher.start();
        await this.workerPool.start();
    }
    async stop() {
        if (!this.engineStarted)
            return;
        this.engineStarted = false;
        this.workersStarted = false;
        await this.workerPool.stop();
        await this.flusher.stop();
    }
    async recoverOnStartup() {
        return this.recovery.recoverOnStartup();
    }
    async runWorkflow(name, input) {
        return this.lifecycle.runWorkflow(name, input);
    }
    async getWorkflowRun(id) {
        return this.lifecycle.getWorkflowRun(id);
    }
    async getStepExecutions(workflowRunId) {
        return this.lifecycle.getStepExecutions(workflowRunId);
    }
    async abortWorkflow(workflowRunId, reason) {
        const started = await this.lifecycle.beginCancelWorkflow(workflowRunId, reason);
        if (!started)
            return false;
        this.workerPool.abortInFlightForRun(workflowRunId);
        await this.lifecycle.finalizeCancelIfReady(workflowRunId, reason);
        return true;
    }
    async recoverStaleRunningSteps() {
        return this.recovery.recoverStaleRunningSteps();
    }
    async recoverInflightSteps() {
        return this.recovery.recoverInflightSteps();
    }
    async recoverOrphanedPendingSteps() {
        return this.recovery.recoverOrphanedPendingSteps();
    }
}
//# sourceMappingURL=engine.js.map