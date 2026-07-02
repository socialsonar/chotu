export class ChotuHookRunner {
    hooks;
    logger;
    constructor(hooks, logger) {
        this.hooks = hooks;
        this.logger = logger;
    }
    async workflowStarted(ctx) {
        await this.invoke("onWorkflowStarted", () => this.hooks?.onWorkflowStarted?.(ctx));
    }
    async workflowCompleted(ctx) {
        await this.invoke("onWorkflowCompleted", () => this.hooks?.onWorkflowCompleted?.(ctx));
    }
    async workflowError(ctx) {
        await this.invoke("onWorkflowError", () => this.hooks?.onWorkflowError?.(ctx));
    }
    async workflowCancelled(ctx) {
        await this.invoke("onWorkflowCancelled", () => this.hooks?.onWorkflowCancelled?.(ctx));
    }
    async stepStarted(ctx) {
        await this.invoke("onStepStarted", () => this.hooks?.onStepStarted?.(ctx));
    }
    async stepCompleted(ctx) {
        await this.invoke("onStepCompleted", () => this.hooks?.onStepCompleted?.(ctx));
    }
    async stepFailed(ctx) {
        await this.invoke("onStepFailed", () => this.hooks?.onStepFailed?.(ctx));
    }
    async stepCancelled(ctx) {
        await this.invoke("onStepCancelled", () => this.hooks?.onStepCancelled?.(ctx));
    }
    async invoke(name, fn) {
        if (!this.hooks)
            return;
        try {
            await fn();
        }
        catch (err) {
            this.logger.error(`[chotu] Hook ${name} failed:`, err);
        }
    }
}
//# sourceMappingURL=hook-runner.js.map