import type {
    ChotuHooks,
    StepCompletedContext,
    StepFailedContext,
    StepHookContext,
    WorkflowCompletedContext,
    WorkflowErrorContext,
    WorkflowHookContext,
} from "../interfaces/hooks.interface";
import type { ChotuLogger } from "../logger";

export class ChotuHookRunner {
    constructor(
        private readonly hooks: ChotuHooks | undefined,
        private readonly logger: ChotuLogger,
    ) {}

    async workflowStarted(ctx: WorkflowHookContext): Promise<void> {
        await this.invoke("onWorkflowStarted", () => this.hooks?.onWorkflowStarted?.(ctx));
    }

    async workflowCompleted(ctx: WorkflowCompletedContext): Promise<void> {
        await this.invoke("onWorkflowCompleted", () => this.hooks?.onWorkflowCompleted?.(ctx));
    }

    async workflowError(ctx: WorkflowErrorContext): Promise<void> {
        await this.invoke("onWorkflowError", () => this.hooks?.onWorkflowError?.(ctx));
    }

    async stepStarted(ctx: StepHookContext): Promise<void> {
        await this.invoke("onStepStarted", () => this.hooks?.onStepStarted?.(ctx));
    }

    async stepCompleted(ctx: StepCompletedContext): Promise<void> {
        await this.invoke("onStepCompleted", () => this.hooks?.onStepCompleted?.(ctx));
    }

    async stepFailed(ctx: StepFailedContext): Promise<void> {
        await this.invoke("onStepFailed", () => this.hooks?.onStepFailed?.(ctx));
    }

    private async invoke(name: string, fn: () => Promise<void> | void | undefined): Promise<void> {
        if (!this.hooks) return;
        try {
            await fn();
        } catch (err) {
            this.logger.error(`[chotu] Hook ${name} failed:`, err);
        }
    }
}
