import type { ChotuHooks, StepCancelledContext, StepCompletedContext, StepFailedContext, StepHookContext, WorkflowCancelledContext, WorkflowCompletedContext, WorkflowErrorContext, WorkflowHookContext } from "../interfaces/hooks.interface";
import type { ChotuLogger } from "../logger";
export declare class ChotuHookRunner {
    private readonly hooks;
    private readonly logger;
    constructor(hooks: ChotuHooks | undefined, logger: ChotuLogger);
    workflowStarted(ctx: WorkflowHookContext): Promise<void>;
    workflowCompleted(ctx: WorkflowCompletedContext): Promise<void>;
    workflowError(ctx: WorkflowErrorContext): Promise<void>;
    workflowCancelled(ctx: WorkflowCancelledContext): Promise<void>;
    stepStarted(ctx: StepHookContext): Promise<void>;
    stepCompleted(ctx: StepCompletedContext): Promise<void>;
    stepFailed(ctx: StepFailedContext): Promise<void>;
    stepCancelled(ctx: StepCancelledContext): Promise<void>;
    private invoke;
}
//# sourceMappingURL=hook-runner.d.ts.map