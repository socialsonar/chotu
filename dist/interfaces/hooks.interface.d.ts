export interface WorkflowHookContext {
    workflowRunId: string;
    workflowName: string;
    input: Record<string, any>;
}
export interface WorkflowCompletedContext extends WorkflowHookContext {
    output: Record<string, any> | null;
}
export interface WorkflowErrorContext extends WorkflowHookContext {
    reason?: string;
}
export interface StepHookContext {
    stepExecId: string;
    stepName: string;
    queue: string;
    workflowRunId: string;
    workflowName: string;
    attempt: number;
}
export interface StepCompletedContext extends StepHookContext {
    output: Record<string, any>;
}
export interface StepFailedContext extends StepHookContext {
    error: Error;
    willRetry: boolean;
}
export interface WorkflowCancelledContext extends WorkflowHookContext {
    reason?: string;
}
export interface StepCancelledContext extends StepHookContext {
    reason?: string;
}
export interface ChotuHooks {
    onWorkflowStarted?(ctx: WorkflowHookContext): Promise<void> | void;
    onWorkflowCompleted?(ctx: WorkflowCompletedContext): Promise<void> | void;
    onWorkflowError?(ctx: WorkflowErrorContext): Promise<void> | void;
    onWorkflowCancelled?(ctx: WorkflowCancelledContext): Promise<void> | void;
    onStepStarted?(ctx: StepHookContext): Promise<void> | void;
    onStepCompleted?(ctx: StepCompletedContext): Promise<void> | void;
    onStepFailed?(ctx: StepFailedContext): Promise<void> | void;
    onStepCancelled?(ctx: StepCancelledContext): Promise<void> | void;
}
//# sourceMappingURL=hooks.interface.d.ts.map