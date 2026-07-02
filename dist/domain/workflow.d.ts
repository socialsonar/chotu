import type { WorkflowHookContext } from "../interfaces/hooks.interface";
import { type StepClass } from "./step";
import type { QueueConfig } from "../interfaces/queue.interface";
export declare abstract class Workflow<I = any, O = any> {
    abstract readonly name: string;
    abstract readonly firstStep: StepClass<I, any>;
    abstract readonly steps: StepClass<any, any>[];
    readonly completeStep?: StepClass<any, any>;
    readonly terminalSteps?: StepClass<any, any>[];
    onBeforeStart(_input: I, _ctx: WorkflowHookContext, _signal: AbortSignal): Promise<I | void>;
    onAfterCompleted(_input: I, _output: O | null, _ctx: WorkflowHookContext, _signal: AbortSignal): Promise<void>;
}
export type WorkflowClass<I = any, O = any> = new () => Workflow<I, O>;
export declare function defineWorkflow<I, O>(workflowOrClass: Workflow<I, O> | WorkflowClass<I, O>): Workflow<I, O>;
export declare function validateStepQueues(stepQueues: Record<string, string>, workflows: Workflow[]): void;
export declare function validateConfig(queues: QueueConfig[], stepQueues: Record<string, string>, workflows: Workflow[]): void;
//# sourceMappingURL=workflow.d.ts.map