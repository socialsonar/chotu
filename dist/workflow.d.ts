import { type StepClass } from "./step";
import type { QueueConfig } from "./interfaces/queue.interface";
export interface WorkflowDefinition<I = any> {
    name: string;
    firstStep: StepClass<I, any>;
    steps: StepClass<any, any>[];
    completeStep?: StepClass<any, any>;
    terminalSteps?: StepClass<any, any>[];
}
export declare function defineWorkflow<I>(config: WorkflowDefinition<I>): WorkflowDefinition<I>;
export declare function validateStepQueues(stepQueues: Record<string, string>, workflows: WorkflowDefinition[]): void;
export declare function validateConfig(queues: QueueConfig[], stepQueues: Record<string, string>, workflows: WorkflowDefinition[]): void;
//# sourceMappingURL=workflow.d.ts.map