import { type StepExecution, type StepExecutionRecord, type WorkflowRunRecord } from "../interfaces/workflow.interface";
export declare function parseRedisFields(fields: string[] | Record<string, string>): Record<string, string>;
export declare function fromRedisHash(hash: Record<string, string>): StepExecutionRecord | null;
export declare function fromRedisRunHash(hash: Record<string, string>, workflowRunId: string): WorkflowRunRecord | null;
export declare function fromPgRow(row: Record<string, unknown>): StepExecution;
export declare function pgRowToExecutionRecord(row: Record<string, unknown>): StepExecutionRecord;
export declare function toStepExecution(record: StepExecutionRecord): StepExecution;
//# sourceMappingURL=execution.mapper.d.ts.map