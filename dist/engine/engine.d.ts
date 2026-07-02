import type { PgFlusher } from "../persistence/pg/flusher";
import type { StepExecution, WorkflowRun } from "../interfaces/workflow.interface";
import type { QueueWorkerPool } from "./queue-worker";
import type { RecoveryService } from "./recovery.service";
import type { WorkflowLifecycle } from "./workflow-lifecycle";
export declare class ChotuEngine {
    private readonly flusher;
    private readonly workerPool;
    private readonly lifecycle;
    private readonly recovery;
    private workersStarted;
    private engineStarted;
    constructor(flusher: PgFlusher, workerPool: QueueWorkerPool, lifecycle: WorkflowLifecycle, recovery: RecoveryService);
    setWorkersStarted(value: boolean): void;
    areWorkersStarted(): boolean;
    start(): Promise<void>;
    stop(): Promise<void>;
    recoverOnStartup(): Promise<number>;
    runWorkflow<I>(name: string, input: I): Promise<{
        id: string;
    }>;
    getWorkflowRun(id: string): Promise<WorkflowRun | null>;
    getStepExecutions(workflowRunId: string): Promise<StepExecution[]>;
    abortWorkflow(workflowRunId: string, reason?: string): Promise<boolean>;
    recoverStaleRunningSteps(): Promise<number>;
    recoverInflightSteps(): Promise<number>;
    recoverOrphanedPendingSteps(): Promise<number>;
    recoverAbortingRuns(): Promise<number>;
}
//# sourceMappingURL=engine.d.ts.map