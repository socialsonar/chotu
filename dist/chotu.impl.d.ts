import { Chotu, ChotuConfig, ChotuHealth } from "./interfaces/chotu.interface";
import { StepExecution, WorkflowRun } from "./interfaces/workflow.interface";
export default class ChotuImpl implements Chotu {
    private readonly config;
    private readonly onShutdown?;
    private readonly sql;
    private readonly redis;
    private readonly repository;
    private readonly stateStore;
    private readonly fairQueue;
    private readonly flusher;
    private readonly engine;
    private readonly logger;
    private readonly instanceId;
    private started;
    private workersStarted;
    constructor(config: ChotuConfig, onShutdown?: (() => void) | undefined);
    isStarted(): boolean;
    areWorkersStarted(): boolean;
    private assertStarted;
    listen(options?: {
        deferWorkers?: boolean;
    }): Promise<void>;
    startWorkers(): Promise<void>;
    shutdown(): Promise<void>;
    runWorkflow<I>(name: string, input: I): Promise<{
        id: string;
    }>;
    getWorkflowRun(id: string): Promise<WorkflowRun | null>;
    getStepExecutions(workflowRunId: string): Promise<StepExecution[]>;
    abortWorkflow(workflowRunId: string, reason?: string): Promise<boolean>;
    recoverAbortingRuns(): Promise<number>;
    health(): Promise<ChotuHealth>;
}
//# sourceMappingURL=chotu.impl.d.ts.map