import type { ChotuRedis } from "../platform";
import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { IWorkflowRepository } from "../interfaces/repository.interface";
import type { IStateStore } from "../interfaces/state-store.interface";
import type { ChotuLogger } from "../logger";
import { StepRegistry } from "./step-registry";
import { WorkflowLifecycle } from "./workflow-lifecycle";
export declare class RecoveryService {
    private readonly stateStore;
    private readonly repository;
    private readonly fairQueue;
    private readonly lifecycle;
    private readonly registry;
    private readonly logger;
    private readonly redis;
    private readonly instanceId;
    constructor(stateStore: IStateStore, repository: IWorkflowRepository, fairQueue: IFairQueue, lifecycle: WorkflowLifecycle, registry: StepRegistry, logger: ChotuLogger, redis: ChotuRedis, instanceId: string);
    recoverOnStartup(): Promise<number>;
    coldStartupReconcile(isLeader: boolean): Promise<number>;
    recoverAbortingRuns(): Promise<number>;
    recoverStaleRunningSteps(): Promise<number>;
    recoverInflightSteps(): Promise<number>;
    recoverOrphanedPendingSteps(): Promise<number>;
    rebuildJoinStateFromRedis(): Promise<void>;
    reEnqueueIfPending(stepExecId: string, queueName: string, workflowRunId: string): Promise<boolean>;
    private shouldSkipRun;
}
//# sourceMappingURL=recovery.service.d.ts.map