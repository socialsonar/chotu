import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { IStateStore } from "../interfaces/state-store.interface";
import type { ChotuLogger } from "../logger";
import { ChotuHookRunner } from "./hook-runner";
import { StepRegistry } from "./step-registry";
import { RecoveryService } from "./recovery.service";
import { StepExecutor } from "./step-executor";
export declare class QueueWorkerPool {
    private readonly fairQueue;
    private readonly stateStore;
    private readonly stepExecutor;
    private readonly recovery;
    private readonly registry;
    private readonly logger;
    private readonly instanceId;
    private readonly hookRunner;
    private started;
    private workers;
    private readonly inFlight;
    private readonly inFlightStepIds;
    private readonly inFlightStepNames;
    private lastRecoveryAt;
    private readonly abortControllers;
    private readonly inFlightRunIds;
    constructor(fairQueue: IFairQueue, stateStore: IStateStore, stepExecutor: StepExecutor, recovery: RecoveryService, registry: StepRegistry, logger: ChotuLogger, instanceId: string, hookRunner: ChotuHookRunner);
    isStarted(): boolean;
    start(): Promise<void>;
    stop(): Promise<void>;
    abortInFlightForRun(workflowRunId: string): void;
    private runWorker;
    private renewInFlightLeases;
    private runLeaderRecovery;
    private handleFailedClaim;
}
//# sourceMappingURL=queue-worker.d.ts.map