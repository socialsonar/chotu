import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { IWorkflowRepository } from "../interfaces/repository.interface";
import type { IStateStore } from "../interfaces/state-store.interface";
import type { ChotuLogger } from "../logger";
import type { StepRegistry } from "./step-registry";
export declare class RunPurger {
    private readonly stateStore;
    private readonly repository;
    private readonly fairQueue;
    private readonly registry;
    private readonly logger;
    private readonly enabled;
    constructor(stateStore: IStateStore, repository: IWorkflowRepository, fairQueue: IFairQueue, registry: StepRegistry, logger: ChotuLogger, enabled: boolean);
    purgeTerminalRun(workflowRunId: string): Promise<void>;
}
//# sourceMappingURL=run-purger.d.ts.map