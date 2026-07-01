import { joinBranchesKey } from "../persistence/redis/keys";
import type { IFairQueue } from "../interfaces/fair-queue.interface";
import type { IWorkflowRepository } from "../interfaces/repository.interface";
import type { IStateStore } from "../interfaces/state-store.interface";
import type { ChotuLogger } from "../logger";
import type { StepRegistry } from "./step-registry";

export class RunPurger {
    constructor(
        private readonly stateStore: IStateStore,
        private readonly repository: IWorkflowRepository,
        private readonly fairQueue: IFairQueue,
        private readonly registry: StepRegistry,
        private readonly logger: ChotuLogger,
        private readonly enabled: boolean,
    ) {}

    async purgeTerminalRun(workflowRunId: string): Promise<void> {
        if (!this.enabled) return;

        try {
            const steps = await this.stateStore.listStepsForRun(workflowRunId);
            const stepExecIds = steps.map((step) => step.id);
            const joinBranchKeys = steps
                .filter((step) => step.join_total != null)
                .map((step) => joinBranchesKey(step.id));

            await this.repository.deleteStepsForRun(workflowRunId);
            await this.fairQueue.purgeRunFromQueues(
                workflowRunId,
                this.registry.queueNames(),
                stepExecIds,
            );
            await this.stateStore.purgeRun(workflowRunId, stepExecIds, joinBranchKeys);

            this.logger.info(`[chotu] Purged terminal run ${workflowRunId}`);
        } catch (err) {
            this.logger.error(`[chotu] Failed to purge terminal run ${workflowRunId}:`, err);
        }
    }
}
