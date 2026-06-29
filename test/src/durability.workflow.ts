import {
    defineWorkflow,
    isChotuStepError,
    next,
    parallel,
    Step,
    type NextStepsResult,
    type StepHookContext,
} from "chotu";

export interface DurabilityInput {
    taskCount: number;
    seed: number;
    permanentFailIndex: number;
    /** When set, this task sleeps long on attempt 1 (for crash/lease tests). */
    hangTaskIndex?: number;
}

export interface TaskInput {
    taskIndex: number;
    seed: number;
    permanentFailIndex: number;
    hangTaskIndex?: number;
}

export interface TaskResult {
    taskIndex: number;
    value: number;
}

export interface DurabilityOutput {
    completedTasks: number;
    failedTasks: number;
    errors: string[];
    totalTasks: number;
}

export interface WorkflowCompleteInput {
    workflowInput: DurabilityInput;
    workflowRunId: string;
}

function rng(seed: number, n: number): number {
    const x = Math.sin(seed * 9999 + n * 7919) * 10000;
    return x - Math.floor(x);
}

async function sleep(ms: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted) throw new Error("Aborted");
    await Bun.sleep(ms);
    if (signal.aborted) throw new Error("Aborted");
}

class OrchestratorDurabilityStep extends Step<DurabilityInput, { tasks: TaskInput[] }> {
    static stepName = "OrchestratorDurabilityStep";

    async onAfterRun(
        _input: DurabilityInput,
        _output: { tasks: TaskInput[] },
        ctx: StepHookContext,
        _signal: AbortSignal,
    ): Promise<void> {
        if (ctx.attempt === 1) {
            throw new Error("transient orchestrator onAfterRun failure");
        }
    }

    async run(input: DurabilityInput, signal: AbortSignal): Promise<{ tasks: TaskInput[] }> {
        await sleep(30 + Math.floor(rng(input.seed, 0) * 50), signal);
        const tasks: TaskInput[] = [];
        for (let i = 0; i < input.taskCount; i++) {
            tasks.push({
                taskIndex: i,
                seed: input.seed,
                permanentFailIndex: input.permanentFailIndex,
                hangTaskIndex: input.hangTaskIndex,
            });
        }
        return { tasks };
    }

    getNextSteps(
        _input: DurabilityInput,
        output: { tasks: TaskInput[] },
        _signal: AbortSignal,
    ): NextStepsResult {
        return parallel(
            output.tasks.map((t) => next(TaskDurabilityStep, t)),
            JoinDurabilityStep,
        );
    }
}

export class TaskDurabilityStep extends Step<TaskInput, TaskResult> {
    static stepName = "TaskDurabilityStep";

    async onBeforeRun(
        input: TaskInput,
        ctx: StepHookContext,
        signal: AbortSignal,
    ): Promise<void> {
        if (input.hangTaskIndex === input.taskIndex && ctx.attempt === 1) {
            await sleep(2500, signal);
        }
    }

    async run(input: TaskInput, signal: AbortSignal): Promise<TaskResult> {
        const { taskIndex, seed, permanentFailIndex } = input;

        if (taskIndex === permanentFailIndex) {
            throw new Error(`permanent failure task=${taskIndex}`);
        }

        await sleep(20 + Math.floor(rng(seed, taskIndex) * 60), signal);
        return { taskIndex, value: taskIndex * 10 + seed };
    }

    getNextSteps(
        _input: TaskInput,
        _output: TaskResult,
        _signal: AbortSignal,
    ): NextStepsResult {
        return "END";
    }
}

class JoinDurabilityStep extends Step<(TaskResult | import("chotu").ChotuStepError)[], TaskResult[]> {
    static stepName = "JoinDurabilityStep";

    async run(
        inputs: (TaskResult | import("chotu").ChotuStepError)[],
        signal: AbortSignal,
    ): Promise<TaskResult[]> {
        await sleep(40, signal);
        const results: TaskResult[] = [];
        for (const item of inputs) {
            if (isChotuStepError(item)) continue;
            results.push(item);
        }
        return results;
    }

    getNextSteps(
        _input: (TaskResult | import("chotu").ChotuStepError)[],
        _output: TaskResult[],
        _signal: AbortSignal,
    ): NextStepsResult {
        return "END";
    }
}

class SummaryDurabilityStep extends Step<WorkflowCompleteInput, DurabilityOutput> {
    static stepName = "SummaryDurabilityStep";

    async run(input: WorkflowCompleteInput, _signal: AbortSignal): Promise<DurabilityOutput> {
        const wf = input.workflowInput;
        const hasPermanentFail = wf.permanentFailIndex < wf.taskCount;
        return {
            completedTasks: hasPermanentFail ? wf.taskCount - 1 : wf.taskCount,
            failedTasks: hasPermanentFail ? 1 : 0,
            errors: hasPermanentFail
                ? [`permanent failure task=${wf.permanentFailIndex}`]
                : [],
            totalTasks: wf.taskCount,
        };
    }

    getNextSteps(
        _input: WorkflowCompleteInput,
        _output: DurabilityOutput,
        _signal: AbortSignal,
    ): NextStepsResult {
        return "END";
    }
}

export const DurabilityWorkflow = defineWorkflow<DurabilityInput>({
    name: "durability",
    firstStep: OrchestratorDurabilityStep,
    steps: [
        OrchestratorDurabilityStep,
        TaskDurabilityStep,
        JoinDurabilityStep,
        SummaryDurabilityStep,
    ],
    completeStep: SummaryDurabilityStep,
});

export function durabilityBaseConfig() {
    return {
        postgresUrl: process.env.POSTGRES_URL!,
        redisUrl: process.env.REDIS_URL!,
        defaultStepTimeoutMs: 2_000,
        leaseBufferMs: 500,
        flushIntervalMs: 200,
        queues: [
            { name: "orchestrator", concurrency: 2, maxRetries: 3, pollIntervalMs: 50 },
            { name: "tasks", concurrency: 6, maxRetries: 3, pollIntervalMs: 50 },
            { name: "join", concurrency: 2, maxRetries: 2, pollIntervalMs: 50 },
            { name: "summary", concurrency: 1, maxRetries: 2, pollIntervalMs: 50 },
        ],
        stepQueues: {
            OrchestratorDurabilityStep: "orchestrator",
            TaskDurabilityStep: "tasks",
            JoinDurabilityStep: "join",
            SummaryDurabilityStep: "summary",
        },
        workflows: [DurabilityWorkflow],
    };
}

export class TimeoutProbeStep extends Step<{ v: number }, { v: number }> {
    static stepName = "TimeoutProbeStep";
    static timeoutMs = 400;

    async onBeforeRun(_input: { v: number }, ctx: StepHookContext, signal: AbortSignal) {
        if (ctx.attempt === 1) {
            await sleep(800, signal);
        }
    }

    async run(input: { v: number }) {
        return { v: input.v };
    }

    getNextSteps() {
        return "END" as const;
    }
}

export const TimeoutProbeWorkflow = defineWorkflow({
    name: "durability-timeout-probe",
    firstStep: TimeoutProbeStep,
    steps: [TimeoutProbeStep],
    terminalSteps: [TimeoutProbeStep],
});

export function timeoutProbeConfig() {
    return {
        postgresUrl: process.env.POSTGRES_URL!,
        redisUrl: process.env.REDIS_URL!,
        flushIntervalMs: 200,
        queues: [{ name: "probe", concurrency: 1, maxRetries: 3, pollIntervalMs: 50 }],
        stepQueues: { TimeoutProbeStep: "probe" },
        workflows: [TimeoutProbeWorkflow],
    };
}
